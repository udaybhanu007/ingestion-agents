"""
Neo4j Graph Ingestion Tool with Hybrid MCP + LLM Support

Architecture Flow:
Document Ingestion → LLM Schema Discovery & Entity Extraction → MCP Server Validation & Execution
"""

import logging
import json
import sys
import os
import requests
import re
import time
import csv
from typing import Dict, Any, List, Optional, Tuple
from langchain_openai import AzureChatOpenAI
from pydantic import SecretStr

# Add parent directory to path for config import
parent_dir = os.path.dirname(os.path.dirname(__file__))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from config.config_manager import get_config
    from config.logger_config import get_tool_logger
    config_manager = get_config()
    logger = get_tool_logger("graph_tool")
except ImportError:
    # Fallback config for development
    class MockConfig:
        def get_config(self, section, key=None):
            configs = {
                "openai": {
                    "deployment_name": os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini"),
                    "azure_api_version": os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
                    "azure_endpoint": os.getenv("AZURE_OPENAI_ENDPOINT", ""),
                    "azure_api_key": os.getenv("AZURE_OPENAI_API_KEY", ""),
                }
            }
            return configs.get(section, {}).get(key) if key else configs.get(section, {})
        
        @property
        def max_content_length(self):
            return int(os.getenv("MAX_CONTENT_LENGTH", "100000"))

    config_manager = MockConfig()
    logger = logging.getLogger("graph_tool")


class GraphIngestionTool:
    @staticmethod
    def _sanitize_property_name(prop_name: str) -> str:
        """Sanitize property names for Neo4j compatibility (no spaces or special chars)."""
        import re
        # Replace spaces and non-alphanumeric characters with underscores
        return re.sub(r'[^a-zA-Z0-9_]', '_', prop_name)

    """
    Complete Neo4j Graph Ingestion Tool implementing the hybrid architecture:
    Document Ingestion → LLM Schema Discovery & Entity Extraction → MCP Server Validation & Execution
    """
    
    def __init__(self):
        """Initialize the Graph Ingestion Tool with MCP servers and LLM client."""
        self.logger = logger
        
        # Setup MCP request logging file
        self.setup_mcp_logging()
        
        # Always load .env.dev for credentials
        try:
            from dotenv import load_dotenv
            env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env.dev")
            if os.path.exists(env_path):
                load_dotenv(env_path, override=True)
                self.logger.info(f"Loaded environment from {env_path}")
            else:
                self.logger.warning(f".env.dev not found at {env_path}")
        except Exception as e:
            self.logger.warning(f"Could not load .env.dev: {str(e)}")
        # === EXTRACTED ENTITY LIST ===
        self.setup_llm()
        self.setup_mcp_servers()
        self.initialize_processing_stats()
        
    def setup_mcp_logging(self):
        """Setup MCP request logging to file and console."""
        try:
            # Create logs directory if it doesn't exist
            logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "logs")
            os.makedirs(logs_dir, exist_ok=True)
            
            # Set up MCP request log file path with timestamp
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            self.mcp_log_file = os.path.join(logs_dir, f"mcp_requests_{timestamp}.log")
            
            # Initialize the log file with header
            with open(self.mcp_log_file, 'w', encoding='utf-8') as f:
                f.write(f"MCP API Request Log - Started at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 80 + "\n\n")
            
            self.logger.info(f"MCP request logging initialized. Log file: {self.mcp_log_file}")
            # MCP logging initialized (console message removed for cleaner output)
            
        except Exception as e:
            self.logger.error(f"Failed to setup MCP logging: {str(e)}")
            self.mcp_log_file = None
    
    def log_mcp_request(self, server_url: str, tool_name: str, request_payload: Dict[str, Any], params: Dict[str, Any]):
        """Log MCP request payload to both console and file."""
        try:
            # Update statistics
            self.processing_stats["mcp_requests_made"] += 1
            
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Create formatted log entry
            log_entry = f"""
[{timestamp}] MCP API REQUEST #{self.processing_stats["mcp_requests_made"]}
Server URL: {server_url}
Tool Name: {tool_name}
Request Payload:
{json.dumps(request_payload, indent=2, ensure_ascii=False)}

Tool Parameters:
{json.dumps(params, indent=2, ensure_ascii=False)}

{"-" * 80}
"""
            
            # MCP request logging moved to file only for cleaner console output
            self.logger.debug(f"MCP REQUEST #{self.processing_stats['mcp_requests_made']} to {server_url}/{tool_name}")
            
            # Log to file if available
            if self.mcp_log_file:
                with open(self.mcp_log_file, 'a', encoding='utf-8') as f:
                    f.write(log_entry + "\n")
            
        except Exception as e:
            self.logger.error(f"Failed to log MCP request: {str(e)}")
    
    def log_mcp_response(self, server_url: str, tool_name: str, response: Dict[str, Any], status_code: int = None, raw_response: str = None):
        """Log MCP response to both console and file."""
        try:
            # Update statistics based on response
            if response.get("success", False):
                self.processing_stats["mcp_requests_successful"] += 1
            else:
                self.processing_stats["mcp_requests_failed"] += 1
                
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            current_request_num = self.processing_stats["mcp_requests_made"]
            
            # Create formatted log entry
            log_entry = f"""
[{timestamp}] MCP API RESPONSE #{current_request_num}
Server URL: {server_url}
Tool Name: {tool_name}
Status Code: {status_code}
Success: {response.get("success", False)}
Response:
{json.dumps(response, indent=2, ensure_ascii=False)}
"""
            
            if raw_response and len(raw_response) < 1000:  # Only log short raw responses
                log_entry += f"\nRaw Response:\n{raw_response}\n"
            
            log_entry += f"\n{"-" * 80}\n"
            
            # Log to console with colored output
            success_status = "✅ SUCCESS" if response.get("success", False) else "❌ FAILED"
            # MCP response logging moved to file only for cleaner console output  
            self.logger.debug(f"MCP RESPONSE #{current_request_num} from {server_url}/{tool_name} - {success_status}")
            
            # Log to file if available
            if self.mcp_log_file:
                with open(self.mcp_log_file, 'a', encoding='utf-8') as f:
                    f.write(log_entry + "\n")
            
        except Exception as e:
            self.logger.error(f"Failed to log MCP response: {str(e)}")
        
    def setup_llm(self):
        """Setup Azure OpenAI LLM client."""
        try:
            openai_config = config_manager.get_config("openai")
            
            self.llm = AzureChatOpenAI(
                deployment_name=openai_config.get("deployment_name", "gpt-4o-mini"),
                api_version=openai_config.get("azure_api_version", "2024-08-01-preview"),
                azure_endpoint=openai_config.get("azure_endpoint", ""),
                api_key=SecretStr(openai_config.get("azure_api_key", "")),
                temperature=0.1,
                max_tokens=16000  # Increased from 4000 to handle larger responses for entity extraction
            )
            
            self.logger.info("LLM client initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize LLM client: {str(e)}")
            self.llm = None
    
    def setup_mcp_servers(self):
        """Setup MCP server URLs."""
        # Use user-provided endpoints for MCP servers
        self.data_modeling_server_url = os.getenv("MCP_DATA_MODELING_SERVER_URL", "http://127.0.0.1:8004")
        self.cypher_server_url = os.getenv("MCP_CYPHER_SERVER_URL", "http://127.0.0.1:8003")
        self.logger.info(f"MCP servers configured - Data Modeling: {self.data_modeling_server_url}, Cypher: {self.cypher_server_url}")
    
    def _is_valid_entity_type_name(self, name: str) -> bool:
        """Check if a name looks like a valid entity type name."""
        if not name or len(name.strip()) == 0:
            return False
        
        name = name.strip()
        
        # Should be a reasonable length (not too short or too long)
        if len(name) < 2 or len(name) > 50:
            return False
            
        # Should start with a letter and contain only letters, numbers, underscores
        if not name.replace("_", "").replace("-", "").isalnum():
            return False
            
        # Should start with uppercase (PascalCase) or be all uppercase
        if not (name[0].isupper() or name.isupper()):
            return False
            
        # Exclude common descriptive words that aren't entity types
        descriptive_words = ["for", "to", "from", "with", "by", "and", "or", "the", "a", "an", 
                           "this", "that", "these", "those", "up", "down", "in", "out"]
        if name.lower() in descriptive_words:
            return False
            
        return True
    
    def _is_valid_relationship_type_name(self, name: str) -> bool:
        """Check if a name looks like a valid relationship type name."""
        if not name or len(name.strip()) == 0:
            return False
        
        name = name.strip()
        
        # Should be a reasonable length
        if len(name) < 2 or len(name) > 50:
            return False
            
        # Should contain only letters, numbers, underscores (relationship names)
        if not name.replace("_", "").replace("-", "").isalnum():
            return False
            
        # Should be UPPERCASE or PascalCase for relationship types
        if not (name.isupper() or (name[0].isupper() and "_" not in name)):
            return False
            
        # Exclude single words that are too generic
        generic_words = ["TO", "FROM", "WITH", "BY", "FOR", "AND", "OR", "THE"]
        if name.upper() in generic_words:
            return False
            
        return True
    
    def _is_descriptive_text(self, text: str) -> bool:
        """Check if text appears to be descriptive rather than a relationship instance."""
        if not text:
            return False
            
        text_lower = text.lower().strip()
        
        # Check for common descriptive phrases that get incorrectly parsed
        descriptive_patterns = [
            "for", "to", "from", "with", "by", "and", "or", "the", "a", "an",
            "this", "that", "these", "those", "up", "down", "in", "out",
            "entities", "instances", "records", "data", "values", "properties",
            "relationship", "entity", "type", "class", "structure", "format",
            "example", "sample", "list", "array", "collection", "set",
            "note", "notes", "explanation", "description", "details",
            "summary", "overview", "analysis", "result", "output"
        ]
        
        # If the text contains too many descriptive words, it's likely descriptive
        word_count = len(text.split())
        descriptive_word_count = sum(1 for word in text.split() if word.lower() in descriptive_patterns)
        
        if word_count > 2 and descriptive_word_count / word_count > 0.3:
            return True
            
        # Check for specific patterns that indicate descriptive text
        if any(pattern in text_lower for pattern in [
            "up to", "from row", "to row", "for each", "in the", "of the", 
            "with the", "by the", "such as", "including", "excluding",
            "represents", "contains", "consists", "composed", "structured",
            "formatted", "organized", "arranged", "presented"
        ]):
            return True
            
        return False
    
    def initialize_processing_stats(self):
        """Initialize processing statistics for tracking pipeline performance."""
        self.processing_stats = {
            "documents_processed": 0,
            "schemas_discovered": 0,           # LLM schema discovery
            "entities_extracted": 0,          # LLM entity extraction  
            "schemas_validated": 0,           # MCP validation
            "entities_ingested": 0,           # MCP execution
            "relationships_created": 0,       # MCP execution
            "mcp_requests_made": 0,           # Total MCP API calls
            "mcp_requests_successful": 0,     # Successful MCP API calls
            "mcp_requests_failed": 0,         # Failed MCP API calls
            "parsing_errors": 0,             # LLM response parsing errors
            "validation_warnings": 0,        # Dynamic validation warnings
            "processing_time": 0.0,          # Total processing time
            "llm_response_time": 0.0,        # LLM processing time
            "mcp_response_time": 0.0,        # MCP processing time
            "errors": []
        }

    def ingest_content_with_schema_validation(self, content: str) -> Dict[str, Any]:
        """
        Main entry point for document ingestion following the architecture:
        Document Ingestion → LLM Schema Discovery & Entity Extraction → MCP Server Validation & Execution
        
        Args:
            content: Document content to process
            
        Returns:
            Dict with ingestion results and processing stats
        """
        try:
            self.logger.info("=== Starting Document Ingestion Pipeline ===")
            
            # LAYER 1: Document Ingestion Layer (Input Processing)
            self.logger.info("Layer 1: Processing document input...")
            processed_content = self._preprocess_document(content)
            

            # LAYER 2: LLM Schema Discovery & Entity Extraction
            self.logger.info("Layer 2: LLM Schema Discovery & Entity Extraction...")
            llm_result = self._llm_schema_discovery_and_extraction(processed_content)

            if not llm_result.get("success"):
                return {
                    "success": False,
                    "error": "LLM schema discovery and entity extraction failed",
                    "details": llm_result.get("error"),
                    "stats": self.processing_stats
                }

            # --- POST-PROCESSING: Ensure key_property and data_model ---
            # Entities
            entities = llm_result.get("entity_extraction", {}).get("entities", [])
            for idx, entity in enumerate(entities):
                props = entity.get("properties", {})
                # Synthesize key_property if missing
                if "id" not in props:
                    props["id"] = f"auto_id_{idx+1}"
                entity["properties"] = props
            
            # Store entities for relationship validation
            self._current_entities = entities
            
            # Debug: Log sample entity IDs for relationship validation
            print(f"[DEBUG] Storing entities for relationship validation - count: {len(entities)}")
            if entities:
                sample_entity_ids = [e.get("properties", {}).get("id") for e in entities[:5]]
                self.logger.debug(f"Sample entity IDs from _current_entities: {sample_entity_ids}")
                print(f"[DEBUG] Sample entity IDs stored in _current_entities: {sample_entity_ids}")
                
                # Show full entity structure for first entity
                if entities:
                    print(f"[DEBUG] Full first entity structure: {entities[0]}")
            else:
                print(f"[DEBUG] WARNING: No entities to store in _current_entities!")
            
            # Relationships - ensure data_model is set
            relationships = llm_result.get("entity_extraction", {}).get("relationships", [])
            for idx, rel in enumerate(relationships):
                # Patch: Accept both 'start_entity'/'end_entity' and 'from'/'to' fields
                if "from" not in rel and "start_entity" in rel:
                    rel["from"] = rel["start_entity"]
                if "to" not in rel and "end_entity" in rel:
                    rel["to"] = rel["end_entity"]
                # Synthesize data_model if missing
                    # ...existing code...
            self.logger.info("Layer 3: MCP Server Validation & Execution...")
            mcp_result = self._mcp_validation_and_execution(llm_result)

            # Update final stats
            if mcp_result.get("success"):
                self.processing_stats["documents_processed"] += 1
                mcp_result["pipeline"] = "document_ingestion → llm_discovery → mcp_execution"

            self.logger.info("=== Document Ingestion Pipeline Complete ===")
            return mcp_result
            
        except Exception as e:
            error_msg = f"Document ingestion pipeline failed: {str(e)}"
            self.logger.error(error_msg)
            self.processing_stats["errors"].append(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "stats": self.processing_stats
            }

    def ingest_content(self, content: str, metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Simplified entry point for document ingestion compatible with execution agent.
        
        This method acts as a wrapper around ingest_content_with_schema_validation
        to maintain compatibility with the execution agent interface.
        
        Args:
            content: Document content to process
            metadata: Optional metadata dict (for compatibility, currently unused)
            
        Returns:
            Dict with ingestion results compatible with execution agent expectations
        """
        try:
            # Log metadata if provided for debugging
            if metadata:
                self.logger.info(f"Graph ingestion with metadata: {metadata}")
            
            # Call the main ingestion method
            result = self.ingest_content_with_schema_validation(content)
            
            # Transform result to match expected execution agent format
            return {
                "success": result.get("success", False),
                "nodes_created": result.get("entities_created", 0),  # Map entities to nodes
                "relationships_created": result.get("relationships_created", 0),
                "entities_ingested": result.get("entities_created", 0),
                "method": result.get("method", "mcp_cypher_write"),
                "stats": result.get("stats", {}),
                "error": result.get("error") if not result.get("success") else None,
                "pipeline": result.get("pipeline", "document_ingestion → llm_discovery → mcp_execution")
            }
            
        except Exception as e:
            error_msg = f"Graph ingestion wrapper failed: {str(e)}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "nodes_created": 0,
                "relationships_created": 0,
                "entities_ingested": 0
            }

    def _preprocess_document(self, content: str) -> str:
        """
        Layer 1: Document Ingestion Layer - Preprocess and normalize input content.
        """
        try:
            # Basic content preprocessing
            processed = content.strip()
            
            # Remove excessive whitespace
            processed = re.sub(r'\s+', ' ', processed)
            
            # Ensure reasonable content length
            max_length = getattr(config_manager, 'max_content_length', 100000)
            if len(processed) > max_length:
                processed = processed[:max_length]
                self.logger.warning(f"Content truncated to {max_length} characters for processing")
            
            self.logger.info(f"Document preprocessed: {len(processed)} characters")
            return processed
            
        except Exception as e:
            self.logger.error(f"Document preprocessing failed: {str(e)}")
            raise

    def _llm_schema_discovery_and_extraction(self, content: str) -> Dict[str, Any]:
        """
        Layer 2: LLM Schema Discovery & Entity Extraction
        
        This method performs schema discovery and entity extraction, returning structured lists
        of entities and relationships instead of JSON format.
        
        Returns:
            Dict containing:
            - extracted_entities: List of entities with type, properties, and key_property
            - extracted_relationships: List of relationships with type, start_entity, end_entity, cardinality
        """
        try:
            self.logger.info("Starting LLM schema discovery and entity extraction...")
            
            # Enhanced prompt for structured list-based response
            enhanced_prompt = f"""
            You are an expert data analyst and knowledge graph architect. Analyze the provided content and perform comprehensive schema discovery and entity extraction for knowledge graph construction.

            TASK OVERVIEW:
            1. Identify the data domain and format (CSV, JSON, etc.)
            2. Discover entity types and their properties
            3. Identify relationships between entities
            4. Extract ALL individual data records as entity instances
            5. Create relationship instances between entities

            SCHEMA DISCOVERY:
            - Analyze ALL columns/fields to identify distinct entity types
            - Group related attributes under logical entity types  
            - Identify primary keys and relationships between entities
            - Use clear, descriptive names (PascalCase for entities, UPPER_CASE for relationships)

            ENTITY EXTRACTION:
            - Extract ALL data records as entities with complete attribute sets
            - Use ORIGINAL data values as IDs - DO NOT add prefixes like FND_, PAT_, IMG_, etc.
            - If ID field exists, use it exactly as provided in source data
            - For entities without explicit IDs, create simple numeric or descriptive IDs
            - Preserve all data values exactly as they appear in source and handle missing values appropriately

            RELATIONSHIP EXTRACTION:
            - Identify direct references (foreign keys, shared IDs)
            - Create relationship instances for detected connections
            - Use semantic relationships based on domain knowledge

            Content to analyze:
            {content}

            RESPONSE FORMAT (Use clear section headers, not JSON):

            === EXTRACTED ENTITY LIST ===
            Entity Type: <EntityTypeName>
            Properties: <comma-separated list of properties>
            Key Property: <primary identifier property>

            === EXTRACTED RELATIONSHIP LIST ===
            Relationship Type: <RELATIONSHIP_NAME>
            Start Entity: <StartEntityType>
            End Entity: <EndEntityType>
            Cardinality: <1:1|1:many|many:many>

            === DATA INSTANCES ===
            Entity Instance: <EntityType>|<UniqueID>|<Property1=Value1,Property2=Value2,...>

            === RELATIONSHIP INSTANCES ===
            <SourceEntityType> <SourceID> <RELATIONSHIP_TYPE> <TargetEntityType> <TargetID>

            REQUIREMENTS:
            - Extract ALL data records completely
            - Use consistent naming conventions  
            - Use ORIGINAL data values as entity IDs - NO artificial prefixes
            - Preserve exact field values from source data (e.g., "Cardiomegaly", not "FND_Cardiomegaly")
            - Create meaningful relationships based on data connections
            """

            # Make LLM request using existing infrastructure with retry logic
            if not self.llm:
                self.logger.error("LLM client not available")
                return {"success": False, "error": "LLM client not initialized"}

            self.logger.info("Making LLM request for structured schema discovery...")
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.logger.info(f"LLM request attempt {attempt + 1}/{max_retries}")
                    response = self.llm.invoke(enhanced_prompt)
                    response_text = response.content if hasattr(response, 'content') else str(response)
                    
                    # Optionally save raw LLM response if DEBUG_LLM_RESPONSE=1 is set in environment
                    import os
                    if os.getenv("DEBUG_LLM_RESPONSE", "0") == "1":
                        with open("debug_current_llm_response.txt", "w", encoding="utf-8") as f:
                            f.write(response_text)
                        self.logger.info("Raw LLM response saved to debug_current_llm_response.txt")
                    self.logger.info(f"LLM response received, length: {len(response_text)} characters")
                    break
                except Exception as e:
                    error_details = f"LLM attempt {attempt + 1} failed: {str(e)}"
                    self.logger.warning(error_details)
                    
                    if attempt == max_retries - 1:
                        final_error = f"LLM failed after {max_retries} attempts: {str(e)}"
                        self.logger.error(final_error)
                        return {"success": False, "error": final_error}
                    # Wait before retry
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff

            # Parse the structured response
            parsed_result = self._parse_structured_llm_response(response_text)
            
            if parsed_result.get("success"):
                entities_count = len(parsed_result.get("extracted_entities", []))
                relationships_count = len(parsed_result.get("extracted_relationships", []))
                
                self.processing_stats["schemas_discovered"] += 1
                self.processing_stats["entities_extracted"] += entities_count
                
                self.logger.info(f"LLM processing completed - Entities: {entities_count}, Relationships: {relationships_count}")
            
            return parsed_result
            
        except Exception as e:
            error_msg = f"LLM schema discovery and extraction failed: {str(e)}"
            self.logger.error(error_msg)
            return {"success": False, "error": error_msg}

    def _parse_structured_llm_response(self, response_text: str) -> Dict[str, Any]:
        """
        Parse the structured LLM response for schema discovery and entity extraction.
        
        Returns:
            Dict containing:
            - extracted_entities: List of entity dictionaries with type, properties, key_property
            - extracted_relationships: List of relationship dictionaries with type, start_entity, end_entity, cardinality
        """
        try:
            self.logger.info("Parsing structured LLM response...")
            
            # Initialize result structure
            extracted_entities = []
            extracted_relationships = []
            entity_types = []
            relationship_types = []
            
            # Save raw response for debugging
            try:
                with open("debug_structured_response.txt", "w", encoding="utf-8") as f:
                    f.write(f"Structured Response:\n{response_text}\n")
            except Exception as debug_e:
                self.logger.warning(f"Failed to save debug response: {debug_e}")
            
            # Try to parse as JSON first (preferred format)
            try:
                json_data = json.loads(response_text.strip())
                self.logger.info("LLM response is JSON format, parsing accordingly...")
                
                # Extract schema discovery
                schema_discovery = json_data.get("schema_discovery", {})
                
                # Parse entity types from JSON
                for entity in schema_discovery.get("entity_types", []):
                    entity_type = {
                        "type": entity.get("type", ""),
                        "properties": entity.get("properties", []),
                        "description": entity.get("description", ""),
                        "key_property": entity.get("key_property", "")
                    }
                    entity_types.append(entity_type)
                
                # Parse relationship types from JSON
                for rel in schema_discovery.get("relationship_types", []):
                    rel_type = {
                        "type": rel.get("type", ""),
                        "start_entity": rel.get("start_entity", ""),
                        "end_entity": rel.get("end_entity", ""),
                        "description": rel.get("description", ""),
                        "cardinality": rel.get("cardinality", "1:many")
                    }
                    relationship_types.append(rel_type)
                
                # Extract entity extraction data
                entity_extraction = json_data.get("entity_extraction", {})
                
                # Parse entities from JSON
                for entity in entity_extraction.get("entities", []):
                    entity_instance = {
                        "type": entity.get("type", ""),
                        "properties": entity.get("properties", {})
                    }
                    extracted_entities.append(entity_instance)
                
                # Parse relationships from JSON
                for rel in entity_extraction.get("relationships", []):
                    rel_instance = {
                        "type": rel.get("type", ""),
                        "from": rel.get("from", ""),
                        "to": rel.get("to", ""),
                        "start_node_label": rel.get("start_node_label", ""),
                        "end_node_label": rel.get("end_node_label", ""),
                        "properties": rel.get("properties", {})
                    }
                    extracted_relationships.append(rel_instance)
                
                self.logger.info(f"JSON parsing successful - Entities: {len(extracted_entities)}, Relationships: {len(extracted_relationships)}")
                
                return {
                    "success": True,
                    "extracted_entities": extracted_entities,
                    "extracted_relationships": extracted_relationships,
                    "entity_types": entity_types,
                    "relationship_types": relationship_types,
                    "confidence": schema_discovery.get("confidence", 0.8)
                }
                
            except json.JSONDecodeError:
                self.logger.info("LLM response is not JSON, parsing as structured text...")
                # Continue with structured text parsing below
            except Exception:
                pass  # Don't fail if we can't save debug file
            
            lines = response_text.strip().split('\n')
            current_section = None
            current_entity = {}
            current_relationship = {}
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Detect sections
                if line.startswith("=== EXTRACTED ENTITY LIST ==="):
                    current_section = "entity_types"
                    continue
                elif line.startswith("=== EXTRACTED RELATIONSHIP LIST ==="):
                    current_section = "relationship_types"
                    continue
                elif line.startswith("=== DATA INSTANCES ==="):
                    current_section = "data_instances"
                    continue
                elif line.startswith("=== RELATIONSHIP INSTANCES ==="):
                    current_section = "relationship_instances"
                    continue
                elif line.startswith("==="):
                    # Any other section header resets current_section to None
                    current_section = None
                    continue
                elif line.startswith("**NOTES") or "NOTES ON EDGE CASES" in line:
                    # Stop parsing when we reach the notes section
                    break
                
                # Parse content based on current section
                if current_section == "entity_types":
                    if line.startswith("**Entity Type:") or line.startswith("Entity Type:"):
                        # Save previous entity type if exists
                        if current_entity:
                            entity_types.append(current_entity)
                        # Handle both formats: "**Entity Type: Patient**" and "Entity Type: Patient"
                        entity_name = line.split(":", 1)[1].strip().replace("**", "").strip()
                        current_entity = {"type": entity_name}
                    elif (line.startswith("- Properties:") or line.startswith("Properties:")) and current_entity:
                        props_str = line.split(":", 1)[1].strip()
                        current_entity["properties"] = [p.strip() for p in props_str.split(",")]
                    elif (line.startswith("- Key Property:") or line.startswith("Key Property:")) and current_entity:
                        current_entity["key_property"] = line.split(":", 1)[1].strip()
                
                elif current_section == "relationship_types":
                    if line.startswith("**Relationship Type:") or line.startswith("Relationship Type:"):
                        # Save previous relationship type if exists
                        if current_relationship:
                            relationship_types.append(current_relationship)
                        # Handle both formats
                        rel_name = line.split(":", 1)[1].strip().replace("**", "").strip()
                        current_relationship = {"type": rel_name}
                    elif (line.startswith("- Start Entity:") or line.startswith("Start Entity:")) and current_relationship:
                        current_relationship["start_entity"] = line.split(":", 1)[1].strip()
                    elif (line.startswith("- End Entity:") or line.startswith("End Entity:")) and current_relationship:
                        current_relationship["end_entity"] = line.split(":", 1)[1].strip()
                    elif (line.startswith("- Cardinality:") or line.startswith("Cardinality:")) and current_relationship:
                        current_relationship["cardinality"] = line.split(":", 1)[1].strip()
                
                elif current_section == "data_instances":
                    # Handle entity instances that can be nested under sub-sections or stand-alone
                    if (line.startswith("Entity Instance:") or line.startswith("**Entity Instance:") or
                        (line.startswith("-") and "Entity Instance:" in line)):
                        
                        # Extract the instance line, handling both formats:
                        # "Entity Instance: EntityType|UniqueID|Property1=Value1,Property2=Value2,..."
                        # "**Entity Instance: EntityType|UniqueID|Property1=Value1,Property2=Value2,...**"
                        # "- Entity Instance: EntityType|UniqueID|Property1=Value1,Property2=Value2,..."
                        instance_line = line.strip("*").strip()
                        if instance_line.startswith("-"):
                            instance_line = instance_line[1:].strip()  # Remove leading dash
                        
                        # Parse format: Entity Instance: EntityType|UniqueID|Property1=Value1,Property2=Value2,...
                        instance_parts = instance_line.split(":", 1)[1].strip().split("|")
                        if len(instance_parts) >= 3:
                            entity_type = instance_parts[0].strip()
                            entity_id = instance_parts[1].strip()
                            properties_str = instance_parts[2].strip()
                            
                            print(f"DEBUG: Parsing entity instance: {entity_type} {entity_id}")
                            
                            # Parse properties with better error handling
                            properties = {}
                            if properties_str:
                                for prop_pair in properties_str.split(","):
                                    if "=" in prop_pair:
                                        try:
                                            key, value = prop_pair.split("=", 1)
                                            key_clean = key.strip()
                                            value_clean = value.strip().strip('"\'')  # Remove quotes
                                            properties[key_clean] = value_clean
                                        except Exception as e:
                                            self.logger.warning(f"Failed to parse property pair '{prop_pair}': {e}")
                                            continue
                            
                            # Add ID to properties
                            properties["id"] = entity_id
                            
                            # Create entity instance
                            entity_instance = {
                                "type": entity_type,
                                "entity_id": entity_id,
                                "properties": properties
                            }
                            extracted_entities.append(entity_instance)
                    elif line and not line.startswith("---") and not line.startswith("**"):
                        # Parse relationship instances: "SourceEntity SourceID RELATIONSHIP_TYPE TargetEntity"
                        parts = line.split()
                        if len(parts) >= 4:
                            source_entity = parts[0]
                            source_id = parts[1]
                            rel_type = parts[2]
                            target_entity = " ".join(parts[3:])  # Handle multi-word entity names
                            
                            # Create relationship instance
                            rel_instance = {
                                "type": rel_type,
                                "from": source_id,
                                "to": target_entity,
                                "start_node_label": source_entity,
                                "end_node_label": target_entity.split()[0] if " " in target_entity else target_entity,
                                "properties": {}
                            }
                            extracted_relationships.append(rel_instance)
                
                elif current_section == "relationship_instances":
                    # Parse relationship instances in multiple formats:
                    # Format 1: "SourceEntityType SourceID RELATIONSHIP_TYPE TargetEntityType TargetID"
                    # Format 2: "**SourceEntityType SourceID RELATIONSHIP_TYPE TargetEntityType TargetID**"
                    # Format 3: "- SourceEntityType SourceID RELATIONSHIP_TYPE TargetEntityType TargetID"
                    
                    if (line and not line.startswith("===") and not line.startswith("---") and 
                        not line.startswith("NOTES") and not line.startswith("...") and
                        not ":" in line):  # Exclude section headers and notes
                        
                        line_content = line.strip("*").strip()
                        if line_content.startswith("-"):
                            line_content = line_content[1:].strip()  # Remove leading dash
                        
                        # Skip empty lines or lines that look like descriptions
                        if (not line_content or len(line_content.split()) < 5 or 
                            line_content.startswith("(") or "Continue" in line_content):
                            continue
                        
                        parts = line_content.split()
                        if len(parts) >= 5:
                            source_entity_type = parts[0]
                            source_id = parts[1]
                            rel_type = parts[2]
                            target_entity_type = parts[3]
                            target_id = " ".join(parts[4:])  # Handle multi-word target IDs
                            
                            # DYNAMIC VALIDATION: Validate structure and format rather than specific values
                            # Get dynamic entity types from discovered schema
                            discovered_entity_types = [et.get("type", "") for et in entity_types if et.get("type")]
                            discovered_relationship_types = [rt.get("type", "") for rt in relationship_types if rt.get("type")]
                            
                            # Validate relationship format and content quality
                            is_valid_relationship = (
                                # Check basic structure
                                len(source_entity_type) > 0 and len(target_entity_type) > 0 and len(rel_type) > 0 and
                                len(source_id) > 0 and len(target_id) > 0 and
                                # Validate entity types match discovered schema (if available) or are reasonable names
                                (not discovered_entity_types or 
                                 source_entity_type in discovered_entity_types or 
                                 self._is_valid_entity_type_name(source_entity_type)) and
                                (not discovered_entity_types or 
                                 target_entity_type in discovered_entity_types or 
                                 self._is_valid_entity_type_name(target_entity_type)) and
                                # Validate relationship type format (should be UPPERCASE or CamelCase, not descriptive text)
                                (not discovered_relationship_types or 
                                 rel_type in discovered_relationship_types or 
                                 self._is_valid_relationship_type_name(rel_type)) and
                                # Exclude common descriptive phrases that get parsed incorrectly
                                not self._is_descriptive_text(line_content)
                            )
                            
                            if is_valid_relationship:
                                print(f"DEBUG: Parsing relationship instance: {source_entity_type} {source_id} -> {rel_type} -> {target_entity_type} {target_id}")
                                
                                # Create relationship instance
                                rel_instance = {
                                    "type": rel_type,
                                    "from": source_id,
                                    "to": target_id,
                                    "start_node_label": source_entity_type,
                                    "end_node_label": target_entity_type,
                                    "properties": {}
                                }
                                extracted_relationships.append(rel_instance)
                            else:
                                print(f"DEBUG: Skipping invalid relationship format: {line_content[:100]}...")
            
            # Don't forget to add the last items
            if current_entity:
                entity_types.append(current_entity)
            
            if current_relationship:
                relationship_types.append(current_relationship)
            
            # extracted_entities and extracted_relationships are populated from parsing above
            
            # Validate and clean up the results with dynamic validation
            valid_entities = []
            discovered_entity_types = [et.get("type", "") for et in entity_types if et.get("type")]

            for entity in extracted_entities:
                # Relaxed validation: accept all entities with type and entity_id
                if entity.get("type") and entity.get("entity_id"):
                    # Ensure properties dict exists
                    if "properties" not in entity:
                        entity["properties"] = {}
                    # Add the entity_id to properties if not present
                    if "id" not in entity["properties"]:
                        entity["properties"]["id"] = entity["entity_id"]
                    valid_entities.append(entity)
                else:
                    self.logger.warning(f"Skipping malformed entity: {entity}")

            valid_relationships = []
            discovered_relationship_types = [rt.get("type", "") for rt in relationship_types if rt.get("type")]

            for rel in extracted_relationships:
                # Relaxed validation: accept all relationships with type, from, and to
                if rel.get("type") and rel.get("from") and rel.get("to"):
                    # Ensure properties dict exists
                    if "properties" not in rel:
                        rel["properties"] = {}
                    valid_relationships.append(rel)
                else:
                    self.logger.warning(f"Skipping malformed relationship: {rel}")

            self.logger.info(f"Parsed {len(entity_types)} entity types, {len(relationship_types)} relationship types")
            self.logger.info(f"Extracted {len(valid_entities)} entities, {len(valid_relationships)} relationships")

            return {
                "success": True,
                "extracted_entities": valid_entities,
                "extracted_relationships": valid_relationships,
                "entity_types": entity_types,
                "relationship_types": relationship_types
            }
            
        except Exception as e:
            error_msg = f"Structured response parsing failed: {str(e)}"
            self.logger.error(error_msg)
            return {"success": False, "error": error_msg}

    def _mcp_validation_and_execution(self, llm_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Layer 3: MCP Server Validation & Execution
        
        Takes the LLM results and validates the schema, then executes the ingestion.
        Updated to work with structured lists instead of JSON schema format.
        """
        try:
            self.logger.info("Starting MCP validation and execution...")
            
            # Extract the new structured format
            extracted_entities = llm_result.get("extracted_entities", [])
            extracted_relationships = llm_result.get("extracted_relationships", [])
            entity_types = llm_result.get("entity_types", [])
            relationship_types = llm_result.get("relationship_types", [])
            
            # DEBUG: Check LLM result structure
            print(f"[DEBUG] LLM RESULT KEYS: {list(llm_result.keys())}")
            
            # Try alternative key structures from 2-list format
            if not extracted_entities and "entities" in llm_result:
                extracted_entities = llm_result["entities"]
                print(f"[DEBUG] Used alternative entities key: {len(extracted_entities)} entities")
            if not extracted_relationships and "relationships" in llm_result:
                extracted_relationships = llm_result["relationships"] 
                print(f"[DEBUG] Used alternative relationships key: {len(extracted_relationships)} relationships")
            if not entity_types and "entity_types" in llm_result:
                entity_types = llm_result["entity_types"]
            if not relationship_types and "relationship_types" in llm_result:
                relationship_types = llm_result["relationship_types"]
            
            # DEBUG: Print what was actually extracted from LLM
            print(f"[DEBUG] LLM EXTRACTION RESULTS:")
            print(f"  - Entity types: {len(entity_types)}")
            print(f"  - Relationship types: {len(relationship_types)}")  
            print(f"  - Extracted entities: {len(extracted_entities)}")
            print(f"  - Extracted relationships: {len(extracted_relationships)}")
            
            if relationship_types:
                print(f"[DEBUG] Relationship types found:")
                for i, rt in enumerate(relationship_types):
                    print(f"  {i+1}. Type: {rt.get('type', 'MISSING')}")
                    print(f"      Start: {rt.get('start_entity', 'MISSING')}")
                    print(f"      End: {rt.get('end_entity', 'MISSING')}")
                    print(f"      Raw: {rt}")
            else:
                print(f"[DEBUG] NO RELATIONSHIP TYPES FOUND!")
                    
            if extracted_relationships:
                print(f"[DEBUG] Extracted relationships:")
                for i, rel in enumerate(extracted_relationships[:5]):  # Show first 5
                    print(f"  {i+1}. {rel.get('type', 'unknown')}: {rel.get('from', 'unknown')} -> {rel.get('to', 'unknown')}")
            else:
                print(f"[DEBUG] NO RELATIONSHIPS EXTRACTED - This is the root cause!")
            
            # Step 1: Build Neo4j data model from extracted entities and relationships
            data_model = self._build_neo4j_data_model_from_structured_results(
                entity_types, relationship_types, extracted_entities, extracted_relationships
            )
            
            if not data_model:
                return {
                    "success": False,
                    "error": "Failed to build Neo4j data model from structured results",
                    "stats": self.processing_stats
                }
            
            # Step 2: Execute ingestion using MCP Cypher server  
            ingestion_result = self._execute_mcp_ingestion(data_model, {"entities": extracted_entities, "relationships": extracted_relationships})
            
            if ingestion_result.get("success"):
                self.logger.info("MCP ingestion execution successful")
                ingestion_result["validation_passed"] = True
                # No confidence score in new structure, default to high confidence
                ingestion_result["schema_confidence"] = 0.9
            
            return ingestion_result
            
        except Exception as e:
            error_msg = f"MCP validation and execution failed: {str(e)}"
            self.logger.error(error_msg)
            return {"success": False, "error": error_msg, "stats": self.processing_stats}

    def _validate_schema_with_mcp_server(self, schema_discovery: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate the discovered schema using MCP Data Modeling server validation tools.
        Returns success=True if validation passes or if validation is not available.
        """
        try:
            # Convert schema discovery to format expected by MCP validation
            validation_payload = {
                "entity_types": schema_discovery.get("entity_types", []),
                "relationship_types": schema_discovery.get("relationship_types", [])
            }
            
            # Call MCP Data Modeling server validate_data_model tool
            result = self.make_mcp_request(
                self.data_modeling_server_url,
                "validate_data_model",
                {"data_model": validation_payload}
            )
            
            # If the tool doesn't exist (404), consider it a non-critical warning
            if not result.get("success") and "404" in str(result.get("error", "")):
                self.logger.warning("MCP validation tool not available - skipping validation")
                return {"success": True, "skipped": True, "reason": "validation_tool_not_available"}
            
            return result
            
        except Exception as e:
            self.logger.error(f"MCP schema validation error: {str(e)}")
            # Don't fail the entire pipeline for validation errors
            return {"success": True, "skipped": True, "reason": f"validation_error: {str(e)}"}

    def _build_neo4j_data_model_from_structured_results(self, entity_types: List[Dict], 
                                                       relationship_types: List[Dict],
                                                       extracted_entities: List[Dict], 
                                                       extracted_relationships: List[Dict]) -> Optional[Dict[str, Any]]:
        """
        Build Neo4j data model from structured entity and relationship lists.
        
        Args:
            entity_types: List of entity type definitions with type, properties, key_property
            relationship_types: List of relationship type definitions with type, start_entity, end_entity, cardinality
            extracted_entities: List of actual entity instances
            extracted_relationships: List of actual relationship instances
        """
        try:
            # Build nodes from extracted entities - Group by type
            node_groups = {}
            for entity in extracted_entities:
                entity_type = entity.get("type", "Entity")
                if entity_type not in node_groups:
                    node_groups[entity_type] = []
                node_groups[entity_type].append(entity)

            # Create node definitions for each entity type
            nodes = []
            for entity_type, entities in node_groups.items():
                # Find the corresponding entity type definition
                type_def = next((et for et in entity_types if et.get("type") == entity_type), None)
                
                # Collect all properties from all entities of this type
                all_properties = set()
                for entity in entities:
                    all_properties.update(entity.get("properties", {}).keys())
                
                # Also add properties from type definition if available
                if type_def and type_def.get("properties"):
                    all_properties.update(type_def["properties"])
                
                # Ensure 'id' property exists
                if "id" not in all_properties:
                    all_properties.add("id")
                
                # Build property schema for MCP
                property_schema = []
                for prop_name in sorted(all_properties):
                    prop_obj = {"name": prop_name, "type": "string"}  # Default to string
                    property_schema.append(prop_obj)
                
                # Prepare all records for this entity type
                records = []
                for idx, entity in enumerate(entities):
                    props = entity.get("properties", {})
                    # Ensure id exists
                    if "id" not in props:
                        entity_id = entity.get("entity_id", f"auto_id_{entity_type}_{idx+1}")
                        props["id"] = entity_id
                    
                    # Create record with all expected properties - avoid NULL values
                    record = {}
                    for prop_name in all_properties:
                        value = props.get(prop_name, "")
                        # Convert None/null values to empty string to avoid Neo4j merge errors
                        if value is None or str(value).lower() == "null":
                            value = ""
                        record[prop_name] = str(value)  # Ensure string type
                    records.append(record)
                
                # Determine key property - always use 'id' to avoid NULL issues
                key_property_name = "id"
                # We force the use of 'id' as key property to avoid NULL key issues
                # that were causing Neo4j merge errors
                
                # Create node definition
                node = {
                    "label": entity_type,
                    "properties": property_schema,
                    "records": records,
                    "key_property": {"name": key_property_name, "type": "string"},
                    "data_model": "auto"
                }
                nodes.append(node)
                
                self.logger.info(f"Created node group {entity_type} with {len(records)} records")

            # Build relationships from extracted relationship instances - group by type
            relationship_groups = {}
            for rel in extracted_relationships:
                # Only process relationships that have required fields - use JSON format fields
                if not rel.get("type") or not rel.get("from") or not rel.get("to"):
                    self.logger.warning(f"Skipping incomplete relationship: {rel}")
                    continue
                    
                rel_type = rel.get("type")
                if rel_type not in relationship_groups:
                    # Find the corresponding relationship type definition
                    type_def = next((rt for rt in relationship_types if rt.get("type") == rel_type), None)
                    
                    relationship_groups[rel_type] = {
                        "type": rel_type,
                        "start_node_label": rel.get("start_node_label", 
                                                  type_def.get("start_entity", "Entity") if type_def else "Entity"),
                        "end_node_label": rel.get("end_node_label", 
                                                type_def.get("end_entity", "Entity") if type_def else "Entity"),
                        "properties": [],
                        "records": [],
                        "data_model": "auto"
                    }
                
                # Create proper record format
                rel_record = {
                    "type": rel.get("type"),
                    "from": rel.get("from"),  # Use JSON format fields
                    "to": rel.get("to"),      # Use JSON format fields
                    "start_node_label": rel.get("start_node_label", relationship_groups[rel_type]["start_node_label"]),
                    "end_node_label": rel.get("end_node_label", relationship_groups[rel_type]["end_node_label"]),
                    "properties": rel.get("properties", {})
                }
                
                relationship_groups[rel_type]["records"].append(rel_record)
            
            # Convert groups to relationship list
            relationships = list(relationship_groups.values())
            
            for rel_type, rel_group in relationship_groups.items():
                self.logger.info(f"Created relationship group {rel_type} with {len(rel_group['records'])} records")

            data_model = {
                "nodes": nodes,
                "relationships": relationships,
                "schema_metadata": {
                    "entity_types_count": len(entity_types),
                    "relationship_types_count": len(relationship_types),
                    "extracted_entities_count": len(extracted_entities),
                    "extracted_relationships_count": len(extracted_relationships)
                }
            }

            self.logger.info(f"Data model built: {len(nodes)} node types, {len(relationships)} relationship types")
            return data_model

        except Exception as e:
            error_msg = f"Failed to build Neo4j data model from structured results: {str(e)}"
            self.logger.error(error_msg)
            return None

    def _execute_mcp_ingestion(self, data_model: Dict[str, Any], entities_relationships: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute ingestion following the MCP write format:
        Uses the correct MCP Cypher server format with 'write_neo4j_cypher' and 'records' parameter.
        """
        try:
            entities_created = 0
            relationships_created = 0
            self.logger.info("Starting MCP ingestion execution...")

            # Step 1: Create constraints using MCP Data Modeling server
            constraints_success = self._create_constraints_via_mcp(data_model)
            if constraints_success:
                self.logger.info("Constraints created successfully")

            # Step 2: Ingest nodes using MCP Cypher server
            for node in data_model.get("nodes", []):
                node_count = self._ingest_nodes_via_mcp(node, entities_relationships)
                entities_created += node_count
                self.logger.info(f"Node ingestion for {node.get('label', 'unknown')}: {node_count} entities created")
                
            # Step 3: Ingest relationships using MCP Cypher server  
            print(f"[DEBUG] Data model has {len(data_model.get('relationships', []))} relationship types")
            if data_model.get("relationships"):
                for i, rel in enumerate(data_model.get("relationships", [])):
                    print(f"[DEBUG] Relationship {i+1}: {rel.get('type', 'unknown')} from {rel.get('start_node_label', 'unknown')} to {rel.get('end_node_label', 'unknown')}")
                    print(f"[DEBUG] Relationship has {len(rel.get('records', []))} records")
            
            for relationship in data_model.get("relationships", []):
                rel_type = relationship.get("type", "").upper()
                # Pass actual relationships from the parsed LLM output
                rel_count = self._ingest_relationships_via_mcp(relationship, entities_relationships)
                relationships_created += rel_count
                self.logger.info(f"Relationship ingestion for {relationship.get('type', 'unknown')}: {rel_count} relationships created")

            # Update processing stats
            self.processing_stats["entities_ingested"] += entities_created
            self.processing_stats["relationships_created"] += relationships_created

            self.logger.info(f"MCP ingestion completed: {entities_created} entities, {relationships_created} relationships")
            
            return {
                "success": True,
                "entities_created": entities_created,
                "relationships_created": relationships_created,
                "method": "mcp_cypher_write",
                "stats": self.processing_stats
            }

        except Exception as e:
            error_msg = f"MCP ingestion execution failed: {str(e)}"
            self.logger.error(error_msg)
            self.processing_stats["errors"].append(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "stats": self.processing_stats
            }

    def _create_constraints_via_mcp(self, data_model: Dict[str, Any]) -> bool:
        """
        Create database constraints using MCP Data Modeling server and execute via MCP Cypher server.
        """
        try:
            # Get constraint queries from MCP Data Modeling server
            constraints_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "get_constraints_cypher_queries",
                {"data_model": data_model}
            )

            if not constraints_result.get("success", False):
                self.logger.warning(f"Failed to get constraints: {constraints_result.get('error', 'Unknown error')}")
                return False

            constraints = constraints_result.get("result", [])
            if not isinstance(constraints, list) or len(constraints) == 0:
                self.logger.info("No constraints to create")
                return True

            # Execute each constraint via MCP Cypher server
            for constraint_query in constraints:
                result = self.make_mcp_request(
                    self.cypher_server_url,
                    "write_neo4j_cypher",
                    {
                        "query": constraint_query,
                        "params": {}  # Constraints typically don't need parameters
                    }
                )
                
                if result.get("success", False):
                    self.logger.debug(f"Constraint created: {constraint_query[:100]}...")
                else:
                    self.logger.error(f"Constraint creation failed: {result.get('error', 'Unknown error')}")
                    
            return True

        except Exception as e:
            self.logger.error(f"Constraint creation via MCP failed: {str(e)}")
            return False

    def _ingest_nodes_via_mcp(self, node: Dict[str, Any], entities_relationships: Dict[str, Any]) -> int:
        """
        Ingest nodes using MCP Data Modeling server for query generation and MCP Cypher server for execution.
        """
        try:
            node_label = node.get("label", "unknown")
            
            # Build proper Node structure for MCP call (based on documentation)
            mcp_node = {
                "label": node_label,
                "properties": node.get("properties", []),
                "key_property": node.get("key_property", {"name": "id", "type": "string"})
            }
            
            # Step 1: Get node ingestion Cypher query from MCP Data Modeling server
            self.logger.debug(f"Getting node query for {node_label}")
            
            query_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "get_node_cypher_ingest_query",
                {"node": mcp_node}
            )

            if not query_result.get("success", False):
                self.logger.error(f"Failed to get node query for {node_label}: {query_result.get('error')}")
                return 0

            # Step 2: Extract Cypher query from MCP response
            cypher_query = self._extract_cypher_from_mcp_response(query_result.get("result"))

            self.logger.debug(f"Generated Cypher for {node_label}: {cypher_query[:100]}...")
            if not cypher_query:
                self.logger.error(f"No valid Cypher query returned for node {node_label}")
                return 0

            # Only allow write queries (CREATE, MERGE, CALL, or UNWIND with MERGE/CREATE)
            query_upper = cypher_query.strip().upper()
            is_write_query = (
                query_upper.startswith(("CREATE", "MERGE", "CALL")) or
                (query_upper.startswith("UNWIND") and ("MERGE" in query_upper or "CREATE" in query_upper))
            )
            if not is_write_query:
                self.logger.warning(f"Skipping non-write Cypher query for node {node_label}")
                return 0

            # Step 3: Prepare records for this node type
            records = self._prepare_node_records(node, entities_relationships.get("entities", []))
            self.logger.debug(f"Prepared {len(records)} records for {node_label}")
            if not records:
                self.logger.warning(f"No records to ingest for node {node_label}")
                return 0

            # Step 4: Execute ingestion via MCP Cypher server using correct format
            mcp_params = {
                "query": cypher_query,
                "params": {"records": records}  # Correct format as specified
            }
            self.logger.debug(f"Executing MCP request for {node_label} with {len(records)} records")
            
            result = self.make_mcp_request(
                self.cypher_server_url,
                "write_neo4j_cypher",
                mcp_params
            )

            if result.get("success", False):
                self.logger.info(f"Successfully ingested {len(records)} {node_label} nodes")
                return len(records)
            else:
                self.logger.error(f"Node ingestion failed for {node_label}: {result.get('error', 'Unknown error')}")
                return 0

        except Exception as e:
            self.logger.error(f"Node ingestion via MCP failed for {node.get('label', 'unknown')}: {str(e)}")
            return 0

    def _ingest_relationships_via_mcp(self, relationship: Dict[str, Any], entities_relationships: Dict[str, Any]) -> int:
        """
        Ingest relationships using MCP Data Modeling server for query generation and MCP Cypher server for execution.
        """
        try:
            # Extract relationship details
            rel_type = relationship.get("type", "UNKNOWN")
            start_label = relationship.get("start_node_label", "Entity")
            end_label = relationship.get("end_node_label", "Entity")
            
            # Info print removed for production cleanup
            
            # Build proper data model structure for MCP call
            data_model = {
                "nodes": [
                    {
                        "label": start_label,
                        "properties": [
                            {"name": "id", "type": "string"}
                        ],
                        "key_property": {"name": "id", "type": "string"}
                    },
                    {
                        "label": end_label,
                        "properties": [
                            {"name": "id", "type": "string"}
                        ],
                        "key_property": {"name": "id", "type": "string"}
                    }
                ],
                "relationships": [
                    {
                        "type": rel_type,
                        "start_node_label": start_label,
                        "end_node_label": end_label,
                        "properties": relationship.get("properties", [])
                    }
                ]
            }
            
            # Step 1: Get relationship ingestion Cypher query from MCP Data Modeling server
            # Using the correct parameter format from the documentation
            self.logger.debug(f"Getting relationship query for {rel_type}")
            
            query_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "get_relationship_cypher_ingest_query",
                {
                    "data_model": data_model,
                    "relationship_type": rel_type,
                    "relationship_start_node_label": start_label,
                    "relationship_end_node_label": end_label
                }
            )
            
            # Enhanced debugging for MCP response
            # Debug print removed for production cleanup
            if not query_result.get("success", False):
                # Debug print removed for production cleanup
                self.logger.error(f"Failed to get relationship query for {rel_type}: {query_result.get('error')}")
                # Fallback to direct Cypher generation
                return self._fallback_relationship_generation(relationship, entities_relationships)

            # Step 2: Extract Cypher query from MCP response
            cypher_query = self._extract_cypher_from_mcp_response(query_result.get("result"))

            # Debug print removed for production cleanup
            self.logger.debug(f"Generated Cypher for {rel_type}: {cypher_query[:100]}...")
            if not cypher_query:
                # Debug print removed for production cleanup
                self.logger.error(f"No valid Cypher query returned for relationship {rel_type}")
                # Fallback to direct Cypher generation
                return self._fallback_relationship_generation(relationship, entities_relationships)

            # Only allow write queries (CREATE, MERGE, CALL, or UNWIND with MERGE/CREATE)
            query_upper = cypher_query.strip().upper()
            is_write_query = (
                query_upper.startswith(("CREATE", "MERGE", "CALL")) or
                (query_upper.startswith("UNWIND") and ("MERGE" in query_upper or "CREATE" in query_upper))
            )
            if not is_write_query:
                self.logger.warning(f"MCP query not suitable for {rel_type}, using fallback")
                # Fallback to direct Cypher generation
                return self._fallback_relationship_generation(relationship, entities_relationships)

            # Step 3: Prepare records for this relationship type
            records = self._prepare_relationship_records(
                relationship, 
                entities_relationships.get("relationships", []),
                entities_relationships.get("entities", [])
            )
            # Debug logging removed as per user request
            self.logger.debug(f"Prepared {len(records)} records for {rel_type}")
            if not records:
                # Info print removed for production cleanup
                return 0

            # Step 4: Execute ingestion via MCP Cypher server using correct format
            mcp_params = {
                "query": cypher_query,
                "params": {"records": records}  # Correct format as specified
            }
            self.logger.debug(f"Executing MCP request for {rel_type} with {len(records)} records")
            
            result = self.make_mcp_request(
                self.cypher_server_url,
                "write_neo4j_cypher",
                mcp_params
            )

            if result.get("success", False):
                self.logger.info(f"Successfully ingested {len(records)} {rel_type} relationships")
                return len(records)
            else:
                self.logger.error(f"Relationship ingestion failed for {rel_type}: {result.get('error', 'Unknown error')}")
                return 0

        except Exception as e:
            self.logger.error(f"Relationship ingestion via MCP failed for {relationship.get('type', 'unknown')}: {str(e)}")
            return 0

    def _fallback_relationship_generation(self, relationship: Dict[str, Any], entities_relationships: Dict[str, Any]) -> int:
        """
        Fallback method for relationship generation using direct Cypher when MCP query generation fails.
        """
        try:
            rel_type = relationship.get("type", "UNKNOWN")
            start_label = relationship.get("start_node_label", "Entity")
            end_label = relationship.get("end_node_label", "Entity")
            
            # Fallback print removed for production cleanup
            
            # Prepare relationship records
            records = self._prepare_relationship_records(
                relationship, 
                entities_relationships.get("relationships", []),
                entities_relationships.get("entities", [])
            )
            if not records:
                # Debug logging removed as per user request
                return 0
            # Generate direct Cypher query for relationship creation
            cypher_query = self._generate_relationship_cypher(rel_type, start_label, end_label, records)
            if not cypher_query:
                # Error print removed for production cleanup
                return 0
            
            # Execute directly via MCP Cypher server
            mcp_params = {
                "query": cypher_query,
                "params": {"records": records}
            }
            
            result = self.make_mcp_request(
                self.cypher_server_url,
                "write_neo4j_cypher",
                mcp_params
            )

            if result.get("success", False):
                self.logger.info(f"Successfully ingested {len(records)} {rel_type} relationships via fallback")
                return len(records)
            else:
                self.logger.error(f"Relationship fallback ingestion failed for {rel_type}: {result.get('error', 'Unknown error')}")
                return 0

        except Exception as e:
            self.logger.error(f"Relationship fallback generation failed for {relationship.get('type', 'unknown')}: {str(e)}")
            return 0
    
    def _generate_relationship_cypher(self, rel_type: str, start_label: str, end_label: str, records: List[Dict]) -> str:
        """Generate Cypher query for relationship creation."""
        try:
            # Sanitize labels to ensure valid Cypher syntax
            def sanitize_label(label):
                # Remove any parentheses, spaces, or invalid characters
                label = str(label)
                label = re.sub(r'[^a-zA-Z0-9_]', '_', label)
                # Remove leading/trailing underscores
                label = label.strip('_')
                # If label is empty after sanitization, use 'Entity'
                return label if label else 'Entity'

            start_label_clean = sanitize_label(start_label)
            end_label_clean = sanitize_label(end_label)
            rel_type_clean = sanitize_label(rel_type)

            cypher = f"""
            UNWIND $records AS record
            MATCH (start:{start_label_clean} {{id: record.sourceId}})
            MATCH (end:{end_label_clean} {{id: record.targetId}})
            MERGE (start)-[r:{rel_type_clean}]->(end)
            RETURN count(r) as relationships_created
            """
            return cypher.strip()
        except Exception as e:
            # Debug print removed for production cleanup
            return ""

    def _extract_cypher_from_mcp_response(self, result_data: Any) -> Optional[str]:
        """Extract Cypher query from various MCP response formats and sanitize property keys in SET n += {...}."""
        try:
            import re
            # Helper to sanitize property keys in SET n += {...} clauses
            def sanitize_set_clause(match):
                set_body = match.group(1)
                # Split by commas not inside brackets
                props = re.split(r',(?![^\[]*\])', set_body)
                sanitized_props = []
                for prop in props:
                    if ':' in prop:
                        key, value = prop.split(':', 1)
                        key_stripped = key.strip()
                        # Remove possible quotes
                        key_unquoted = key_stripped.strip('"\'')
                        sanitized_key = self._sanitize_property_name(key_unquoted)
                        # Try to find the referenced record property and sanitize it
                        value_stripped = value.strip()
                        # Match record.<property> (with possible brackets)
                        value_match = re.match(r'record\.([a-zA-Z0-9_\[\]#\- ]+)', value_stripped)
                        if value_match:
                            orig_value_key = value_match.group(1)
                            sanitized_value_key = self._sanitize_property_name(orig_value_key)
                            sanitized_value = f"record.{sanitized_value_key}"
                            sanitized_props.append(f"{sanitized_key}: {sanitized_value}")
                        else:
                            sanitized_props.append(f"{sanitized_key}: {value_stripped}")
                    else:
                        sanitized_props.append(prop)
                return f"SET n += {{{', '.join(sanitized_props)}}}"

            # Handle structured content format
            if isinstance(result_data, dict) and "structuredContent" in result_data:
                structured_content = result_data["structuredContent"]
                if isinstance(structured_content, dict) and "result" in structured_content:
                    cypher = structured_content["result"]
                    # Sanitize SET n += {...} property keys
                    cypher = re.sub(r'SET\s+n \+= \{([^}]*)\}', sanitize_set_clause, cypher)
                    return cypher

            # Handle direct string result
            if isinstance(result_data, str):
                cypher = result_data.strip()
                cypher = re.sub(r'SET\s+n \+= \{([^}]*)\}', sanitize_set_clause, cypher)
                return cypher

            # Handle content array format
            if isinstance(result_data, dict) and "content" in result_data:
                content = result_data["content"]
                if isinstance(content, list) and len(content) > 0:
                    first_content = content[0]
                    if isinstance(first_content, dict) and "text" in first_content:
                        cypher = first_content["text"].strip()
                        cypher = re.sub(r'SET\s+n \+= \{([^}]*)\}', sanitize_set_clause, cypher)
                        return cypher

            # Handle direct result field
            if isinstance(result_data, dict) and "result" in result_data:
                cypher = result_data["result"]
                cypher = re.sub(r'SET\s+n \+= \{([^}]*)\}', sanitize_set_clause, cypher)
                return cypher

            return None

        except Exception as e:
            self.logger.error(f"Failed to extract Cypher from MCP response: {str(e)}")
            return None

    def _prepare_node_records(self, node: Dict[str, Any], entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare node records for MCP Cypher ingestion with correct parameter format and sanitized property names."""
        try:
            records = []
            node_label = node.get("label", "")
            data_model = node.get("data_model", "auto")
            if node_label.lower() == "finding":
                # Collect all unique findings from entities
                findings_set = set()
                for entity in entities:
                    properties = entity.get("properties", {})
                    # Dynamically find the property name for finding label
                    finding_prop_name = None
                    for prop in properties.keys():
                        if "finding" in prop.lower() and "label" in prop.lower():
                            finding_prop_name = prop
                            break
                    findings = properties.get(finding_prop_name) if finding_prop_name else None
                    if findings:
                        if isinstance(findings, list):
                            findings_list = findings
                        else:
                            findings_list = re.split(r"\||,", str(findings))
                        for f in findings_list:
                            findings_set.add(f.strip())
                # Use dynamic property names from node schema
                for finding in findings_set:
                    if finding:
                        record = {"id": finding, "data_model": data_model}
                        for prop_def in node.get("properties", []):
                            prop_name = prop_def["name"] if isinstance(prop_def, dict) and "name" in prop_def else prop_def
                            if prop_name != "id" and prop_name != "data_model":
                                record[prop_name] = finding
                        records.append(record)
                self.logger.debug(f"Prepared {len(records)} records for node type {node_label}")
                return records
            else:
                # Default: just return entities as-is (existing logic)
                for entity in entities:
                    if entity.get("type") == node_label:
                        record = {"data_model": data_model}
                        properties = entity.get("properties", {})
                        expected_properties = []
                        for prop_def in node.get("properties", []):
                            if isinstance(prop_def, dict) and "name" in prop_def:
                                expected_properties.append(prop_def["name"])
                            elif isinstance(prop_def, str):
                                expected_properties.append(prop_def)
                        for prop_name in expected_properties:
                            sanitized = self._sanitize_property_name(prop_name)
                            if prop_name in properties:
                                record[sanitized] = properties[prop_name]
                            else:
                                record[sanitized] = None
                        for prop_name, prop_value in properties.items():
                            sanitized = self._sanitize_property_name(prop_name)
                            if sanitized not in record:
                                record[sanitized] = prop_value
                        records.append(record)
                self.logger.debug(f"Prepared {len(records)} records for node type {node_label}")
                return records
        except Exception as e:
            self.logger.error(f"Failed to prepare node records: {str(e)}")
            return []

    def _prepare_relationship_records(self, relationship: Dict[str, Any], relationships: List[Dict[str, Any]], entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Prepare relationship records for MCP Cypher ingestion with correct parameter format (sourceId, targetId, properties flat, sanitized property names).
        Supports one-to-many relationships and logs skipped records with reasons.
        Validates that relationship IDs reference actual created entities.
        """
        try:
            rel_type = relationship.get("type")
            start_label = relationship.get("start_node_label")
            end_label = relationship.get("end_node_label")
            cardinality = relationship.get("cardinality", "1:1")
            records = []
            skipped = []
            
            # Build lookup of available entity IDs from the passed entities parameter
            available_entity_ids = set()
            
            # Get entities from the passed entities parameter (most reliable source)
            for entity in entities:
                entity_id = entity.get("properties", {}).get("id")
                if entity_id:
                    available_entity_ids.add(str(entity_id))
                    
            # Fallback: also try to get from _current_entities if available
            if not available_entity_ids and hasattr(self, '_current_entities'):
                for entity in self._current_entities:
                    entity_id = entity.get("properties", {}).get("id")
                    if entity_id:
                        available_entity_ids.add(str(entity_id))
            
            # Filter relationships to only those matching this relationship type
            matching_relationships = []
            for rel in relationships:
                if rel.get("type") == rel_type:
                    matching_relationships.append(rel)

            self.logger.debug(f"Found {len(matching_relationships)} relationships of type {rel_type}")
            print(f"[DEBUG] Found {len(matching_relationships)} relationships of type {rel_type}")

            # Debug logging to understand the ID mismatch
            self.logger.debug(f"Available entity IDs: {len(available_entity_ids)} total")
            if available_entity_ids:
                sample_ids = list(available_entity_ids)[:10]
                self.logger.debug(f"Sample entity IDs: {sample_ids}")
                print(f"[DEBUG] Sample available entity IDs: {sample_ids}")
            else:
                print(f"[DEBUG] NO entity IDs found in available_entity_ids set!")
                print(f"[DEBUG] _current_entities exists: {hasattr(self, '_current_entities')}")
                if hasattr(self, '_current_entities'):
                    print(f"[DEBUG] _current_entities count: {len(self._current_entities)}")
                    if self._current_entities:
                        sample_entity = self._current_entities[0]
                        print(f"[DEBUG] Sample entity structure: {sample_entity}")
                        entity_id = sample_entity.get("properties", {}).get("id")
                        print(f"[DEBUG] Sample entity ID: {entity_id}")
            
            # Log relationship source/target patterns
            if matching_relationships:
                rel_sources = [r.get('from') for r in matching_relationships[:5]]
                rel_targets = [r.get('to') for r in matching_relationships[:5]]
                print(f"[DEBUG] Sample relationship sources: {rel_sources}")
                print(f"[DEBUG] Sample relationship targets: {rel_targets}")

            if matching_relationships:
                sample_rel = matching_relationships[0]
                print(f"[DEBUG] Sample relationship: from='{sample_rel.get('from')}', to='{sample_rel.get('to')}'")
            
            # Process each matching relationship instance
            for rel in matching_relationships:
                src = rel.get("from")
                tgt = rel.get("to") 
                
                if not src or not tgt:
                    skipped.append({"reason": "missing from/to fields", "record": rel})
                    continue
                
                # Validate that source and target IDs exist in created entities
                src_str = str(src)
                tgt_str = str(tgt)
                
                if src_str not in available_entity_ids:
                    skipped.append({"reason": f"source ID '{src}' not found in created entities", "record": rel})
                    continue
                    
                if tgt_str not in available_entity_ids:
                    skipped.append({"reason": f"target ID '{tgt}' not found in created entities", "record": rel})
                    continue
                
                # Valid relationship - create record
                records.append({
                    "sourceId": src_str,
                    "targetId": tgt_str,
                    "data_model": "auto"
                })
            
            # Log skipped records for diagnosis
            if skipped:
                self.logger.warning(f"Skipped {len(skipped)} relationships for {rel_type}: {skipped[:3]}...")  # Only show first 3
            
            self.logger.info(f"Prepared {len(records)} relationship records for {rel_type} (cardinality: {cardinality})")
            return records
            
        except Exception as e:
            self.logger.error(f"Failed to prepare relationship records for {relationship.get('type')}: {str(e)}")
            return []

    def make_mcp_request(self, server_url: str, tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make request to MCP server using JSON-RPC protocol.
        
        Args:
            server_url: MCP server URL
            tool_name: Name of the MCP tool to call
            params: Parameters for the tool
            
        Returns:
            Dict with success status and result/error
        """
        try:
            # Construct JSON-RPC request
            request_payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "arguments": params
                }
            }
            
            # Log the request payload to console and file
            self.log_mcp_request(server_url, tool_name, request_payload, params)
            
            # Make HTTP request to MCP server
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream"
            }
            
            response = requests.post(
                f"{server_url}/mcp/",
                json=request_payload,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                
                if 'text/event-stream' in content_type:
                    # Handle SSE response
                    result = self._parse_sse_response(response.text)
                    # Log the response
                    self.log_mcp_response(server_url, tool_name, result, response.status_code, response.text[:500])
                    return result
                else:
                    # Handle JSON response
                    try:
                        result_data = response.json()
                        
                        if "error" in result_data:
                            result = {
                                "success": False,
                                "error": result_data["error"].get("message", "Unknown MCP error")
                            }
                        elif "result" in result_data:
                            result = {
                                "success": True,
                                "result": result_data.get("result", {})
                            }
                        else:
                            result = {
                                "success": True,
                                "result": result_data
                            }
                        
                        # Log the response
                        self.log_mcp_response(server_url, tool_name, result, response.status_code)
                        return result
                        
                    except json.JSONDecodeError:
                        # Try SSE parsing as fallback
                        result = self._parse_sse_response(response.text)
                        # Log the response
                        self.log_mcp_response(server_url, tool_name, result, response.status_code, response.text[:500])
                        return result
            else:
                result = {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text}"
                }
                # Log the error response
                self.log_mcp_response(server_url, tool_name, result, response.status_code, response.text[:500])
                return result
                
        except requests.RequestException as e:
            self.logger.error(f"MCP request failed: {str(e)}")
            result = {
                "success": False,
                "error": f"Request failed: {str(e)}"
            }
            # Log the exception
            self.log_mcp_response(server_url, tool_name, result, None, f"RequestException: {str(e)}")
            return result
        except Exception as e:
            self.logger.error(f"Unexpected error in MCP request: {str(e)}")
            result = {
                "success": False,
                "error": f"Unexpected error: {str(e)}"
            }
            # Log the exception
            self.log_mcp_response(server_url, tool_name, result, None, f"Exception: {str(e)}")
            return result

    def _parse_sse_response(self, response_text: str) -> Dict[str, Any]:
        """Parse Server-Sent Events response format from MCP servers."""
        try:
            lines = response_text.strip().split('\n')
            
            for line in lines:
                if line.startswith('data: '):
                    json_str = line[6:]
                    if json_str.strip() == '[DONE]':
                        continue
                    try:
                        parsed_data = json.loads(json_str)
                        
                        if "result" in parsed_data:
                            return {"success": True, "result": parsed_data["result"]}
                        elif "error" in parsed_data:
                            error_details = parsed_data["error"]
                            error_msg = f"MCP Error {error_details.get('code', 'unknown')}: {error_details.get('message', 'No message')}"
                            return {"success": False, "error": error_msg}
                        else:
                            return {"success": True, "result": parsed_data}
                            
                    except json.JSONDecodeError:
                        continue
            
            # Try direct JSON parse as fallback
            try:
                parsed_data = json.loads(response_text)
                if "result" in parsed_data:
                    return {"success": True, "result": parsed_data["result"]}
                elif "error" in parsed_data:
                    error_details = parsed_data["error"]
                    error_msg = f"MCP Error {error_details.get('code', 'unknown')}: {error_details.get('message', 'No message')}"
                    return {"success": False, "error": error_msg}
                else:
                    return {"success": True, "result": parsed_data}
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decode error: {str(e)}, Response: {response_text[:500]}")
                return {"success": False, "error": f"Failed to parse JSON response: {str(e)}"}
            
            return {"success": False, "error": f"Failed to parse response: {response_text[:200]}"}
            
        except Exception as e:
            return {"success": False, "error": f"Failed to parse response: {str(e)}"}

    def log_mcp_summary(self):
        """Log a summary of all MCP requests to console and file."""
        try:
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            total_requests = self.processing_stats["mcp_requests_made"]
            successful_requests = self.processing_stats["mcp_requests_successful"]
            failed_requests = self.processing_stats["mcp_requests_failed"]
            success_rate = (successful_requests / total_requests * 100) if total_requests > 0 else 0
            
            summary = f"""
[{timestamp}] MCP API REQUEST SUMMARY
{'=' * 80}
Total MCP Requests Made: {total_requests}
Successful Requests: {successful_requests}
Failed Requests: {failed_requests}
Success Rate: {success_rate:.1f}%

MCP Server Endpoints Used:
- Data Modeling Server: {getattr(self, 'data_modeling_server_url', 'Not configured')}
- Cypher Server: {getattr(self, 'cypher_server_url', 'Not configured')}

Log File Location: {getattr(self, 'mcp_log_file', 'Not configured')}
{'=' * 80}
"""
            
            # Log to console
            print(f"\n{summary}")
            
            # Log to file if available
            if getattr(self, 'mcp_log_file', None):
                with open(self.mcp_log_file, 'a', encoding='utf-8') as f:
                    f.write(summary + "\n")
            
            self.logger.info(f"MCP Summary: {total_requests} requests, {success_rate:.1f}% success rate")
            
        except Exception as e:
            self.logger.error(f"Failed to log MCP summary: {str(e)}")

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get current processing statistics."""
        return self.processing_stats.copy()

    def reset_processing_stats(self):
        """Reset processing statistics."""
        self.initialize_processing_stats()


class RobustKGParser:
    def __init__(self):
        self.entity_types = []
        self.relationship_types = []
        self.entities = []
        self.relationships = []

    def parse(self, text):
        section = None
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            # Section detection
            if re.match(r"=+ EXTRACTED ENTITY LIST =+", line):
                section = "entity_types"
                continue
            elif re.match(r"=+ EXTRACTED RELATIONSHIP LIST =+", line):
                section = "relationship_types"
                continue
            elif re.match(r"=+ DATA INSTANCES =+", line):
                section = "entities"
                continue
            elif re.match(r"=+ RELATIONSHIP INSTANCES =+", line):
                section = "relationships"
                continue

            # Entity type parsing
            if section == "entity_types" and "Entity Type:" in line:
                etype = line.split(":", 1)[1].strip()
                self.entity_types.append({"type": etype})
            elif section == "entity_types" and "Properties:" in line:
                props = [p.strip() for p in line.split(":", 1)[1].split(",")]
                if self.entity_types:
                    self.entity_types[-1]["properties"] = props
            elif section == "entity_types" and "Key Property:" in line:
                key = line.split(":", 1)[1].strip()
                if self.entity_types:
                    self.entity_types[-1]["key_property"] = key

            # Relationship type parsing
            elif section == "relationship_types" and "Relationship Type:" in line:
                rtype = line.split(":", 1)[1].strip()
                self.relationship_types.append({"type": rtype})
            elif section == "relationship_types" and "Start Entity:" in line:
                start = line.split(":", 1)[1].strip()
                if self.relationship_types:
                    self.relationship_types[-1]["start_entity"] = start
            elif section == "relationship_types" and "End Entity:" in line:
                end = line.split(":", 1)[1].strip()
                if self.relationship_types:
                    self.relationship_types[-1]["end_entity"] = end
            elif section == "relationship_types" and "Cardinality:" in line:
                card = line.split(":", 1)[1].strip()
                if self.relationship_types:
                    self.relationship_types[-1]["cardinality"] = card

            # Entity instance parsing
            elif section == "entities" and "|" in line:
                parts = line.split("|")
                if len(parts) >= 3:
                    etype, eid, props_str = parts[0], parts[1], parts[2]
                    props = {}
                    for p in props_str.split(","):
                        if "=" in p:
                            k, v = p.split("=", 1)
                            props[k.strip()] = v.strip()
                    props["id"] = eid
                    self.entities.append({"type": etype, "id": eid, "properties": props})

            # Relationship instance parsing
            elif section == "relationships":
                parts = line.split()
                if len(parts) >= 5:
                    self.relationships.append({
                        "type": parts[2],
                        "from": parts[1],
                        "to": parts[4],
                        "start_node_label": parts[0],
                        "end_node_label": parts[3]
                    })

    def get_result(self):
        return {
            "entity_types": self.entity_types,
            "relationship_types": self.relationship_types,
            "entities": self.entities,
            "relationships": self.relationships
        }
# ...existing code...

# Usage Example and Main Function
def main():
    """
    Example usage of the GraphIngestionTool
    """
    # Initialize the tool
    tool = GraphIngestionTool()
    

    # Generic document ingestion: parse CSV or text, synthesize key property, format for LLM
    # doc_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "doc", "Data_Entry_2017.csv")
    doc_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "doc", "Data_Entry_2017-small.csv")
    summary_blocks = []
    try:
        with open(doc_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            header = [h.strip() for h in lines[0].strip().split(",")]
            for idx, line in enumerate(lines[1:], 1):
                parts = [p.strip() for p in line.strip().split(",")]
                entity = {header[i]: parts[i] for i in range(min(len(header), len(parts)))}
                # Always add a synthetic key property if not present
                if "id" not in entity:
                    entity["id"] = f"row_{idx}"  # Unique per row
                block = f"Entity {idx}:\n" + json.dumps(entity, indent=2)
                summary_blocks.append(block)
        formatted_content = "\n\n".join(summary_blocks)
    except Exception as e:
        print(f"Error reading document: {e}")
        formatted_content = ""


    # CHUNKING LOGIC COMMENTED OUT - Process all data at once instead
    # chunk_size = 15  # Reduced from 25 to improve LLM success rate
    # total_chunks = min(2, (len(summary_blocks) + chunk_size - 1) // chunk_size)  # Process only 2 chunks for testing
    # all_results = []
    # print(f"FINAL TEST: Processing Data_Entry_2017.csv for ingestion in {total_chunks} chunks (size: {chunk_size})...")

    # for i in range(0, min(2 * chunk_size, len(summary_blocks)), chunk_size):
    #     chunk_blocks = summary_blocks[i:i+chunk_size]
    #     chunk_content = "\n".join(chunk_blocks)
    #     chunk_num = i//chunk_size+1
    #     print(f"\n--- Processing chunk {chunk_num}/{total_chunks} ---")
    #     # Minimal logging for performance
    #     print(f"  Chunk size: {len(chunk_blocks)} entities")
    #     result = tool.ingest_content_with_schema_validation(chunk_content)
    #     all_results.append(result)
    #     print(f"  Success: {result.get('success', False)} | Entities: {result.get('entities_created', 0)} | Relationships: {result.get('relationships_created', 0)} | Confidence: {result.get('schema_confidence', 0.0):.2f}")
    #     # Only show errors, not full debug details
    #     if not result.get('success', True):
    #         print(f"  Error: {result.get('error', 'Unknown error')}")

    # PROCESS ALL DATA IN CHUNKS: Process entire dataset in manageable chunks
    print(f"Processing ENTIRE Data_Entry_2017.csv dataset in chunks...")
    print(f"Total entities to process: {len(summary_blocks)}")
    
    # Process in chunks to handle large dataset efficiently
    chunk_size = 500  # Process 500 entities at a time (reduced for better LLM handling)
    total_chunks = (len(summary_blocks) + chunk_size - 1) // chunk_size
    all_results = []
    total_entities_created = 0
    total_relationships_created = 0
    
    print(f"Processing {len(summary_blocks)} entities in {total_chunks} chunks of {chunk_size}...")
    
    for chunk_idx in range(total_chunks):
        start_idx = chunk_idx * chunk_size
        end_idx = min(start_idx + chunk_size, len(summary_blocks))
        chunk_blocks = summary_blocks[start_idx:end_idx]
        chunk_content = "\n\n".join(chunk_blocks)
        
        print(f"\n--- Processing chunk {chunk_idx + 1}/{total_chunks} (entities {start_idx + 1}-{end_idx}) ---")
        
        try:
            result = tool.ingest_content_with_schema_validation(chunk_content)
            all_results.append(result)
            
            # Accumulate results
            entities_created = result.get('entities_created', 0)
            relationships_created = result.get('relationships_created', 0)
            total_entities_created += entities_created
            total_relationships_created += relationships_created
            
            print(f"  Chunk Success: {result.get('success', False)}")
            print(f"  Entities Created: {entities_created}")
            print(f"  Relationships Created: {relationships_created}")
            
            if not result.get('success', False):
                print(f"  Error: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"  Chunk {chunk_idx + 1} failed with exception: {str(e)}")
            all_results.append({
                'success': False, 
                'error': str(e),
                'entities_created': 0,
                'relationships_created': 0
            })
    
    # Create summary result
    result = {
        'success': all(r.get('success', False) for r in all_results),
        'entities_created': total_entities_created,
        'relationships_created': total_relationships_created,
        'schema_confidence': sum(r.get('schema_confidence', 0.0) for r in all_results) / len(all_results) if all_results else 0.0,
        'chunks_processed': len(all_results)
    }
    
    print(f"\n--- COMPLETE DATASET PROCESSING RESULT ---")
    print(f"Success: {result.get('success', False)}")
    print(f"Total Entities Created: {result.get('entities_created', 0)}")
    print(f"Total Relationships Created: {result.get('relationships_created', 0)}")
    print(f"Chunks Processed: {result.get('chunks_processed', 0)}")
    print(f"Average Schema Confidence: {result.get('schema_confidence', 0.0):.2f}")
    
    if not result.get('success', True):
        print(f"Error: {result.get('error', 'Unknown error')}")
        
    # Set results for summary
    total_entities = result.get('entities_created', 0)
    total_relationships = result.get('relationships_created', 0)
    avg_confidence = result.get('schema_confidence', 0.0)

    print("\n=== CHUNKED BATCH INGESTION SUMMARY ===")
    print(f"Processing Mode: Chunked processing ({chunk_size} entities per chunk)")
    print(f"Total Entities Created: {total_entities}")
    print(f"Total Relationships Created: {total_relationships}")
    print(f"Schema Confidence: {avg_confidence:.2f}")
    print(f"Dataset Size: {len(summary_blocks)} entities processed in {total_chunks} chunks")
    print(f"Successful Chunks: {sum(1 for r in all_results if r.get('success', False))}/{len(all_results)}")

    # Display error if any exist
    if not result.get('success', True):
        print(f"\nError: {result.get('error', 'Unknown error')}")
    else:
        print("\nSuccess - entire dataset processed successfully!")

    # Display MCP API request summary
    print("\n" + "="*80)
    print("MCP API REQUESTS SUMMARY")
    print("="*80)
    tool.log_mcp_summary()


if __name__ == "__main__":
    main()
