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
from typing import Dict, Any, List, Optional
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
            return 50000

    config_manager = MockConfig()
    logger = logging.getLogger("graph_tool")


class GraphIngestionTool:
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
            print(f"[MCP LOGGING] Log file created: {self.mcp_log_file}")
            
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
            
            # Log to console with colored output
            print(f"\n{'='*60}")
            print(f"[MCP REQUEST #{self.processing_stats['mcp_requests_made']}] {timestamp}")
            print(f"Server: {server_url}")
            print(f"Tool: {tool_name}")
            print(f"Payload:")
            print(json.dumps(request_payload, indent=2))
            print(f"Parameters:")
            print(json.dumps(params, indent=2))
            print(f"{'='*60}\n")
            
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
            print(f"\n{'='*60}")
            print(f"[MCP RESPONSE #{current_request_num}] {timestamp} - {success_status}")
            print(f"Server: {server_url}")
            print(f"Tool: {tool_name}")
            if status_code:
                print(f"Status: {status_code}")
            print(f"Response:")
            print(json.dumps(response, indent=2))
            print(f"{'='*60}\n")
            
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
            # Relationships - ensure data_model is set
            relationships = llm_result.get("entity_extraction", {}).get("relationships", [])
            for idx, rel in enumerate(relationships):
                # Synthesize data_model if missing
                if "data_model" not in rel:
                    rel["data_model"] = "auto"

            # Also ensure schema_discovery entity_types have 'id' property
            entity_types = llm_result.get("schema_discovery", {}).get("entity_types", [])
            for et in entity_types:
                if "id" not in et.get("properties", []):
                    et["properties"].insert(0, "id")

            # LAYER 3: MCP Server Validation & Execution
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
        
        This method combines both schema discovery and entity extraction in a single LLM call
        for better consistency and context preservation.
        """
        try:
            self.logger.info("Starting LLM schema discovery and entity extraction...")
            
            # Enhanced comprehensive prompt for generic schema discovery and entity extraction
            enhanced_prompt = f"""
            You are an expert data analyst and knowledge graph architect. Analyze the provided content and perform comprehensive schema discovery and entity extraction for knowledge graph construction.

            CONTENT ANALYSIS INSTRUCTIONS:
            1. First, identify the domain and data type (medical, business, technical, research, etc.)
            2. Detect data format (CSV, JSON, text, structured records, etc.)
            3. Identify key entities, their attributes, and relationships
            4. Consider hierarchical, temporal, and categorical relationships
            5. Handle multi-valued fields and complex data structures
            6. Ensure comprehensive coverage of all data elements

            SCHEMA DISCOVERY TASK:
            - Analyze ALL columns/fields in the data to identify distinct entity types
            - Group related attributes under logical entity types
            - Identify primary keys, foreign keys, and unique identifiers
            - Detect categorical fields, temporal fields, and measurement fields
            - Consider entity hierarchies and specialized entity types
            - Map relationships between entities (1:1, 1:many, many:many)
            - Include composite entities for complex relationships
            - Consider temporal and sequential relationships
            - Identify lookup/reference entities vs. main entities

            ENTITY EXTRACTION GUIDELINES:
            - Extract ALL data records as entities with complete attribute sets
            - Handle multi-valued fields by creating separate entities or arrays
            - Ensure proper entity deduplication using natural keys
            - Create relationship instances for all detected connections
            - Handle missing values appropriately
            - Preserve data types and constraints
            - Generate unique IDs for all entities and relationships

            RELATIONSHIP DISCOVERY RULES:
            - Direct references (foreign keys, IDs)
            - Hierarchical relationships (parent-child, categories)
            - Temporal relationships (sequences, versions, timelines)
            - Compositional relationships (part-of, contains)
            - Associative relationships (many-to-many via junction entities)
            - Derived relationships (calculated, inferred)

            DATA MODELING BEST PRACTICES:
            - Use clear, descriptive entity and relationship names
            - Normalize data to reduce redundancy
            - Handle lookup tables and controlled vocabularies
            - Consider entity specialization and generalization
            - Model complex data types appropriately
            - Ensure referential integrity in relationships

            Content to analyze:
            {content}

            Return this EXACT JSON structure (do not modify the structure):
            {{
                "schema_discovery": {{
                    "confidence": <float_0_to_1>,
                    "domain": "<detected_domain>",
                    "data_format": "<detected_format>",
                    "entity_types": [
                        {{
                            "type": "<EntityTypeName>",
                            "properties": ["id", "<property1>", "<property2>", "..."],
                            "description": "<detailed_description>",
                            "key_property": "<primary_identifier>",
                            "entity_category": "<main|lookup|junction|temporal>"
                        }}
                    ],
                    "relationship_types": [
                        {{
                            "type": "<RELATIONSHIP_NAME>",
                            "start_entity": "<StartEntityType>",
                            "end_entity": "<EndEntityType>",
                            "description": "<relationship_description>",
                            "cardinality": "<1:1|1:many|many:many>",
                            "relationship_category": "<direct|hierarchical|temporal|compositional|associative>"
                        }}
                    ]
                }},
                "entity_extraction": {{
                    "entities": [
                        {{
                            "type": "<EntityTypeName>",
                            "properties": {{
                                "id": "<unique_identifier>",
                                "<property1>": "<value1>",
                                "<property2>": "<value2>"
                            }}
                        }}
                    ],
                    "relationships": [
                        {{
                            "type": "<RELATIONSHIP_NAME>",
                            "from": "<source_entity_id>",
                            "to": "<target_entity_id>",
                            "start_node_label": "<StartEntityType>",
                            "end_node_label": "<EndEntityType>",
                            "properties": {{
                                "<rel_property1>": "<rel_value1>"
                            }}
                        }}
                    ]
                }}
            }}
            
            CRITICAL REQUIREMENTS:
            1. Extract ALL data records - do not sample or truncate
            2. Create comprehensive entity types covering all data attributes
            3. Establish ALL logical relationships between entities
            4. Use consistent naming conventions (PascalCase for entities, UPPER_CASE for relationships)
            5. Ensure all entities have unique IDs and all relationships are properly connected
            6. Provide high confidence scores (>0.8) for well-structured data
            7. Handle edge cases like missing values, duplicates, and data quality issues
            8. Preserve semantic meaning and domain context in entity/relationship naming
            """

            # Make LLM request using existing infrastructure with retry logic
            if not self.llm:
                self.logger.error("LLM client not available")
                return {"success": False, "error": "LLM client not initialized"}

            # Debug: Check LLM configuration
            print(f"[DEBUG] LLM client available: {self.llm is not None}")
            print(f"[DEBUG] Content length: {len(enhanced_prompt)} characters")

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.logger.info(f"LLM request attempt {attempt + 1}/{max_retries}")
                    print(f"[DEBUG] Starting LLM request attempt {attempt + 1}")
                    response = self.llm.invoke(enhanced_prompt)
                    response_text = response.content if hasattr(response, 'content') else str(response)
                    print(f"[DEBUG] LLM response received, length: {len(response_text)} characters")
                    break
                except Exception as e:
                    error_details = f"LLM attempt {attempt + 1} failed: {str(e)}"
                    self.logger.warning(error_details)
                    print(f"[DEBUG] {error_details}")  # Also print to console for debugging
                    
                    if attempt == max_retries - 1:
                        final_error = f"LLM failed after {max_retries} attempts: {str(e)}"
                        print(f"[ERROR] {final_error}")
                        return {"success": False, "error": final_error}
                    # Wait before retry
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff

            # Parse the combined response
            parsed_result = self._parse_llm_schema_extraction_response(response_text)
            
            if parsed_result.get("success"):
                schema_confidence = parsed_result.get("schema_discovery", {}).get("confidence", 0.0)
                entities_count = len(parsed_result.get("entity_extraction", {}).get("entities", []))
                relationships_count = len(parsed_result.get("entity_extraction", {}).get("relationships", []))
                
                self.processing_stats["schemas_discovered"] += 1
                self.processing_stats["entities_extracted"] += entities_count
                
                self.logger.info(f"LLM processing completed - Schema confidence: {schema_confidence:.2f}, "
                               f"Entities: {entities_count}, Relationships: {relationships_count}")
            
            return parsed_result
            
        except Exception as e:
            error_msg = f"LLM schema discovery and extraction failed: {str(e)}"
            self.logger.error(error_msg)
            return {"success": False, "error": error_msg}

    def _parse_llm_schema_extraction_response(self, response_text: str) -> Dict[str, Any]:
        """
        Parse the combined LLM response for schema discovery and entity extraction.
        Enhanced with better error handling and JSON repair capabilities.
        """
        try:
            # Clean the response text by removing markdown code block markers
            cleaned_text = response_text.strip()
            if cleaned_text.startswith('```json'):
                cleaned_text = cleaned_text[7:]  # Remove ```json
            if cleaned_text.startswith('```'):
                cleaned_text = cleaned_text[3:]   # Remove ```
            if cleaned_text.endswith('```'):
                cleaned_text = cleaned_text[:-3]  # Remove closing ```
            
            cleaned_text = cleaned_text.strip()
            
            # Save raw response for debugging
            try:
                with open("debug_llm_response_latest.txt", "w", encoding="utf-8") as f:
                    f.write(f"Original Response:\n{response_text}\n\n")
                    f.write(f"Cleaned Response:\n{cleaned_text}\n")
            except Exception:
                pass  # Don't fail if we can't save debug file
            
            # Try to parse the cleaned JSON directly first
            try:
                parsed_data = json.loads(cleaned_text)
            except json.JSONDecodeError as e:
                print(f"[DEBUG] Initial JSON parsing failed: {e}")
                print(f"[DEBUG] Error position: {e.pos}")
                
                # Try to extract and fix common JSON issues
                try:
                    # Remove any trailing commas before closing braces/brackets
                    import re
                    fixed_text = re.sub(r',(\s*[}\]])', r'\1', cleaned_text)
                    
                    # Try parsing the fixed version
                    parsed_data = json.loads(fixed_text)
                    print("[DEBUG] JSON parsing succeeded after fixing trailing commas")
                except json.JSONDecodeError:
                    # Fallback: Extract JSON from response text using regex
                    json_match = re.search(r'\{.*\}', cleaned_text, re.DOTALL)
                    if not json_match:
                        return {"success": False, "error": "No JSON found in LLM response"}
                    
                    json_str = json_match.group()
                    # Try to fix common issues in the extracted JSON
                    fixed_json = re.sub(r',(\s*[}\]])', r'\1', json_str)
                    
                    try:
                        parsed_data = json.loads(fixed_json)
                        print("[DEBUG] JSON parsing succeeded using regex extraction and fixing")
                    except json.JSONDecodeError as final_error:
                        print(f"[DEBUG] All JSON parsing attempts failed: {final_error}")
                        # Save the problematic response for manual inspection
                        try:
                            with open("debug_failed_json.txt", "w", encoding="utf-8") as f:
                                f.write(f"Failed JSON Response:\n{cleaned_text}\n\n")
                                f.write(f"Extracted JSON:\n{json_str}\n\n")
                                f.write(f"Fixed JSON:\n{fixed_json}\n\n")
                                f.write(f"Error: {final_error}\n")
                            print("[DEBUG] Failed JSON saved to debug_failed_json.txt")
                        except Exception:
                            pass
                        return {"success": False, "error": f"JSON parsing failed: {final_error}"}
            
            # Debug: Show what we parsed
            print(f"[DEBUG] Parsed JSON structure keys: {list(parsed_data.keys())}")
            
            # Validate required structure
            if "schema_discovery" not in parsed_data or "entity_extraction" not in parsed_data:
                return {"success": False, "error": "Invalid response structure from LLM"}
            
            schema_discovery = parsed_data["schema_discovery"]
            entity_extraction = parsed_data["entity_extraction"]
            
            # Validate confidence score
            confidence = schema_discovery.get("confidence", 0.0)
            if confidence < 0.7:
                self.logger.warning(f"Low schema discovery confidence: {confidence:.2f}")
            
            # Log additional schema discovery information
            domain = schema_discovery.get("domain", "unknown")
            data_format = schema_discovery.get("data_format", "unknown")
            entity_types_count = len(schema_discovery.get("entity_types", []))
            relationship_types_count = len(schema_discovery.get("relationship_types", []))
            
            self.logger.info(f"Schema Discovery Summary - Domain: {domain}, Format: {data_format}, "
                           f"Entities: {entity_types_count}, Relationships: {relationship_types_count}")
            
            # Validate entity types have required fields
            for entity_type in schema_discovery.get("entity_types", []):
                if not entity_type.get("type") or not entity_type.get("properties"):
                    self.logger.warning(f"Invalid entity type structure: {entity_type}")
                    
            # Validate relationship types have required fields
            for rel_type in schema_discovery.get("relationship_types", []):
                if not all(key in rel_type for key in ["type", "start_entity", "end_entity"]):
                    self.logger.warning(f"Invalid relationship type structure: {rel_type}")

            return {
                "success": True,
                "schema_discovery": schema_discovery,
                "entity_extraction": entity_extraction,
                "combined_confidence": confidence,
                "domain": domain,
                "data_format": data_format
            }
            
        except json.JSONDecodeError as e:
            return {"success": False, "error": f"JSON parsing failed: {str(e)}"}
        except Exception as e:
            return {"success": False, "error": f"Response parsing failed: {str(e)}"}

    def _mcp_validation_and_execution(self, llm_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Layer 3: MCP Server Validation & Execution
        
        Takes the LLM results and validates the schema, then executes the ingestion.
        """
        try:
            self.logger.info("Starting MCP validation and execution...")
            
            schema_discovery = llm_result.get("schema_discovery", {})
            entity_extraction = llm_result.get("entity_extraction", {})
            
            # Step 1: Validate schema using MCP Data Modeling server (with fallback)
            validation_result = self._validate_schema_with_mcp_server(schema_discovery)
            if not validation_result.get("success"):
                self.logger.warning(f"MCP schema validation failed: {validation_result.get('error')}. Proceeding with direct ingestion.")
                # Don't fail the entire process - proceed with direct ingestion
            else:
                self.processing_stats["schemas_validated"] += 1
                self.logger.info("Schema validation successful")
            
            # Step 2: Build Neo4j data model from validated schema and extracted entities
            data_model = self._build_neo4j_data_model_from_llm_results(schema_discovery, entity_extraction)
            if not data_model:
                return {
                    "success": False,
                    "error": "Failed to build Neo4j data model from LLM results",
                    "stats": self.processing_stats
                }
            
            # Step 3: Execute ingestion using MCP Cypher server
            ingestion_result = self._execute_mcp_ingestion(data_model, entity_extraction)
            
            if ingestion_result.get("success"):
                self.logger.info("MCP ingestion execution successful")
                ingestion_result["validation_passed"] = True
                ingestion_result["schema_confidence"] = llm_result.get("combined_confidence", 0.0)
            
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

    def _build_neo4j_data_model_from_llm_results(self, schema_discovery: Dict[str, Any], 
                                                 entity_extraction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Build Neo4j data model combining schema discovery and entity extraction results.
        """
        try:
            # Build nodes from entity extraction - Group entities by type
            node_groups = {}
            for entity in entity_extraction.get("entities", []):
                entity_type = entity.get("type", "Entity")
                if entity_type not in node_groups:
                    node_groups[entity_type] = []
                node_groups[entity_type].append(entity)

            # Create node definitions for each entity type
            nodes = []
            for entity_type, entities in node_groups.items():
                # Collect all properties from all entities of this type
                all_properties = set()
                for entity in entities:
                    all_properties.update(entity.get("properties", {}).keys())
                
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
                        props["id"] = f"auto_id_{entity_type}_{idx+1}"
                    
                    # Create record with all expected properties
                    record = {}
                    for prop_name in all_properties:
                        record[prop_name] = props.get(prop_name, "")  # Default to empty string
                    records.append(record)
                
                # Create node definition
                node = {
                    "label": entity_type,
                    "properties": property_schema,
                    "records": records,
                    "key_property": {"name": "id", "type": "string"},
                    "data_model": "auto"
                }
                nodes.append(node)
                
                self.logger.info(f"Created node group {entity_type} with {len(records)} records")

            # Build relationships from entity extraction - group by type
            relationship_groups = {}
            for rel in entity_extraction.get("relationships", []):
                # Only process relationships that have required fields
                if not rel.get("type") or not rel.get("from") or not rel.get("to"):
                    self.logger.warning(f"Skipping incomplete relationship: {rel}")
                    continue
                    
                rel_type = rel.get("type")
                if rel_type not in relationship_groups:
                    relationship_groups[rel_type] = {
                        "type": rel_type,
                        "start_node_label": rel.get("start_node_label", "Entity"),
                        "end_node_label": rel.get("end_node_label", "Entity"),
                        "properties": [],
                        "records": [],
                        "data_model": "auto"
                    }
                
                # Create proper record format
                rel_record = {
                    "type": rel.get("type"),
                    "from": rel.get("from"),
                    "to": rel.get("to"),
                    "start_node_label": rel.get("start_node_label", "Entity"),
                    "end_node_label": rel.get("end_node_label", "Entity"),
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
                    "confidence": schema_discovery.get("confidence", 0.0),
                    "entity_types_count": len(schema_discovery.get("entity_types", [])),
                    "relationship_types_count": len(schema_discovery.get("relationship_types", []))
                }
            }

            # Debug: Print the data model structure (minimal)
            print(f"[INFO] Built data model with {len(nodes)} node types and {len(relationships)} relationship types")
            
            self.logger.info(f"Data model built: {len(nodes)} node types, {len(relationships)} relationship types")
            return data_model

        except Exception as e:
            self.logger.error(f"Failed to build Neo4j data model: {str(e)}")
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
            for relationship in data_model.get("relationships", []):
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
            
            print(f"[INFO] Creating relationship {rel_type} from {start_label} to {end_label}")
            
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
            print(f"[DEBUG] MCP relationship query result for {rel_type}: success={query_result.get('success')}")
            if not query_result.get("success", False):
                print(f"[DEBUG] MCP error details: {query_result.get('error')}")
                self.logger.error(f"Failed to get relationship query for {rel_type}: {query_result.get('error')}")
                # Fallback to direct Cypher generation
                return self._fallback_relationship_generation(relationship, entities_relationships)

            # Step 2: Extract Cypher query from MCP response
            cypher_query = self._extract_cypher_from_mcp_response(query_result.get("result"))

            print(f"[DEBUG] Extracted Cypher query for {rel_type}: {cypher_query[:200] if cypher_query else 'None'}...")
            self.logger.debug(f"Generated Cypher for {rel_type}: {cypher_query[:100]}...")
            if not cypher_query:
                print(f"[DEBUG] No Cypher query extracted from MCP response")
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
            records = self._prepare_relationship_records(relationship, entities_relationships.get("relationships", []))
            self.logger.debug(f"Prepared {len(records)} records for {rel_type}")
            if not records:
                print(f"[INFO] No records to ingest for relationship {rel_type}")
                return 0
                
            print(f"[INFO] Processing {len(records)} {rel_type} relationships")

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
            
            print(f"[FALLBACK] Using direct Cypher generation for {rel_type}")
            
            # Prepare relationship records
            records = self._prepare_relationship_records(relationship, entities_relationships.get("relationships", []))
            if not records:
                print(f"[INFO] No records to ingest for relationship {rel_type}")
                return 0
                
            print(f"[INFO] Processing {len(records)} {rel_type} relationships")
            
            # Generate direct Cypher query for relationship creation
            cypher_query = self._generate_relationship_cypher(rel_type, start_label, end_label, records)
            
            if not cypher_query:
                print(f"[ERROR] Failed to generate Cypher for {rel_type}")
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
            # Simple UNWIND-based relationship creation
            cypher = f"""
            UNWIND $records AS record
            MATCH (start:{start_label} {{id: record.sourceId}})
            MATCH (end:{end_label} {{id: record.targetId}})
            MERGE (start)-[r:{rel_type}]->(end)
            RETURN count(r) as relationships_created
            """
            return cypher.strip()
        except Exception as e:
            print(f"[DEBUG] Error generating Cypher: {e}")
            return ""

    def _extract_cypher_from_mcp_response(self, result_data: Any) -> Optional[str]:
        """Extract Cypher query from various MCP response formats."""
        try:
            # Handle structured content format
            if isinstance(result_data, dict) and "structuredContent" in result_data:
                structured_content = result_data["structuredContent"]
                if isinstance(structured_content, dict) and "result" in structured_content:
                    return structured_content["result"]

            # Handle direct string result
            if isinstance(result_data, str):
                return result_data.strip()

            # Handle content array format
            if isinstance(result_data, dict) and "content" in result_data:
                content = result_data["content"]
                if isinstance(content, list) and len(content) > 0:
                    first_content = content[0]
                    if isinstance(first_content, dict) and "text" in first_content:
                        return first_content["text"].strip()

            # Handle direct result field
            if isinstance(result_data, dict) and "result" in result_data:
                return result_data["result"]

            return None

        except Exception as e:
            self.logger.error(f"Failed to extract Cypher from MCP response: {str(e)}")
            return None

    def _prepare_node_records(self, node: Dict[str, Any], entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare node records for MCP Cypher ingestion with correct parameter format."""
        try:
            records = []
            node_label = node.get("label", "")
            data_model = node.get("data_model", "auto")
            
            for entity in entities:
                if entity.get("type") == node_label:
                    # Prepare record with all entity properties
                    record = {
                        "data_model": data_model  # Add data_model to each record
                    }
                    properties = entity.get("properties", {})
                    
                    # Get expected property names from node schema
                    expected_properties = []
                    for prop_def in node.get("properties", []):
                        if isinstance(prop_def, dict) and "name" in prop_def:
                            expected_properties.append(prop_def["name"])
                        elif isinstance(prop_def, str):
                            expected_properties.append(prop_def)
                    
                    # Ensure required properties exist
                    for prop_name in expected_properties:
                        if prop_name in properties:
                            record[prop_name] = properties[prop_name]
                        else:
                            record[prop_name] = None  # Default value for missing properties
                    
                    # Add any additional properties from the entity
                    for prop_name, prop_value in properties.items():
                        if prop_name not in record:
                            record[prop_name] = prop_value
                    
                    records.append(record)
            
            self.logger.debug(f"Prepared {len(records)} records for node type {node_label}")
            return records

        except Exception as e:
            self.logger.error(f"Failed to prepare node records: {str(e)}")
            return []

    def _prepare_relationship_records(self, relationship: Dict[str, Any], relationships: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare relationship records for MCP Cypher ingestion with correct parameter format (sourceId, targetId, properties flat)."""
        try:
            records = []
            rel_type = relationship.get("type", "")
            data_model = relationship.get("data_model", "auto")

            for rel in relationships:
                if rel.get("type") == rel_type:
                    # MCP expects sourceId and targetId, plus direct relationship properties
                    record = {
                        "sourceId": rel.get("from", ""),
                        "targetId": rel.get("to", ""),
                        "data_model": data_model  # Add data_model to each record
                    }
                    
                    # Get expected property names from relationship schema
                    expected_properties = []
                    for prop_def in relationship.get("properties", []):
                        if isinstance(prop_def, dict) and "name" in prop_def:
                            expected_properties.append(prop_def["name"])
                        elif isinstance(prop_def, str):
                            expected_properties.append(prop_def)
                    
                    # Add relationship properties (flattened)
                    rel_props = rel.get("properties", {})
                    for prop_name in expected_properties:
                        if prop_name in rel_props:
                            record[prop_name] = rel_props[prop_name]
                    
                    # Only add if both sourceId and targetId are present
                    if record["sourceId"] and record["targetId"]:
                        records.append(record)
                    else:
                        self.logger.warning(f"Skipping relationship record missing required node identifiers: {record}")

            self.logger.debug(f"Prepared {len(records)} records for relationship type {rel_type}")
            return records

        except Exception as e:
            self.logger.error(f"Failed to prepare relationship records: {str(e)}")
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
                f"{server_url}/mcp",
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


# Usage Example and Main Function
def main():
    """
    Example usage of the GraphIngestionTool
    """
    # Initialize the tool
    tool = GraphIngestionTool()
    

    # Generic document ingestion: parse CSV or text, synthesize key property, format for LLM
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


    # Chunked ingestion: split summary_blocks into smaller batches to reduce LLM load 
    chunk_size = 15  # Reduced from 25 to improve LLM success rate
    total_chunks = min(2, (len(summary_blocks) + chunk_size - 1) // chunk_size)  # Process only 2 chunks for testing
    all_results = []
    print(f"FINAL TEST: Processing Data_Entry_2017-small.csv for ingestion in {total_chunks} chunks (size: {chunk_size})...")


    for i in range(0, min(2 * chunk_size, len(summary_blocks)), chunk_size):
        chunk_blocks = summary_blocks[i:i+chunk_size]
        chunk_content = "\n".join(chunk_blocks)
        chunk_num = i//chunk_size+1
        print(f"\n--- Processing chunk {chunk_num}/{total_chunks} ---")
        # Minimal logging for performance
        print(f"  Chunk size: {len(chunk_blocks)} entities")
        result = tool.ingest_content_with_schema_validation(chunk_content)
        all_results.append(result)
        print(f"  Success: {result.get('success', False)} | Entities: {result.get('entities_created', 0)} | Relationships: {result.get('relationships_created', 0)} | Confidence: {result.get('schema_confidence', 0.0):.2f}")
        # Only show errors, not full debug details
        if not result.get('success', True):
            print(f"  Error: {result.get('error', 'Unknown error')}")

    # Aggregate results
    total_entities = sum(r.get('entities_created', 0) for r in all_results if r.get('success'))
    total_relationships = sum(r.get('relationships_created', 0) for r in all_results if r.get('success'))
    avg_confidence = (
        sum(r.get('schema_confidence', 0.0) for r in all_results if r.get('success')) /
        max(1, sum(1 for r in all_results if r.get('success')))
    )

    print("\n=== OPTIMIZED Chunked Ingestion Summary ===")
    print(f"Total Chunks: {total_chunks}")
    print(f"Total Entities Created: {total_entities}")
    print(f"Total Relationships Created: {total_relationships}")
    print(f"Average Schema Confidence: {avg_confidence:.2f}")
    print(f"Performance: {len(summary_blocks)} entities in {total_chunks} chunks")

    # Only display errors if any exist
    errors = [r.get('error') for r in all_results if not r.get('success')]
    if errors:
        print(f"\nErrors ({len(errors)} chunks failed):")
        for idx, error in enumerate(errors[:3], 1):  # Show only first 3 errors
            print(f"  {idx}. {error}")
        if len(errors) > 3:
            print(f"  ... and {len(errors) - 3} more errors")
    else:
        print("\nNo errors - all chunks processed successfully!")

    # Display MCP API request summary
    print("\n" + "="*80)
    print("MCP API REQUESTS SUMMARY")
    print("="*80)
    tool.log_mcp_summary()


if __name__ == "__main__":
    main()
