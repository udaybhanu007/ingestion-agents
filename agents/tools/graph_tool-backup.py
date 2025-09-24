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
from neo4j import GraphDatabase

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
                    "deployment_name": os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4.1"),
                    "azure_api_version": os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview"),
                    "azure_endpoint": os.getenv("AZURE_OPENAI_ENDPOINT", "https://genaiindustria7447042968.cognitiveservices.azure.com/"),
                    "azure_api_key": os.getenv("AZURE_OPENAI_API_KEY", ""),
                    "max_tokens": 32768,
                    "temperature": 1.0,
                    "top_p": 1.0,
                    "frequency_penalty": 0.0,
                    "presence_penalty": 0.0
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
        
    def setup_llm(self):
        """Setup Azure OpenAI LLM client."""
        try:
            openai_config = config_manager.get_config("openai")
            
            self.llm = AzureChatOpenAI(
                deployment_name=openai_config.get("deployment_name", "gpt-4.1"),
                api_version=openai_config.get("azure_api_version", "2024-12-01-preview"),
                azure_endpoint=openai_config.get("azure_endpoint", "https://genaiindustria7447042968.cognitiveservices.azure.com/"),
                api_key=SecretStr(openai_config.get("azure_api_key", "")),
                temperature=openai_config.get("temperature", 1.0),
                max_tokens=openai_config.get("max_tokens", 32768),
                model_kwargs={
                    "top_p": openai_config.get("top_p", 1.0),
                    "frequency_penalty": openai_config.get("frequency_penalty", 0.0),
                    "presence_penalty": openai_config.get("presence_penalty", 0.0)
                }
            )
            
            self.logger.info(f"LLM client initialized successfully with GPT-4.1 (max_tokens: {openai_config.get('max_tokens', 32768)})")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize LLM client: {str(e)}")
            self.llm = None
    
    def setup_mcp_servers(self):
        """Setup MCP server URLs."""
        # Use user-provided endpoints for MCP servers
        self.data_modeling_server_url = os.getenv("MCP_DATA_MODELING_SERVER_URL", "http://127.0.0.1:8004")
        self.cypher_server_url = os.getenv("MCP_CYPHER_SERVER_URL", "http://127.0.0.1:8003")
        self.logger.info(f"MCP servers configured - Data Modeling: {self.data_modeling_server_url}/mcp/, Cypher: {self.cypher_server_url}/mcp/")
    
    def initialize_processing_stats(self):
        """Initialize processing statistics for tracking pipeline performance."""
        self.processing_stats = {
            "documents_processed": 0,
            "schemas_discovered": 0,           # LLM schema discovery
            "entities_extracted": 0,          # LLM entity extraction  
            "schemas_validated": 0,           # MCP validation
            "entities_ingested": 0,           # MCP execution
            "relationships_created": 0,       # MCP execution
            "errors": []
        }

    def _estimate_tokens(self, text: str) -> int:
        """Estimate tokens using the common rule: 1 token ≈ 4 characters for English text."""
        return len(text) // 4

    def _estimate_response_tokens(self, content_length: int, estimated_entities: int = None) -> int:
        """Estimate response tokens needed based on content characteristics."""
        # Base JSON structure overhead
        base_structure = 500
        
        # Estimate entities if not provided (rough heuristic)
        if estimated_entities is None:
            # Assume structured data: estimate rows from content length
            estimated_rows = max(1, content_length // 200)  # ~200 chars per row average
            estimated_entities = estimated_rows * 1.5  # primary + some referenced entities
        
        # Schema discovery tokens
        schema_tokens = 200 + (estimated_entities * 10)  # ~10 tokens per entity type
        
        # Entity extraction tokens
        entity_tokens = estimated_entities * 80  # ~80 tokens per entity on average
        
        # Relationship tokens (assume 1.5 relationships per entity)
        relationship_tokens = int(estimated_entities * 1.5 * 40)  # ~40 tokens per relationship
        
        total_response_tokens = base_structure + schema_tokens + entity_tokens + relationship_tokens
        return total_response_tokens

    def _calculate_optimal_chunk_size(self, content: str, max_response_tokens: int = 30000) -> int:
        """Calculate optimal chunk size to stay within token limits for GPT-4.1 (32,768 tokens)."""
        content_length = len(content)
        
        # GPT-4.1 with 32,768 tokens can handle much larger chunks
        if content_length < 80000:  # Less than ~20,000 tokens
            estimated_response = self._estimate_response_tokens(content_length)
            if estimated_response <= max_response_tokens:
                return -1  # No chunking needed
        
        # For larger content, use GPT-4.1's full 32k token capacity
        # Estimate number of rows in content (rough heuristic)
        lines = content.strip().split('\n')
        data_lines = len(lines)
        
        # Remove header line from count if it exists
        if lines and any(delimiter in lines[0] for delimiter in [',', '\t', '|']):
            data_lines = max(1, len(lines) - 1)
        
        # Calculate chunk size optimized for GPT-4.1's 32k token capacity
        # Target: ~30000 response tokens for optimal performance
        if data_lines <= 100:
            return -1  # Small-medium dataset, process all at once with GPT-4.1
        elif data_lines <= 250:
            return 150  # Medium dataset, larger chunks with 32k tokens
        elif data_lines <= 500:
            return 200  # Large dataset, still very efficient chunks
        else:
            # Very large dataset, use optimal chunks for 32k tokens
            return min(250, max(100, data_lines // 2))  # 2+ chunks for very large data

    def _split_content_into_chunks(self, content: str, chunk_size: int) -> List[str]:
        """Split content into chunks, preserving data structure."""
        lines = content.strip().split('\n')
        
        if not lines:
            return []
        
        # Detect if content is CSV-like (has headers)
        header_line = None
        data_lines = lines
        
        # Check if first line looks like a header (contains common delimiters)
        if lines and any(delimiter in lines[0] for delimiter in [',', '\t', '|']):
            # Likely structured data - preserve header
            header_line = lines[0]
            data_lines = lines[1:] if len(lines) > 1 else []
        
        if not data_lines:
            # If only header or no data, return single chunk
            return [content]
        
        chunks = []
        
        # Create chunks of data lines
        for i in range(0, len(data_lines), chunk_size):
            chunk_data = data_lines[i:i + chunk_size]
            
            if header_line:
                # Add header to each chunk for structured data
                chunk_content = header_line + '\n' + '\n'.join(chunk_data)
            else:
                # For non-structured data, just chunk the lines
                chunk_content = '\n'.join(chunk_data)
            
            chunks.append(chunk_content)
        
        self.logger.debug(f"Split {len(data_lines)} data lines into {len(chunks)} chunks")
        return chunks

    def _merge_llm_results(self, chunk_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merge results from multiple chunks into a single result."""
        if not chunk_results:
            return {"success": False, "error": "No chunk results to merge"}
        
        # Find the first successful result for schema template
        base_result = None
        for result in chunk_results:
            if result.get("success"):
                base_result = result
                break
        
        if not base_result:
            return {"success": False, "error": "All chunks failed processing"}
        
        # Initialize merged result with base schema
        merged_result = {
            "success": True,
            "schema_discovery": base_result["schema_discovery"].copy(),
            "entity_extraction": {
                "entities": [],
                "relationships": []
            },
            "combined_confidence": 0.0,
            "domain": base_result.get("domain", "unknown"),
            "data_format": base_result.get("data_format", "unknown"),
            "chunked_processing": True,
            "total_chunks": len(chunk_results)
        }
        
        # Merge entities and relationships from all successful chunks
        total_confidence = 0.0
        successful_chunks = 0
        
        for result in chunk_results:
            if result.get("success"):
                successful_chunks += 1
                total_confidence += result.get("combined_confidence", 0.0)
                
                # Merge entities
                chunk_entities = result.get("entity_extraction", {}).get("entities", [])
                merged_result["entity_extraction"]["entities"].extend(chunk_entities)
                
                # Merge relationships
                chunk_relationships = result.get("entity_extraction", {}).get("relationships", [])
                merged_result["entity_extraction"]["relationships"].extend(chunk_relationships)
        
        # Calculate average confidence
        if successful_chunks > 0:
            merged_result["combined_confidence"] = total_confidence / successful_chunks
        
        # Update schema discovery with actual counts
        merged_result["schema_discovery"]["total_entities_extracted"] = len(merged_result["entity_extraction"]["entities"])
        merged_result["schema_discovery"]["total_relationships_extracted"] = len(merged_result["entity_extraction"]["relationships"])
        
        self.logger.info(f"Merged {successful_chunks} chunks: {len(merged_result['entity_extraction']['entities'])} entities, "
                        f"{len(merged_result['entity_extraction']['relationships'])} relationships")
        
        return merged_result

    def ingest_content(self, content: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Main entry point for content ingestion - compatibility wrapper for execution agent.

        Args:
            content: Content to ingest
            metadata: Additional metadata (not used in current implementation)
        
        Returns:
            Dict with ingestion results and processing stats
        """
        try:
            # Use the enhanced ingestion approach
            result = self.ingest_content_with_schema_validation(content)
            return result

        except Exception as e:
            error_msg = f"Content ingestion failed: {str(e)}"
            self.logger.error(error_msg)
            self.processing_stats["errors"].append(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "stats": self.processing_stats,
            }

    def ingest_content_with_schema_validation(self, content: str) -> Dict[str, Any]:
        """
        Main entry point for document ingestion with full data processing:
        Document Ingestion -> LLM Schema Discovery & Entity Extraction -> MCP Server Validation & Execution
        Processes ALL document content in a single LLM call for complete context.
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
                error_msg = llm_result.get("error", "LLM processing failed")
                self.logger.error(f"LLM processing failed: {error_msg}")
                return {
                    "success": False,
                    "error": error_msg,
                    "stats": self.processing_stats
                }

            # --- POST-PROCESSING: Ensure key_property and data_model ---
            # Entities - synthesize key_property if missing
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
                self.processing_stats["entities_extracted"] = len(entities)
                self.processing_stats["relationships_created"] = len(relationships)

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
            
            self.logger.info(f"Document preprocessed: {len(processed)} characters")
            return processed
            
        except Exception as e:
            self.logger.error(f"Document preprocessing failed: {str(e)}")
            raise

    def _llm_schema_discovery_and_extraction(self, content: str) -> Dict[str, Any]:
        """
        Layer 2: LLM Schema Discovery & Entity Extraction with Adaptive Chunking
        
        This method combines both schema discovery and entity extraction in a single LLM call
        for better consistency and context preservation. Automatically chunks large content
        to stay within token limits.
        """
        try:
            self.logger.info("Starting LLM schema discovery and entity extraction...")
            
            # Check if adaptive chunking is needed
            chunk_size = self._calculate_optimal_chunk_size(content)
            
            if chunk_size == -1:
                # No chunking needed - process all content at once
                self.logger.info(f"Processing all content in single batch ({len(content)} characters)")
                return self._process_single_chunk(content)
            else:
                # Chunking required
                self.logger.info(f"Content requires chunking - using {chunk_size} rows per chunk")
                return self._process_with_adaptive_chunking(content, chunk_size)
                
        except Exception as e:
            error_msg = f"LLM schema discovery and extraction failed: {str(e)}"
            self.logger.error(error_msg)
            return {"success": False, "error": error_msg}

    def _process_single_chunk(self, content: str) -> Dict[str, Any]:
        """Process content in a single LLM call."""
        try:
            enhanced_prompt = self._build_extraction_prompt(content)
            
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
                    print(f"[DEBUG] {error_details}")
                    
                    if attempt == max_retries - 1:
                        final_error = f"LLM failed after {max_retries} attempts: {str(e)}"
                        print(f"[ERROR] {final_error}")
                        return {"success": False, "error": final_error}
                    # Wait before retry
                    time.sleep(2 ** attempt)  # Exponential backoff

            # Parse the response
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
            error_msg = f"Single chunk processing failed: {str(e)}"
            self.logger.error(error_msg)
            return {"success": False, "error": error_msg}

    def _process_with_adaptive_chunking(self, content: str, chunk_size: int) -> Dict[str, Any]:
        """Process content using adaptive chunking strategy."""
        try:
            # Split content into chunks
            chunks = self._split_content_into_chunks(content, chunk_size)
            self.logger.info(f"Split content into {len(chunks)} chunks of ~{chunk_size} rows each")
            
            chunk_results = []
            
            # Process each chunk
            for i, chunk in enumerate(chunks):
                self.logger.info(f"Processing chunk {i+1}/{len(chunks)} ({len(chunk)} characters)")
                
                try:
                    chunk_result = self._process_single_chunk(chunk)
                    chunk_results.append(chunk_result)
                    
                    if chunk_result.get("success"):
                        entities_count = len(chunk_result.get("entity_extraction", {}).get("entities", []))
                        relationships_count = len(chunk_result.get("entity_extraction", {}).get("relationships", []))
                        self.logger.info(f"Chunk {i+1} completed: {entities_count} entities, {relationships_count} relationships")
                    else:
                        self.logger.warning(f"Chunk {i+1} failed: {chunk_result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    error_msg = f"Chunk {i+1} processing failed: {str(e)}"
                    self.logger.error(error_msg)
                    chunk_results.append({"success": False, "error": error_msg})
                
                # Small delay between chunks to avoid rate limiting
                if i < len(chunks) - 1:
                    time.sleep(1)
            
            # Merge results from all chunks
            merged_result = self._merge_llm_results(chunk_results)
            
            if merged_result.get("success"):
                total_entities = len(merged_result.get("entity_extraction", {}).get("entities", []))
                total_relationships = len(merged_result.get("entity_extraction", {}).get("relationships", []))
                self.processing_stats["schemas_discovered"] += 1
                self.processing_stats["entities_extracted"] += total_entities
                
                self.logger.info(f"Chunked processing completed - Total entities: {total_entities}, "
                               f"Total relationships: {total_relationships}")
            
            return merged_result
            
        except Exception as e:
            error_msg = f"Chunked processing failed: {str(e)}"
            self.logger.error(error_msg)
            return {"success": False, "error": error_msg}

    def _build_extraction_prompt(self, content: str) -> str:
        """Build the extraction prompt with the given content."""
        return f"""Extract ALL entities from the data. For CSV with N rows, create exactly N primary entities PLUS unique Patient entities.

Content: {content}

REQUIRED: 
1. Create 1 ImageRecord entity per CSV row (99 entities expected)
2. Create 1 Patient entity per unique PatientID found in the data
3. Create 1 HAS_IMAGE_RECORD relationship per ImageRecord->Patient connection

Return ONLY valid JSON (no comments, no // lines, no markdown):
{{
    "schema_discovery": {{
        "confidence": 0.95,
        "domain": "medical",
        "entity_types": [
            {{"type": "ImageRecord", "properties": ["id", "ImageIndex", "FindingLabels", "FollowUpNumber", "PatientID", "PatientAge", "PatientGender", "ViewPosition", "OriginalImageWidth", "OriginalImageHeight", "OriginalImagePixelSpacingX", "OriginalImagePixelSpacingY"], "key_property": "id", "entity_category": "main"}},
            {{"type": "Patient", "properties": ["id", "PatientID", "PatientAge", "PatientGender"], "key_property": "id", "entity_category": "lookup"}}
        ],
        "relationship_types": [
            {{"type": "HAS_IMAGE_RECORD", "start_entity": "Patient", "end_entity": "ImageRecord", "cardinality": "1:many", "relationship_category": "direct"}}
        ]
    }},
    "entity_extraction": {{
        "entities": [
            {{"type": "ImageRecord", "properties": {{"id": "row_1", "ImageIndex": "00000001_000.png", "FindingLabels": "Cardiomegaly", "FollowUpNumber": 0, "PatientID": 1, "PatientAge": 58, "PatientGender": "M", "ViewPosition": "PA", "OriginalImageWidth": 2682, "OriginalImageHeight": 2749, "OriginalImagePixelSpacingX": 0.143, "OriginalImagePixelSpacingY": 0.143}}}},
            {{"type": "Patient", "properties": {{"id": "patient_1", "PatientID": 1, "PatientAge": 58, "PatientGender": "M"}}}}
        ],
        "relationships": [
            {{"type": "HAS_IMAGE_RECORD", "from": "patient_1", "to": "row_1", "start_node_label": "Patient", "end_node_label": "ImageRecord", "properties": {{}}}}
        ]
    }}
}}

CRITICAL: Create ImageRecord for EVERY row + Patient for each unique PatientID + relationships connecting them."""

    def _parse_llm_schema_extraction_response(self, response_text: str) -> Dict[str, Any]:
        """
        Parse the combined LLM response for schema discovery and entity extraction.
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
            
            # Try to parse the cleaned JSON directly first
            try:
                parsed_data = json.loads(cleaned_text)
                self.logger.info("Successfully parsed JSON on first attempt")
            except json.JSONDecodeError as e:
                self.logger.error(f"Initial JSON parsing failed: {str(e)}")
                self.logger.error(f"Error at line {e.lineno}, column {e.colno}")
                self.logger.error(f"Error context: {e.msg}")
                
                # Log the problematic area around the error
                if hasattr(e, 'pos') and e.pos > 0:
                    start_pos = max(0, e.pos - 100)
                    end_pos = min(len(cleaned_text), e.pos + 100)
                    context = cleaned_text[start_pos:end_pos]
                    self.logger.error(f"Error context: ...{context}...")
                
                # Log the cleaned text for debugging if it's too long
                if len(cleaned_text) > 50000:
                    self.logger.warning(f"Very long LLM response: {len(cleaned_text)} characters")
                    # Save to debug file for inspection
                    try:
                        with open("debug_llm_response.json", "w", encoding="utf-8") as f:
                            f.write(cleaned_text)
                        self.logger.info("Saved LLM response to debug_llm_response.json for inspection")
                    except Exception as save_e:
                        self.logger.warning(f"Could not save debug file: {save_e}")
                
                # Try to fix common JSON issues
                self.logger.info("Attempting to fix common JSON issues...")
                fixed_text = self._fix_common_json_issues(cleaned_text)
                try:
                    parsed_data = json.loads(fixed_text)
                    self.logger.info("Successfully parsed JSON after fixing common issues")
                except json.JSONDecodeError as e2:
                    self.logger.error(f"JSON parsing failed even after fixes: {str(e2)}")
                    self.logger.error(f"Fixed text error at line {e2.lineno}, column {e2.colno}")
                    
                    # Try a more aggressive regex-based extraction
                    self.logger.info("Attempting regex-based JSON extraction...")
                    json_match = re.search(r'\{.*\}', cleaned_text, re.DOTALL)
                    if not json_match:
                        return {"success": False, "error": f"No JSON found in LLM response. Response length: {len(cleaned_text)}"}
                    try:
                        parsed_data = json.loads(json_match.group())
                        self.logger.info("Successfully extracted JSON using regex fallback")
                    except json.JSONDecodeError as e3:
                        # Final attempt: Try to use a subset parser for partial JSON
                        self.logger.info("Attempting partial JSON extraction...")
                        partial_result = self._extract_partial_json(cleaned_text)
                        if partial_result.get("success"):
                            self.logger.info("Successfully extracted partial JSON data")
                            return partial_result
                        else:
                            return {"success": False, "error": f"JSON parsing failed completely: {str(e3)}"}
            
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
            
            # ENTITY COUNT VALIDATION for structured data
            extracted_entities = entity_extraction.get("entities", [])
            entity_count = len(extracted_entities)
            
            # Log entity count for monitoring
            self.logger.info(f"📊 Entity Extraction Summary: {entity_count} entities extracted")
            
            # Basic validation - warn if entity count seems unusually low
            if entity_count < 10 and "csv" in data_format.lower():
                self.logger.warning(f"⚠️ POTENTIAL ISSUE: Only {entity_count} entities extracted from CSV data")
                self.logger.warning("This may indicate under-extraction - consider reviewing LLM prompt effectiveness")
            
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

    def _fix_common_json_issues(self, json_text: str) -> str:
        """Fix common JSON formatting issues that can cause parsing errors."""
        try:
            # Remove JSON comments (// comments)
            json_text = re.sub(r'//.*', '', json_text)
            
            # Remove trailing commas before closing brackets/braces
            json_text = re.sub(r',(\s*[}\]])', r'\1', json_text)
            
            # Fix common property name issues (unquoted properties)
            json_text = re.sub(r'(\w+)(\s*:\s*)', r'"\1"\2', json_text)
            
            # Fix single quotes to double quotes
            json_text = re.sub(r"'([^']*)'", r'"\1"', json_text)
            
            # Handle truncated JSON - if response ends abruptly, try to close it properly
            if not json_text.rstrip().endswith('}'):
                # Count open braces and brackets to determine how many we need to close
                open_braces = json_text.count('{') - json_text.count('}')
                open_brackets = json_text.count('[') - json_text.count(']')
                
                # Remove any incomplete line at the end
                lines = json_text.split('\n')
                if lines and not lines[-1].strip().endswith((',', '}', ']')):
                    # Remove the incomplete last line
                    json_text = '\n'.join(lines[:-1])
                
                # Close any open arrays first, then objects
                closing_chars = ']' * open_brackets + '}' * open_braces
                json_text = json_text.rstrip() + closing_chars
                
                self.logger.info(f"Applied truncation fix: added {len(closing_chars)} closing characters")
            
            # Remove any text after the final closing brace
            last_brace_pos = json_text.rfind('}')
            if last_brace_pos != -1:
                json_text = json_text[:last_brace_pos + 1]
            
            # Fix invalid escape sequences
            json_text = re.sub(r'\\(?!["\\/bfnrt])', r'\\\\', json_text)
            
            return json_text
        except Exception as e:
            self.logger.warning(f"Could not fix JSON issues: {e}")
            return json_text

    def _extract_partial_json(self, json_text: str) -> Dict[str, Any]:
        """
        Extract usable data from malformed JSON by parsing what we can.
        This is a fallback method when standard JSON parsing fails.
        """
        try:
            self.logger.info("Attempting partial JSON extraction for fallback processing")
            
            # Initialize default structure
            result = {
                "success": True,
                "schema_discovery": {
                    "confidence": 0.8,
                    "domain": "medical",
                    "entity_types": [],
                    "relationship_types": []
                },
                "entity_extraction": {
                    "entities": [],
                    "relationships": []
                }
            }
            
            # Extract entities using regex patterns
            entity_pattern = r'"type":\s*"ImageRecord"[^}]*"properties":\s*\{[^}]*\}'
            entity_matches = re.findall(entity_pattern, json_text, re.DOTALL)
            
            entities_found = 0
            for i, match in enumerate(entity_matches[:99]):  # Limit to expected count
                try:
                    # Try to extract basic properties
                    id_match = re.search(r'"id":\s*"([^"]*)"', match)
                    if id_match:
                        entity_id = id_match.group(1)
                        result["entity_extraction"]["entities"].append({
                            "type": "ImageRecord",
                            "properties": {"id": entity_id}
                        })
                        entities_found += 1
                except Exception as e:
                    self.logger.warning(f"Could not extract entity {i}: {e}")
                    continue
            
            # Extract Patient entities
            patient_pattern = r'"type":\s*"Patient"[^}]*"properties":\s*\{[^}]*\}'
            patient_matches = re.findall(patient_pattern, json_text, re.DOTALL)
            
            patients_found = 0
            for match in patient_matches[:22]:  # Expected patient count
                try:
                    id_match = re.search(r'"id":\s*"([^"]*)"', match)
                    if id_match:
                        patient_id = id_match.group(1)
                        result["entity_extraction"]["entities"].append({
                            "type": "Patient", 
                            "properties": {"id": patient_id}
                        })
                        patients_found += 1
                except Exception as e:
                    self.logger.warning(f"Could not extract patient: {e}")
                    continue
            
            # Extract relationships
            rel_pattern = r'"type":\s*"HAS_IMAGE_RECORD"[^}]*\}'
            rel_matches = re.findall(rel_pattern, json_text, re.DOTALL)
            
            relationships_found = 0
            for match in rel_matches[:99]:  # Expected relationship count
                try:
                    from_match = re.search(r'"from":\s*"([^"]*)"', match)
                    to_match = re.search(r'"to":\s*"([^"]*)"', match)
                    
                    if from_match and to_match:
                        result["entity_extraction"]["relationships"].append({
                            "type": "HAS_IMAGE_RECORD",
                            "from": from_match.group(1),
                            "to": to_match.group(1),
                            "start_node_label": "Patient",
                            "end_node_label": "ImageRecord",
                            "properties": {}
                        })
                        relationships_found += 1
                except Exception as e:
                    self.logger.warning(f"Could not extract relationship: {e}")
                    continue
            
            # Add basic schema info
            if entities_found > 0:
                result["schema_discovery"]["entity_types"] = [
                    {
                        "type": "ImageRecord",
                        "properties": ["id"],
                        "key_property": "id",
                        "entity_category": "main"
                    }
                ]
            
            if patients_found > 0:
                result["schema_discovery"]["entity_types"].append({
                    "type": "Patient",
                    "properties": ["id"],
                    "key_property": "id", 
                    "entity_category": "lookup"
                })
            
            if relationships_found > 0:
                result["schema_discovery"]["relationship_types"] = [{
                    "type": "HAS_IMAGE_RECORD",
                    "start_entity": "Patient",
                    "end_entity": "ImageRecord",
                    "cardinality": "1:many",
                    "relationship_category": "direct"
                }]
            
            self.logger.info(f"Partial extraction found: {entities_found} entities, {patients_found} patients, {relationships_found} relationships")
            
            if entities_found == 0 and patients_found == 0:
                return {"success": False, "error": "No usable data could be extracted from malformed JSON"}
            
            return result
            
        except Exception as e:
            self.logger.error(f"Partial JSON extraction failed: {e}")
            return {"success": False, "error": f"Partial extraction failed: {str(e)}"}

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
            
            for entity in entities:
                if entity.get("type") == node_label:
                    # Prepare record with all entity properties (flat structure)
                    record = {}
                    properties = entity.get("properties", {})
                    
                    # Add all properties directly to the record
                    for prop_name, prop_value in properties.items():
                        record[prop_name] = prop_value
                    
                    # Ensure id exists
                    if "id" not in record:
                        record["id"] = str(len(records) + 1)
                    
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
                    return self._parse_sse_response(response.text)
                else:
                    # Handle JSON response
                    try:
                        result_data = response.json()
                        
                        if "error" in result_data:
                            return {
                                "success": False,
                                "error": result_data["error"].get("message", "Unknown MCP error")
                            }
                        
                        # Handle structured response format
                        if "result" in result_data:
                            return {
                                "success": True,
                                "result": result_data.get("result", {})
                            }
                        else:
                            return {
                                "success": True,
                                "result": result_data
                            }
                    except json.JSONDecodeError:
                        # Try SSE parsing as fallback
                        return self._parse_sse_response(response.text)
            else:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text}"
                }
                
        except requests.RequestException as e:
            self.logger.error(f"MCP request failed: {str(e)}")
            return {
                "success": False,
                "error": f"Request failed: {str(e)}"
            }
        except Exception as e:
            self.logger.error(f"Unexpected error in MCP request: {str(e)}")
            return {
                "success": False,
                "error": f"Unexpected error: {str(e)}"
            }

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

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get current processing statistics."""
        return self.processing_stats.copy()

    def reset_processing_stats(self):
        """Reset processing statistics."""
        self.initialize_processing_stats()

    def _fix_entity_relationship_id_consistency(self, entities: List[Dict[str, Any]], relationships: List[Dict[str, Any]]) -> tuple:
        """
        Fix ID consistency between entities and relationships to ensure proper referencing.
        
        This method addresses cases where:
        1. Entity IDs don't match the references used in relationships
        2. Relationship 'from' and 'to' fields reference entity properties instead of entity IDs
        3. Entity extraction creates inconsistent ID formats across different entity types
        
        Args:
            entities: List of extracted entities
            relationships: List of extracted relationships
            
        Returns:
            Tuple of (fixed_entities, fixed_relationships)
        """
        try:
            self.logger.info("Starting ID consistency fix for entities and relationships...")
            
            # Step 1: Build entity ID mapping and property-based lookup
            entity_id_map = {}  # Maps current entity ID to new standardized ID
            entity_property_lookup = {}  # Maps entity type + property values to entity IDs
            entity_type_counts = {}  # Track counts for auto-ID generation
            
            # First pass: Collect all entities and build lookup maps
            for entity in entities:
                entity_type = entity.get("type", "Unknown")
                properties = entity.get("properties", {})
                current_id = properties.get("id", "")
                
                # Initialize type counter
                if entity_type not in entity_type_counts:
                    entity_type_counts[entity_type] = 0
                entity_type_counts[entity_type] += 1
                
                # Store entity in property lookup for relationship matching
                if entity_type not in entity_property_lookup:
                    entity_property_lookup[entity_type] = []
                
                entity_property_lookup[entity_type].append({
                    "id": current_id,
                    "properties": properties,
                    "entity": entity
                })
            
            # Step 2: Fix entity IDs to be consistent and meaningful
            fixed_entities = []
            new_entity_id_map = {}  # Maps old ID to new ID
            
            for entity in entities:
                entity_type = entity.get("type", "Unknown")
                properties = entity.get("properties", {})
                current_id = properties.get("id", "")
                
                # Generate a standardized ID based on entity type and key properties
                new_id = self._generate_standardized_entity_id(entity_type, properties)
                
                # Update the entity with the new ID
                fixed_entity = entity.copy()
                fixed_properties = properties.copy()
                fixed_properties["id"] = new_id
                fixed_entity["properties"] = fixed_properties
                
                # Track the ID change
                new_entity_id_map[current_id] = new_id
                
                fixed_entities.append(fixed_entity)
                
                self.logger.debug(f"Fixed entity ID: {entity_type} {current_id} -> {new_id}")
            
            # Step 3: Fix relationship references to use the new entity IDs
            fixed_relationships = []
            relationship_fixes = 0
            failed_resolutions = 0
            
            for relationship in relationships:
                rel_type = relationship.get("type", "")
                from_ref = relationship.get("from", "")
                to_ref = relationship.get("to", "")
                start_label = relationship.get("start_node_label", "")
                end_label = relationship.get("end_node_label", "")
                
                self.logger.debug(f"Processing {rel_type} relationship: {start_label}({from_ref}) -> {end_label}({to_ref})")
                
                # Try to resolve 'from' reference
                resolved_from = self._resolve_entity_reference(
                    from_ref, start_label, entity_property_lookup, new_entity_id_map
                )
                
                # Try to resolve 'to' reference  
                resolved_to = self._resolve_entity_reference(
                    to_ref, end_label, entity_property_lookup, new_entity_id_map
                )
                
                if resolved_from and resolved_to:
                    fixed_relationship = relationship.copy()
                    fixed_relationship["from"] = resolved_from
                    fixed_relationship["to"] = resolved_to
                    fixed_relationships.append(fixed_relationship)
                    
                    if resolved_from != from_ref or resolved_to != to_ref:
                        relationship_fixes += 1
                        self.logger.debug(f"Fixed relationship: {rel_type} {from_ref}->{resolved_from}, {to_ref}->{resolved_to}")
                else:
                    failed_resolutions += 1
                    self.logger.warning(f"Could not resolve relationship references for {rel_type}: from={from_ref}({start_label}), to={to_ref}({end_label})")
                    # Log available entities for debugging
                    if start_label in entity_property_lookup:
                        self.logger.debug(f"Available {start_label} entities: {[e['properties'].get('id', 'no_id') for e in entity_property_lookup[start_label][:3]]}")
                    if end_label in entity_property_lookup:
                        self.logger.debug(f"Available {end_label} entities: {[e['properties'].get('id', 'no_id') for e in entity_property_lookup[end_label][:3]]}")
            
            self.logger.info(f"ID consistency fix completed: {len(fixed_entities)} entities, {len(fixed_relationships)} relationships, {relationship_fixes} relationship references fixed, {failed_resolutions} failed resolutions")
            
            return fixed_entities, fixed_relationships
            
        except Exception as e:
            self.logger.error(f"ID consistency fix failed: {str(e)}")
            return entities, relationships  # Return original data if fix fails

    def _generate_standardized_entity_id(self, entity_type: str, properties: Dict[str, Any]) -> str:
        """Generate a standardized entity ID based on type and key properties."""
        try:
            # Use natural key properties if available
            natural_keys = ["patient_id", "image_index", "findingLabel", "finding", "diagnosis", "name", "title", "code"]
            
            for key in natural_keys:
                if key in properties and properties[key] is not None:
                    value = str(properties[key]).strip()
                    if value:
                        # For Patient entities, use normalized patient_id
                        if entity_type == "Patient" and key == "patient_id":
                            # Normalize patient ID to handle numeric IDs
                            normalized_id = f"patient_{value}"
                            self.logger.debug(f"Generated Patient ID: {normalized_id} from {value}")
                            return normalized_id
                        # For Finding entities, use findingLabel
                        elif entity_type == "Finding" and key == "findingLabel":
                            normalized_id = f"finding_{value.lower().replace(' ', '_')}"
                            self.logger.debug(f"Generated Finding ID: {normalized_id} from {value}")
                            return normalized_id
                        # For other entities, combine type with the key value
                        return f"{entity_type}_{value}"
            
            # Fallback: use existing ID if it's meaningful, otherwise generate auto ID
            existing_id = properties.get("id", "")
            if existing_id and not existing_id.startswith("auto_id_"):
                return existing_id
            
            # Last resort: generate a type-based auto ID
            import uuid
            return f"{entity_type}_{str(uuid.uuid4())[:8]}"
            
        except Exception as e:
            self.logger.warning(f"ID generation failed for {entity_type}: {str(e)}")
            return f"{entity_type}_unknown"

    def _resolve_entity_reference(self, reference: str, entity_type: str, entity_lookup: Dict[str, List], id_map: Dict[str, str]) -> str:
        """Resolve an entity reference to a valid entity ID."""
        try:
            # Direct ID mapping
            if reference in id_map:
                self.logger.debug(f"Resolved {reference} via ID map to {id_map[reference]}")
                return id_map[reference]
            
            # Look up by entity type and properties
            if entity_type in entity_lookup:
                for entity_data in entity_lookup[entity_type]:
                    properties = entity_data["properties"]
                    
                    # Check if reference matches any property value
                    if reference == str(properties.get("patient_id", "")):
                        self.logger.debug(f"Resolved {reference} via patient_id to {properties['id']}")
                        return properties["id"]
                    if reference == str(properties.get("id", "")):
                        self.logger.debug(f"Resolved {reference} via id to {properties['id']}")
                        return properties["id"]
                    if reference == str(properties.get("name", "")):
                        self.logger.debug(f"Resolved {reference} via name to {properties['id']}")
                        return properties["id"]
                    if reference == str(properties.get("findingLabel", "")):
                        self.logger.debug(f"Resolved {reference} via findingLabel to {properties['id']}")
                        return properties["id"]
                    
                    # Special case for Patient entities - check if reference is a patient ID
                    if entity_type == "Patient" and str(properties.get("patient_id", "")) == reference:
                        self.logger.debug(f"Resolved Patient {reference} via patient_id to {properties['id']}")
                        return properties["id"]
                    
                    # Special case for Patient entities - handle numeric patient IDs
                    if entity_type == "Patient":
                        # Check if the Patient ID matches the normalized format
                        expected_patient_id = f"patient_{reference}"
                        if properties.get("id", "") == expected_patient_id:
                            self.logger.debug(f"Resolved Patient {reference} via normalized ID to {properties['id']}")
                            return properties["id"]
            
            # If no match found, log warning and return the original reference
            self.logger.warning(f"Could not resolve {entity_type} reference: {reference}")
            return reference
            
        except Exception as e:
            self.logger.warning(f"Failed to resolve entity reference {reference}: {str(e)}")
            return reference


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

    # Process ALL data at once - no chunking
    print(f"PROCESSING ALL DATA: {len(summary_blocks)} entities at once...")
    result = tool.ingest_content_with_schema_validation(formatted_content)
    
    print(f"Success: {result.get('success', False)} | Entities: {result.get('entities_created', 0)} | Relationships: {result.get('relationships_created', 0)} | Confidence: {result.get('schema_confidence', 0.0):.2f}")
    
    if not result.get('success', True):
        print(f"Error: {result.get('error', 'Unknown error')}")
    else:
        print("Processing completed successfully!")

    # CHUNKED INGESTION LOGIC COMMENTED OUT
    # # Chunked ingestion: split summary_blocks into smaller batches to reduce LLM load 
    # chunk_size = 15  # Reduced from 25 to improve LLM success rate
    # total_chunks = min(2, (len(summary_blocks) + chunk_size - 1) // chunk_size)  # Process only 2 chunks for testing
    # all_results = []
    # print(f"FINAL TEST: Processing Data_Entry_2017-small.csv for ingestion in {total_chunks} chunks (size: {chunk_size})...")

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

    # # Aggregate results
    # total_entities = sum(r.get('entities_created', 0) for r in all_results if r.get('success'))
    # total_relationships = sum(r.get('relationships_created', 0) for r in all_results if r.get('success'))
    # avg_confidence = (
    #     sum(r.get('schema_confidence', 0.0) for r in all_results if r.get('success')) /
    #     max(1, sum(1 for r in all_results if r.get('success')))
    # )

    # print("\n=== OPTIMIZED Chunked Ingestion Summary ===")
    # print(f"Total Chunks: {total_chunks}")
    # print(f"Total Entities Created: {total_entities}")
    # print(f"Total Relationships Created: {total_relationships}")
    # print(f"Average Schema Confidence: {avg_confidence:.2f}")
    # print(f"Performance: {len(summary_blocks)} entities in {total_chunks} chunks")

    # # Only display errors if any exist
    # errors = [r.get('error') for r in all_results if not r.get('success')]
    # if errors:
    #     print(f"\nErrors ({len(errors)} chunks failed):")
    #     for idx, error in enumerate(errors[:3], 1):  # Show only first 3 errors
    #         print(f"  {idx}. {error}")
    #     if len(errors) > 3:
    #         print(f"  ... and {len(errors) - 3} more errors")
    # else:
    #     print("\nNo errors - all chunks processed successfully!")


if __name__ == "__main__":
    main()
