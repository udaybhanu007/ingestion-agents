"""
Graph Ingestion Tool for Neo4j using MCP

This tool handles ingestion of content into Neo4j graph database using MCP (Model Context Protocol).
Based on the successful dual-server approach from ingest_working.py.
"""

import logging
import json
import sys
import os
import requests
import re
import math
import time
from typing import Dict, Any, List, Optional, Union
from langchain_openai import AzureChatOpenAI
from pydantic import SecretStr

# Add parent directory to path for config import
parent_dir = os.path.dirname(os.path.dirname(__file__))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from config.config_manager import get_config
    config_manager = get_config()
    def get_graph_config():
        return config_manager
except ImportError:
    # Fallback config
    class MockConfig:
        def get_config(self, section, key=None):
            configs = {
                "neo4j": {
                    "uri": "bolt://localhost:7687",
                    "username": "neo4j",
                    "password": "password"
                },
                "openai": {
                    "deployment_name": "gpt-4o-mini",
                    "azure_api_version": "2024-08-01-preview",
                    "azure_endpoint": os.getenv("AZURE_OPENAI_ENDPOINT", ""),
                    "azure_api_key": os.getenv("AZURE_OPENAI_API_KEY", "")
                }
            }
            if key:
                return configs.get(section, {}).get(key)
            return configs.get(section, {})
        
        @property
        def max_content_length(self):
            return 50000
    
    def get_graph_config():
        return MockConfig()

# Import structured logging
try:
    from config.logger_config import get_tool_logger
    STRUCTURED_LOGGING_AVAILABLE = True
except ImportError:
    STRUCTURED_LOGGING_AVAILABLE = False

logger = logging.getLogger(__name__)


class GraphIngestionTool:
    """Tool for ingesting content into Neo4j graph database using MCP."""
    
    def __init__(self):
        self.config = get_graph_config()
        
        # Initialize structured logging
        if STRUCTURED_LOGGING_AVAILABLE:
            self.logger = get_tool_logger("graph_tool")
        else:
            self.logger = logging.getLogger(__name__)
        
        # Initialize Azure OpenAI client using correct config structure
        openai_config = self.config.get_config('openai')
        self.llm = AzureChatOpenAI(
            azure_deployment=openai_config.get('deployment_name'),
            api_version=openai_config.get('azure_api_version'),
            azure_endpoint=openai_config.get('azure_endpoint'),
            api_key=SecretStr(openai_config.get('azure_api_key', ''))
        )
        
        # MCP server configurations - using working dual server approach
        self.cypher_server_url = "http://127.0.0.1:8003/mcp/"
        self.data_modeling_server_url = "http://127.0.0.1:8004/mcp/" 
        
        self.logger.info(f"GraphIngestionTool initialized - cypher_server: {self.cypher_server_url}, data_modeling_server: {self.data_modeling_server_url}")
        
        # Track processing state
        self.processing_stats = {
            "documents_processed": 0,
            "schemas_generated": 0,
            "entities_ingested": 0,
            "relationships_created": 0,
            "errors": []
        }
        
        self.logger.info("GraphIngestionTool initialized with dual MCP server configuration")
    
    @property
    def name(self) -> str:
        return "graph_ingest"
    
    @property
    def description(self) -> str:
        return "Ingests structured and unstructured content into Neo4j graph database using MCP with simplified 3-step workflow: analyze structure, generate schema, and ingest data."
    
    @property
    def priority(self) -> int:
        return 10
    
    @property 
    def enabled(self) -> bool:
        return True
    
    def parse_sse_response(self, response_text: str) -> Dict[str, Any]:
        """Parse Server-Sent Events response format."""
        try:
            # Split by lines and find data lines
            lines = response_text.strip().split('\n')
            
            for line in lines:
                if line.startswith('data: '):
                    json_str = line[6:]  # Remove 'data: ' prefix
                    if json_str.strip() == '[DONE]':
                        continue
                    try:
                        return json.loads(json_str)
                    except json.JSONDecodeError:
                        continue
            
            # If no valid SSE format found, try parsing as direct JSON
            return json.loads(response_text)
            
        except Exception as e:
            self.logger.error(f"Error parsing SSE response: {e}")
            return {"error": f"Failed to parse response: {str(e)}"}

    def make_mcp_request(self, server_url: str, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Make a request to an MCP server."""
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "arguments": arguments
                }
            }
            
            response = requests.post(
                server_url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream"
                },
                timeout=30
            )
            
            if response.status_code == 200:
                return self.parse_sse_response(response.text)
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                self.logger.error(f"MCP request failed: {error_msg}")
                return {"error": error_msg}
                
        except Exception as e:
            error_msg = f"Request failed: {str(e)}"
            self.logger.error(f"MCP request error: {error_msg}")
            return {"error": error_msg}



    def ingest_content_with_schema_validation(self, content: str, document_type: str = "document") -> Dict[str, Any]:
        """
        Enhanced ingestion method using proper Neo4j MCP data modeling tools.
        
        Workflow:
        1. Extract entities using LLM
        2. Build data model using Neo4j structures
        3. Validate data model using validate_data_model
        4. Generate proper Cypher queries using MCP tools
        5. Execute via write_neo4j_cypher
        
        Args:
            content: Content to ingest
            document_type: Type of document being ingested
        """
        try:
            self.logger.info(f"Starting enhanced Neo4j ingestion for {document_type}")
            
            # Step 1: Extract entities and relationships using LLM
            self.logger.info("Step 1: Extracting entities using LLM...")
            entities_relationships = self._extract_entities_with_llm(content, document_type)
            
            if not entities_relationships:
                return {
                    "success": False,
                    "error": "Failed to extract entities from content",
                    "stats": self.processing_stats
                }
            
            # Step 2: Build and validate Neo4j data model
            self.logger.info("Step 2: Building and validating data model...")
            data_model = self._build_neo4j_data_model(entities_relationships)
            
            if not data_model:
                return {
                    "success": False,
                    "error": "Failed to build valid data model",
                    "stats": self.processing_stats
                }
            
            # Step 3: Generate and execute Cypher queries using MCP tools
            self.logger.info("Step 3: Generating and executing Cypher queries...")
            execution_result = self._execute_mcp_ingestion(data_model, entities_relationships)
            
            if execution_result["success"]:
                self.processing_stats["documents_processed"] += 1
                self.processing_stats["schemas_generated"] += 1
                self.logger.info("Enhanced Neo4j ingestion completed successfully")
            
            return execution_result
            
        except Exception as e:
            error_msg = f"Enhanced ingestion failed: {str(e)}"
            self.logger.error(error_msg)
            self.processing_stats["errors"].append(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "stats": self.processing_stats
            }
    
    def _extract_entities_with_llm(self, content: str, document_type: str) -> Dict[str, Any]:
        """Extract entities and relationships using LLM."""
        try:
            prompt = f"""
            Analyze the following {document_type} content and extract entities and relationships.
            
            Content: {content}
            
            Return a JSON structure with:
            {{
                "entities": [
                    {{"type": "EntityType", "properties": {{"name": "value", "description": "text"}}}}
                ],
                "relationships": [
                    {{"from": "entity1", "to": "entity2", "type": "RELATIONSHIP_TYPE", "properties": {{}}}}
                ]
            }}
            
            Focus on key concepts, people, organizations, locations, and their relationships.
            Make entity types PascalCase and relationship types SCREAMING_SNAKE_CASE.
            """
            
            response = self.llm.invoke(prompt)
            
            # Parse LLM response to extract JSON
            content_text = response.content
            
            # Find JSON in the response
            json_match = re.search(r'\{.*\}', content_text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            
            return None
            
        except Exception as e:
            self.logger.error(f"LLM entity extraction failed: {e}")
            return None
    
    def _build_neo4j_data_model(self, entities_relationships: Dict[str, Any]) -> Dict[str, Any]:
        """Build a proper Neo4j data model structure and validate it."""
        try:
            nodes = []
            relationships = []
            
            # Build nodes from entities
            for entity in entities_relationships.get("entities", []):
                entity_type = entity.get("type", "Entity")
                properties = entity.get("properties", {})
                
                # Create key property (required by Neo4j data model)
                key_property = {
                    "name": "name",  # Default key property
                    "type": "STRING",
                    "description": "The name/identifier of the entity"
                }
                
                # Build other properties
                node_properties = []
                for prop_name, prop_value in properties.items():
                    if prop_name != "name":  # Skip key property
                        node_properties.append({
                            "name": prop_name,
                            "type": "STRING",
                            "description": f"Property {prop_name} of {entity_type}"
                        })
                
                node = {
                    "label": entity_type,
                    "key_property": key_property,
                    "properties": node_properties
                }
                
                nodes.append(node)
                self.logger.debug(f"Built node: {entity_type}")
            
            # Build relationships
            for rel in entities_relationships.get("relationships", []):
                rel_type = rel.get("type", "RELATED_TO")
                from_entity = rel.get("from", "")
                to_entity = rel.get("to", "")
                
                # Find corresponding node labels
                from_label = self._find_entity_label(from_entity, entities_relationships.get("entities", []))
                to_label = self._find_entity_label(to_entity, entities_relationships.get("entities", []))
                
                if from_label and to_label:
                    relationship = {
                        "type": rel_type,
                        "start_node_label": from_label,
                        "end_node_label": to_label,
                        "properties": []
                    }
                    
                    relationships.append(relationship)
                    self.logger.debug(f"Built relationship: {rel_type}")
            
            # Build complete data model
            data_model = {
                "nodes": nodes,
                "relationships": relationships
            }
            
            # Validate entire data model using MCP
            validation_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "validate_data_model",
                {"data_model": data_model}
            )
            
            if "error" not in validation_result:
                self.logger.info("Data model validation successful")
                return data_model
            else:
                self.logger.error(f"Data model validation failed: {validation_result['error']}")
                return None
                
        except Exception as e:
            self.logger.error(f"Data model building failed: {e}")
            return None
    
    def _find_entity_label(self, entity_name: str, entities: List[Dict]) -> Optional[str]:
        """Find the label of an entity by its name."""
        for entity in entities:
            properties = entity.get("properties", {})
            if properties.get("name") == entity_name:
                return entity.get("type")
        return None
    
    def _execute_mcp_ingestion(self, data_model: Dict[str, Any], entities_relationships: Dict[str, Any]) -> Dict[str, Any]:
        """Execute ingestion using MCP-generated Cypher queries."""
        try:
            entities_created = 0
            relationships_created = 0
            
            # Step 1: Create constraints first
            constraints_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "get_constraints_cypher_queries",
                {"data_model": data_model}
            )
            
            if "error" not in constraints_result:
                constraints = constraints_result.get("result", [])
                for constraint_query in constraints:
                    result = self.make_mcp_request(
                        self.cypher_server_url,
                        "write_neo4j_cypher",
                        {"query": constraint_query}
                    )
                    if "error" in result:
                        self.logger.warning(f"Constraint creation failed: {result['error']}")
            
            # Step 2: Create nodes using MCP-generated queries
            for node in data_model.get("nodes", []):
                # Get proper ingestion query from MCP
                query_result = self.make_mcp_request(
                    self.data_modeling_server_url,
                    "get_node_cypher_ingest_query",
                    {"node": node}
                )
                
                if "error" not in query_result:
                    cypher_query = query_result.get("result", "")
                    
                    # Prepare records for this node type
                    records = self._prepare_node_records(node, entities_relationships.get("entities", []))
                    
                    if records:
                        # Execute with records
                        result = self.make_mcp_request(
                            self.cypher_server_url,
                            "write_neo4j_cypher",
                            {"query": cypher_query, "params": {"records": records}}
                        )
                        
                        if "error" not in result:
                            entities_created += len(records)
                            self.logger.debug(f"Created {len(records)} {node['label']} nodes")
                        else:
                            self.logger.warning(f"Node creation failed: {result['error']}")
            
            # Step 3: Create relationships using MCP-generated queries
            for relationship in data_model.get("relationships", []):
                query_result = self.make_mcp_request(
                    self.data_modeling_server_url,
                    "get_relationship_cypher_ingest_query",
                    {
                        "data_model": data_model,
                        "relationship_type": relationship["type"],
                        "relationship_start_node_label": relationship["start_node_label"],
                        "relationship_end_node_label": relationship["end_node_label"]
                    }
                )
                
                if "error" not in query_result:
                    cypher_query = query_result.get("result", "")
                    
                    # Prepare relationship records
                    records = self._prepare_relationship_records(relationship, entities_relationships.get("relationships", []))
                    
                    if records:
                        result = self.make_mcp_request(
                            self.cypher_server_url,
                            "write_neo4j_cypher",
                            {"query": cypher_query, "params": {"records": records}}
                        )
                        
                        if "error" not in result:
                            relationships_created += len(records)
                            self.logger.debug(f"Created {len(records)} {relationship['type']} relationships")
                        else:
                            self.logger.warning(f"Relationship creation failed: {result['error']}")
            
            self.processing_stats["entities_ingested"] += entities_created
            self.processing_stats["relationships_created"] += relationships_created
            
            return {
                "success": True,
                "entities_created": entities_created,
                "relationships_created": relationships_created,
                "stats": self.processing_stats
            }
            
        except Exception as e:
            error_msg = f"MCP ingestion execution failed: {str(e)}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg
            }
    
    def _prepare_node_records(self, node: Dict[str, Any], entities: List[Dict]) -> List[Dict]:
        """Prepare node records for ingestion."""
        records = []
        node_label = node["label"]
        
        for entity in entities:
            if entity.get("type") == node_label:
                record = {}
                properties = entity.get("properties", {})
                
                # Add key property
                key_prop_name = node["key_property"]["name"]
                record[key_prop_name] = properties.get(key_prop_name, properties.get("name", ""))
                
                # Add other properties
                for prop in node.get("properties", []):
                    prop_name = prop["name"]
                    if prop_name in properties:
                        record[prop_name] = properties[prop_name]
                
                records.append(record)
        
        return records
    
    def _prepare_relationship_records(self, relationship: Dict[str, Any], relationships: List[Dict]) -> List[Dict]:
        """Prepare relationship records for ingestion."""
        records = []
        rel_type = relationship["type"]
        
        for rel in relationships:
            if rel.get("type") == rel_type:
                record = {
                    "sourceId": rel.get("from", ""),
                    "targetId": rel.get("to", "")
                }
                
                # Add relationship properties if any
                rel_props = rel.get("properties", {})
                record.update(rel_props)
                
                records.append(record)
        
        return records    

    def _split_paragraphs(self, text):
        """Split text into paragraphs, skipping empty lines and page headers."""
        paragraphs = []
        for para in re.split(r'\n\s*\n', text):
            para = para.strip()
            if para and not para.startswith("## Page "):
                paragraphs.append(para)
        return paragraphs

    def _merge_short_paragraphs(self, paragraphs, min_words=30):
        """Merge paragraphs shorter than min_words with the next one."""
        merged = []
        buffer = ""
        for para in paragraphs:
            if len(para.split()) < min_words:
                buffer += " " + para if buffer else para
            else:
                if buffer:
                    merged.append(buffer.strip())
                    buffer = ""
                merged.append(para)
        if buffer:
            merged.append(buffer.strip())
        return merged

    def _chunk_with_overlap(self, paragraphs, chunk_size=3, overlap=1):
        """Create overlapping chunks of paragraphs."""
        chunks = []
        i = 0
        n = len(paragraphs)
        while i < n:
            chunk = paragraphs[i:i+chunk_size]
            if chunk:
                chunks.append("\n\n".join(chunk))
            i += chunk_size - overlap
        return chunks

    def _split_large_chunks(self, chunks, max_words=400):
        """Split chunks that are too large by sentences."""
        result = []
        for chunk in chunks:
            words = chunk.split()
            if len(words) <= max_words:
                result.append(chunk)
            else:
                # Split by sentences if too large
                sentences = re.split(r'(?<=[.!?])\s+', chunk)
                temp = ""
                for sent in sentences:
                    if len((temp + " " + sent).split()) > max_words:
                        if temp:
                            result.append(temp.strip())
                        temp = sent
                    else:
                        temp += " " + sent if temp else sent
                if temp:
                    result.append(temp.strip())
        return result

    def chunk_content(self, content, chunk_size=3, overlap=1, min_words=30, max_words=400):
        """Full chunking pipeline."""
        paragraphs = self._split_paragraphs(content)
        merged = self._merge_short_paragraphs(paragraphs, min_words)
        chunks = self._chunk_with_overlap(merged, chunk_size, overlap)
        final_chunks = self._split_large_chunks(chunks, max_words)
        return final_chunks

    def ingest_content(self, content: str, source_name: str = "document", content_type: str = "text", metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Main entry point for content ingestion - delegates to schema-driven approach.
        Now uses paragraph-based overlapping chunking.
        """
        try:
            # Determine document type from content_type and metadata
            document_type = content_type
            if metadata and "document_type" in metadata:
                document_type = metadata["document_type"]

            self.logger.info(f"Starting content ingestion for '{source_name}' (type: {document_type})")

            # Chunk the content before ingestion
            chunked_contents = self.chunk_content(content)
            results = []
            for chunk in chunked_contents:
                # Use schema-driven ingestion approach for each chunk
                result = self.ingest_content_with_schema_validation(chunk, document_type)
                if result.get("success"):
                    result["source_name"] = source_name
                    result["content_type"] = content_type
                    if metadata:
                        result["metadata"] = metadata
                results.append(result)

            # Aggregate results
            all_success = all(r.get("success") for r in results)
            return {
                "success": all_success,
                "results": results
            }

        except Exception as e:
            error_msg = f"Content ingestion failed: {str(e)}"
            self.logger.error(error_msg)
            self.processing_stats["errors"].append(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "stats": self.processing_stats
            }

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get current processing statistics."""
        return self.processing_stats.copy()

    def reset_stats(self):
        """Reset processing statistics."""
        self.processing_stats = {
            "documents_processed": 0,
            "schemas_generated": 0,
            "entities_ingested": 0,
            "relationships_created": 0,
            "errors": []
        }
        self.logger.info("Processing statistics reset")


# For backward compatibility - export the main class and key methods
__all__ = ["GraphIngestionTool"]
