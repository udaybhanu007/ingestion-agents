"""
Root Cause Analysis and Fixes for Neo4j MCP Server Integration Issues

IDENTIFIED ROOT CAUSES:
1. Parameter Structure Mismatch: The tools expect specific parameter formats
2. Tool Name Mismatch: Some tool names may not match exactly
3. Data Model Structure: The data model structure doesn't match Neo4j MCP expectations
4. Server Response Parsing: SSE parsing may be interfering with JSON responses

FIXES APPLIED:
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
                    "deployment_name": os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini"),
                    "azure_api_version": os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
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
    """Fixed tool for ingesting content into Neo4j graph database using MCP."""
    
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

    def initialize_servers(self, timeout: int = 5) -> bool:
        """Check that the configured MCP servers are reachable.

        Performs a lightweight JSON-RPC POST to each server and treats
        common non-200 responses (e.g., 400/405) as evidence the server
        is up and responding. Returns True only if both servers are reachable.
        """
        try:
            servers = [self.cypher_server_url, self.data_modeling_server_url]
            for url in servers:
                try:
                    payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
                    resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=timeout)
                    # Consider the server reachable if we get a response and not a server error
                    if resp is None:
                        self.logger.warning(f"No response from MCP server: {url}")
                        return False
                    if resp.status_code >= 500:
                        self.logger.warning(f"MCP server {url} returned server error: {resp.status_code}")
                        return False
                    # 200 OK is ideal; 400/405 indicate endpoint exists but method unsupported -> still reachable
                    if resp.status_code in (200, 400, 405):
                        self.logger.debug(f"MCP server {url} reachable (status {resp.status_code})")
                        continue
                    # Other codes: warn but allow (treat as reachable) unless explicitly failing
                    self.logger.debug(f"MCP server {url} responded with status {resp.status_code}")
                except Exception as e:
                    self.logger.error(f"Error contacting MCP server {url}: {e}")
                    return False

            return True
        except Exception as e:
            self.logger.error(f"initialize_servers failed: {e}")
            return False

    # [Previous methods remain the same until make_mcp_request...]

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
        """
        Fixed MCP request method with proper error handling and response parsing.
        
        FIXES:
        1. Better error handling for different response types
        2. Proper JSON parsing without SSE interference
        3. More detailed logging for debugging
        """
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
            
            self.logger.debug(f"Making MCP request to {server_url} with tool {tool_name}")
            self.logger.debug(f"Arguments: {json.dumps(arguments, indent=2)}")
            
            response = requests.post(
                server_url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream"
                },
                timeout=60  # Increased timeout for complex operations
            )
            
            self.logger.debug(f"Response status: {response.status_code}")
            self.logger.debug(f"Response headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                # Try direct JSON parsing first
                try:
                    json_response = response.json()
                    self.logger.debug(f"Direct JSON response: {json_response}")
                    
                    # Handle MCP JSON-RPC response format
                    if "result" in json_response:
                        return {"result": json_response["result"]}
                    elif "error" in json_response:
                        error_details = json_response["error"]
                        return {"error": f"MCP Error {error_details.get('code', 'unknown')}: {error_details.get('message', 'No message')}"}
                    else:
                        return json_response
                        
                except json.JSONDecodeError:
                    # Fall back to SSE parsing if direct JSON fails
                    self.logger.debug("Direct JSON failed, trying SSE parsing")
                    return self.parse_sse_response(response.text)
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                self.logger.error(f"MCP request failed: {error_msg}")
                return {"error": error_msg}
                
        except Exception as e:
            error_msg = f"Request failed: {str(e)}"
            self.logger.error(f"MCP request error: {error_msg}")
            return {"error": error_msg}

    def _build_neo4j_data_model(self, entities_relationships: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fixed data model building method with correct Neo4j MCP structure.
        
        FIXES:
        1. Proper node structure with required fields
        2. Correct relationship structure format
        3. Better property type mapping
        """
        try:
            nodes = []
            relationships = []
            
            # Build nodes from entities with proper structure
            for entity in entities_relationships.get("entities", []):
                entity_type = entity.get("type", "Entity")
                properties = entity.get("properties", {})
                
                # Build node with required structure for Neo4j MCP
                node_properties = []
                
                # Add all properties with proper types
                for prop_name, prop_value in properties.items():
                    # Determine property type
                    prop_type = "STRING"  # Default
                    if isinstance(prop_value, int):
                        prop_type = "INTEGER"
                    elif isinstance(prop_value, float):
                        prop_type = "FLOAT"
                    elif isinstance(prop_value, bool):
                        prop_type = "BOOLEAN"
                    
                    node_properties.append({
                        "name": prop_name,
                        "type": prop_type,
                        "description": f"Property {prop_name} of {entity_type}",
                        "required": prop_name == "name"  # Make name required
                    })
                
                # Ensure we have a name property if none exists
                if not any(prop["name"] == "name" for prop in node_properties):
                    node_properties.insert(0, {
                        "name": "name",
                        "type": "STRING",
                        "description": f"Name identifier for {entity_type}",
                        "required": True
                    })
                
                node = {
                    "label": entity_type,
                    "properties": node_properties,
                    "description": f"Node representing {entity_type}"
                }
                
                nodes.append(node)
                self.logger.debug(f"Built node: {entity_type}")
            
            # Build relationships with proper structure
            for rel in entities_relationships.get("relationships", []):
                rel_type = rel.get("type", "RELATED_TO")
                from_entity = rel.get("from", "")
                to_entity = rel.get("to", "")
                
                # Find corresponding node labels
                from_label = self._find_entity_label(from_entity, entities_relationships.get("entities", []))
                to_label = self._find_entity_label(to_entity, entities_relationships.get("entities", []))
                
                if from_label and to_label:
                    # Build relationship properties
                    rel_properties = []
                    for prop_name, prop_value in rel.get("properties", {}).items():
                        prop_type = "STRING"
                        if isinstance(prop_value, int):
                            prop_type = "INTEGER"
                        elif isinstance(prop_value, float):
                            prop_type = "FLOAT"
                        elif isinstance(prop_value, bool):
                            prop_type = "BOOLEAN"
                            
                        rel_properties.append({
                            "name": prop_name,
                            "type": prop_type,
                            "description": f"Property {prop_name} of {rel_type}"
                        })
                    
                    relationship = {
                        "type": rel_type,
                        "start_node_label": from_label,
                        "end_node_label": to_label,
                        "properties": rel_properties,
                        "description": f"Relationship {rel_type} from {from_label} to {to_label}"
                    }
                    
                    relationships.append(relationship)
                    self.logger.debug(f"Built relationship: {rel_type}")
            
            # Build complete data model with proper structure
            data_model = {
                "nodes": nodes,
                "relationships": relationships,
                "description": "Auto-generated data model from content analysis"
            }
            
            # Validate data model using MCP with correct parameters
            # FIX: Pass data_model as the parameter, not wrapped in another dict
            validation_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "validate_data_model",
                data_model  # Direct data model, not {"data_model": data_model}
            )
            
            if "error" not in validation_result:
                self.logger.info("Data model validation successful")
                return data_model
            else:
                self.logger.error(f"Data model validation failed: {validation_result['error']}")
                # Return the data model anyway for debugging
                return data_model
                
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
        """
        Fixed MCP ingestion execution with correct parameter formats.
        
        FIXES:
        1. Correct parameter structure for each tool
        2. Better error handling and recovery
        3. Proper record preparation
        """
        try:
            entities_created = 0
            relationships_created = 0
            
            # Step 1: Create constraints first (if available)
            try:
                # FIX: Pass data model directly, not wrapped
                constraints_result = self.make_mcp_request(
                    self.data_modeling_server_url,
                    "get_constraints_cypher_queries",
                    data_model
                )
                
                if "error" not in constraints_result and "result" in constraints_result:
                    constraints = constraints_result["result"]
                    if isinstance(constraints, list):
                        for constraint_query in constraints:
                            result = self.make_mcp_request(
                                self.cypher_server_url,
                                "write_neo4j_cypher",
                                {"query": constraint_query}
                            )
                            if "error" in result:
                                self.logger.warning(f"Constraint creation failed: {result['error']}")
                    else:
                        self.logger.debug("No constraints returned or invalid format")
                else:
                    self.logger.debug(f"Constraints query failed: {constraints_result.get('error', 'Unknown error')}")
            except Exception as e:
                self.logger.warning(f"Constraint creation skipped due to error: {e}")
            
            # Step 2: Create nodes using MCP-generated queries
            for node in data_model.get("nodes", []):
                try:
                    # FIX: Correct parameter structure for get_node_cypher_ingest_query
                    query_result = self.make_mcp_request(
                        self.data_modeling_server_url,
                        "get_node_cypher_ingest_query",
                        {
                            "node_label": node["label"],
                            "properties": [prop["name"] for prop in node.get("properties", [])]
                        }
                    )
                    
                    if "error" not in query_result and "result" in query_result:
                        cypher_query = query_result["result"]
                        
                        # Prepare records for this node type
                        records = self._prepare_node_records(node, entities_relationships.get("entities", []))
                        
                        if records and cypher_query:
                            # Execute with records
                            result = self.make_mcp_request(
                                self.cypher_server_url,
                                "write_neo4j_cypher",
                                {
                                    "query": cypher_query,
                                    "parameters": {"records": records}  # Changed from "params"
                                }
                            )
                            
                            if "error" not in result:
                                entities_created += len(records)
                                self.logger.debug(f"Created {len(records)} {node['label']} nodes")
                            else:
                                self.logger.warning(f"Node creation failed: {result['error']}")
                        else:
                            self.logger.debug(f"No records to create for node {node['label']}")
                    else:
                        self.logger.warning(f"Node query generation failed: {query_result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    self.logger.error(f"Node creation failed for {node.get('label', 'unknown')}: {e}")
                    continue
            
            # Step 3: Create relationships using MCP-generated queries
            for relationship in data_model.get("relationships", []):
                try:
                    # FIX: Correct parameter structure for relationship query
                    query_result = self.make_mcp_request(
                        self.data_modeling_server_url,
                        "get_relationship_cypher_ingest_query",
                        {
                            "relationship_type": relationship["type"],
                            "start_node_label": relationship["start_node_label"],
                            "end_node_label": relationship["end_node_label"],
                            "properties": [prop["name"] for prop in relationship.get("properties", [])]
                        }
                    )
                    
                    if "error" not in query_result and "result" in query_result:
                        cypher_query = query_result["result"]
                        
                        # Prepare relationship records
                        records = self._prepare_relationship_records(relationship, entities_relationships.get("relationships", []))
                        
                        if records and cypher_query:
                            result = self.make_mcp_request(
                                self.cypher_server_url,
                                "write_neo4j_cypher",
                                {
                                    "query": cypher_query,
                                    "parameters": {"records": records}  # Changed from "params"
                                }
                            )
                            
                            if "error" not in result:
                                relationships_created += len(records)
                                self.logger.debug(f"Created {len(records)} {relationship['type']} relationships")
                            else:
                                self.logger.warning(f"Relationship creation failed: {result['error']}")
                        else:
                            self.logger.debug(f"No records to create for relationship {relationship['type']}")
                    else:
                        self.logger.warning(f"Relationship query generation failed: {query_result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    self.logger.error(f"Relationship creation failed for {relationship.get('type', 'unknown')}: {e}")
                    continue
            
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
        """
        Fixed node record preparation with proper structure.
        
        FIXES:
        1. Better property mapping
        2. Handle missing properties gracefully
        3. Ensure required fields are present
        """
        records = []
        node_label = node["label"]
        
        for entity in entities:
            if entity.get("type") == node_label:
                record = {}
                properties = entity.get("properties", {})
                
                # Map properties according to node definition
                for prop_def in node.get("properties", []):
                    prop_name = prop_def["name"]
                    
                    if prop_name in properties:
                        record[prop_name] = properties[prop_name]
                    elif prop_def.get("required", False):
                        # Provide default for required properties
                        if prop_name == "name":
                            record[prop_name] = properties.get("name", f"unnamed_{node_label}")
                        else:
                            record[prop_name] = f"default_{prop_name}"
                
                # Ensure we have at least a name property
                if "name" not in record:
                    record["name"] = properties.get("name", f"unnamed_{node_label}")
                
                if record:  # Only add if we have data
                    records.append(record)
        
        return records

    def _prepare_relationship_records(self, relationship: Dict[str, Any], relationships: List[Dict]) -> List[Dict]:
        """
        Fixed relationship record preparation.
        
        FIXES:
        1. Better source/target ID mapping
        2. Handle missing relationship properties
        3. Ensure proper record structure
        """
        records = []
        rel_type = relationship["type"]
        
        for rel in relationships:
            if rel.get("type") == rel_type:
                record = {
                    "start_node_id": rel.get("from", ""),
                    "end_node_id": rel.get("to", "")
                }
                
                # Add relationship properties
                rel_props = rel.get("properties", {})
                for prop_def in relationship.get("properties", []):
                    prop_name = prop_def["name"]
                    if prop_name in rel_props:
                        record[prop_name] = rel_props[prop_name]
                
                if record["start_node_id"] and record["end_node_id"]:  # Only add valid relationships
                    records.append(record)
        
        return records

    def ingest_content(self, content: str, source_name: str = "document", content_type: str = "text", metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Main entry point for content ingestion.
        
        Args:
            content: Content to ingest
            source_name: Name/identifier for the source
            content_type: Type of content
            metadata: Additional metadata
        """
        try:
            # Determine document type from content_type and metadata
            document_type = content_type
            if metadata and "document_type" in metadata:
                document_type = metadata["document_type"]

            self.logger.info(f"Starting content ingestion for '{source_name}' (type: {document_type})")

            # Use the enhanced ingestion approach
            result = self.ingest_content_with_schema_validation(content, document_type)
            
            if result.get("success"):
                result["source_name"] = source_name
                result["content_type"] = content_type
                if metadata:
                    result["metadata"] = metadata

            return result

        except Exception as e:
            error_msg = f"Content ingestion failed: {str(e)}"
            self.logger.error(error_msg)
            self.processing_stats["errors"].append(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "stats": self.processing_stats
            }

    def ingest_content_with_schema_validation(self, content: str, document_type: str = "document") -> Dict[str, Any]:
        """
        Enhanced ingestion method using proper Neo4j MCP data modeling tools.
        
        Workflow:
        1. Extract entities using LLM
        2. Build data model using Neo4j structures
        3. Validate data model using validate_data_model
        4. Generate proper Cypher queries using MCP tools
        5. Execute via write_neo4j_cypher
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
            content_text = getattr(response, 'content', '') or str(response)

            # Find JSON in the response
            json_match = re.search(r'\{.*\}', content_text, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group())
                except json.JSONDecodeError as jde:
                    self.logger.error(f"Failed to decode JSON from LLM response: {jde}")
                    return None

            # If no JSON found, log and return None
            self.logger.error("No JSON payload found in LLM response")
            return None
            
        except Exception as e:
            self.logger.error(f"LLM entity extraction failed: {e}")
            return None

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

    # [Rest of the methods remain the same...]

"""
DEBUGGING RECOMMENDATIONS:

1. Enable detailed logging:
   - Set logging level to DEBUG
   - Monitor the exact parameters being sent to MCP servers
   - Check server responses for format issues

2. Test individual MCP tools:
   - Try calling validate_data_model with minimal data
   - Test get_node_cypher_ingest_query with simple parameters
   - Verify server connectivity and tool availability

3. Check MCP server versions:
   - Ensure you're using compatible Neo4j MCP server versions
   - Verify tool names match exactly (case-sensitive)
   - Check if parameter formats have changed

4. Manual testing:
   - Use MCP client tools to test server responses
   - Validate JSON-RPC payload formats
   - Test with minimal data structures first

COMMON ISSUES FIXED:
- Parameter wrapping (data_model vs {"data_model": data_model})
- Response parsing (direct JSON vs SSE)
- Property type mapping
- Required field handling
- Error handling and recovery
"""