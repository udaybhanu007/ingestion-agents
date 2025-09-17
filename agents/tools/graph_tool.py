"""
Neo4j Graph Ingestion Tool using MCP Servers
"""

import logging
import json
import sys
import os
import requests
import re
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
                    "azure_api_key": os.getenv("AZURE_OPENAI_API_KEY", "")
                }
            }
            return configs.get(section, {}).get(key) if key else configs.get(section, {})
        
        @property
        def max_content_length(self):
            return 50000
    
    config_manager = MockConfig()
    logger = logging.getLogger(__name__)


class GraphIngestionTool:
    """Tool for ingesting content into Neo4j graph database using MCP servers."""
    
    def __init__(self):
        self.config = config_manager
        self.logger = logger
        
        # Initialize Azure OpenAI client
        openai_config = self.config.get_config('openai')
        self.llm = AzureChatOpenAI(
            azure_deployment=openai_config.get('deployment_name'),
            api_version=openai_config.get('azure_api_version'),
            azure_endpoint=openai_config.get('azure_endpoint'),
            api_key=SecretStr(openai_config.get('azure_api_key', ''))
        )
        
        # MCP server configurations
        self.cypher_server_url = "http://127.0.0.1:8003/mcp/"
        self.data_modeling_server_url = "http://127.0.0.1:8004/mcp/" 
        
        # Track processing state
        self.processing_stats = {
            "documents_processed": 0,
            "schemas_generated": 0,
            "entities_ingested": 0,
            "relationships_created": 0,
            "errors": []
        }
        
        self.logger.info("GraphIngestionTool initialized")

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
        """Parse Server-Sent Events response format from MCP servers."""
        try:
            # Split by lines and find data lines
            lines = response_text.strip().split('\n')
            
            for line in lines:
                if line.startswith('data: '):
                    json_str = line[6:]  # Remove 'data: ' prefix
                    if json_str.strip() == '[DONE]':
                        continue
                    try:
                        parsed_data = json.loads(json_str)
                        self.logger.debug(f"Parsed SSE data: {parsed_data}")
                        
                        # Handle MCP JSON-RPC response format from SSE
                        if "result" in parsed_data:
                            return {"success": True, "result": parsed_data["result"]}
                        elif "error" in parsed_data:
                            error_details = parsed_data["error"]
                            error_msg = f"MCP Error {error_details.get('code', 'unknown')}: {error_details.get('message', 'No message')}"
                            return {"success": False, "error": error_msg}
                        else:
                            return {"success": True, "result": parsed_data}
                            
                    except json.JSONDecodeError as jde:
                        self.logger.debug(f"Failed to parse SSE JSON: {jde}")
                        continue
            
            # If no valid SSE format found, try parsing as direct JSON
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
            except json.JSONDecodeError:
                pass
            
            return {"success": False, "error": f"Failed to parse response: {response_text[:200]}"}
            
        except Exception as e:
            self.logger.error(f"Error parsing SSE response: {e}")
            return {"success": False, "error": f"Failed to parse response: {str(e)}"}

    def make_mcp_request(self, server_url: str, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Make MCP request with proper headers and error handling."""
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
            
            self.logger.info(f"Making MCP request to {tool_name}")
            self.logger.debug(f"Arguments: {json.dumps(arguments, indent=2)}")
            
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "User-Agent": "neo4j-graph-tool/1.0"
            }
            
            response = requests.post(server_url, json=payload, headers=headers, timeout=60)
            
            self.logger.debug(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                
                if 'text/event-stream' in content_type:
                    return self.parse_sse_response(response.text)
                else:
                    try:
                        json_response = response.json()
                        if "result" in json_response:
                            return {"success": True, "result": json_response["result"]}
                        elif "error" in json_response:
                            error_details = json_response["error"]
                            error_msg = f"MCP Error {error_details.get('code', 'unknown')}: {error_details.get('message', 'No message')}"
                            return {"success": False, "error": error_msg}
                        else:
                            return {"success": True, "result": json_response}
                    except json.JSONDecodeError:
                        return self.parse_sse_response(response.text)
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                self.logger.error(error_msg)
                return {"success": False, "error": error_msg}
                
        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {str(e)}"
            self.logger.error(error_msg)
            return {"success": False, "error": error_msg}
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            self.logger.error(error_msg)
            return {"success": False, "error": error_msg}

    def _build_neo4j_data_model(self, entities_relationships: Dict[str, Any]) -> Dict[str, Any]:
        """
        FIXED data model building method with correct Neo4j MCP structure.
        
        Based on the actual MCP schema, nodes require:
        - label (string)
        - key_property (Property object)
        - properties (array of Property objects)
        
        Relationships require:
        - type (string)
        - start_node_label (string)  
        - end_node_label (string)
        - key_property (optional Property object)
        - properties (array of Property objects)
        """
        try:
            # Use dictionaries to deduplicate by label/type
            nodes_by_label = {}
            relationships_by_type = {}
            
            # Build nodes from entities with proper MCP schema structure and deduplication
            for entity in entities_relationships.get("entities", []):
                entity_type = entity.get("type", "Entity")
                properties = entity.get("properties", {})
                
                # Check if we already have a node for this label
                if entity_type in nodes_by_label:
                    # Merge properties from this entity into existing node
                    existing_node = nodes_by_label[entity_type]
                    existing_prop_names = {prop["name"] for prop in existing_node["properties"]}
                    
                    # Add new properties that don't already exist
                    for prop_name, prop_value in properties.items():
                        if prop_name not in existing_prop_names:
                            # Determine property type
                            prop_type = "STRING"  # Default
                            if isinstance(prop_value, int):
                                prop_type = "INTEGER"
                            elif isinstance(prop_value, float):
                                prop_type = "FLOAT"
                            elif isinstance(prop_value, bool):
                                prop_type = "BOOLEAN"
                            
                            property_obj = {
                                "name": prop_name,
                                "type": prop_type,
                                "description": f"Property {prop_name} of {entity_type}"
                            }
                            
                            existing_node["properties"].append(property_obj)
                    
                    self.logger.debug(f"Merged properties into existing MCP node: {entity_type}")
                    continue
                
                # Build node properties according to MCP schema
                node_properties = []
                key_property = None
                
                # Create properties according to MCP Property schema
                for prop_name, prop_value in properties.items():
                    # Determine property type
                    prop_type = "STRING"  # Default
                    if isinstance(prop_value, int):
                        prop_type = "INTEGER"
                    elif isinstance(prop_value, float):
                        prop_type = "FLOAT"
                    elif isinstance(prop_value, bool):
                        prop_type = "BOOLEAN"
                    
                    property_obj = {
                        "name": prop_name,
                        "type": prop_type,
                        "description": f"Property {prop_name} of {entity_type}"
                    }
                    
                    node_properties.append(property_obj)
                    
                    # Use the first property or 'name' as key property
                    if prop_name == "name" or key_property is None:
                        key_property = property_obj
                
                # Ensure we have a key property (required by MCP schema)
                if key_property is None:
                    key_property = {
                        "name": "name",
                        "type": "STRING",
                        "description": f"Name identifier for {entity_type}"
                    }
                    node_properties.insert(0, key_property)
                
                # Build node according to MCP Node schema
                node = {
                    "label": entity_type,
                    "key_property": key_property,  # REQUIRED by MCP schema
                    "properties": node_properties
                }
                
                nodes_by_label[entity_type] = node
                self.logger.debug(f"Built new MCP node: {entity_type}")
            
            # Build relationships with proper MCP schema structure and deduplication
            for rel in entities_relationships.get("relationships", []):
                rel_type = rel.get("type", "RELATED_TO")
                from_entity = rel.get("from", "")
                to_entity = rel.get("to", "")
                
                # Find corresponding node labels
                from_label = self._find_entity_label(from_entity, entities_relationships.get("entities", []))
                to_label = self._find_entity_label(to_entity, entities_relationships.get("entities", []))
                
                if from_label and to_label:
                    # Create unique relationship key
                    rel_key = f"{rel_type}_{from_label}_{to_label}"
                    
                    # Check if we already have this relationship type combination
                    if rel_key in relationships_by_type:
                        # Merge properties from this relationship into existing relationship
                        existing_rel = relationships_by_type[rel_key]
                        existing_prop_names = {prop["name"] for prop in existing_rel["properties"]}
                        
                        # Add new properties that don't already exist
                        for prop_name, prop_value in rel.get("properties", {}).items():
                            if prop_name not in existing_prop_names:
                                prop_type = "STRING"
                                if isinstance(prop_value, int):
                                    prop_type = "INTEGER"
                                elif isinstance(prop_value, float):
                                    prop_type = "FLOAT"
                                elif isinstance(prop_value, bool):
                                    prop_type = "BOOLEAN"
                                    
                                property_obj = {
                                    "name": prop_name,
                                    "type": prop_type,
                                    "description": f"Property {prop_name} of {rel_type}"
                                }
                                
                                existing_rel["properties"].append(property_obj)
                        
                        self.logger.debug(f"Merged properties into existing MCP relationship: {rel_type}")
                        continue
                    
                    # Build relationship properties according to MCP schema
                    rel_properties = []
                    key_property = None
                    
                    for prop_name, prop_value in rel.get("properties", {}).items():
                        prop_type = "STRING"
                        if isinstance(prop_value, int):
                            prop_type = "INTEGER"
                        elif isinstance(prop_value, float):
                            prop_type = "FLOAT"
                        elif isinstance(prop_value, bool):
                            prop_type = "BOOLEAN"
                            
                        property_obj = {
                            "name": prop_name,
                            "type": prop_type,
                            "description": f"Property {prop_name} of {rel_type}"
                        }
                        
                        rel_properties.append(property_obj)
                        
                        # First property can be key property
                        if key_property is None:
                            key_property = property_obj
                    
                    # Build relationship according to MCP Relationship schema
                    relationship = {
                        "type": rel_type,
                        "start_node_label": from_label,
                        "end_node_label": to_label,
                        "properties": rel_properties
                    }
                    
                    # Add key_property if we have one (optional for relationships)
                    if key_property:
                        relationship["key_property"] = key_property
                    
                    relationships_by_type[rel_key] = relationship
                    self.logger.debug(f"Built new MCP relationship: {rel_type}")
            
            # Convert dictionaries to lists
            nodes = list(nodes_by_label.values())
            relationships = list(relationships_by_type.values())
            
            # Build complete data model with proper MCP DataModel schema
            data_model = {
                "nodes": nodes,
                "relationships": relationships
            }
            
            # Log summary
            self.logger.info(f"Built data model with {len(nodes)} unique node types and {len(relationships)} unique relationship types")
            
            # Validate data model using MCP validate_data_model tool
            validation_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "validate_data_model",
                {"data_model": data_model}  # Pass as object, not JSON string
            )
            
            if validation_result.get("success", False):
                self.logger.info("MCP data model validation successful")
                validation_details = validation_result.get("result", {})
                self.logger.info(f"Validation result: {validation_details}")
                return data_model
            else:
                error_msg = validation_result.get('error', 'Unknown error')
                self.logger.error(f"MCP data model validation failed: {error_msg}")
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
                # Use correct parameter structure for get_constraints_cypher_queries
                constraints_result = self.make_mcp_request(
                    self.data_modeling_server_url,
                    "get_constraints_cypher_queries",
                    {"data_model": data_model}  # Pass as object, not JSON string
                )
                
                if constraints_result.get("success", False) and "result" in constraints_result:
                    constraints = constraints_result["result"]
                    self.logger.info(f"Retrieved {len(constraints) if isinstance(constraints, list) else 0} constraint queries")
                    
                    if isinstance(constraints, list):
                        for constraint_query in constraints:
                            result = self.make_mcp_request(
                                self.cypher_server_url,
                                "write_neo4j_cypher",
                                {"query": constraint_query}
                            )
                            if result.get("success", False):
                                self.logger.info("Constraint created successfully")
                            else:
                                self.logger.warning(f"Constraint creation failed: {result.get('error', 'Unknown error')}")
                    else:
                        self.logger.debug("No constraints returned or invalid format")
                else:
                    error_msg = constraints_result.get('error', 'Unknown error')
                    self.logger.warning(f"Constraints query failed: {error_msg}")
            except Exception as e:
                self.logger.warning(f"Constraint creation skipped due to error: {e}")
            
            # Step 2: Create nodes using MCP-generated queries
            for node in data_model.get("nodes", []):
                try:
                    # Use correct parameter structure for get_node_cypher_ingest_query
                    query_result = self.make_mcp_request(
                        self.data_modeling_server_url,
                        "get_node_cypher_ingest_query",
                        {"node": node}  # Pass as object, not JSON string
                    )
                    
                    if query_result.get("success", False) and "result" in query_result:
                        # Extract Cypher query from nested MCP response structure
                        result_data = query_result["result"]
                        cypher_query = None
                        
                        # Check for structuredContent first (preferred format)
                        if isinstance(result_data, dict) and "structuredContent" in result_data:
                            structured_content = result_data["structuredContent"]
                            if isinstance(structured_content, dict) and "result" in structured_content:
                                cypher_query = structured_content["result"]
                        
                        # Fallback: check for direct string result
                        if not cypher_query and isinstance(result_data, str):
                            cypher_query = result_data
                        
                        # Fallback: check for content array format
                        if not cypher_query and isinstance(result_data, dict) and "content" in result_data:
                            content = result_data["content"]
                            if isinstance(content, list) and len(content) > 0:
                                first_content = content[0]
                                if isinstance(first_content, dict) and "text" in first_content:
                                    cypher_query = first_content["text"]
                        
                        if cypher_query:
                            self.logger.info(f"Generated Cypher query for {node['label']} nodes")
                            self.logger.debug(f"Query: {cypher_query}")
                            
                            # Prepare records for this node type
                            records = self._prepare_node_records(node, entities_relationships.get("entities", []))
                            
                            if records and cypher_query:
                                # Execute with records - use "params" as expected by mcp-neo4j-cypher
                                result = self.make_mcp_request(
                                    self.cypher_server_url,
                                    "write_neo4j_cypher",
                                    {
                                        "query": cypher_query,
                                        "params": {"records": records}
                                    }
                                )
                                
                                if result.get("success", False):
                                    entities_created += len(records)
                                    result_details = result.get("result", {})
                                    self.logger.info(f"Successfully created {len(records)} {node['label']} nodes")
                                    self.logger.debug(f"Creation result: {result_details}")
                                else:
                                    error_msg = result.get('error', 'Unknown error')
                                    self.logger.error(f"Node creation failed for {node['label']}: {error_msg}")
                            else:
                                self.logger.warning(f"No records to create for node {node['label']}")
                        else:
                            self.logger.error(f"Could not extract Cypher query from MCP response for {node['label']}")
                            self.logger.debug(f"Raw response: {json.dumps(result_data)}")
                    else:
                        error_msg = query_result.get('error', 'Unknown error')
                        self.logger.error(f"Node query generation failed for {node['label']}: {error_msg}")
                        
                except Exception as e:
                    self.logger.error(f"Node creation failed for {node.get('label', 'unknown')}: {e}")
                    continue
            
            # Step 3: Create relationships using MCP-generated queries
            for relationship in data_model.get("relationships", []):
                try:
                    # Find the corresponding nodes for this relationship
                    start_node = None
                    end_node = None
                    for node in data_model.get("nodes", []):
                        if node["label"] == relationship["start_node_label"]:
                            start_node = node
                        if node["label"] == relationship["end_node_label"]:
                            end_node = node
                    
                    if start_node and end_node:
                        # Use correct parameter structure for get_relationship_cypher_ingest_query
                        # Required: data_model, relationship_type, relationship_start_node_label, relationship_end_node_label
                        data_model_snippet = {
                            "nodes": [start_node, end_node],
                            "relationships": [relationship]
                        }
                        
                        query_result = self.make_mcp_request(
                            self.data_modeling_server_url,
                            "get_relationship_cypher_ingest_query",
                            {
                                "data_model": data_model_snippet,  # Pass as object, not JSON string
                                "relationship_type": relationship["type"],     # Required: relationship type
                                "relationship_start_node_label": relationship["start_node_label"],  # Required: start node label
                                "relationship_end_node_label": relationship["end_node_label"]       # Required: end node label
                            }
                        )
                        
                        if query_result.get("success", False) and "result" in query_result:
                            # Extract Cypher query from nested MCP response structure
                            result_data = query_result["result"]
                            cypher_query = None
                            
                            # Check for structuredContent first (preferred format)
                            if isinstance(result_data, dict) and "structuredContent" in result_data:
                                structured_content = result_data["structuredContent"]
                                if isinstance(structured_content, dict) and "result" in structured_content:
                                    cypher_query = structured_content["result"]
                            
                            # Fallback: check for direct string result
                            if not cypher_query and isinstance(result_data, str):
                                cypher_query = result_data
                            
                            # Fallback: check for content array format
                            if not cypher_query and isinstance(result_data, dict) and "content" in result_data:
                                content = result_data["content"]
                                if isinstance(content, list) and len(content) > 0:
                                    first_content = content[0]
                                    if isinstance(first_content, dict) and "text" in first_content:
                                        cypher_query = first_content["text"]
                            
                            if cypher_query:
                                self.logger.info(f"Generated Cypher query for {relationship['type']} relationships")
                                self.logger.debug(f"Query: {cypher_query}")
                                
                                # Prepare relationship records
                                records = self._prepare_relationship_records(relationship, entities_relationships.get("relationships", []))
                                
                                if records and cypher_query:
                                    # Execute with records - use "params" as expected by mcp-neo4j-cypher
                                    result = self.make_mcp_request(
                                        self.cypher_server_url,
                                        "write_neo4j_cypher",
                                        {
                                            "query": cypher_query,
                                            "params": {"records": records}
                                        }
                                    )
                                    
                                    if result.get("success", False):
                                        relationships_created += len(records)
                                        result_details = result.get("result", {})
                                        self.logger.info(f"Successfully created {len(records)} {relationship['type']} relationships")
                                        self.logger.debug(f"Creation result: {result_details}")
                                    else:
                                        error_msg = result.get('error', 'Unknown error')
                                        self.logger.error(f"Relationship creation failed for {relationship['type']}: {error_msg}")
                                else:
                                    self.logger.warning(f"No records to create for relationship {relationship['type']}")
                            else:
                                self.logger.error(f"Could not extract Cypher query from MCP response for {relationship['type']}")
                                self.logger.debug(f"Raw response: {json.dumps(result_data)}")
                        else:
                            error_msg = query_result.get('error', 'Unknown error')
                            self.logger.error(f"Relationship query generation failed for {relationship['type']}: {error_msg}")
                    else:
                        self.logger.warning(f"Could not find start_node ({relationship['start_node_label']}) or end_node ({relationship['end_node_label']}) for relationship {relationship['type']}")
                        
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
        FIXED node record preparation matching MCP expectations.
        
        The MCP-generated Cypher queries expect records with the exact property names
        as defined in the node's property schema.
        """
        records = []
        node_label = node["label"]
        key_property_name = node.get("key_property", {}).get("name", "name")
        
        for entity in entities:
            if entity.get("type") == node_label:
                record = {}
                properties = entity.get("properties", {})
                
                # Map properties according to node definition
                for prop_def in node.get("properties", []):
                    prop_name = prop_def["name"]
                    
                    if prop_name in properties:
                        record[prop_name] = properties[prop_name]
                    elif prop_name == key_property_name:
                        # Ensure key property has a value
                        record[prop_name] = properties.get("name", f"unnamed_{node_label}_{len(records)}")
                
                # Ensure we have the key property
                if key_property_name not in record:
                    record[key_property_name] = properties.get("name", f"unnamed_{node_label}_{len(records)}")
                
                if record:  # Only add if we have data
                    records.append(record)
                    self.logger.debug(f"Prepared node record: {record}")
        
        return records

    def _prepare_relationship_records(self, relationship: Dict[str, Any], relationships: List[Dict]) -> List[Dict]:
        """
        FIXED relationship record preparation matching MCP expectations.
        
        The MCP-generated Cypher queries expect records with sourceId and targetId
        for connecting nodes, plus any relationship properties.
        """
        records = []
        rel_type = relationship["type"]
        
        for rel in relationships:
            if rel.get("type") == rel_type:
                record = {
                    "sourceId": rel.get("from", ""),  # MCP expects sourceId/targetId
                    "targetId": rel.get("to", "")
                }
                
                # Add relationship properties
                rel_props = rel.get("properties", {})
                for prop_def in relationship.get("properties", []):
                    prop_name = prop_def["name"]
                    if prop_name in rel_props:
                        record[prop_name] = rel_props[prop_name]
                
                if record["sourceId"] and record["targetId"]:  # Only add valid relationships
                    records.append(record)
                    self.logger.debug(f"Prepared relationship record: {record}")
        
        return records

    def ingest_content(self, content: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Main entry point for content ingestion.
        
        Args:
            content: Content to ingest
            source_name: Name/identifier for the source
            content_type: Type of content
            metadata: Additional metadata
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
                "stats": self.processing_stats
            }

    def ingest_content_with_schema_validation(self, content: str) -> Dict[str, Any]:
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
            self.logger.info(f"Starting enhanced Neo4j ingestion")
            
            # Step 1: Extract entities and relationships using LLM
            self.logger.info("Step 1: Extracting entities using LLM...")
            print("Document content:", content)
            entities_relationships = self._extract_entities_with_llm(content)
            
            if not entities_relationships:
                return {
                    "success": False,
                    "error": "Failed to extract entities from content",
                    "stats": self.processing_stats
                }
            print("Extracted Entities and Relationships:", json.dumps(entities_relationships))
            
            # Step 2: Build and validate Neo4j data model
            self.logger.info("Step 2: Building and validating data model...")
            data_model = self._build_neo4j_data_model(entities_relationships)
            
            if not data_model:
                return {
                    "success": False,
                    "error": "Failed to build valid data model",
                    "stats": self.processing_stats
                }
            print("Data Model:", json.dumps(data_model))
            
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

    def _extract_entities_with_llm(self, content: str) -> Dict[str, Any]:
        """Extract entities and relationships using LLM."""
        try:
            prompt = f"""
            Analyze the following content and extract entities and relationships.            
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
            
            IMPORTANT GUIDELINES:
            1. Focus on key concepts, people, organizations, locations, and their relationships
            2. Make entity types PascalCase (e.g., Person, Organization, Location, Concept)
            3. Make relationship types SCREAMING_SNAKE_CASE (e.g., WORKS_AT, LOCATED_IN, RELATED_TO)
            4. Use GENERIC entity types when possible:
               - Use "Person" for all people (not PersonA, PersonB, etc.)
               - Use "Organization" for all organizations
               - Use "Location" for all places
               - Use "Document" for all documents
               - Use "Concept" for abstract ideas or topics
            5. Make each entity unique by using different "name" properties, NOT different types
            6. Entity names in "from" and "to" relationships should match the "name" property of entities
            7. Keep entity types broad and general to avoid duplication
            
            Example:
            {{
                "entities": [
                    {{"type": "Person", "properties": {{"name": "John Smith", "role": "Engineer"}}}},
                    {{"type": "Person", "properties": {{"name": "Mary Johnson", "role": "Manager"}}}},
                    {{"type": "Organization", "properties": {{"name": "TechCorp", "industry": "Technology"}}}}
                ],
                "relationships": [
                    {{"from": "John Smith", "to": "TechCorp", "type": "WORKS_AT", "properties": {{"since": "2020"}}}},
                    {{"from": "Mary Johnson", "to": "John Smith", "type": "MANAGES", "properties": {{}}}}
                ]
            }}
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