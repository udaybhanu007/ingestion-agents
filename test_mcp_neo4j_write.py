#!/usr/bin/env python3
"""
Test Neo4j data writing using MCP servers
"""

import os
import json
import requests
import logging
import time
from typing import Dict, Any, List

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MCPNeo4jTester:
    """Test class for Neo4j data writing via MCP servers."""
    
    def __init__(self):
        logger.info("Initializing MCPNeo4jTester...")
        self.cypher_server_url = "http://127.0.0.1:8003/mcp/"
        self.data_modeling_server_url = "http://127.0.0.1:8004/mcp/"
        
        logger.info(f"Cypher server URL: {self.cypher_server_url}")
        logger.info(f"Data modeling server URL: {self.data_modeling_server_url}")
        
        # Test data
        self.test_entities = [
            {
                "type": "Person",
                "properties": {
                    "name": "John Doe",
                    "age": 35,
                    "occupation": "Software Engineer"
                }
            },
            {
                "type": "Person", 
                "properties": {
                    "name": "Jane Smith",
                    "age": 28,
                    "occupation": "Data Scientist"
                }
            },
            {
                "type": "Company",
                "properties": {
                    "name": "TechCorp",
                    "industry": "Technology",
                    "founded": 2010
                }
            }
        ]
        
        logger.info(f"Loaded {len(self.test_entities)} test entities")
        logger.debug(f"Test entities: {json.dumps(self.test_entities, indent=2)}")
        
        self.test_relationships = [
            {
                "from": "John Doe",
                "to": "TechCorp", 
                "type": "WORKS_AT",
                "properties": {
                    "since": "2020-01-01",
                    "position": "Senior Engineer"
                }
            },
            {
                "from": "Jane Smith",
                "to": "TechCorp",
                "type": "WORKS_AT", 
                "properties": {
                    "since": "2021-06-15",
                    "position": "Lead Data Scientist"
                }
            },
            {
                "from": "John Doe",
                "to": "Jane Smith",
                "type": "COLLABORATES_WITH",
                "properties": {
                    "projects": "AI Platform"
                }
            }
        ]

        logger.info(f"Loaded {len(self.test_relationships)} test relationships")
        logger.debug(f"Test relationships: {json.dumps(self.test_relationships, indent=2)}")
        logger.info("MCPNeo4jTester initialization completed")

    def make_mcp_request(self, server_url: str, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Make MCP request with proper headers and error handling."""
        logger.debug(f"Starting MCP request to {tool_name} at {server_url}")
        start_time = time.time()
        
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
            
            logger.info(f"Making MCP request to {tool_name}")
            logger.debug(f"Request payload: {json.dumps(payload, indent=2)}")
            logger.debug(f"Arguments: {json.dumps(arguments, indent=2)}")
            
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "User-Agent": "neo4j-test-tool/1.0"
            }
            
            logger.debug(f"Request headers: {headers}")
            
            response = requests.post(server_url, json=payload, headers=headers, timeout=60)
            
            elapsed_time = time.time() - start_time
            logger.info(f"Response status: {response.status_code} (took {elapsed_time:.2f}s)")
            logger.debug(f"Response headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                logger.debug(f"Response content-type: {content_type}")
                
                if 'text/event-stream' in content_type:
                    logger.debug("Processing SSE response")
                    return self.parse_sse_response(response.text)
                else:
                    logger.debug("Processing JSON response")
                    try:
                        json_response = response.json()
                        logger.debug(f"Parsed JSON response: {json.dumps(json_response, indent=2)}")
                        if "result" in json_response:
                            logger.debug("Found 'result' in JSON response")
                            return {"success": True, "result": json_response["result"]}
                        elif "error" in json_response:
                            error_details = json_response["error"]
                            error_msg = f"MCP Error {error_details.get('code', 'unknown')}: {error_details.get('message', 'No message')}"
                            logger.error(f"MCP error in response: {error_msg}")
                            return {"success": False, "error": error_msg}
                        else:
                            logger.debug("No 'result' or 'error' found, returning full response")
                            return {"success": True, "result": json_response}
                    except json.JSONDecodeError:
                        logger.warning("Failed to parse as JSON, trying SSE parser")
                        return self.parse_sse_response(response.text)
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                logger.error(f"HTTP error response: {error_msg}")
                logger.debug(f"Response content: {response.text[:500]}...")
                return {"success": False, "error": error_msg}
                
        except requests.exceptions.Timeout as e:
            error_msg = f"Request timeout: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Connection error: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(error_msg)
            logger.exception("Full exception details:")
            return {"success": False, "error": error_msg}

    def parse_sse_response(self, response_text: str) -> Dict[str, Any]:
        """Parse Server-Sent Events response format from MCP servers."""
        logger.debug("Starting SSE response parsing")
        logger.debug(f"Response text length: {len(response_text)}")
        
        try:
            lines = response_text.strip().split('\n')
            logger.debug(f"Split response into {len(lines)} lines")
            
            for i, line in enumerate(lines):
                logger.debug(f"Processing line {i}: {line[:100]}...")
                if line.startswith('data: '):
                    json_str = line[6:]
                    if json_str.strip() == '[DONE]':
                        logger.debug("Found [DONE] marker, skipping")
                        continue
                    try:
                        parsed_data = json.loads(json_str)
                        logger.debug(f"Parsed SSE data: {parsed_data}")
                        
                        if "result" in parsed_data:
                            logger.debug("Found 'result' in SSE data")
                            return {"success": True, "result": parsed_data["result"]}
                        elif "error" in parsed_data:
                            error_details = parsed_data["error"]
                            error_msg = f"MCP Error {error_details.get('code', 'unknown')}: {error_details.get('message', 'No message')}"
                            logger.error(f"Error in SSE data: {error_msg}")
                            return {"success": False, "error": error_msg}
                        else:
                            logger.debug("No 'result' or 'error' in SSE data, returning full data")
                            return {"success": True, "result": parsed_data}
                            
                    except json.JSONDecodeError as jde:
                        logger.debug(f"Failed to parse SSE JSON on line {i}: {jde}")
                        continue
            
            logger.debug("No SSE data found, trying direct JSON parse")
            try:
                parsed_data = json.loads(response_text)
                logger.debug(f"Direct JSON parse successful: {parsed_data}")
                if "result" in parsed_data:
                    return {"success": True, "result": parsed_data["result"]}
                elif "error" in parsed_data:
                    error_details = parsed_data["error"]
                    error_msg = f"MCP Error {error_details.get('code', 'unknown')}: {error_details.get('message', 'No message')}"
                    return {"success": False, "error": error_msg}
                else:
                    return {"success": True, "result": parsed_data}
            except json.JSONDecodeError:
                logger.warning("Direct JSON parse also failed")
                pass
            
            error_msg = f"Failed to parse response: {response_text[:200]}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
            
        except Exception as e:
            logger.error(f"Error parsing SSE response: {e}")
            logger.exception("Full exception details:")
            return {"success": False, "error": f"Failed to parse response: {str(e)}"}

    def build_data_model(self) -> Dict[str, Any]:
        """Build Neo4j data model from test entities and relationships."""
        logger.info("Starting data model construction")
        try:
            # Build nodes
            nodes = []
            logger.debug("Building node definitions...")
            
            # Person node
            person_node = {
                "label": "Person",
                "key_property": {
                    "name": "name",
                    "type": "STRING",
                    "description": "Property name of Person"
                },
                "properties": [
                    {"name": "name", "type": "STRING", "description": "Property name of Person"},
                    {"name": "age", "type": "INTEGER", "description": "Property age of Person"},
                    {"name": "occupation", "type": "STRING", "description": "Property occupation of Person"}
                ]
            }
            nodes.append(person_node)
            logger.debug("Added Person node definition")
            
            # Company node
            company_node = {
                "label": "Company",
                "key_property": {
                    "name": "name",
                    "type": "STRING", 
                    "description": "Property name of Company"
                },
                "properties": [
                    {"name": "name", "type": "STRING", "description": "Property name of Company"},
                    {"name": "industry", "type": "STRING", "description": "Property industry of Company"},
                    {"name": "founded", "type": "INTEGER", "description": "Property founded of Company"}
                ]
            }
            nodes.append(company_node)
            logger.debug("Added Company node definition")
            
            # Build relationships
            relationships = []
            logger.debug("Building relationship definitions...")
            
            # WORKS_AT relationship
            works_at_rel = {
                "type": "WORKS_AT",
                "start_node_label": "Person",
                "end_node_label": "Company",
                "key_property": {
                    "name": "since",
                    "type": "STRING",
                    "description": "Property since of WORKS_AT"
                },
                "properties": [
                    {"name": "since", "type": "STRING", "description": "Property since of WORKS_AT"},
                    {"name": "position", "type": "STRING", "description": "Property position of WORKS_AT"}
                ]
            }
            relationships.append(works_at_rel)
            logger.debug("Added WORKS_AT relationship definition")
            
            # COLLABORATES_WITH relationship
            collab_rel = {
                "type": "COLLABORATES_WITH",
                "start_node_label": "Person",
                "end_node_label": "Person",
                "key_property": {
                    "name": "projects",
                    "type": "STRING",
                    "description": "Property projects of COLLABORATES_WITH"
                },
                "properties": [
                    {"name": "projects", "type": "STRING", "description": "Property projects of COLLABORATES_WITH"}
                ]
            }
            relationships.append(collab_rel)
            logger.debug("Added COLLABORATES_WITH relationship definition")
            
            data_model = {
                "nodes": nodes,
                "relationships": relationships
            }
            
            logger.info(f"Built data model with {len(nodes)} node types and {len(relationships)} relationship types")
            logger.debug(f"Complete data model: {json.dumps(data_model, indent=2)}")
            return data_model
            
        except Exception as e:
            logger.error(f"Failed to build data model: {e}")
            logger.exception("Full exception details:")
            return None

    def test_create_nodes(self) -> bool:
        """Test creating nodes in Neo4j using MCP."""
        try:
            logger.info("=== Testing Node Creation ===")
            
            logger.info("Building data model for node creation test...")
            data_model = self.build_data_model()
            if not data_model:
                logger.error("Failed to build data model")
                return False
            
            # Test creating Person nodes
            logger.info("Testing Person node creation...")
            person_node = data_model["nodes"][0]  # Person node
            logger.debug(f"Person node definition: {json.dumps(person_node, indent=2)}")
            
            # Generate Cypher query for Person nodes
            logger.info("Generating Cypher query for Person nodes...")
            query_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "get_node_cypher_ingest_query",
                {"node": person_node}
            )
            
            if not query_result.get("success", False):
                logger.error(f"Failed to generate Person node query: {query_result.get('error', 'Unknown error')}")
                return False
            
            # Extract Cypher query
            logger.debug("Extracting Cypher query from MCP response...")
            result_data = query_result["result"]
            cypher_query = None
            
            if isinstance(result_data, dict) and "structuredContent" in result_data:
                structured_content = result_data["structuredContent"]
                if isinstance(structured_content, dict) and "result" in structured_content:
                    cypher_query = structured_content["result"]
                    logger.debug("Extracted query from structuredContent.result")
            elif isinstance(result_data, str):
                cypher_query = result_data
                logger.debug("Used result_data directly as string")
            elif isinstance(result_data, dict) and "content" in result_data:
                content = result_data["content"]
                if isinstance(content, list) and len(content) > 0:
                    first_content = content[0]
                    if isinstance(first_content, dict) and "text" in first_content:
                        cypher_query = first_content["text"]
                        logger.debug("Extracted query from content[0].text")
            
            if not cypher_query:
                logger.error("Could not extract Cypher query from MCP response")
                logger.debug(f"Raw MCP response: {json.dumps(result_data, indent=2)}")
                return False
            
            logger.info(f"Generated Cypher query: {cypher_query}")
            
            # Prepare Person records
            logger.info("Preparing Person records...")
            person_records = []
            for entity in self.test_entities:
                if entity["type"] == "Person":
                    record = {
                        "name": entity["properties"]["name"],
                        "age": entity["properties"]["age"],
                        "occupation": entity["properties"]["occupation"]
                    }
                    person_records.append(record)
                    logger.debug(f"Added Person record: {record}")
            
            logger.info(f"Person records to create: {json.dumps(person_records, indent=2)}")
            
            # Execute node creation
            logger.info("Executing Person node creation...")
            result = self.make_mcp_request(
                self.cypher_server_url,
                "write_neo4j_cypher",
                {
                    "query": cypher_query,
                    "params": {"records": person_records}
                }
            )
            
            if result.get("success", False):
                logger.info(f"✅ Successfully created {len(person_records)} Person nodes")
                logger.info(f"Result: {result.get('result', {})}")
            else:
                logger.error(f"❌ Failed to create Person nodes: {result.get('error', 'Unknown error')}")
                return False
            
            # Test creating Company nodes
            company_node = data_model["nodes"][1]  # Company node
            
            query_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "get_node_cypher_ingest_query",
                {"node": company_node}
            )
            
            if not query_result.get("success", False):
                logger.error(f"Failed to generate Company node query: {query_result.get('error', 'Unknown error')}")
                return False
            
            # Extract Company Cypher query
            result_data = query_result["result"]
            cypher_query = None
            
            if isinstance(result_data, dict) and "structuredContent" in result_data:
                structured_content = result_data["structuredContent"]
                if isinstance(structured_content, dict) and "result" in structured_content:
                    cypher_query = structured_content["result"]
            elif isinstance(result_data, str):
                cypher_query = result_data
            elif isinstance(result_data, dict) and "content" in result_data:
                content = result_data["content"]
                if isinstance(content, list) and len(content) > 0:
                    first_content = content[0]
                    if isinstance(first_content, dict) and "text" in first_content:
                        cypher_query = first_content["text"]
            
            if not cypher_query:
                logger.error("Could not extract Company Cypher query from MCP response")
                return False
            
            logger.info(f"Generated Company Cypher query: {cypher_query}")
            
            # Prepare Company records
            company_records = []
            for entity in self.test_entities:
                if entity["type"] == "Company":
                    record = {
                        "name": entity["properties"]["name"],
                        "industry": entity["properties"]["industry"],
                        "founded": entity["properties"]["founded"]
                    }
                    company_records.append(record)
            
            logger.info(f"Company records to create: {json.dumps(company_records, indent=2)}")
            
            # Execute Company node creation
            result = self.make_mcp_request(
                self.cypher_server_url,
                "write_neo4j_cypher",
                {
                    "query": cypher_query,
                    "params": {"records": company_records}
                }
            )
            
            if result.get("success", False):
                logger.info(f"✅ Successfully created {len(company_records)} Company nodes")
                logger.info(f"Result: {result.get('result', {})}")
                return True
            else:
                logger.error(f"❌ Failed to create Company nodes: {result.get('error', 'Unknown error')}")
                return False
                
        except Exception as e:
            logger.error(f"Node creation test failed: {e}")
            return False

    def test_create_relationships(self) -> bool:
        """Test creating relationships in Neo4j using MCP."""
        try:
            logger.info("=== Testing Relationship Creation ===")
            
            data_model = self.build_data_model()
            if not data_model:
                logger.error("Failed to build data model")
                return False
            
            # Test WORKS_AT relationships
            works_at_rel = data_model["relationships"][0]  # WORKS_AT relationship
            
            # Generate Cypher query for WORKS_AT relationships
            query_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "get_relationship_cypher_ingest_query",
                {
                    "data_model": data_model,
                    "relationship_type": "WORKS_AT",
                    "relationship_start_node_label": "Person",
                    "relationship_end_node_label": "Company"
                }
            )
            
            if not query_result.get("success", False):
                logger.error(f"Failed to generate WORKS_AT relationship query: {query_result.get('error', 'Unknown error')}")
                return False
            
            # Extract Cypher query
            result_data = query_result["result"]
            cypher_query = None
            
            if isinstance(result_data, dict) and "structuredContent" in result_data:
                structured_content = result_data["structuredContent"]
                if isinstance(structured_content, dict) and "result" in structured_content:
                    cypher_query = structured_content["result"]
            elif isinstance(result_data, str):
                cypher_query = result_data
            elif isinstance(result_data, dict) and "content" in result_data:
                content = result_data["content"]
                if isinstance(content, list) and len(content) > 0:
                    first_content = content[0]
                    if isinstance(first_content, dict) and "text" in first_content:
                        cypher_query = first_content["text"]
            
            if not cypher_query:
                logger.error("Could not extract WORKS_AT Cypher query from MCP response")
                return False
            
            logger.info(f"Generated WORKS_AT Cypher query: {cypher_query}")
            
            # Prepare WORKS_AT relationship records
            works_at_records = []
            for rel in self.test_relationships:
                if rel["type"] == "WORKS_AT":
                    record = {
                        "sourceId": rel["from"],
                        "targetId": rel["to"],
                        "since": rel["properties"]["since"],
                        "position": rel["properties"]["position"]
                    }
                    works_at_records.append(record)
            
            logger.info(f"WORKS_AT records to create: {json.dumps(works_at_records, indent=2)}")
            
            # Execute WORKS_AT relationship creation
            result = self.make_mcp_request(
                self.cypher_server_url,
                "write_neo4j_cypher",
                {
                    "query": cypher_query,
                    "params": {"records": works_at_records}
                }
            )
            
            if result.get("success", False):
                logger.info(f"✅ Successfully created {len(works_at_records)} WORKS_AT relationships")
                logger.info(f"Result: {result.get('result', {})}")
            else:
                logger.error(f"❌ Failed to create WORKS_AT relationships: {result.get('error', 'Unknown error')}")
                return False
            
            # Test COLLABORATES_WITH relationships
            collab_rel = data_model["relationships"][1]  # COLLABORATES_WITH relationship
            
            query_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "get_relationship_cypher_ingest_query",
                {
                    "data_model": data_model,
                    "relationship_type": "COLLABORATES_WITH",
                    "relationship_start_node_label": "Person",
                    "relationship_end_node_label": "Person"
                }
            )
            
            if not query_result.get("success", False):
                logger.error(f"Failed to generate COLLABORATES_WITH relationship query: {query_result.get('error', 'Unknown error')}")
                return False
            
            # Extract Cypher query
            result_data = query_result["result"]
            cypher_query = None
            
            if isinstance(result_data, dict) and "structuredContent" in result_data:
                structured_content = result_data["structuredContent"]
                if isinstance(structured_content, dict) and "result" in structured_content:
                    cypher_query = structured_content["result"]
            elif isinstance(result_data, str):
                cypher_query = result_data
            elif isinstance(result_data, dict) and "content" in result_data:
                content = result_data["content"]
                if isinstance(content, list) and len(content) > 0:
                    first_content = content[0]
                    if isinstance(first_content, dict) and "text" in first_content:
                        cypher_query = first_content["text"]
            
            if not cypher_query:
                logger.error("Could not extract COLLABORATES_WITH Cypher query from MCP response")
                return False
            
            logger.info(f"Generated COLLABORATES_WITH Cypher query: {cypher_query}")
            
            # Prepare COLLABORATES_WITH relationship records
            collab_records = []
            for rel in self.test_relationships:
                if rel["type"] == "COLLABORATES_WITH":
                    record = {
                        "sourceId": rel["from"],
                        "targetId": rel["to"],
                        "projects": rel["properties"]["projects"]
                    }
                    collab_records.append(record)
            
            logger.info(f"COLLABORATES_WITH records to create: {json.dumps(collab_records, indent=2)}")
            
            # Execute COLLABORATES_WITH relationship creation
            result = self.make_mcp_request(
                self.cypher_server_url,
                "write_neo4j_cypher",
                {
                    "query": cypher_query,
                    "params": {"records": collab_records}
                }
            )
            
            if result.get("success", False):
                logger.info(f"✅ Successfully created {len(collab_records)} COLLABORATES_WITH relationships")
                logger.info(f"Result: {result.get('result', {})}")
                return True
            else:
                logger.error(f"❌ Failed to create COLLABORATES_WITH relationships: {result.get('error', 'Unknown error')}")
                return False
                
        except Exception as e:
            logger.error(f"Relationship creation test failed: {e}")
            return False

    def test_mcp_servers_connectivity(self) -> bool:
        """Test MCP servers connectivity."""
        try:
            logger.info("=== Testing MCP Server Connectivity ===")
            
            servers = [
                ("Cypher Server", self.cypher_server_url),
                ("Data Modeling Server", self.data_modeling_server_url)
            ]
            
            all_reachable = True
            
            for name, url in servers:
                logger.info(f"Testing connectivity to {name} at {url}")
                try:
                    payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
                    logger.debug(f"Sending connectivity test payload: {payload}")
                    
                    start_time = time.time()
                    resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=5)
                    elapsed_time = time.time() - start_time
                    
                    logger.debug(f"Connectivity test response: status={resp.status_code}, time={elapsed_time:.2f}s")
                    
                    if resp.status_code in (200, 400, 405, 406):
                        logger.info(f"✅ {name} is reachable at {url} (status {resp.status_code})")
                    else:
                        logger.warning(f"⚠️ {name} responded with status {resp.status_code}")
                        logger.debug(f"Response content: {resp.text[:200]}...")
                        
                except Exception as e:
                    logger.error(f"❌ {name} is not reachable: {e}")
                    all_reachable = False
            
            if all_reachable:
                logger.info("All MCP servers are reachable")
            else:
                logger.error("Some MCP servers are not reachable")
            
            return all_reachable
            
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            logger.exception("Full exception details:")
            return False

    def run_all_tests(self):
        """Run all tests."""
        test_start_time = time.time()
        try:
            logger.info("🚀 Starting Neo4j MCP Write Tests")
            logger.info("=" * 50)
            
            # Test connectivity
            logger.info("STEP 1: Testing MCP server connectivity...")
            if not self.test_mcp_servers_connectivity():
                logger.error("❌ MCP server connectivity test failed")
                return False
            logger.info("✅ MCP server connectivity test passed")
            
            # Test node creation
            logger.info("STEP 2: Testing node creation...")
            if not self.test_create_nodes():
                logger.error("❌ Node creation test failed")
                return False
            logger.info("✅ Node creation test passed")
            
            # Test relationship creation
            logger.info("STEP 3: Testing relationship creation...")
            if not self.test_create_relationships():
                logger.error("❌ Relationship creation test failed")
                return False
            logger.info("✅ Relationship creation test passed")
            
            total_elapsed = time.time() - test_start_time
            logger.info("=" * 50)
            logger.info(f"🎉 All tests completed successfully in {total_elapsed:.2f} seconds!")
            return True
            
        except Exception as e:
            total_elapsed = time.time() - test_start_time
            logger.error(f"Test execution failed after {total_elapsed:.2f} seconds: {e}")
            logger.exception("Full exception details:")
            return False


def main():
    """Main function to run the tests."""
    logger.info("=" * 60)
    logger.info("STARTING NEO4J MCP WRITE TESTS")
    logger.info("=" * 60)
    
    try:
        logger.info("Initializing test suite...")
        tester = MCPNeo4jTester()
        
        logger.info("Running test suite...")
        success = tester.run_all_tests()
        
        if success:
            logger.info("=" * 60)
            logger.info("✅ SUCCESS: All Neo4j MCP write tests passed!")
            logger.info("=" * 60)
        else:
            logger.error("=" * 60)
            logger.error("❌ FAILED: Some tests failed")
            logger.error("=" * 60)
            
        return success
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"Main execution failed: {e}")
        logger.exception("Full exception details:")
        logger.error("=" * 60)
        return False


if __name__ == "__main__":
    main()
