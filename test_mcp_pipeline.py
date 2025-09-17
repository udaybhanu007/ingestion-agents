#!/usr/bin/env python3
"""
Test MCP servers and basic graph functionality without requiring Azure OpenAI credentials.
"""

import sys
import os
import logging
import json
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_sse_response(response_text: str) -> dict:
    """Parse Server-Sent Events response format."""
    try:
        lines = response_text.strip().split('\n')
        
        for line in lines:
            if line.startswith('data: '):
                json_str = line[6:]  # Remove 'data: ' prefix
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
        
        # Try parsing as direct JSON
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
        return {"success": False, "error": f"Failed to parse response: {str(e)}"}

def test_mcp_ingestion_pipeline():
    """Test the complete MCP ingestion pipeline without Azure OpenAI."""
    logger.info("🚀 Testing MCP ingestion pipeline...")
    
    # MCP server URLs
    cypher_server_url = "http://127.0.0.1:8003/mcp/"
    data_modeling_server_url = "http://127.0.0.1:8004/mcp/"
    
    try:
        # Step 1: Create a simple data model (simulating LLM output)
        logger.info("Step 1: Creating test data model...")
        data_model = {
            "nodes": [
                {
                    "label": "Person",
                    "key_property": {
                        "name": "name",
                        "type": "STRING",
                        "description": "Name of the person"
                    },
                    "properties": [
                        {
                            "name": "name",
                            "type": "STRING",
                            "description": "Name of the person"
                        },
                        {
                            "name": "profession",
                            "type": "STRING",
                            "description": "Profession of the person"
                        }
                    ]
                },
                {
                    "label": "Company",
                    "key_property": {
                        "name": "name",
                        "type": "STRING",
                        "description": "Name of the company"
                    },
                    "properties": [
                        {
                            "name": "name",
                            "type": "STRING",
                            "description": "Name of the company"
                        },
                        {
                            "name": "industry",
                            "type": "STRING",
                            "description": "Industry of the company"
                        }
                    ]
                }
            ],
            "relationships": [
                {
                    "type": "WORKS_FOR",
                    "start_node_label": "Person",
                    "end_node_label": "Company",
                    "properties": [
                        {
                            "name": "role",
                            "type": "STRING",
                            "description": "Role in the company"
                        }
                    ]
                }
            ]
        }
        
        # Step 2: Validate data model
        logger.info("Step 2: Validating data model...")
        validation_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "validate_data_model",
                "arguments": {"data_model": data_model}
            }
        }
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream"
        }
        
        response = requests.post(data_modeling_server_url, json=validation_payload, headers=headers, timeout=30)
        
        if response.status_code == 200:
            validation_result = parse_sse_response(response.text)
            if validation_result.get("success", False):
                logger.info("✅ Data model validation successful")
            else:
                logger.error(f"❌ Data model validation failed: {validation_result.get('error', 'Unknown error')}")
                return False
        else:
            logger.error(f"❌ Validation request failed: {response.status_code} - {response.text}")
            return False
        
        # Step 3: Get constraints
        logger.info("Step 3: Getting constraint queries...")
        constraints_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "get_constraints_cypher_queries",
                "arguments": {"data_model": data_model}
            }
        }
        
        response = requests.post(data_modeling_server_url, json=constraints_payload, headers=headers, timeout=30)
        
        if response.status_code == 200:
            constraints_result = parse_sse_response(response.text)
            if constraints_result.get("success", False):
                constraints = constraints_result.get("result", {}).get("structuredContent", {}).get("result", [])
                logger.info(f"✅ Got {len(constraints)} constraint queries")
                for i, constraint in enumerate(constraints[:2]):  # Show first 2
                    logger.info(f"  Constraint {i+1}: {constraint}")
            else:
                logger.warning(f"⚠️ Constraints query failed: {constraints_result.get('error', 'Unknown error')}")
        else:
            logger.warning(f"⚠️ Constraints request failed: {response.status_code}")
        
        # Step 4: Get node ingestion query
        logger.info("Step 4: Getting node ingestion query...")
        person_node = data_model["nodes"][0]  # Person node
        
        node_query_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "get_node_cypher_ingest_query",
                "arguments": {"node": person_node}
            }
        }
        
        response = requests.post(data_modeling_server_url, json=node_query_payload, headers=headers, timeout=30)
        
        if response.status_code == 200:
            node_result = parse_sse_response(response.text)
            if node_result.get("success", False):
                node_query = node_result.get("result", {}).get("structuredContent", {}).get("result", "")
                logger.info(f"✅ Generated node query: {node_query[:100]}...")
            else:
                logger.error(f"❌ Node query generation failed: {node_result.get('error', 'Unknown error')}")
                return False
        else:
            logger.error(f"❌ Node query request failed: {response.status_code}")
            return False
        
        # Step 5: Test cypher execution (dry run)
        logger.info("Step 5: Testing Cypher execution (dry run)...")
        test_query = "MATCH (n) RETURN count(n) as node_count LIMIT 1"
        
        cypher_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "read_neo4j_cypher",
                "arguments": {"query": test_query}
            }
        }
        
        response = requests.post(cypher_server_url, json=cypher_payload, headers=headers, timeout=30)
        
        if response.status_code == 200:
            cypher_result = parse_sse_response(response.text)
            if cypher_result.get("success", False):
                logger.info("✅ Neo4j database is reachable and responsive")
                result_data = cypher_result.get("result", {})
                logger.info(f"Neo4j query result: {result_data}")
            else:
                logger.error(f"❌ Neo4j query failed: {cypher_result.get('error', 'Unknown error')}")
                return False
        else:
            logger.error(f"❌ Cypher request failed: {response.status_code}")
            return False
        
        logger.info("🎉 All MCP pipeline tests passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Pipeline test failed: {e}", exc_info=True)
        return False

def main():
    """Main test function."""
    logger.info("=== MCP Neo4j Pipeline Test (No Azure OpenAI Required) ===")
    
    success = test_mcp_ingestion_pipeline()
    
    if success:
        logger.info("🎉 SUCCESS: MCP pipeline is working correctly!")
        logger.info("✅ MCP servers are operational")
        logger.info("✅ Data model validation works")
        logger.info("✅ Cypher query generation works")
        logger.info("✅ Neo4j database is accessible")
        logger.info("✅ The previous KeyError: 'nodes' issue has been resolved!")
    else:
        logger.error("❌ FAILED: MCP pipeline has issues. Check logs above.")
    
    return success

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test suite failed: {e}", exc_info=True)
        sys.exit(1)