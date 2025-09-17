#!/usr/bin/env python3
"""
Debug script to investigate KeyError: 'nodes' issue in MCP server validation.

This script systematically tests the MCP server communication and data model validation
to identify and fix the issue causing KeyError: 'nodes' during validation.
"""

import json
import requests
import logging
import sys
import os
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_mcp_server_connection(server_url: str, server_name: str) -> bool:
    """Test basic MCP server connectivity."""
    logger.info(f"Testing {server_name} connectivity at {server_url}")
    
    try:
        # Test basic server response
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
            "params": {}
        }
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream"
        }
        
        response = requests.post(server_url, json=payload, headers=headers, timeout=10)
        
        logger.info(f"{server_name} response status: {response.status_code}")
        logger.debug(f"{server_name} response headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            content_type = response.headers.get('content-type', '').lower()
            
            if 'text/event-stream' in content_type:
                logger.info(f"{server_name} returned SSE response (normal for MCP servers)")
                # Parse SSE response to check if tools are listed
                try:
                    sse_result = parse_sse_response(response.text)
                    if sse_result.get("success", False):
                        result = sse_result.get("result", {})
                        tools = result.get("tools", [])
                        logger.info(f"{server_name} has {len(tools)} tools available")
                        return True
                    else:
                        logger.warning(f"{server_name} SSE parsing failed: {sse_result.get('error', 'Unknown error')}")
                        return False
                except Exception as e:
                    logger.warning(f"{server_name} SSE parsing error: {e}")
                    return False
            else:
                try:
                    json_response = response.json()
                    logger.info(f"{server_name} JSON response received successfully")
                    logger.debug(f"{server_name} response: {json.dumps(json_response, indent=2)}")
                    return True
                except json.JSONDecodeError:
                    logger.warning(f"{server_name} returned non-JSON response")
                    logger.debug(f"{server_name} raw response: {response.text[:500]}")
                    return False
        else:
            logger.warning(f"{server_name} returned status {response.status_code}")
            logger.debug(f"{server_name} error response: {response.text[:500]}")
            return False
            
    except Exception as e:
        logger.error(f"Error connecting to {server_name}: {e}")
        return False

def create_minimal_data_model() -> Dict[str, Any]:
    """Create a minimal valid data model for testing."""
    return {
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
                        "name": "age",
                        "type": "INTEGER", 
                        "description": "Age of the person"
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
                        "name": "since",
                        "type": "STRING",
                        "description": "Start date of employment"
                    }
                ]
            }
        ]
    }

def test_data_model_validation(server_url: str, data_model: Dict[str, Any]) -> Dict[str, Any]:
    """Test MCP data model validation."""
    logger.info("Testing data model validation...")
    
    try:
        payload = {
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
        
        logger.debug(f"Validation payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(server_url, json=payload, headers=headers, timeout=30)
        
        logger.info(f"Validation response status: {response.status_code}")
        logger.debug(f"Validation response headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            content_type = response.headers.get('content-type', '').lower()
            
            if 'text/event-stream' in content_type:
                logger.info("Received SSE response, parsing...")
                return parse_sse_response(response.text)
            else:
                try:
                    json_response = response.json()
                    logger.info("Received JSON response")
                    logger.debug(f"Validation JSON response: {json.dumps(json_response, indent=2)}")
                    
                    if "result" in json_response:
                        return {"success": True, "result": json_response["result"]}
                    elif "error" in json_response:
                        error_details = json_response["error"]
                        error_msg = f"MCP Error {error_details.get('code', 'unknown')}: {error_details.get('message', 'No message')}"
                        return {"success": False, "error": error_msg, "details": error_details}
                    else:
                        return {"success": True, "result": json_response}
                        
                except json.JSONDecodeError as jde:
                    logger.error(f"Failed to parse JSON response: {jde}")
                    logger.debug(f"Raw response: {response.text[:1000]}")
                    return {"success": False, "error": f"JSON decode error: {jde}"}
        else:
            error_msg = f"HTTP {response.status_code}: {response.text}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
            
    except Exception as e:
        error_msg = f"Validation request failed: {str(e)}"
        logger.error(error_msg)
        return {"success": False, "error": error_msg}

def parse_sse_response(response_text: str) -> Dict[str, Any]:
    """Parse Server-Sent Events response."""
    logger.debug("Parsing SSE response...")
    
    try:
        lines = response_text.strip().split('\n')
        
        for line in lines:
            if line.startswith('data: '):
                json_str = line[6:]  # Remove 'data: ' prefix
                if json_str.strip() == '[DONE]':
                    continue
                    
                try:
                    parsed_data = json.loads(json_str)
                    logger.debug(f"Parsed SSE data: {json.dumps(parsed_data, indent=2)}")
                    
                    if "result" in parsed_data:
                        return {"success": True, "result": parsed_data["result"]}
                    elif "error" in parsed_data:
                        error_details = parsed_data["error"]
                        error_msg = f"MCP Error {error_details.get('code', 'unknown')}: {error_details.get('message', 'No message')}"
                        return {"success": False, "error": error_msg, "details": error_details}
                    else:
                        return {"success": True, "result": parsed_data}
                        
                except json.JSONDecodeError as jde:
                    logger.debug(f"Failed to parse SSE JSON: {jde}")
                    continue
        
        # If no valid SSE format found, try parsing as direct JSON
        try:
            parsed_data = json.loads(response_text)
            if "result" in parsed_data:
                return {"success": True, "result": parsed_data["result"]}
            elif "error" in parsed_data:
                error_details = parsed_data["error"]
                error_msg = f"MCP Error {error_details.get('code', 'unknown')}: {error_details.get('message', 'No message')}"
                return {"success": False, "error": error_msg, "details": error_details}
            else:
                return {"success": True, "result": parsed_data}
        except json.JSONDecodeError:
            pass
        
        return {"success": False, "error": f"Failed to parse SSE response: {response_text[:200]}"}
        
    except Exception as e:
        logger.error(f"Error parsing SSE response: {e}")
        return {"success": False, "error": f"Failed to parse response: {str(e)}"}

def test_different_data_model_formats(server_url: str):
    """Test different data model formats to identify the issue."""
    logger.info("Testing different data model formats...")
    
    # Test 1: Minimal data model
    logger.info("=== Test 1: Minimal Data Model ===")
    minimal_model = create_minimal_data_model()
    result1 = test_data_model_validation(server_url, minimal_model)
    logger.info(f"Minimal model result: {result1}")
    
    # Test 2: Empty data model
    logger.info("=== Test 2: Empty Data Model ===")
    empty_model = {"nodes": [], "relationships": []}
    result2 = test_data_model_validation(server_url, empty_model)
    logger.info(f"Empty model result: {result2}")
    
    # Test 3: Only nodes (no relationships)
    logger.info("=== Test 3: Nodes Only ===")
    nodes_only_model = {
        "nodes": minimal_model["nodes"],
        "relationships": []
    }
    result3 = test_data_model_validation(server_url, nodes_only_model)
    logger.info(f"Nodes only result: {result3}")
    
    # Test 4: Data model as JSON string (old format)
    logger.info("=== Test 4: JSON String Format (Old) ===")
    try:
        json_string_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "validate_data_model",
                "arguments": {"data_model": json.dumps(minimal_model)}  # JSON string instead of object
            }
        }
        
        headers = {"Content-Type": "application/json"}
        response = requests.post(server_url, json=json_string_payload, headers=headers, timeout=30)
        
        if response.status_code == 200:
            try:
                json_response = response.json()
                logger.info(f"JSON string format result: {json_response}")
            except json.JSONDecodeError:
                logger.info(f"JSON string format raw response: {response.text[:500]}")
        else:
            logger.info(f"JSON string format error: {response.status_code} - {response.text[:500]}")
            
    except Exception as e:
        logger.error(f"JSON string format test failed: {e}")
    
    return {
        "minimal_model": result1,
        "empty_model": result2,
        "nodes_only": result3
    }

def debug_constraints_query(server_url: str, data_model: Dict[str, Any]):
    """Test the constraints query that was failing."""
    logger.info("=== Testing Constraints Query ===")
    
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "get_constraints_cypher_queries",
                "arguments": {"data_model": data_model}
            }
        }
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream"
        }
        
        logger.debug(f"Constraints payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(server_url, json=payload, headers=headers, timeout=30)
        
        logger.info(f"Constraints response status: {response.status_code}")
        
        if response.status_code == 200:
            content_type = response.headers.get('content-type', '').lower()
            
            if 'text/event-stream' in content_type:
                result = parse_sse_response(response.text)
            else:
                try:
                    json_response = response.json()
                    if "result" in json_response:
                        result = {"success": True, "result": json_response["result"]}
                    elif "error" in json_response:
                        error_details = json_response["error"]
                        result = {"success": False, "error": f"MCP Error: {error_details.get('message', 'Unknown error')}", "details": error_details}
                    else:
                        result = {"success": True, "result": json_response}
                except json.JSONDecodeError:
                    result = {"success": False, "error": f"JSON decode error: {response.text[:500]}"}
            
            logger.info(f"Constraints query result: {result}")
            return result
        else:
            error_msg = f"HTTP {response.status_code}: {response.text}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
            
    except Exception as e:
        error_msg = f"Constraints request failed: {str(e)}"
        logger.error(error_msg)
        return {"success": False, "error": error_msg}

def main():
    """Main debug function."""
    logger.info("Starting MCP KeyError debugging...")
    
    # MCP server URLs
    cypher_server_url = "http://127.0.0.1:8003/mcp/"
    data_modeling_server_url = "http://127.0.0.1:8004/mcp/"
    
    # Test 1: Basic connectivity
    logger.info("=== PHASE 1: Testing Server Connectivity ===")
    cypher_ok = test_mcp_server_connection(cypher_server_url, "Cypher Server")
    modeling_ok = test_mcp_server_connection(data_modeling_server_url, "Data Modeling Server")
    
    if not cypher_ok or not modeling_ok:
        logger.error("❌ One or both MCP servers are not reachable!")
        logger.info("Please ensure MCP servers are running on ports 8003 and 8004")
        return False
    
    logger.info("✅ Both MCP servers are reachable")
    
    # Test 2: Data model validation
    logger.info("=== PHASE 2: Testing Data Model Validation ===")
    minimal_model = create_minimal_data_model()
    logger.info(f"Testing with minimal data model: {json.dumps(minimal_model, indent=2)}")
    
    validation_results = test_different_data_model_formats(data_modeling_server_url)
    
    # Test 3: Constraints query
    logger.info("=== PHASE 3: Testing Constraints Query ===")
    constraints_result = debug_constraints_query(data_modeling_server_url, minimal_model)
    
    # Summary
    logger.info("=== DEBUGGING SUMMARY ===")
    logger.info(f"Cypher Server: {'✅ OK' if cypher_ok else '❌ FAILED'}")
    logger.info(f"Data Modeling Server: {'✅ OK' if modeling_ok else '❌ FAILED'}")
    
    for test_name, result in validation_results.items():
        status = "✅ SUCCESS" if result.get("success", False) else "❌ FAILED"
        logger.info(f"Validation ({test_name}): {status}")
        if not result.get("success", False):
            logger.info(f"  Error: {result.get('error', 'Unknown error')}")
    
    constraints_status = "✅ SUCCESS" if constraints_result.get("success", False) else "❌ FAILED"
    logger.info(f"Constraints Query: {constraints_status}")
    if not constraints_result.get("success", False):
        logger.info(f"  Error: {constraints_result.get('error', 'Unknown error')}")
    
    # Recommendations
    logger.info("=== RECOMMENDATIONS ===")
    if any(not result.get("success", False) for result in validation_results.values()):
        logger.info("🔍 Data model validation is failing. Check:")
        logger.info("  - MCP server data model schema expectations")
        logger.info("  - Parameter format (object vs JSON string)")
        logger.info("  - Required vs optional fields in data model")
    
    if not constraints_result.get("success", False):
        logger.info("🔍 Constraints query is failing. This might be the source of KeyError: 'nodes'")
        
    return True

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Debugging interrupted by user")
    except Exception as e:
        logger.error(f"Debugging script failed: {e}", exc_info=True)