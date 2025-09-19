#!/usr/bin/env python3

"""
Quick test to debug MCP relationship query issue
"""

import requests
import json
import sys
import os

def test_mcp_relationship_call():
    """Test the MCP relationship call to see exact error"""
    
    data_modeling_server_url = "http://127.0.0.1:8004"
    
    # Test simple MCP call structure
    request_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "get_relationship_cypher_ingest_query",
            "arguments": {
                "relationship": {
                    "type": "HAS_IMAGE",
                    "start_node_label": "Patient",
                    "end_node_label": "MedicalImage",
                    "properties": []
                },
                "data_model": {
                    "entity_types": [
                        {
                            "type": "Patient",
                            "properties": ["id"],
                            "description": "Patient entity"
                        },
                        {
                            "type": "MedicalImage",
                            "properties": ["id"], 
                            "description": "MedicalImage entity"
                        }
                    ],
                    "relationship_types": [
                        {
                            "type": "HAS_IMAGE",
                            "start_entity": "Patient",
                            "end_entity": "MedicalImage",
                            "description": "Relationship from Patient to MedicalImage"
                        }
                    ]
                }
            }
        }
    }
    
    print("Testing MCP relationship call...")
    print(f"Request payload: {json.dumps(request_payload, indent=2)}")
    
    try:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream"
        }
        
        response = requests.post(
            f"{data_modeling_server_url}/mcp",
            json=request_payload,
            headers=headers,
            timeout=10
        )
        
        print(f"Response status: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        print(f"Raw response: {response.text}")
        
        if response.status_code == 200:
            try:
                result_data = response.json()
                print(f"Parsed JSON: {json.dumps(result_data, indent=2)}")
                
                if "error" in result_data:
                    print(f"❌ MCP Error: {result_data['error']}")
                    return False
                elif "result" in result_data:
                    print(f"✅ MCP Success: {result_data['result']}")
                    return True
                else:
                    print(f"❓ Unexpected response format: {result_data}")
                    return False
                    
            except json.JSONDecodeError as e:
                print(f"❌ JSON decode error: {e}")
                return False
        else:
            print(f"❌ HTTP error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return False

if __name__ == "__main__":
    success = test_mcp_relationship_call()
    sys.exit(0 if success else 1)
