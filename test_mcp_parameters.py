#!/usr/bin/env python3
"""
Test the exact parameter structures for MCP tools to identify the issue
"""

import json
import requests

def parse_sse_response(response_text):
    """Parse Server-Sent Events response format"""
    try:
        lines = response_text.strip().split('\n')
        for line in lines:
            if line.startswith('data: '):
                data_content = line[6:]  # Remove 'data: ' prefix
                return json.loads(data_content)
    except Exception as e:
        print(f"Error parsing SSE response: {e}")
        print(f"Raw response: {response_text}")
    return None

def make_mcp_request(server_url, tool_name, params=None):
    """Make a request to MCP server and return response"""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": params or {}
        }
    }
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream"
    }
    
    try:
        response = requests.post(server_url, json=payload, headers=headers)
        print(f"\n🔍 Tool: {tool_name}")
        print(f"📤 Request params: {json.dumps(params, indent=2) if params else 'No params'}")
        print(f"📥 Response status: {response.status_code}")
        
        if response.status_code == 200:
            parsed = parse_sse_response(response.text)
            if parsed and 'result' in parsed:
                result = parsed['result']
                print(f"✅ Success: {result}")
                return True
            elif parsed and 'error' in parsed:
                error = parsed['error']
                print(f"❌ Error: {error}")
                return False
        else:
            print(f"❌ HTTP Error: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return False

def test_mcp_parameters():
    """Test the exact parameter structures for problematic MCP tools"""
    
    print("🚀 Testing MCP Parameter Structures")
    
    data_modeling_server = "http://127.0.0.1:8004/mcp/"
    
    # Create a minimal valid data model
    simple_data_model = {
        "nodes": [
            {
                "label": "Person",
                "key_property": {
                    "name": "name",
                    "type": "STRING"
                },
                "properties": [
                    {
                        "name": "name",
                        "type": "STRING"
                    }
                ]
            },
            {
                "label": "Organization", 
                "key_property": {
                    "name": "name",
                    "type": "STRING"
                },
                "properties": [
                    {
                        "name": "name",
                        "type": "STRING"
                    }
                ]
            }
        ],
        "relationships": [
            {
                "type": "WORKS_AT",
                "start_node_label": "Person",
                "end_node_label": "Organization",
                "properties": []
            }
        ]
    }
    
    print("\n" + "="*50)
    print("📊 Testing validate_data_model")
    print("="*50)
    success1 = make_mcp_request(data_modeling_server, "validate_data_model", {
        "data_model": simple_data_model
    })
    
    print("\n" + "="*50)
    print("🔧 Testing get_constraints_cypher_queries")
    print("="*50)
    success2 = make_mcp_request(data_modeling_server, "get_constraints_cypher_queries", {
        "data_model": simple_data_model
    })
    
    print("\n" + "="*50)
    print("📝 Testing get_node_cypher_ingest_query")
    print("="*50)
    success3 = make_mcp_request(data_modeling_server, "get_node_cypher_ingest_query", {
        "node": simple_data_model["nodes"][0]  # Pass single node
    })
    
    print("\n" + "="*50)
    print("🔗 Testing get_relationship_cypher_ingest_query")
    print("="*50)
    success4 = make_mcp_request(data_modeling_server, "get_relationship_cypher_ingest_query", {
        "data_model": simple_data_model,  # Full data model
        "relationship_type": "WORKS_AT",
        "relationship_start_node_label": "Person",
        "relationship_end_node_label": "Organization"
    })
    
    print("\n" + "="*50)
    print("📊 SUMMARY")
    print("="*50)
    
    results = {
        "validate_data_model": success1,
        "get_constraints_cypher_queries": success2,
        "get_node_cypher_ingest_query": success3,
        "get_relationship_cypher_ingest_query": success4
    }
    
    for tool, success in results.items():
        status = "✅" if success else "❌"
        print(f"{status} {tool}")
    
    all_success = all(results.values())
    print(f"\n🎯 Overall: {'✅ All tools working' if all_success else '❌ Some tools failing'}")
    
    return all_success

if __name__ == "__main__":
    test_mcp_parameters()