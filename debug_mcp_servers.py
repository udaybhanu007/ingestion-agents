#!/usr/bin/env python3
"""
Debug script to test MCP server connections and identify available tools.
"""

import requests
import json
import sys

def parse_sse_response(response_text):
    """Parse Server-Sent Events response format."""
    try:
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
        return json.loads(response_text)
    except Exception as e:
        return {"error": f"Failed to parse response: {str(e)}"}

def test_mcp_server(server_url, server_name):
    """Test MCP server connectivity and list available tools."""
    print(f"\n=== Testing {server_name} at {server_url} ===")
    
    try:
        # Test tools/list
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list"
        }
        
        response = requests.post(
            server_url,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream"
            },
            timeout=10
        )
        
        if response.status_code == 200:
            result = parse_sse_response(response.text)
            
            if "result" in result and "tools" in result["result"]:
                tools = result["result"]["tools"]
                print(f"✅ Server connected successfully!")
                print(f"Available tools ({len(tools)}):")
                for tool in tools:
                    name = tool.get("name", "unknown")
                    desc = tool.get("description", "No description")[:80]
                    print(f"  - {name}: {desc}")
                return tools
            else:
                print(f"❌ Unexpected response: {result}")
                return None
        else:
            print(f"❌ HTTP {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        return None

def test_tool_call(server_url, tool_name, arguments, server_name):
    """Test a specific tool call."""
    print(f"\n--- Testing {tool_name} on {server_name} ---")
    
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
            timeout=10
        )
        
        if response.status_code == 200:
            result = parse_sse_response(response.text)
            print(f"✅ Tool call successful: {result}")
            return result
        else:
            print(f"❌ Tool call failed: HTTP {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Tool call error: {str(e)}")
        return None

def main():
    print("🔍 MCP Server Diagnostics")
    print("=" * 50)
    
    # Test both servers
    cypher_url = "http://127.0.0.1:8003/mcp/"
    modeling_url = "http://127.0.0.1:8004/mcp/"
    
    # Test cypher server
    cypher_tools = test_mcp_server(cypher_url, "Cypher Server")
    
    # Test data modeling server  
    modeling_tools = test_mcp_server(modeling_url, "Data Modeling Server")
    
    # Test specific tool calls if servers are available
    if cypher_tools:
        # Find a read tool to test
        read_tools = [t for t in cypher_tools if 'read' in t.get('name', '').lower() or 'get' in t.get('name', '').lower()]
        if read_tools:
            tool_name = read_tools[0]['name']
            test_tool_call(cypher_url, tool_name, {}, "Cypher Server")
    
    if modeling_tools:
        # Find a validation tool to test
        validation_tools = [t for t in modeling_tools if 'validate' in t.get('name', '').lower()]
        if validation_tools:
            tool_name = validation_tools[0]['name']
            test_tool_call(modeling_url, tool_name, {"node": {"type": "test"}}, "Data Modeling Server")
    
    print(f"\n🏁 Diagnosis complete!")
    
    # Summary
    print(f"\n📋 Summary:")
    print(f"  Cypher Server ({cypher_url}): {'✅ Available' if cypher_tools else '❌ Failed'}")
    print(f"  Data Modeling Server ({modeling_url}): {'✅ Available' if modeling_tools else '❌ Failed'}")
    
    if cypher_tools or modeling_tools:
        print(f"\n💡 Issues found:")
        print(f"  - The graph_tool.py is using incorrect tool names")
        print(f"  - Available tools don't match expected names:")
        print(f"    Expected: execute_cypher, analyze_document_structure")
        if cypher_tools:
            print(f"    Cypher tools: {[t['name'] for t in cypher_tools[:3]]}")
        if modeling_tools:
            print(f"    Modeling tools: {[t['name'] for t in modeling_tools[:3]]}")

if __name__ == "__main__":
    main()
