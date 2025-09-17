"""
Test script to verify MCP tool parameters for neo4j-data-modeling server
"""

import requests
import json

def parse_sse_response(response_text):
    """Parse Server-Sent Events response format."""
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
    
    # If no SSE format found, try parsing as direct JSON
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        return None

def check_mcp_tools():
    """Check available tools and their schemas on the neo4j-data-modeling MCP server."""
    
    server_url = "http://127.0.0.1:8004/mcp/"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream"
    }
    
    # Get list of tools
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": {}
    }
    
    try:
        print("Checking available tools on neo4j-data-modeling MCP server...")
        response = requests.post(server_url, json=payload, headers=headers, timeout=10)
        print(f"Status Code: {response.status_code}")
        print(f"Content Type: {response.headers.get('content-type', 'unknown')}")
        
        if response.status_code == 200:
            # Parse response (could be SSE or JSON)
            result = parse_sse_response(response.text)
            
            if result:
                print("\nParsed Response:")
                print("=" * 50)
                print(json.dumps(result, indent=2))
                
                if "result" in result and "tools" in result["result"]:
                    tools = result["result"]["tools"]
                    print(f"\nFound {len(tools)} tools:")
                    print("=" * 50)
                    
                    for tool in tools:
                        print(f"\nTool: {tool['name']}")
                        print(f"Description: {tool.get('description', 'No description')}")
                        
                        if "inputSchema" in tool:
                            print("Parameters:")
                            schema = tool["inputSchema"]
                            if "properties" in schema:
                                for prop_name, prop_info in schema["properties"].items():
                                    prop_type = prop_info.get("type", "unknown")
                                    required = prop_name in schema.get("required", [])
                                    print(f"  - {prop_name} ({prop_type}) {'[REQUIRED]' if required else '[OPTIONAL]'}")
                                    if "description" in prop_info:
                                        print(f"    Description: {prop_info['description']}")
                else:
                    print("No tools found in response")
            else:
                print("Failed to parse response")
                print(f"Raw response: {response.text[:500]}...")
        else:
            print(f"Error response: {response.text}")
            
    except Exception as e:
        print(f"Error checking MCP tools: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_mcp_tools()