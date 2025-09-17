"""
Test MCP server connectivity for both servers used in graph ingestion
"""
import requests
import json

def test_mcp_server(server_url, server_name):
    """Test connectivity to an MCP server"""
    try:
        print(f"\n🔗 Testing {server_name} at {server_url}", flush=True)
        
        # Test basic connectivity
        response = requests.get(server_url, timeout=5)
        print(f"  Status: {response.status_code}")
        
        if response.status_code == 200:
            print(f"  ✅ {server_name} is accessible")
        else:
            print(f"  ❌ {server_name} returned status {response.status_code}")
            return False
            
        # Test with a simple MCP request
        test_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
            "params": {}
        }
        
        mcp_response = requests.post(
            server_url,
            json=test_payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream"
            },
            timeout=10
        )
        
        print(f"  MCP Request Status: {mcp_response.status_code}")
        print(f"  Response: {mcp_response.text[:200]}...")
        
        if mcp_response.status_code == 200:
            print(f"  ✅ {server_name} MCP protocol working")
            return True
        else:
            print(f"  ❌ {server_name} MCP protocol failed")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"  ❌ Cannot connect to {server_name} - server may not be running")
        return False
    except requests.exceptions.Timeout:
        print(f"  ❌ Timeout connecting to {server_name}")
        return False
    except Exception as e:
        print(f"  ❌ Error testing {server_name}: {str(e)}")
        return False

def main():
    """Test both MCP servers"""
    import sys
    print("🧪 Testing MCP Server Connectivity", flush=True)
    print("=" * 50, flush=True)
    
    # Server URLs from graph_tool.py
    cypher_server_url = "http://127.0.0.1:8003/mcp/"
    data_modeling_server_url = "http://127.0.0.1:8004/mcp/"
    
    # Test both servers
    cypher_result = test_mcp_server(cypher_server_url, "Cypher Server (Port 8003)")
    modeling_result = test_mcp_server(data_modeling_server_url, "Data Modeling Server (Port 8004)")
    
    print("\n" + "=" * 50)
    print("📊 Summary:")
    print(f"  Cypher Server (8003): {'✅ Working' if cypher_result else '❌ Failed'}")
    print(f"  Data Modeling Server (8004): {'✅ Working' if modeling_result else '❌ Failed'}")
    
    if cypher_result and modeling_result:
        print("\n🎉 Both MCP servers are working correctly!")
        return True
    else:
        print("\n⚠️  One or more MCP servers are not accessible")
        print("   Make sure both servers are running before using graph ingestion")
        return False

if __name__ == "__main__":
    success = main()
