"""
Test MCP server connectivity to diagnose graph ingestion issues
"""
import requests
import json
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def test_mcp_server(server_url, server_name):
    """Test if MCP server is reachable and responding"""
    try:
        print(f"\n🔍 Testing {server_name} at {server_url}")
        
        # Test basic connectivity
        try:
            response = requests.get(server_url, timeout=5)
            print(f"✅ Server reachable - Status: {response.status_code}")
        except requests.exceptions.ConnectionError:
            print(f"❌ Connection failed - Server not running at {server_url}")
            return False
        except Exception as e:
            print(f"❌ Connection error: {e}")
            return False
        
        # Test JSON-RPC endpoint
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
            "params": {}
        }
        
        try:
            rpc_response = requests.post(
                server_url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream",  # FIXED: Proper headers
                    "User-Agent": "test-client/1.0"
                },
                timeout=10
            )
            print(f"📋 JSON-RPC Response - Status: {rpc_response.status_code}")
            
            if rpc_response.status_code == 200:
                try:
                    # Try direct JSON first
                    json_data = rpc_response.json()
                    print(f"📄 JSON Response: {json.dumps(json_data, indent=2)}")
                except json.JSONDecodeError:
                    # Handle SSE format
                    response_text = rpc_response.text
                    print(f"📄 SSE Response: {response_text[:500]}...")
                    
                    # Parse SSE format
                    for line in response_text.split('\n'):
                        if line.startswith('data: '):
                            try:
                                json_data = json.loads(line[6:])  # Remove 'data: '
                                print(f"📄 Parsed SSE JSON: {json.dumps(json_data, indent=2)}")
                                
                                # Check for available tools
                                if "result" in json_data and "tools" in json_data["result"]:
                                    tools = json_data["result"]["tools"]
                                    print(f"🛠️  Available tools: {[tool.get('name', 'unnamed') for tool in tools]}")
                                    return True
                                break
                            except json.JSONDecodeError:
                                continue
                    
                # Check for available tools in direct JSON
                if "result" in json_data and isinstance(json_data["result"], dict) and "tools" in json_data["result"]:
                    tools = json_data["result"]["tools"]
                    print(f"🛠️  Available tools: {[tool.get('name', 'unnamed') for tool in tools]}")
                    return True
            else:
                print(f"❌ JSON-RPC failed: {rpc_response.text[:200]}")
                
        except Exception as e:
            print(f"❌ JSON-RPC error: {e}")
            
        return False
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_direct_neo4j_write():
    """Test direct Neo4j write to verify database connectivity"""
    try:
        print(f"\n🔍 Testing direct Neo4j write...")
        
        from neo4j import GraphDatabase
        import os
        from dotenv import load_dotenv
        
        load_dotenv('.env.dev')
        
        uri = os.getenv('NEO4J_URI')
        username = os.getenv('NEO4J_USERNAME')
        password = os.getenv('NEO4J_PASSWORD')
        
        driver = GraphDatabase.driver(uri, auth=(username, password))
        
        with driver.session() as session:
            # Try to create a test node
            result = session.run(
                "CREATE (t:TestNode {name: 'connectivity_test', timestamp: datetime()}) RETURN t"
            )
            
            test_node = result.single()
            if test_node:
                print("✅ Direct Neo4j write successful!")
                
                # Clean up the test node
                session.run("MATCH (t:TestNode {name: 'connectivity_test'}) DELETE t")
                print("🧹 Test node cleaned up")
                return True
            else:
                print("❌ Direct Neo4j write failed - no result")
                return False
                
        driver.close()
        
    except Exception as e:
        print(f"❌ Direct Neo4j write error: {e}")
        return False

def main():
    """Main test function"""
    print("🚀 Starting MCP connectivity diagnostics...")
    
    # Test MCP servers
    cypher_server_url = "http://127.0.0.1:8003/mcp/"
    data_modeling_server_url = "http://127.0.0.1:8004/mcp/"
    
    cypher_ok = test_mcp_server(cypher_server_url, "Cypher Server")
    data_modeling_ok = test_mcp_server(data_modeling_server_url, "Data Modeling Server")
    
    # Test direct Neo4j connectivity
    neo4j_ok = test_direct_neo4j_write()
    
    print(f"\n📊 Summary:")
    print(f"Cypher Server (8003): {'✅' if cypher_ok else '❌'}")
    print(f"Data Modeling Server (8004): {'✅' if data_modeling_ok else '❌'}")
    print(f"Direct Neo4j: {'✅' if neo4j_ok else '❌'}")
    
    if not cypher_ok or not data_modeling_ok:
        print(f"\n⚠️  MCP servers not running! This is likely the root cause.")
        print(f"💡 You need to start the Neo4j MCP servers on ports 8003 and 8004")
        print(f"💡 Alternative: Use direct Neo4j ingestion instead of MCP")
    
    if neo4j_ok and not (cypher_ok and data_modeling_ok):
        print(f"\n🔧 Recommendation: Implement direct Neo4j ingestion as fallback")

if __name__ == "__main__":
    main()