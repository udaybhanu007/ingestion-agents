#!/usr/bin/env python3
"""
Test MCP transaction behavior to understand why data doesn't persist
"""

import json
import requests
import time

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
        if response.status_code == 200:
            parsed = parse_sse_response(response.text)
            if parsed and 'result' in parsed:
                return parsed['result']
        return None
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return None

def test_mcp_transaction_behavior():
    """Test MCP transaction and persistence behavior"""
    
    print("🚀 Testing MCP Transaction Behavior")
    
    cypher_server = "http://127.0.0.1:8003/mcp/"
    
    # Generate unique test identifier
    test_id = f"mcp_transaction_test_{int(time.time())}"
    
    print(f"\n📝 Test ID: {test_id}")
    
    # Step 1: Create a test node via MCP
    print("\n" + "="*50)
    print("📊 Step 1: Creating test node via MCP")
    print("="*50)
    
    create_query = f"""
    CREATE (t:TransactionTest {{
        name: '{test_id}',
        created_at: datetime(),
        source: 'mcp_server'
    }})
    RETURN t.name as name, id(t) as node_id
    """
    
    create_result = make_mcp_request(cypher_server, "write_neo4j_cypher", {
        "query": create_query
    })
    
    if create_result:
        print(f"✅ MCP Create result: {create_result}")
    else:
        print("❌ MCP Create failed")
        return
    
    # Step 2: Immediately read back via MCP
    print("\n" + "="*50)
    print("📊 Step 2: Reading back via MCP (same session?)")
    print("="*50)
    
    read_query = f"""
    MATCH (t:TransactionTest {{name: '{test_id}'}})
    RETURN t.name as name, t.created_at as created_at, id(t) as node_id
    """
    
    read_result = make_mcp_request(cypher_server, "read_neo4j_cypher", {
        "query": read_query
    })
    
    if read_result:
        print(f"✅ MCP Read result: {read_result}")
    else:
        print("❌ MCP Read failed - data not visible in same session")
    
    # Step 3: Check if there's a commit/flush mechanism
    print("\n" + "="*50)
    print("📊 Step 3: Testing potential commit mechanisms")
    print("="*50)
    
    # Try a simple query that might trigger a commit
    count_query = "MATCH (n) RETURN count(n) as total_nodes"
    count_result = make_mcp_request(cypher_server, "read_neo4j_cypher", {
        "query": count_query
    })
    
    if count_result:
        print(f"📊 Total nodes via MCP: {count_result}")
    
    # Step 4: Wait and check direct Neo4j connection
    print("\n" + "="*50)
    print("📊 Step 4: Checking via direct Neo4j connection")
    print("="*50)
    
    # Import Neo4j driver for direct connection
    try:
        from neo4j import GraphDatabase
        import os
        
        uri = "neo4j+s://e127e70a.databases.neo4j.io"
        username = "neo4j"
        password = os.getenv("NEO4J_PASSWORD")
        
        if not password:
            print("❌ NEO4J_PASSWORD environment variable not set")
            return
            
        driver = GraphDatabase.driver(uri, auth=(username, password))
        
        with driver.session() as session:
            # Check for our test node
            result = session.run(f"""
                MATCH (t:TransactionTest {{name: '{test_id}'}})
                RETURN t.name as name, t.created_at as created_at, id(t) as node_id
            """)
            
            records = list(result)
            if records:
                print(f"✅ Direct Neo4j found test node: {records[0].data()}")
            else:
                print("❌ Direct Neo4j cannot find test node - MCP data not committed!")
                
            # Check total nodes via direct connection
            total_result = session.run("MATCH (n) RETURN count(n) as total_nodes")
            total_count = total_result.single()["total_nodes"]
            print(f"📊 Total nodes via direct Neo4j: {total_count}")
        
        driver.close()
        
    except ImportError:
        print("❌ Neo4j driver not available for direct connection test")
    except Exception as e:
        print(f"❌ Direct Neo4j connection failed: {e}")
    
    # Step 5: Cleanup via MCP (if it works)
    print("\n" + "="*50)
    print("📊 Step 5: Cleanup test data")
    print("="*50)
    
    cleanup_query = f"""
    MATCH (t:TransactionTest {{name: '{test_id}'}})
    DELETE t
    RETURN count(t) as deleted_count
    """
    
    cleanup_result = make_mcp_request(cypher_server, "write_neo4j_cypher", {
        "query": cleanup_query
    })
    
    if cleanup_result:
        print(f"🧹 Cleanup result: {cleanup_result}")
    else:
        print("❌ Cleanup failed")

if __name__ == "__main__":
    test_mcp_transaction_behavior()