#!/usr/bin/env python3
"""
Test MCP Neo4j Connection - Check if nodes were created via MCP servers
"""

import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env.dev')

def test_mcp_neo4j_connection():
    """Test connection to Neo4j via MCP servers (same as graph tool uses)"""
    
    cypher_server_url = "http://127.0.0.1:8003/mcp/"
    
    print("🚀 Testing MCP Neo4j connection...")
    print(f"📡 Cypher server: {cypher_server_url}")
    
    # Test 1: Check if MCP server is reachable
    try:
        payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
        response = requests.post(
            cypher_server_url, 
            json=payload, 
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        
        if response.status_code == 200:
            print("✅ MCP Cypher server is reachable")
        else:
            print(f"⚠️  MCP server responded with status {response.status_code}")
            
    except Exception as e:
        print(f"❌ Cannot reach MCP server: {e}")
        return False
    
    # Test 2: Query Neo4j via MCP to count nodes
    print("\n📊 Querying Neo4j via MCP...")
    
    try:
        # Count all nodes
        query_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "read_neo4j_cypher",
                "arguments": {
                    "query": "MATCH (n) RETURN count(n) as node_count"
                }
            }
        }
        
        response = requests.post(
            cypher_server_url,
            json=query_payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"📈 MCP Response: {json.dumps(result, indent=2)}")
            
            # Extract node count from response
            if "result" in result and result["result"]:
                records = result["result"]
                if isinstance(records, list) and len(records) > 0:
                    node_count = records[0].get("node_count", 0)
                    print(f"🎯 Total nodes in Neo4j: {node_count}")
                    
                    if node_count > 0:
                        print("✅ Nodes found in Neo4j database!")
                        return True
                    else:
                        print("⚠️  No nodes found in Neo4j database")
                        return False
                else:
                    print("⚠️  No records returned from query")
                    return False
            else:
                print("⚠️  Unexpected response format")
                return False
        else:
            print(f"❌ MCP query failed with status {response.status_code}: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error querying Neo4j via MCP: {e}")
        return False

def query_recent_nodes():
    """Query recent nodes via MCP to see what was created"""
    
    cypher_server_url = "http://127.0.0.1:8003/mcp/"
    
    print("\n🔍 Querying recent nodes...")
    
    try:
        # Get recent nodes
        query_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "read_neo4j_cypher",
                "arguments": {
                    "query": "MATCH (n) RETURN labels(n) as labels, properties(n) as props ORDER BY id(n) DESC LIMIT 10"
                }
            }
        }
        
        response = requests.post(
            cypher_server_url,
            json=query_payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            
            if "result" in result and result["result"]:
                records = result["result"]
                print(f"📋 Found {len(records)} recent nodes:")
                
                for i, record in enumerate(records, 1):
                    labels = record.get("labels", [])
                    props = record.get("props", {})
                    label_str = ":".join(labels) if labels else "NoLabel"
                    print(f"   {i}. [{label_str}] {props}")
                    
                return True
            else:
                print("⚠️  No nodes returned")
                return False
        else:
            print(f"❌ Query failed: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error querying recent nodes: {e}")
        return False

def query_relationships():
    """Query relationships via MCP"""
    
    cypher_server_url = "http://127.0.0.1:8003/mcp/"
    
    print("\n🔗 Querying relationships...")
    
    try:
        query_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "read_neo4j_cypher",
                "arguments": {
                    "query": "MATCH (a)-[r]->(b) RETURN type(r) as rel_type, properties(a) as start_props, properties(b) as end_props LIMIT 10"
                }
            }
        }
        
        response = requests.post(
            cypher_server_url,
            json=query_payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            
            if "result" in result and result["result"]:
                records = result["result"]
                print(f"🔗 Found {len(records)} relationships:")
                
                for i, record in enumerate(records, 1):
                    rel_type = record.get("rel_type", "UNKNOWN")
                    start_props = record.get("start_props", {})
                    end_props = record.get("end_props", {})
                    print(f"   {i}. {start_props} -[{rel_type}]-> {end_props}")
                    
                return True
            else:
                print("⚠️  No relationships returned")
                return False
        else:
            print(f"❌ Query failed: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error querying relationships: {e}")
        return False

def main():
    print("="*60)
    print("🧪 MCP NEO4J VALIDATION TEST")
    print("="*60)
    
    # Test MCP connection and basic queries
    if test_mcp_neo4j_connection():
        query_recent_nodes()
        query_relationships()
        
        print("\n" + "="*60)
        print("✅ MCP Neo4j validation completed!")
        print("="*60)
    else:
        print("\n" + "="*60)
        print("❌ MCP Neo4j validation failed!")
        print("="*60)

if __name__ == "__main__":
    main()