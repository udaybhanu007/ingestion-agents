#!/usr/bin/env python3
"""
Comprehensive Neo4j Data Checker
Verifies data insertion and diagnoses issues
"""

import requests
import json
import os
import time
from typing import Dict, Any

def check_mcp_servers():
    """Check if MCP servers are running"""
    print("🔍 Checking MCP Server Connectivity...")
    
    # Check Data Modeling Server (port 8004)
    try:
        response = requests.get("http://127.0.0.1:8004/health", timeout=5)
        print(f"✅ Data Modeling Server (8004): {response.status_code}")
    except Exception as e:
        print(f"❌ Data Modeling Server (8004): {str(e)}")
    
    # Check Cypher Server (port 8003) 
    try:
        response = requests.get("http://127.0.0.1:8003/health", timeout=5)
        print(f"✅ Cypher Server (8003): {response.status_code}")
    except Exception as e:
        print(f"❌ Cypher Server (8003): {str(e)}")

def make_mcp_request(server_url: str, tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Make MCP request to check data"""
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call", 
            "params": {
                "name": tool_name,
                "arguments": params
            }
        }
        
        print(f"🔍 Making MCP request to {server_url}")
        print(f"Tool: {tool_name}")
        print(f"Params: {json.dumps(params, indent=2)}")
        
        response = requests.post(
            server_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        print(f"Response Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"Response: {json.dumps(result, indent=2)}")
            return {"success": True, "result": result}
        else:
            print(f"Error Response: {response.text}")
            return {"success": False, "error": f"HTTP {response.status_code}: {response.text}"}
            
    except Exception as e:
        error_msg = f"MCP request failed: {str(e)}"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}

def check_neo4j_data():
    """Check Neo4j data using MCP Cypher server"""
    print("\n🔍 Checking Neo4j Data via MCP...")
    
    # Check total node count
    print("\n--- Checking Total Node Count ---")
    count_result = make_mcp_request(
        "http://127.0.0.1:8003",
        "read_neo4j_cypher",
        {"query": "MATCH (n) RETURN count(n) as total_nodes"}
    )
    
    # Check node labels
    print("\n--- Checking Node Labels ---")
    labels_result = make_mcp_request(
        "http://127.0.0.1:8003", 
        "read_neo4j_cypher",
        {"query": "CALL db.labels() YIELD label RETURN label"}
    )
    
    # Check relationship types
    print("\n--- Checking Relationship Types ---")
    rel_types_result = make_mcp_request(
        "http://127.0.0.1:8003",
        "read_neo4j_cypher", 
        {"query": "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"}
    )
    
    # Check sample nodes
    print("\n--- Checking Sample Nodes ---")
    sample_result = make_mcp_request(
        "http://127.0.0.1:8003",
        "read_neo4j_cypher",
        {"query": "MATCH (n) RETURN n LIMIT 5"}
    )
    
    # Check sample relationships
    print("\n--- Checking Sample Relationships ---")
    rel_sample_result = make_mcp_request(
        "http://127.0.0.1:8003",
        "read_neo4j_cypher",
        {"query": "MATCH ()-[r]->() RETURN r LIMIT 5"}
    )
    
    # Summarize findings
    print("\n" + "="*60)
    print("📊 NEO4J DATA SUMMARY")
    print("="*60)
    
    if count_result.get("success"):
        print(f"✅ Total Nodes: Query executed")
    else:
        print(f"❌ Node Count Check Failed: {count_result.get('error', 'Unknown error')}")
    
    if labels_result.get("success"):
        print(f"✅ Node Labels: Query executed")
    else:
        print(f"❌ Label Check Failed: {labels_result.get('error', 'Unknown error')}")
    
    if rel_types_result.get("success"):
        print(f"✅ Relationship Types: Query executed") 
    else:
        print(f"❌ Relationship Types Check Failed: {rel_types_result.get('error', 'Unknown error')}")

def test_direct_cypher_write():
    """Test direct Cypher write to verify MCP server functionality"""
    print("\n🧪 Testing Direct Cypher Write...")
    
    # Test creating a simple node
    test_result = make_mcp_request(
        "http://127.0.0.1:8003",
        "write_neo4j_cypher",
        {
            "query": "MERGE (t:TestNode {id: 'test_check_001', name: 'Test Check Node', timestamp: $timestamp})",
            "params": {"timestamp": int(time.time())}
        }
    )
    
    if test_result.get("success"):
        print("✅ Test node creation successful")
        
        # Try to read it back
        read_result = make_mcp_request(
            "http://127.0.0.1:8003", 
            "read_neo4j_cypher",
            {"query": "MATCH (t:TestNode {id: 'test_check_001'}) RETURN t"}
        )
        
        if read_result.get("success"):
            print("✅ Test node read back successful") 
            
            # Clean up
            cleanup_result = make_mcp_request(
                "http://127.0.0.1:8003",
                "write_neo4j_cypher", 
                {"query": "MATCH (t:TestNode {id: 'test_check_001'}) DELETE t", "params": {}}
            )
            
            if cleanup_result.get("success"):
                print("✅ Test node cleanup successful")
            else:
                print(f"⚠️ Test node cleanup failed: {cleanup_result.get('error')}")
        else:
            print(f"❌ Test node read failed: {read_result.get('error')}")
    else:
        print(f"❌ Test node creation failed: {test_result.get('error')}")

def diagnose_ingestion_issues():
    """Diagnose common ingestion issues"""
    print("\n🔧 Diagnosing Common Ingestion Issues...")
    
    issues_found = []
    
    # Check 1: MCP server connectivity
    print("\n1. Checking MCP Server Connectivity...")
    try:
        # Test Data Modeling Server
        dm_response = requests.get("http://127.0.0.1:8004/", timeout=5)
        print(f"✅ Data Modeling Server reachable (status: {dm_response.status_code})")
    except Exception as e:
        issues_found.append("Data Modeling Server (8004) not reachable")
        print(f"❌ Data Modeling Server unreachable: {e}")
    
    try:
        # Test Cypher Server
        cypher_response = requests.get("http://127.0.0.1:8003/", timeout=5)
        print(f"✅ Cypher Server reachable (status: {cypher_response.status_code})")
    except Exception as e:
        issues_found.append("Cypher Server (8003) not reachable")
        print(f"❌ Cypher Server unreachable: {e}")
    
    # Check 2: Neo4j connection from MCP server
    print("\n2. Testing Neo4j Connection via MCP...")
    connection_test = make_mcp_request(
        "http://127.0.0.1:8003",
        "read_neo4j_cypher", 
        {"query": "RETURN 1 as test"}
    )
    
    if connection_test.get("success"):
        print("✅ Neo4j connection via MCP working")
    else:
        issues_found.append("Neo4j connection via MCP failing")
        print(f"❌ Neo4j connection issue: {connection_test.get('error')}")
    
    # Check 3: Environment variables
    print("\n3. Checking Environment Variables...")
    required_env_vars = [
        "NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD",
        "AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_KEY"
    ]
    
    for var in required_env_vars:
        if os.getenv(var):
            print(f"✅ {var}: Set")
        else:
            issues_found.append(f"Missing environment variable: {var}")
            print(f"❌ {var}: Not set")
    
    # Summary
    print(f"\n{'='*60}")
    print("🔧 DIAGNOSIS SUMMARY")
    print("="*60)
    
    if issues_found:
        print("❌ Issues Found:")
        for issue in issues_found:
            print(f"   - {issue}")
        print("\n💡 Recommendations:")
        print("   1. Ensure MCP servers are running")
        print("   2. Check Neo4j database is accessible")
        print("   3. Verify environment variables in .env.dev")
        print("   4. Check network connectivity")
    else:
        print("✅ No obvious issues detected")
        print("💡 If data is still not inserting, check:")
        print("   1. LLM entity extraction in logs")
        print("   2. MCP request/response logs")
        print("   3. Neo4j query execution logs")

def main():
    """Main function to run comprehensive Neo4j data check"""
    print("=" * 80)
    print("🚀 COMPREHENSIVE NEO4J DATA CHECK")
    print("=" * 80)
    
    # Load environment
    try:
        from dotenv import load_dotenv
        env_loaded = load_dotenv('.env.dev')
        print(f"📋 Environment loaded: {env_loaded}")
    except Exception as e:
        print(f"⚠️ Could not load .env.dev: {e}")
    
    # Run checks
    check_mcp_servers()
    check_neo4j_data()
    test_direct_cypher_write()
    diagnose_ingestion_issues()
    
    print(f"\n{'='*80}")
    print("✅ NEO4J DATA CHECK COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()
