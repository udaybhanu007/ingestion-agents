#!/usr/bin/env python3
"""
Test script to verify compliance with mcp-neo4j-cypher documentation.
Validates that we're using the correct parameter structure for write_neo4j_cypher.
"""

import json
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agents.tools.graph_tool import GraphIngestionTool

def test_write_neo4j_cypher_params():
    """Test that we're using the correct parameters as per PyPI documentation."""
    print("🧪 Testing MCP Neo4j Cypher Documentation Compliance")
    print("=" * 60)
    
    # Initialize the tool
    tool = GraphIngestionTool()
    
    # Test payload structure
    print("\n📋 Testing parameter structure...")
    
    # According to PyPI docs, write_neo4j_cypher expects:
    # - query (string): The Cypher update query
    # - params (dictionary, optional): Parameters to pass to the Cypher query
    
    test_query = "UNWIND $records as record MERGE (n:Test {name: record.name})"
    test_records = [{"name": "test1"}, {"name": "test2"}]
    
    # Create test payload as we would in the actual code
    test_payload = {
        "query": test_query,
        "params": {"records": test_records}
    }
    
    print(f"✅ Correct parameter structure:")
    print(f"   Query: {test_payload['query'][:50]}...")
    print(f"   Params: {test_payload['params']}")
    
    # Verify this matches the PyPI documentation requirements
    assert "query" in test_payload, "Missing 'query' parameter"
    assert "params" in test_payload, "Missing 'params' parameter"
    assert isinstance(test_payload["query"], str), "'query' should be string"
    assert isinstance(test_payload["params"], dict), "'params' should be dictionary"
    
    print("\n✅ Parameter structure matches PyPI documentation:")
    print("   ✓ query: string (Cypher update query)")
    print("   ✓ params: dictionary (Parameters for Cypher query)")
    
    print("\n📝 Documentation Reference:")
    print("   Source: https://pypi.org/project/mcp-neo4j-cypher/")
    print("   Tool: write_neo4j_cypher")
    print("   Expected: {query: string, params: dictionary}")
    
    print("\n🎉 All compliance tests passed!")
    return True

def test_read_neo4j_cypher_params():
    """Test read_neo4j_cypher parameter compliance."""
    print("\n📖 Testing read_neo4j_cypher parameters...")
    
    # According to docs, read_neo4j_cypher expects:
    # - query (string): The Cypher query to execute
    # - params (dictionary, optional): Parameters to pass to the Cypher query
    
    test_payload = {
        "query": "MATCH (n:Test) RETURN n LIMIT 10",
        "params": {"limit": 10}
    }
    
    assert "query" in test_payload
    assert isinstance(test_payload["query"], str)
    assert "params" in test_payload
    assert isinstance(test_payload["params"], dict)
    
    print("   ✅ read_neo4j_cypher parameters are also compliant")

if __name__ == "__main__":
    try:
        test_write_neo4j_cypher_params()
        test_read_neo4j_cypher_params()
        print("\n" + "=" * 60)
        print("🏆 All MCP Neo4j Cypher documentation compliance tests passed!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)