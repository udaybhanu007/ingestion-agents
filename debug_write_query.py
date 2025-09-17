#!/usr/bin/env python3
"""
Quick debug script to capture actual Cypher queries that are failing
"""

import os
import sys
import json
from dotenv import load_dotenv

# Load environment
load_dotenv(".env.dev")

# Add agents directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "agents"))

from tools.graph_tool import GraphIngestionTool

def test_query_generation():
    """Test what queries are actually being generated and failing."""
    print("🔍 Testing Actual Query Generation")
    print("=" * 50)
    
    # Initialize tool
    tool = GraphIngestionTool()
    
    # Use simple test data
    entities_relationships = {
        "entities": [
            {"type": "Person", "properties": {"name": "Alice", "role": "Engineer"}}
        ],
        "relationships": []
    }
    
    # Build data model
    data_model = tool._build_neo4j_data_model(entities_relationships)
    print("Data Model:", json.dumps(data_model, indent=2))
    print()
    
    # Test node query generation
    if data_model and data_model.get("nodes"):
        node = data_model["nodes"][0]  # Get first node
        print(f"Testing node query generation for: {node['label']}")
        
        # Generate query
        query_result = tool.make_mcp_request(
            tool.data_modeling_server_url,
            "get_node_cypher_ingest_query",
            {"node": node}
        )
        
        print("Query generation result:", json.dumps(query_result, indent=2))
        
        if query_result.get("success", False):
            # Try to extract and examine the Cypher query
            result_data = query_result["result"]
            cypher_query = None
            
            if isinstance(result_data, dict) and "structuredContent" in result_data:
                structured_content = result_data["structuredContent"]
                if isinstance(structured_content, dict) and "result" in structured_content:
                    cypher_query = structured_content["result"]
            
            if cypher_query:
                print()
                print("Generated Cypher Query:")
                print("```")
                print(cypher_query)
                print("```")
                print()
                
                # Now test if this query would be accepted
                print("Testing if query would be accepted by write_neo4j_cypher...")
                
                # Prepare records
                records = [{"name": "Alice", "role": "Engineer"}]
                
                # Try to execute (this will fail but we'll see the exact error)
                write_result = tool.make_mcp_request(
                    tool.cypher_server_url,
                    "write_neo4j_cypher",
                    {
                        "query": cypher_query,
                        "params": {"records": records}
                    }
                )
                
                print("Write result:", json.dumps(write_result, indent=2))
                
                if not write_result.get("success", False):
                    error_msg = write_result.get("error", "Unknown error")
                    print(f"❌ Write failed: {error_msg}")
                    
                    # Check if it's the "only write queries" error
                    if "Only write queries are allowed" in error_msg:
                        print()
                        print("🔍 ANALYSIS: Query rejected as non-write query")
                        print("Query content analysis:")
                        query_upper = cypher_query.upper()
                        print(f"- Contains CREATE: {'CREATE' in query_upper}")
                        print(f"- Contains MERGE: {'MERGE' in query_upper}")  
                        print(f"- Contains SET: {'SET' in query_upper}")
                        print(f"- Contains DELETE: {'DELETE' in query_upper}")
                        print(f"- Contains REMOVE: {'REMOVE' in query_upper}")
                        print(f"- Contains UNWIND: {'UNWIND' in query_upper}")
                        print(f"- First word: {cypher_query.strip().split()[0] if cypher_query.strip() else 'EMPTY'}")
                        
                        # Check for problematic patterns
                        if cypher_query.strip().upper().startswith('UNWIND'):
                            print("⚠️  Query starts with UNWIND - this should be a write query!")
                        
                else:
                    print("✅ Write succeeded!")

if __name__ == "__main__":
    test_query_generation()