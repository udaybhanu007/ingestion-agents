"""
Test script for the cleaned up GraphIngestionTool with proper MCP parameter validation
"""

import json
import logging
from agents.tools.graph_tool import GraphIngestionTool

# Configure logging to see detailed output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_mcp_parameter_validation():
    """Test that all MCP parameters are correctly formatted and all required fields included."""
    
    print("🔍 Testing MCP Parameter Validation")
    print("=" * 50)
    
    try:
        tool = GraphIngestionTool()
        
        # Test data model structure with proper Neo4j schema
        test_data_model = {
            "nodes": [
                {
                    "label": "Person",
                    "key_property": {"name": "name", "type": "STRING", "description": "Person name"},
                    "properties": [
                        {"name": "name", "type": "STRING", "description": "Person name"},
                        {"name": "title", "type": "STRING", "description": "Job title"}
                    ]
                },
                {
                    "label": "Company",
                    "key_property": {"name": "name", "type": "STRING", "description": "Company name"},
                    "properties": [
                        {"name": "name", "type": "STRING", "description": "Company name"},
                        {"name": "industry", "type": "STRING", "description": "Industry type"}
                    ]
                }
            ],
            "relationships": [
                {
                    "type": "WORKS_FOR",
                    "start_node_label": "Person",
                    "end_node_label": "Company",
                    "properties": [
                        {"name": "since", "type": "STRING", "description": "Start date"}
                    ]
                }
            ]
        }
        
        print("1. Testing validate_data_model...")
        result = tool.make_mcp_request(
            tool.data_modeling_server_url,
            "validate_data_model",
            {"data_model": test_data_model}  # Pass as object
        )
        
        if result.get('success'):
            print("   ✅ validate_data_model: PASSED")
            print(f"   📄 Result: {result.get('result', {})}")
        else:
            print(f"   ❌ validate_data_model: FAILED - {result.get('error', 'Unknown error')}")
        
        print("\n2. Testing get_constraints_cypher_queries...")
        result = tool.make_mcp_request(
            tool.data_modeling_server_url,
            "get_constraints_cypher_queries",
            {"data_model": test_data_model}  # Pass as object
        )
        
        if result.get('success'):
            constraints = result.get('result', [])
            print(f"   ✅ get_constraints_cypher_queries: PASSED")
            print(f"   📄 Generated {len(constraints)} constraint queries")
            if isinstance(constraints, list) and len(constraints) > 0:
                for i, query in enumerate(constraints[:2]):  # Show first 2
                    print(f"   🔗 Query {i+1}: {query[:100]}...")
        else:
            print(f"   ❌ get_constraints_cypher_queries: FAILED - {result.get('error', 'Unknown error')}")
        
        print("\n3. Testing get_node_cypher_ingest_query...")
        test_node = test_data_model["nodes"][0]  # Person node
        result = tool.make_mcp_request(
            tool.data_modeling_server_url,
            "get_node_cypher_ingest_query",
            {"node": test_node}  # Pass as object
        )
        
        if result.get('success'):
            query = result.get('result', '')
            print(f"   ✅ get_node_cypher_ingest_query: PASSED")
            print(f"   📄 Generated query: {query[:100]}...")
        else:
            print(f"   ❌ get_node_cypher_ingest_query: FAILED - {result.get('error', 'Unknown error')}")
        
        print("\n4. Testing get_relationship_cypher_ingest_query...")
        test_relationship = test_data_model["relationships"][0]
        data_model_snippet = {
            "nodes": test_data_model["nodes"],
            "relationships": [test_relationship]
        }
        
        result = tool.make_mcp_request(
            tool.data_modeling_server_url,
            "get_relationship_cypher_ingest_query",
            {
                "data_model": data_model_snippet,  # Pass as object
                "relationship_type": test_relationship["type"],
                "relationship_start_node_label": test_relationship["start_node_label"],
                "relationship_end_node_label": test_relationship["end_node_label"]
            }
        )
        
        if result.get('success'):
            query = result.get('result', '')
            print(f"   ✅ get_relationship_cypher_ingest_query: PASSED")
            print(f"   📄 Generated query: {query[:100]}...")
        else:
            print(f"   ❌ get_relationship_cypher_ingest_query: FAILED - {result.get('error', 'Unknown error')}")
        
        print(f"\n🎯 All MCP parameter tests completed!")
        
    except Exception as e:
        print(f"❌ Exception during parameter validation test: {e}")
        import traceback
        traceback.print_exc()

def test_full_content_ingestion():
    """Test the complete content ingestion workflow."""
    
    print("\n� Testing Full Content Ingestion Workflow")
    print("=" * 50)
    
    try:
        tool = GraphIngestionTool()
        
        # Check server connectivity
        if not tool.initialize_servers():
            print("❌ MCP servers are not reachable - skipping full test")
            return
        
        print("✅ MCP servers are reachable")
        
        # Test with sample content
        test_content = """
        John Smith is the CEO of TechCorp. He works closely with Sarah Johnson, 
        who is the CTO. TechCorp is headquartered in San Francisco and specializes 
        in artificial intelligence solutions. The company was founded in 2020.
        """
        
        print(f"\n📄 Test Content: {test_content.strip()}")
        
        # Test the complete ingestion process
        result = tool.ingest_content(
            content=test_content,
            source_name="test_document",
            content_type="text"
        )
        
        print(f"\n📊 Ingestion Result:")
        print(f"Success: {result.get('success', False)}")
        
        if result.get('success'):
            print(f"✅ Content ingestion completed successfully!")
            print(f"Entities created: {result.get('entities_created', 0)}")
            print(f"Relationships created: {result.get('relationships_created', 0)}")
            
            # Show processing stats
            stats = tool.get_processing_stats()
            print(f"\n📈 Processing Statistics:")
            for key, value in stats.items():
                if key != 'errors' or value:  # Only show errors if there are any
                    print(f"  {key}: {value}")
                
        else:
            error = result.get('error', 'Unknown error')
            print(f"❌ Ingestion failed: {error}")
            
    except Exception as e:
        print(f"❌ Exception during full ingestion test: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Run all tests."""
    print("🧪 Testing Cleaned GraphIngestionTool Implementation")
    print("=" * 70)
    
    # Test MCP parameter validation first
    test_mcp_parameter_validation()
    
    # Test full workflow
    test_full_content_ingestion()
    
    print(f"\n🏁 All tests completed!")

if __name__ == "__main__":
    main()