#!/usr/bin/env python3
"""
Test script to verify the fixed GraphIngestionTool works with correct MCP tool names.
"""

import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(__file__))

from agents.tools.graph_tool import GraphIngestionTool

def test_server_initialization():
    """Test if the servers can be initialized with correct tool names."""
    print("🧪 Testing GraphIngestionTool with corrected MCP tool names")
    print("=" * 60)
    
    try:
        # Initialize the tool
        graph_tool = GraphIngestionTool()
        print("✅ GraphIngestionTool initialized successfully")
        
        # Test server initialization
        print("\n🔧 Testing server initialization...")
        result = graph_tool.initialize_servers()
        
        if result:
            print("✅ Both MCP servers initialized successfully!")
            print(f"   - Cypher server: {graph_tool.cypher_server_url}")
            print(f"   - Data modeling server: {graph_tool.data_modeling_server_url}")
        else:
            print("❌ Server initialization failed")
            print("Check if both MCP servers are running on the specified ports")
            
        return result
        
    except Exception as e:
        print(f"❌ Error during initialization: {e}")
        return False

def test_simple_ingestion():
    """Test simple content ingestion."""
    print("\n📝 Testing simple content ingestion...")
    
    try:
        graph_tool = GraphIngestionTool()
        
        # Test content
        test_content = """
        John Smith is a software engineer at TechCorp. 
        He works on AI projects and collaborates with Mary Johnson from the research team.
        TechCorp is headquartered in San Francisco and focuses on machine learning solutions.
        """
        
        result = graph_tool.ingest_content(
            content=test_content,
            source_name="test_document",
            content_type="text",
            metadata={"document_type": "company_info"}
        )
        
        print(f"Ingestion result: {result}")
        
        if result.get("success"):
            print("✅ Content ingestion completed successfully!")
            stats = graph_tool.get_processing_stats()
            print(f"   - Documents processed: {stats['documents_processed']}")
            print(f"   - Entities ingested: {stats['entities_ingested']}")
            print(f"   - Relationships created: {stats['relationships_created']}")
        else:
            print("❌ Content ingestion failed")
            print(f"   Error: {result.get('error', 'Unknown error')}")
            
        return result.get("success", False)
        
    except Exception as e:
        print(f"❌ Error during ingestion: {e}")
        return False

def main():
    print("🚀 Starting GraphIngestionTool tests with corrected MCP tool names")
    
    # Test 1: Server initialization
    init_success = test_server_initialization()
    
    if not init_success:
        print("\n⚠️  Server initialization failed. Ensure MCP servers are running:")
        print("   - Neo4j Cypher server on http://127.0.0.1:8003/mcp/")
        print("   - Data modeling server on http://127.0.0.1:8004/mcp/")
        return
    
    # Test 2: Simple ingestion
    ingestion_success = test_simple_ingestion()
    
    # Summary
    print(f"\n📊 Test Summary:")
    print(f"   Server initialization: {'✅ PASS' if init_success else '❌ FAIL'}")
    print(f"   Content ingestion: {'✅ PASS' if ingestion_success else '❌ FAIL'}")
    
    if init_success and ingestion_success:
        print(f"\n🎉 All tests passed! GraphIngestionTool is working correctly.")
    else:
        print(f"\n⚠️  Some tests failed. Check the errors above.")

if __name__ == "__main__":
    main()
