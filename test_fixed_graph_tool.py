"""
Test the fixed graph tool with actual MCP server integration
"""
import logging
import sys
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add parent directory to path for imports
parent_dir = os.path.dirname(os.path.dirname(__file__))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from agents.tools.graph_tool import GraphIngestionTool

def test_graph_ingestion():
    """Test the fixed graph ingestion tool"""
    try:
        print("🚀 Testing Fixed Graph Ingestion Tool")
        
        # Initialize the tool
        tool = GraphIngestionTool()
        
        # Test MCP server connectivity
        print("\n📡 Testing MCP server connectivity...")
        servers_ok = tool.initialize_servers()
        if not servers_ok:
            print("❌ MCP servers not reachable!")
            return False
        
        print("✅ MCP servers are reachable")
        
        # Test with sample content
        sample_content = """
        John Smith is a cardiologist at TechCorp Medical Center. 
        He works with Mary Johnson, who is a nurse. 
        TechCorp is located in San Francisco and specializes in cardiac care.
        John graduated from Stanford University in 2010.
        """
        
        print("\n📝 Testing content ingestion...")
        print(f"Sample content: {sample_content[:100]}...")
        
        # Perform ingestion
        result = tool.ingest_content(
            content=sample_content,
            source_name="test_document",
            content_type="medical_text"
        )
        
        print(f"\n📊 Ingestion Result:")
        print(f"Success: {result.get('success', False)}")
        if result.get('success'):
            print(f"Entities created: {result.get('entities_created', 0)}")
            print(f"Relationships created: {result.get('relationships_created', 0)}")
            stats = result.get('stats', {})
            print(f"Total documents processed: {stats.get('documents_processed', 0)}")
            print(f"Total schemas generated: {stats.get('schemas_generated', 0)}")
        else:
            print(f"Error: {result.get('error', 'Unknown error')}")
            
        return result.get('success', False)
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        logger.exception("Test error details:")
        return False

if __name__ == "__main__":
    success = test_graph_ingestion()
    if success:
        print("\n✅ Graph ingestion test completed successfully!")
    else:
        print("\n❌ Graph ingestion test failed!")
    
    sys.exit(0 if success else 1)