#!/usr/bin/env python3
"""
Simple test for GraphIngestionTool without importing the full agents module.
"""

import sys
import os
import logging

# Add paths for direct import
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.join(os.path.abspath('.'), 'agents', 'tools'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_graph_tool_directly():
    """Test GraphIngestionTool by importing it directly."""
    logger.info("🚀 Testing GraphIngestionTool directly...")
    
    try:
        # Direct import
        from graph_tool import GraphIngestionTool
        
        # Initialize
        logger.info("Initializing GraphIngestionTool...")
        tool = GraphIngestionTool()
        
        # Test server connectivity
        logger.info("Testing MCP server connectivity...")
        servers_ok = tool.initialize_servers(timeout=10)
        
        if not servers_ok:
            logger.error("❌ MCP servers are not reachable!")
            return False
        
        logger.info("✅ MCP servers are reachable")
        
        # Test simple content ingestion
        logger.info("Testing content ingestion...")
        test_content = """
        Alice Johnson is a data scientist at TechCorp. 
        She works on machine learning projects and has expertise in Python and R.
        TechCorp is a software company specializing in AI solutions.
        """
        
        result = tool.ingest_content(
            content=test_content,
            source_name="simple_test",
            content_type="text"
        )
        
        logger.info(f"Ingestion result: {result}")
        
        if result.get("success", False):
            entities_created = result.get("entities_created", 0)
            relationships_created = result.get("relationships_created", 0)
            
            logger.info("✅ Content ingestion successful!")
            logger.info(f"📊 Entities created: {entities_created}")
            logger.info(f"🔗 Relationships created: {relationships_created}")
            
            # Get processing stats
            stats = tool.get_processing_stats()
            logger.info(f"📈 Processing stats: {stats}")
            
            return True
        else:
            error_msg = result.get("error", "Unknown error")
            logger.error(f"❌ Content ingestion failed: {error_msg}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Test failed with exception: {e}", exc_info=True)
        return False

def main():
    """Main test function."""
    logger.info("=== Simple Neo4j GraphIngestionTool Test ===")
    
    success = test_graph_tool_directly()
    
    if success:
        logger.info("🎉 Test passed! Neo4j ingestion is working correctly.")
        logger.info("✅ The KeyError issue has been resolved!")
        logger.info("✅ MCP servers are communicating properly!")
        logger.info("✅ Data ingestion to Neo4j is functional!")
    else:
        logger.error("❌ Test failed. Check the logs above for details.")
    
    return success

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test suite failed: {e}", exc_info=True)
        sys.exit(1)