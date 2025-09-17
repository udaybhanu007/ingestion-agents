#!/usr/bin/env python3
"""
Test the GraphIngestionTool to verify Neo4j connectivity and data ingestion.
"""

import sys
import os
import logging

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath('.'))

from agents.tools.graph_tool import GraphIngestionTool

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_graph_tool():
    """Test the GraphIngestionTool functionality."""
    logger.info("🚀 Starting GraphIngestionTool test...")
    
    try:
        # Initialize the tool
        logger.info("Initializing GraphIngestionTool...")
        tool = GraphIngestionTool()
        
        # Test server connectivity
        logger.info("Testing MCP server connectivity...")
        servers_ok = tool.initialize_servers(timeout=10)
        
        if not servers_ok:
            logger.error("❌ MCP servers are not reachable!")
            return False
        
        logger.info("✅ MCP servers are reachable")
        
        # Test content ingestion with a simple example
        logger.info("Testing content ingestion...")
        test_content = """
        John Smith works at Acme Corporation as a Software Engineer. 
        He has been with the company since 2020 and leads the development team. 
        Acme Corporation is a technology company founded in 2010 and is based in San Francisco.
        The company specializes in cloud computing solutions.
        """
        
        result = tool.ingest_content(
            content=test_content,
            source_name="test_document",
            content_type="text",
            metadata={"test": True}
        )
        
        logger.info(f"Ingestion result: {result}")
        
        if result.get("success", False):
            stats = result.get("stats", {})
            entities_created = result.get("entities_created", 0)
            relationships_created = result.get("relationships_created", 0)
            
            logger.info("✅ Content ingestion successful!")
            logger.info(f"📊 Entities created: {entities_created}")
            logger.info(f"🔗 Relationships created: {relationships_created}")
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
    logger.info("=== Neo4j GraphIngestionTool Test ===")
    
    success = test_graph_tool()
    
    if success:
        logger.info("🎉 All tests passed! Neo4j ingestion is working correctly.")
    else:
        logger.error("❌ Tests failed. Check the logs above for details.")
    
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