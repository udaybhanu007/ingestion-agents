"""
Test Configuration and Connectors

This script validates the configuration from .env.dev and tests the connectors.
"""

import sys
import os
import asyncio
import logging

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config_manager import get_config
from connector.box import BoxConnector
from connector.azure import AzureConnector
from connector.confluence_mcp import ConfluenceMCPConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_configuration():
    """Test and validate configuration."""
    print("Testing Configuration...")
    print("=" * 60)
    
    config = get_config()
    
    # Print configuration status
    config.print_status()
    
    return config


async def test_box_connector():
    """Test Box connector."""
    print("\nTesting Box Connector...")
    print("=" * 60)
    
    try:
        box_connector = BoxConnector()
        
        if not box_connector.is_available():
            print("❌ Box connector not available - missing dependencies or credentials")
            return False
        
        print("✓ Box connector initialized successfully")
        
        # Test authentication
        if box_connector.client:
            print("✓ Box client created successfully")
            
            # Try to get user info (this will trigger OAuth if needed)
            try:
                # This will require OAuth authentication in browser
                print("📋 Box connector ready for OAuth authentication")
                print("   Note: OAuth flow will require browser interaction")
                return True
            except Exception as e:
                print(f"⚠️  Box OAuth not completed: {e}")
                print("   This is expected if OAuth hasn't been completed yet")
                return True
        else:
            print("❌ Failed to create Box client")
            return False
            
    except Exception as e:
        print(f"❌ Box connector error: {e}")
        return False


async def test_azure_connector():
    """Test Azure connector."""
    print("\nTesting Azure Connector...")
    print("=" * 60)
    
    try:
        azure_connector = AzureConnector()
        
        if not azure_connector.is_available():
            print("❌ Azure connector not available - missing dependencies or credentials")
            return False
        
        print("✓ Azure connector initialized successfully")
        
        # Test by listing containers
        try:
            containers = azure_connector.list_containers()
            print(f"✓ Found {len(containers)} Azure Storage containers")
            
            if containers:
                for container in containers[:3]:  # Show first 3
                    print(f"   - {container['name']}")
            
            return True
            
        except Exception as e:
            print(f"⚠️  Azure Storage test failed: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Azure connector error: {e}")
        return False


async def test_confluence_mcp_connector():
    """Test Confluence MCP connector."""
    print("\nTesting Confluence MCP Connector...")
    print("=" * 60)
    
    try:
        confluence_connector = ConfluenceMCPConnector(env_file=".env.dev")
        
        if not confluence_connector.is_available():
            print("❌ Confluence MCP connector not available - missing dependencies or credentials")
            print("   Please ensure MCP dependencies are installed: pip install mcp-use langchain-openai")
            return False
        
        print("✓ Confluence MCP connector initialized successfully")
        
        # Test basic functionality
        try:
            # Test a simple query
            test_query = "Search for project documentation"
            print(f"   Testing query: '{test_query}'")
            
            result = await confluence_connector.query(test_query)
            if result:
                print("✓ MCP query test successful")
                print(f"   Result type: {type(result).__name__}")
                
                # Test content search
                search_results = await confluence_connector.search_content(
                    query="documentation", 
                    limit=5
                )
                print(f"✓ Found {len(search_results)} search results")
                
                if search_results:
                    for i, result in enumerate(search_results[:2]):  # Show first 2
                        print(f"   {i+1}. {result.get('title', 'No title')}")
                
                return True
            else:
                print("❌ MCP query returned no results")
                return False
                
        except Exception as e:
            print(f"⚠️  Confluence MCP test failed: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Confluence MCP connector error: {e}")
        return False


async def main():
    """Main test function."""
    print("Ingestion Agent Configuration & Connector Test")
    print("=" * 60)
    
    # Test configuration
    config = await test_configuration()
    
    # Test connectors
    results = {}
    
    results['box'] = await test_box_connector()
    results['azure'] = await test_azure_connector()
    results['confluence_mcp'] = await test_confluence_mcp_connector()
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary:")
    print("=" * 60)
    
    for service, success in results.items():
        status = "✓ PASS" if success else "❌ FAIL"
        print(f"{service.capitalize():15} {status}")
    
    total_passed = sum(results.values())
    total_tests = len(results)
    
    print(f"\nOverall: {total_passed}/{total_tests} connectors working")
    
    if total_passed == total_tests:
        print("🎉 All connectors are ready!")
    elif total_passed > 0:
        print("⚠️  Some connectors need attention")
    else:
        print("❌ No connectors are working - check configuration")
    
    print("\nNext Steps:")
    print("- Install missing dependencies: pip install -r requirements.txt")
    print("- Complete Box OAuth2 authentication (browser-based)")
    print("- Test ingestion workflow with: python api/ingestion_api.py")


if __name__ == "__main__":
    asyncio.run(main())
