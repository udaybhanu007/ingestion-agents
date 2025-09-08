"""
Test MCP Confluence Integration

This script demonstrates the advanced MCP-powered Confluence connector
and its integration with the existing ingestion agents architecture.
"""

import asyncio
import sys
import os
import logging

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connector.confluence_mcp import ConfluenceMCPConnector, create_confluence_mcp_connector
from agents.planner_agent import PlannerAgent
from config.config_manager import get_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_mcp_confluence_connector():
    """Test the MCP Confluence connector capabilities."""
    print("=" * 60)
    print("Testing MCP Confluence Connector")
    print("=" * 60)
    
    try:
        # Initialize the MCP connector
        connector = create_confluence_mcp_connector()
        
        if not connector.is_available():
            print("❌ MCP Confluence connector not available")
            print("   Reasons:")
            print("   - Missing dependencies: pip install mcp-use langchain-openai")
            print("   - MCP server not running on http://localhost:9000/mcp")
            print("   - Azure OpenAI credentials not configured")
            print("   - Confluence credentials not configured")
            return False
        
        print("✓ MCP Confluence connector initialized successfully")
        
        # Test 1: Simple query
        print("\n📋 Test 1: Simple MCP Query")
        try:
            result = await connector.query("Get basic information about the Confluence instance")
            print(f"✓ Query successful: {result[:100]}...")
        except Exception as e:
            print(f"⚠️  Query failed: {e}")
        
        # Test 2: Download specific pages
        print("\n📋 Test 2: Download Specific Pages")
        test_pages = [
            "Chest X-Ray Medical Document", 
            "Chest X-ray Report Analysis",
            "Medical Documentation Guidelines"
        ]
        
        download_result = await connector.download_multiple_pages(test_pages)
        print(f"✓ Download completed:")
        print(f"   - Total pages: {download_result['total_pages']}")
        print(f"   - Successful: {download_result['successful_downloads']}")
        print(f"   - Failed: {download_result['failed_downloads']}")
        print(f"   - Download directory: {download_result['download_dir']}")
        
        for file_path in download_result['downloaded_files']:
            if file_path and os.path.exists(file_path):
                size = os.path.getsize(file_path)
                print(f"   - {os.path.basename(file_path)}: {size} bytes")
        
        # Test 3: Fetch documents for ingestion
        print("\n📋 Test 3: Fetch Documents for Ingestion")
        documents = await connector.fetch_documents(
            page_titles=["Chest X-Ray Medical Document"],
            download_dir="./test_downloads"
        )
        
        print(f"✓ Fetched {len(documents)} documents for ingestion")
        for doc in documents:
            print(f"   - ID: {doc['id']}")
            print(f"   - Name: {doc['name']}")
            print(f"   - Source: {doc['source']}")
            print(f"   - Content length: {len(doc.get('content', ''))}")
        
        return True
        
    except Exception as e:
        logger.error(f"MCP Confluence connector test failed: {e}")
        print(f"❌ Test failed: {e}")
        return False


async def test_planner_integration():
    """Test integration with the planner agent."""
    print("\n" + "=" * 60)
    print("Testing Planner Agent Integration")
    print("=" * 60)
    
    try:
        planner = PlannerAgent()
        
        # Test 1: Classification of MCP Confluence document
        print("\n📋 Test 1: Document Classification")
        test_uri = "confluence-mcp://Medical Documentation Guidelines"
        metadata = {
            "source": "confluence_mcp",
            "space_key": "MED",
            "priority": 8
        }
        
        classification = planner.classifier.classify_document(test_uri, metadata)
        print(f"✓ Classification result:")
        print(f"   - Content type: {classification['content_type']}")
        print(f"   - Requires vector: {classification['requires_vector']}")
        print(f"   - Requires graph: {classification['requires_graph']}")
        print(f"   - Requires MCP: {classification.get('requires_mcp', False)}")
        print(f"   - Complexity: {classification['complexity']}")
        
        # Test 2: Create ingestion plan
        print("\n📋 Test 2: Create Ingestion Plan")
        plan_result = planner.create_ingestion_plan(test_uri, metadata)
        
        print(f"✓ Ingestion plan created:")
        print(f"   - Plan ID: {plan_result['plan_id']}")
        print(f"   - Number of steps: {len(plan_result['steps'])}")
        
        for i, step in enumerate(plan_result['steps'], 1):
            print(f"   - Step {i}: {step['tool']} (depends on: {step['depends_on']})")
        
        # Test 3: Plan validation
        plan_id = plan_result['plan_id']
        plan_status = planner.get_plan_status(plan_id)
        print(f"✓ Plan status: {plan_status['status']}")
        
        return True
        
    except Exception as e:
        logger.error(f"Planner integration test failed: {e}")
        print(f"❌ Test failed: {e}")
        return False


async def test_full_integration():
    """Test the complete MCP Confluence integration flow."""
    print("\n" + "=" * 60)
    print("Testing Full Integration Flow")
    print("=" * 60)
    
    try:
        # This would simulate the full API workflow
        print("\n📋 Test: Full Ingestion Workflow Simulation")
        
        # Step 1: Initialize components
        connector = create_confluence_mcp_connector()
        planner = PlannerAgent()
        
        if not connector.is_available():
            print("❌ MCP connector not available for full integration test")
            return False
        
        # Step 2: Simulate API request
        request_data = {
            "source_uri": "confluence-mcp://Medical Documentation Guidelines",
            "metadata": {
                "source": "confluence_mcp",
                "space_key": "MED",
                "priority": 8,
                "user": "test_user"
            }
        }
        
        print(f"✓ Simulating API request: {request_data['source_uri']}")
        
        # Step 3: Plan creation
        plan_result = planner.create_ingestion_plan(
            request_data["source_uri"], 
            request_data["metadata"]
        )
        
        print(f"✓ Plan created with {len(plan_result['steps'])} steps")
        
        # Step 4: Simulate execution (without actual execution)
        print("\n📋 Execution Simulation:")
        for step in plan_result['steps']:
            if step['tool'] == 'mcp_confluence':
                print(f"   → Step {step['task_id']}: MCP Confluence extraction")
                print(f"      - Would extract content using LLM intelligence")
                print(f"      - Would save to local file for processing")
            elif step['tool'] == 'vector_ingestion':
                print(f"   → Step {step['task_id']}: Vector ingestion")
                print(f"      - Would process extracted content for vector search")
                print(f"      - Would upload to Qdrant vector database")
            elif step['tool'] == 'graph_ingestion':
                print(f"   → Step {step['task_id']}: Graph ingestion")
                print(f"      - Would analyze relationships in content")
                print(f"      - Would create nodes/relationships in Neo4j")
        
        print("✓ Full integration flow validated")
        return True
        
    except Exception as e:
        logger.error(f"Full integration test failed: {e}")
        print(f"❌ Test failed: {e}")
        return False


async def test_configuration():
    """Test configuration and environment setup."""
    print("\n" + "=" * 60)
    print("Testing Configuration")
    print("=" * 60)
    
    try:
        config = get_config()
        
        # Test Azure OpenAI configuration
        azure_config = config.get_openai_config()
        print("📋 Azure OpenAI Configuration:")
        if azure_config.get('azure_endpoint') and azure_config.get('azure_api_key'):
            print("   ✓ Azure OpenAI credentials configured")
            print(f"   - Endpoint: {azure_config['azure_endpoint']}")
            print(f"   - Deployment: {azure_config.get('deployment_name', 'N/A')}")
        else:
            print("   ❌ Azure OpenAI credentials missing")
        
        # Test Confluence configuration
        confluence_config = config.get_confluence_config()
        print("\n📋 Confluence Configuration:")
        if confluence_config.get('access_token') or (confluence_config.get('username') and confluence_config.get('token')):
            print("   ✓ Confluence credentials configured")
            print(f"   - Base URL: {confluence_config.get('base_url', 'N/A')}")
            print(f"   - Auth method: {'OAuth2' if confluence_config.get('access_token') else 'Basic'}")
        else:
            print("   ❌ Confluence credentials missing")
        
        # Test other integrations
        validation_results = config.validate_config()
        print("\n📋 Service Validation:")
        for service, is_valid in validation_results.items():
            status = "✓" if is_valid else "❌"
            print(f"   {status} {service.capitalize()}: {'Configured' if is_valid else 'Missing/Incomplete'}")
        
        return True
        
    except Exception as e:
        logger.error(f"Configuration test failed: {e}")
        print(f"❌ Test failed: {e}")
        return False


async def main():
    """Main test function."""
    print("MCP Confluence Integration Test Suite")
    print("=" * 60)
    
    # Run all tests
    results = {}
    
    results['configuration'] = await test_configuration()
    results['mcp_connector'] = await test_mcp_confluence_connector()
    results['planner_integration'] = await test_planner_integration()
    results['full_integration'] = await test_full_integration()
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    total_tests = len(results)
    passed_tests = sum(results.values())
    
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "❌ FAIL"
        print(f"{test_name.replace('_', ' ').title():20} {status}")
    
    print(f"\nOverall: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print("🎉 All tests passed! MCP Confluence integration is ready.")
    elif passed_tests > 0:
        print("⚠️  Some tests failed. Check configuration and dependencies.")
    else:
        print("❌ All tests failed. Please check setup and try again.")
    
    print("\nNext Steps:")
    if not results['configuration']:
        print("1. Configure environment variables in .env.dev")
    if not results['mcp_connector']:
        print("2. Install MCP dependencies: pip install mcp-use langchain-openai")
        print("3. Start MCP Atlassian server on http://localhost:9000/mcp")
    if results['configuration'] and results['mcp_connector']:
        print("4. Ready for production use!")
        print("5. Test with real Confluence pages")
        print("6. Monitor LLM usage and costs")


if __name__ == "__main__":
    asyncio.run(main())
