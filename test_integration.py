"""
Test script for VectorToolV2 integration
"""
import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_tool_registry():
    """Test the tool registry integration."""
    print("🚀 Testing Tool Registry Integration")
    
    try:
        from agents.tools.tool_registry import get_tool_registry
        registry = get_tool_registry()
        
        print(f"Registry info: {registry.get_tool_info()}")
        
        # Test getting vector tool
        vector_tool = registry.get_tool('vector_ingestion')
        print(f"Vector tool available: {vector_tool is not None}")
        
        if vector_tool:
            print(f"Vector tool type: {type(vector_tool)}")
            print(f"Vector tool name: {vector_tool.name}")
            print(f"Vector tool enabled: {vector_tool.enabled}")
        
        return vector_tool
    
    except Exception as e:
        print(f"❌ Tool registry test failed: {e}")
        import traceback
        traceback.print_exc()
        return None

async def test_vector_ingestion():
    """Test vector ingestion functionality."""
    print("\n🔧 Testing Vector Ingestion")
    
    try:
        from agents.tools.tool_registry import get_tool_registry
        registry = get_tool_registry()
        vector_tool = registry.get_tool('vector_ingestion')
        
        if not vector_tool:
            print("❌ Vector tool not available")
            return
        
        if not vector_tool.enabled:
            print("❌ Vector tool not enabled (missing dependencies)")
            return
        
        # Test content
        test_content = """
        This is a test document for vector ingestion.
        It contains multiple sentences to test the chunking functionality.
        The LlamaIndex SentenceSplitter should handle this content properly.
        """
        
        test_metadata = {
            'doc_uri': 'test://document/integration_test',
            'source': 'integration_test',
            'document_type': 'text'
        }
        
        print("Testing vector ingestion...")
        result = await vector_tool.ingest(test_content, test_metadata)
        
        print(f"Ingestion result: {result}")
        
        if result.get('success'):
            print("✅ Vector ingestion test passed!")
        else:
            print(f"❌ Vector ingestion test failed: {result.get('error')}")
        
        return result
    
    except Exception as e:
        print(f"❌ Vector ingestion test failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_execution_agent():
    """Test execution agent integration."""
    print("\n🎯 Testing Execution Agent Integration")
    
    try:
        from agents.execution_agent import ExecutionAgent
        
        # Initialize execution agent
        agent = ExecutionAgent()
        print("✅ Execution agent initialized successfully")
        
        # Check if tools are available
        print(f"Available tools: {list(agent.tools.keys())}")
        
        if 'vector_ingestion' in agent.tools:
            print("✅ Vector ingestion tool is available in execution agent")
        else:
            print("❌ Vector ingestion tool not found in execution agent")
        
        return agent
    
    except Exception as e:
        print(f"❌ Execution agent test failed: {e}")
        import traceback
        traceback.print_exc()
        return None

async def main():
    """Run all integration tests."""
    print("🧪 Starting Integration Tests for VectorToolV2\n")
    
    # Test 1: Tool Registry
    vector_tool = test_tool_registry()
    
    # Test 2: Vector Ingestion (only if tool is available)
    if vector_tool and vector_tool.enabled:
        await test_vector_ingestion()
    else:
        print("\n⏭️ Skipping vector ingestion test (tool not enabled)")
    
    # Test 3: Execution Agent
    test_execution_agent()
    
    print("\n🏁 Integration tests completed!")

if __name__ == "__main__":
    asyncio.run(main())
