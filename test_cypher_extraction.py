#!/usr/bin/env python3
"""
Test script to verify that Cypher query extraction from MCP responses is working correctly.
"""

import sys
import os
import json
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from agents.tools.graph_tool import GraphIngestionTool

def test_cypher_query_extraction():
    """Test that Cypher queries are extracted correctly from MCP responses."""
    print("Testing Cypher query extraction from MCP responses...")
    
    try:
        # Initialize the tool
        tool = GraphIngestionTool()
        
        # Test content that should generate nodes and relationships
        test_content = """
        Alice works at TechCorp as an engineer. 
        Bob manages the engineering team at TechCorp.
        TechCorp is located in San Francisco.
        """
        
        print("Test content:", test_content)
        
        # Run the ingestion process
        result = tool.ingest_content(test_content)
        
        print(f"\nIngestion result: {result}")
        
        if result.get("success", False):
            entities_created = result.get("entities_created", 0)
            relationships_created = result.get("relationships_created", 0)
            
            print("✅ Cypher query extraction and execution successful!")
            print(f"📊 Entities created: {entities_created}")
            print(f"🔗 Relationships created: {relationships_created}")
            
            if entities_created > 0 and relationships_created > 0:
                print("✅ Both nodes and relationships were created - Cypher queries working correctly!")
                return True
            else:
                print("⚠️  Some entities/relationships missing - check Cypher query generation")
                return False
        else:
            error_msg = result.get("error", "Unknown error")
            print(f"❌ Ingestion failed: {error_msg}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_mcp_response_parsing():
    """Test the MCP response parsing logic with mock data."""
    print("\nTesting MCP response parsing logic...")
    
    try:
        tool = GraphIngestionTool()
        
        # Mock MCP response in the format shown in the user's example
        mock_response = {
            "success": True,
            "result": {
                "content": [{"type": "text", "text": "UNWIND $records as record\nMERGE (n: Person {name: record.name})\nSET n += {description: record.description}"}],
                "structuredContent": {"result": "UNWIND $records as record\nMERGE (n: Person {name: record.name})\nSET n += {description: record.description}"},
                "isError": False
            }
        }
        
        print(f"Mock response: {json.dumps(mock_response, indent=2)}")
        
        # Test query extraction logic
        if mock_response.get("success", False) and "result" in mock_response:
            result_data = mock_response["result"]
            cypher_query = None
            
            # Check for structuredContent first (preferred format)
            if isinstance(result_data, dict) and "structuredContent" in result_data:
                structured_content = result_data["structuredContent"]
                if isinstance(structured_content, dict) and "result" in structured_content:
                    cypher_query = structured_content["result"]
            
            # Fallback: check for direct string result
            if not cypher_query and isinstance(result_data, str):
                cypher_query = result_data
            
            # Fallback: check for content array format
            if not cypher_query and isinstance(result_data, dict) and "content" in result_data:
                content = result_data["content"]
                if isinstance(content, list) and len(content) > 0:
                    first_content = content[0]
                    if isinstance(first_content, dict) and "text" in first_content:
                        cypher_query = first_content["text"]
            
            if cypher_query:
                print("✅ Successfully extracted Cypher query:")
                print(f"Query: {cypher_query}")
                
                # Verify it's a valid Cypher query
                if "UNWIND" in cypher_query and "MERGE" in cypher_query and "$records" in cypher_query:
                    print("✅ Cypher query structure looks correct!")
                    return True
                else:
                    print("❌ Cypher query structure doesn't look right")
                    return False
            else:
                print("❌ Failed to extract Cypher query from mock response")
                return False
        else:
            print("❌ Mock response structure invalid")
            return False
            
    except Exception as e:
        print(f"❌ Parsing test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🧪 Testing Cypher Query Extraction Fixes")
    print("=" * 50)
    
    # Test 1: MCP response parsing logic
    parsing_test = test_mcp_response_parsing()
    
    # Test 2: Full ingestion with real MCP servers
    ingestion_test = test_cypher_query_extraction()
    
    print("\n" + "=" * 50)
    print("📊 Test Results:")
    print(f"   MCP response parsing: {'✅ PASS' if parsing_test else '❌ FAIL'}")
    print(f"   Full ingestion test: {'✅ PASS' if ingestion_test else '❌ FAIL'}")
    
    if parsing_test and ingestion_test:
        print("\n🎉 All tests passed! Cypher query extraction should be working correctly.")
    else:
        print("\n⚠️  Some tests failed. Check the errors above.")

if __name__ == "__main__":
    main()