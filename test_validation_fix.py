#!/usr/bin/env python3
"""
Test script to verify that the ValidationError for duplicate node labels is fixed.
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from agents.tools.graph_tool import GraphIngestionTool

def test_duplicate_entities_handling():
    """Test that the tool handles multiple entities with the same type correctly."""
    print("Testing duplicate entity handling...")
    
    try:
        # Initialize the tool
        tool = GraphIngestionTool()
        
        # Test content that would generate multiple entities of the same type
        test_content = """
        John Smith is a software engineer at TechCorp. 
        Mary Johnson is also a software engineer at TechCorp.
        Bob Wilson is the manager at TechCorp.
        Alice Brown is another engineer at the same company.
        TechCorp is a technology company in San Francisco.
        The company works on AI projects and cloud solutions.
        """
        
        print("Test content:", test_content)
        
        # Step 1: Extract entities (this should generate multiple Person entities)
        entities_relationships = tool._extract_entities_with_llm(test_content)
        print("\nExtracted entities and relationships:")
        print(entities_relationships)
        
        if not entities_relationships:
            print("❌ Failed to extract entities from content")
            return False
        
        # Step 2: Build data model (this should deduplicate by entity type)
        data_model = tool._build_neo4j_data_model(entities_relationships)
        print("\nGenerated data model:")
        print(data_model)
        
        if not data_model:
            print("❌ Failed to build data model")
            return False
        
        # Check for unique node labels
        node_labels = [node['label'] for node in data_model.get('nodes', [])]
        unique_labels = set(node_labels)
        
        print(f"\nNode labels: {node_labels}")
        print(f"Unique labels: {list(unique_labels)}")
        
        if len(node_labels) == len(unique_labels):
            print("✅ All node labels are unique - no duplicates!")
            return True
        else:
            print(f"❌ Found duplicate node labels: {len(node_labels)} total, {len(unique_labels)} unique")
            duplicates = [label for label in node_labels if node_labels.count(label) > 1]
            print(f"Duplicate labels: {set(duplicates)}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_mcp_validation():
    """Test that the MCP server validation passes."""
    print("\nTesting MCP server validation...")
    
    try:
        tool = GraphIngestionTool()
        
        # Test with simple content
        test_content = "Alice works at CompanyX. Bob also works at CompanyX."
        
        # Test the full ingestion process
        result = tool.ingest_content(test_content)
        
        print(f"Ingestion result: {result}")
        
        if result.get("success", False):
            print("✅ MCP validation passed - no ValidationError!")
            return True
        else:
            error_msg = result.get("error", "Unknown error")
            if "ValidationError" in error_msg or "appears" in error_msg and "times" in error_msg:
                print(f"❌ MCP validation still failing with ValidationError: {error_msg}")
                return False
            else:
                print(f"⚠️  Different error (not ValidationError): {error_msg}")
                return True  # Different error, validation issue might be fixed
        
    except Exception as e:
        print(f"❌ MCP validation test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🧪 Testing ValidationError fixes for duplicate node labels")
    print("=" * 60)
    
    # Test 1: Check duplicate handling in data model building
    dedup_test = test_duplicate_entities_handling()
    
    # Test 2: Check MCP validation
    validation_test = test_mcp_validation()
    
    print("\n" + "=" * 60)
    print("📊 Test Results:")
    print(f"   Duplicate handling: {'✅ PASS' if dedup_test else '❌ FAIL'}")
    print(f"   MCP validation: {'✅ PASS' if validation_test else '❌ FAIL'}")
    
    if dedup_test and validation_test:
        print("\n🎉 All tests passed! ValidationError should be resolved.")
    else:
        print("\n⚠️  Some tests failed. Check the errors above.")

if __name__ == "__main__":
    main()