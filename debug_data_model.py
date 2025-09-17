"""
Debug the data model structure issue
"""

import json
from agents.tools.graph_tool import GraphIngestionTool

def debug_data_model_structure():
    """Debug what's happening with the data model structure."""
    
    print("🔍 Debugging Data Model Structure Issue")
    print("=" * 50)
    
    try:
        tool = GraphIngestionTool()
        
        # Create a simple test data model that should work
        test_data_model = {
            "nodes": [
                {
                    "label": "Person",
                    "key_property": {
                        "name": "name", 
                        "type": "STRING", 
                        "description": "Person name"
                    },
                    "properties": [
                        {
                            "name": "name", 
                            "type": "STRING", 
                            "description": "Person name"
                        }
                    ]
                }
            ],
            "relationships": [
                {
                    "type": "KNOWS",
                    "start_node_label": "Person",
                    "end_node_label": "Person",
                    "properties": []
                }
            ]
        }
        
        print("📋 Test Data Model Structure:")
        print(json.dumps(test_data_model, indent=2))
        
        print("\n🧪 Testing validate_data_model...")
        result = tool.make_mcp_request(
            tool.data_modeling_server_url,
            "validate_data_model",
            {"data_model": test_data_model}
        )
        
        print(f"✅ Result: {result}")
        
        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            print(f"❌ Validation failed: {error_msg}")
            
            # Try to debug by looking at the actual arguments being sent
            print(f"\n🔍 Debugging arguments structure...")
            print(f"Argument type: {type(test_data_model)}")
            print(f"Has 'nodes' key: {'nodes' in test_data_model}")
            print(f"Has 'relationships' key: {'relationships' in test_data_model}")
            print(f"Nodes count: {len(test_data_model.get('nodes', []))}")
            print(f"Relationships count: {len(test_data_model.get('relationships', []))}")
        
    except Exception as e:
        print(f"❌ Exception: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_data_model_structure()