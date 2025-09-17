#!/usr/bin/env python3
"""
Debug script to capture and analyze the actual Cypher queries being generated and executed
"""

import json
import requests

def parse_sse_response(response_text):
    """Parse Server-Sent Events response format"""
    try:
        lines = response_text.strip().split('\n')
        for line in lines:
            if line.startswith('data: '):
                data_content = line[6:]  # Remove 'data: ' prefix
                return json.loads(data_content)
    except Exception as e:
        print(f"Error parsing SSE response: {e}")
        print(f"Raw response: {response_text}")
    return None

def make_mcp_request(server_url, tool_name, params=None):
    """Make a request to MCP server and return response"""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": params or {}
        }
    }
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream"
    }
    
    try:
        response = requests.post(server_url, json=payload, headers=headers)
        print(f"\n🔍 Tool: {tool_name}")
        print(f"📤 Request: {json.dumps(params, indent=2) if params else 'No params'}")
        print(f"📥 Response status: {response.status_code}")
        
        if response.status_code == 200:
            parsed = parse_sse_response(response.text)
            if parsed and 'result' in parsed:
                result = parsed['result']
                if 'content' in result and result['content']:
                    content = result['content'][0].get('text', '')
                    print(f"📋 Content: {content}")
                    
                    # Try to parse as JSON for better formatting
                    try:
                        json_content = json.loads(content)
                        if isinstance(json_content, list) and json_content:
                            for item in json_content:
                                if 'query' in item:
                                    print(f"🎯 Cypher Query:")
                                    print(f"   {item['query']}")
                    except:
                        pass
                    
                    return content
        else:
            print(f"❌ Error: {response.text}")
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
    
    return None

def test_data_model_generation():
    """Test the data model generation for our sample content"""
    
    print("🚀 Testing Data Model Generation and Cypher Query Capture")
    
    # Sample entities (simplified version of what LLM extracts)
    sample_entities = [
        {
            "name": "John Smith",
            "type": "Person",
            "properties": {
                "name": "John Smith",
                "profession": "cardiologist"
            }
        },
        {
            "name": "TechCorp Medical Center", 
            "type": "Organization",
            "properties": {
                "name": "TechCorp Medical Center",
                "type": "medical_center"
            }
        }
    ]
    
    sample_relationships = [
        {
            "source": "John Smith",
            "target": "TechCorp Medical Center",
            "relationship": "WORKS_AT",
            "properties": {}
        }
    ]
    
    data_model_server = "http://127.0.0.1:8004/mcp/"
    cypher_server = "http://127.0.0.1:8003/mcp/"
    
    # Build data model
    print("\n" + "="*50)
    print("📊 BUILDING DATA MODEL")
    print("="*50)
    
    data_model = {
        "nodes": {},
        "relationships": {}
    }
    
    # Add nodes to data model
    for entity in sample_entities:
        entity_type = entity["type"]
        if entity_type not in data_model["nodes"]:
            data_model["nodes"][entity_type] = {
                "properties": list(entity["properties"].keys()),
                "key_property": "name"  # This is required!
            }
    
    # Add relationships to data model  
    for rel in sample_relationships:
        rel_type = rel["relationship"]
        if rel_type not in data_model["relationships"]:
            data_model["relationships"][rel_type] = {
                "properties": list(rel["properties"].keys()) if rel["properties"] else []
            }
    
    print(f"📋 Data Model: {json.dumps(data_model, indent=2)}")
    
    # Validate data model
    result = make_mcp_request(data_model_server, "validate_data_model", {
        "data_model": data_model
    })
    
    if not result:
        print("❌ Data model validation failed")
        return
    
    print("\n" + "="*50)
    print("🔧 GENERATING CONSTRAINT QUERIES")
    print("="*50)
    
    # Get constraint queries
    make_mcp_request(data_model_server, "get_constraints_cypher_queries", {
        "data_model": data_model
    })
    
    print("\n" + "="*50)
    print("📝 GENERATING NODE CREATION QUERIES")
    print("="*50)
    
    # Generate node creation queries for each entity type
    for entity_type in data_model["nodes"]:
        entities_of_type = [e for e in sample_entities if e["type"] == entity_type]
        if entities_of_type:
            make_mcp_request(data_model_server, "get_node_cypher_ingest_query", {
                "data_model": data_model,
                "node_type": entity_type,
                "nodes_data": entities_of_type
            })
    
    print("\n" + "="*50)
    print("🔗 GENERATING RELATIONSHIP CREATION QUERIES") 
    print("="*50)
    
    # Generate relationship creation queries
    for rel_type in data_model["relationships"]:
        rels_of_type = [r for r in sample_relationships if r["relationship"] == rel_type]
        if rels_of_type:
            make_mcp_request(data_model_server, "get_relationship_cypher_ingest_query", {
                "data_model": data_model,
                "relationship_type": rel_type,
                "relationships_data": rels_of_type
            })

if __name__ == "__main__":
    test_data_model_generation()