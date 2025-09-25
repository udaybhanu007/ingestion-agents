#!/usr/bin/env python3
"""
Clear all nodes from Neo4j database
"""

import requests
import json

def clear_database():
    """Clear all nodes and relationships from Neo4j"""
    cypher_server_url = "http://127.0.0.1:8003"
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream"
    }
    
    clear_query = "MATCH (n) DETACH DELETE n"
    
    request_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "write_neo4j_cypher",
            "arguments": {
                "query": clear_query
            }
        }
    }
    
    print("🗑️ Clearing Neo4j Database...")
    
    try:
        response = requests.post(
            f"{cypher_server_url}/mcp",
            json=request_payload,
            headers=headers,
            timeout=30
        )
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            # Parse SSE response
            lines = response.text.strip().split('\n')
            for line in lines:
                if line.startswith('data: '):
                    json_str = line[6:]
                    try:
                        parsed_data = json.loads(json_str)
                        if "result" in parsed_data:
                            content = parsed_data["result"].get("content", [])
                            if content:
                                text = content[0].get("text", "")
                                print(f"Result: {text}")
                            print("✅ Database cleared successfully")
                            return True
                    except:
                        continue
        
        print("❌ Failed to clear database")
        return False
        
    except Exception as e:
        print(f"❌ Error clearing database: {e}")
        return False

if __name__ == "__main__":
    clear_database()
