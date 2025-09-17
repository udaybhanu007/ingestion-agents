"""
Debug script to check what's happening with the MCP write operations
"""
import requests
import json
from dotenv import load_dotenv

load_dotenv('.env.dev')

def test_direct_cypher_write():
    """Test direct Cypher write via MCP to see what's happening"""
    
    # Test data
    test_query = """
    CREATE (p:TestPerson {name: 'Debug Test', timestamp: datetime()}) 
    RETURN p.name as name, id(p) as id
    """
    
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "write_neo4j_cypher",
            "arguments": {
                "query": test_query
            }
        }
    }
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "User-Agent": "debug-test/1.0"
    }
    
    print("🔍 Testing direct MCP cypher write...")
    print(f"Query: {test_query}")
    
    try:
        response = requests.post(
            "http://127.0.0.1:8003/mcp/",
            json=payload,
            headers=headers,
            timeout=30
        )
        
        print(f"Response status: {response.status_code}")
        print(f"Response content: {response.text}")
        
        # Parse SSE response
        if response.status_code == 200:
            for line in response.text.split('\n'):
                if line.startswith('data: '):
                    try:
                        data = json.loads(line[6:])
                        print(f"Parsed response: {json.dumps(data, indent=2)}")
                        
                        if "result" in data:
                            result = data["result"]
                            print(f"Write result: {result}")
                            return True
                        elif "error" in data:
                            print(f"Error: {data['error']}")
                            return False
                    except json.JSONDecodeError:
                        continue
        
        return False
        
    except Exception as e:
        print(f"Error: {e}")
        return False

def test_read_after_write():
    """Test read query to see if data persists"""
    
    read_query = "MATCH (n) RETURN labels(n) as labels, count(n) as count"
    
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "read_neo4j_cypher",
            "arguments": {
                "query": read_query
            }
        }
    }
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "User-Agent": "debug-test/1.0"
    }
    
    print("\n🔍 Testing read after write...")
    print(f"Query: {read_query}")
    
    try:
        response = requests.post(
            "http://127.0.0.1:8003/mcp/",
            json=payload,
            headers=headers,
            timeout=30
        )
        
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            for line in response.text.split('\n'):
                if line.startswith('data: '):
                    try:
                        data = json.loads(line[6:])
                        print(f"Read result: {json.dumps(data, indent=2)}")
                        return True
                    except json.JSONDecodeError:
                        continue
        
        return False
        
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Testing MCP Cypher Operations Debug")
    
    # Test write
    write_ok = test_direct_cypher_write()
    
    # Test read
    read_ok = test_read_after_write()
    
    if write_ok and read_ok:
        print("\n✅ MCP operations working")
    else:
        print("\n❌ MCP operations may have issues")