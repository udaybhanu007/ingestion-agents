import requests
import sys

print("Testing MCP servers...")

# Test server 1
try:
    response = requests.get("http://127.0.0.1:8003/mcp/", timeout=5)
    print(f"Server 8003: Status {response.status_code}")
except Exception as e:
    print(f"Server 8003: Error - {e}")

# Test server 2  
try:
    response = requests.get("http://127.0.0.1:8004/mcp/", timeout=5)
    print(f"Server 8004: Status {response.status_code}")
except Exception as e:
    print(f"Server 8004: Error - {e}")

print("Test complete")
