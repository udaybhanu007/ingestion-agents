#!/usr/bin/env python3
"""
Test MCP server responses directly to debug relationship ingestion issues.
"""

import json
import requests
import sys
import os

# Add parent directory to path for config import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_mcp_cypher_server():
    """Test MCP Cypher server directly"""
    print("Testing MCP Cypher Server...")
    
    cypher_server_url = "http://localhost:8003"
    
    # Test 1: Simple query
    print("\n1. Testing simple query...")
    
    test_query = """
    CREATE (p:TestPatient {id: 'test1', name: 'Test Patient'})
    CREATE (f:TestFinding {id: 'test1', name: 'Test Finding'})
    CREATE (p)-[r:HAS_FINDING]->(f)
    RETURN p, f, r
    """
    
    request_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "write_neo4j_cypher",
            "arguments": {
                "query": test_query,
                "params": {}
            }
        }
    }
    
    try:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream"
        }
        
        response = requests.post(
            f"{cypher_server_url}/mcp",
            json=request_payload,
            headers=headers,
            timeout=30
        )
        
        print(f"Response status: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        print(f"Response content: {response.text[:500]}...")
        
        if response.status_code == 200:
            try:
                result = response.json()
                print(f"Parsed JSON: {json.dumps(result, indent=2)}")
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")
                print(f"Raw response: {response.text}")
        
    except Exception as e:
        print(f"Request failed: {str(e)}")
    
    # Test 2: Query similar to what the ingestion system uses
    print("\n2. Testing batch relationship creation...")
    
    batch_query = """
    UNWIND $records AS record
    MATCH (start:ImageRecord {id: record.start_id})
    MATCH (end:Patient {id: record.end_id})
    CREATE (start)-[r:BELONGS_TO_PATIENT]->(end)
    RETURN count(r) as created_count
    """
    
    test_records = [
        {"start_id": "image1", "end_id": "patient1"},
        {"start_id": "image2", "end_id": "patient1"}
    ]
    
    request_payload2 = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "write_neo4j_cypher",
            "arguments": {
                "query": batch_query,
                "params": {"records": test_records}
            }
        }
    }
    
    try:
        response2 = requests.post(
            f"{cypher_server_url}/mcp",
            json=request_payload2,
            headers=headers,
            timeout=30
        )
        
        print(f"Batch response status: {response2.status_code}")
        print(f"Batch response: {response2.text[:500]}...")
        
        if response2.status_code == 200:
            try:
                result2 = response2.json()
                print(f"Batch parsed JSON: {json.dumps(result2, indent=2)}")
            except json.JSONDecodeError as e:
                print(f"Batch JSON decode error: {e}")
    
    except Exception as e:
        print(f"Batch request failed: {str(e)}")

def test_mcp_modeling_server():
    """Test MCP Data Modeling server"""
    print("\n\nTesting MCP Data Modeling Server...")
    
    modeling_server_url = "http://localhost:8004"
    
    test_entities = [
        {
            "id": "test_patient_1",
            "type": "Patient",
            "properties": {"name": "Test Patient", "age": 35}
        },
        {
            "id": "test_finding_1", 
            "type": "Finding",
            "properties": {"description": "Test finding"}
        }
    ]
    
    test_relationships = [
        {
            "type": "HAS_FINDING",
            "start_id": "test_patient_1",
            "end_id": "test_finding_1",
            "properties": {}
        }
    ]
    
    request_payload = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call", 
        "params": {
            "name": "generate_cypher_queries",
            "arguments": {
                "entities": test_entities,
                "relationships": test_relationships
            }
        }
    }
    
    try:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream"
        }
        
        response = requests.post(
            f"{modeling_server_url}/mcp",
            json=request_payload,
            headers=headers,
            timeout=30
        )
        
        print(f"Modeling response status: {response.status_code}")
        print(f"Modeling response: {response.text[:1000]}...")
        
        if response.status_code == 200:
            try:
                result = response.json()
                print(f"Modeling parsed JSON: {json.dumps(result, indent=2)}")
            except json.JSONDecodeError as e:
                print(f"Modeling JSON decode error: {e}")
    
    except Exception as e:
        print(f"Modeling request failed: {str(e)}")

if __name__ == "__main__":
    print("=== MCP Server Direct Testing ===")
    
    test_mcp_cypher_server()
    test_mcp_modeling_server()
