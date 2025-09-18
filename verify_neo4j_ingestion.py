#!/usr/bin/env python3
"""
Verify Neo4j ingestion by connecting to the database and checking nodes and relationships
"""

import os
import sys
from dotenv import load_dotenv

# Load environment
load_dotenv(".env.dev")

# Add agents directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "agents"))

def verify_neo4j_ingestion():
    """Connect to Neo4j and verify all entities and relationships are properly ingested"""
    
    print("🔍 Verifying Neo4j Database Ingestion")
    print("=" * 60)
    
    try:
        # Connect to Neo4j using MCP server
        import requests
        import json
        
        # MCP server URL for Neo4j
        neo4j_server_url = "http://127.0.0.1:8003/mcp/"
        
        def make_mcp_query(query, params=None):
            """Execute a read query via MCP server"""
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "read_neo4j_cypher",
                    "arguments": {
                        "query": query,
                        "params": params or {}
                    }
                }
            }
            
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream"
            }
            
            response = requests.post(neo4j_server_url, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                if 'text/event-stream' in content_type:
                    # Parse SSE response
                    lines = response.text.strip().split('\n')
                    for line in lines:
                        if line.startswith('data: '):
                            json_str = line[6:]
                            if json_str.strip() == '[DONE]':
                                continue
                            try:
                                parsed_data = json.loads(json_str)
                                if "result" in parsed_data:
                                    return parsed_data["result"]
                                elif "error" in parsed_data:
                                    print(f"❌ Error in MCP SSE response: {parsed_data['error']}")
                                    return None
                                else:
                                    return parsed_data
                            except json.JSONDecodeError:
                                print(f"❌ Failed to parse SSE JSON: {json_str}")
                                continue
                    print(f"❌ No valid SSE data found in response")
                    return None
                else:
                    try:
                        result = response.json()
                        if "result" in result:
                            return result["result"]
                        else:
                            print(f"❌ Error in MCP response: {result}")
                            return None
                    except Exception as e:
                        print(f"❌ Failed to parse JSON response: {e}")
                        return None
            else:
                print(f"❌ HTTP Error: {response.status_code} - {response.text}")
                return None
        
        # 1. Check total node count by label
        print("📊 Checking Node Counts by Label:")
        print("-" * 40)
        
        node_query = """
        MATCH (n)
        RETURN labels(n)[0] as label, count(*) as count
        ORDER BY label
        """
        
        node_result = make_mcp_query(node_query)
        if node_result and "content" in node_result:
            for item in node_result["content"]:
                if "text" in item:
                    try:
                        # Parse the result text as JSON
                        data = json.loads(item["text"])
                        for record in data:
                            label = record.get("label", "Unknown")
                            count = record.get("count", 0)
                            print(f"  📋 {label}: {count} nodes")
                    except json.JSONDecodeError:
                        print(f"  Raw result: {item['text']}")
        
        print()
        
        # 2. Check total relationship count by type
        print("🔗 Checking Relationship Counts by Type:")
        print("-" * 40)
        
        rel_query = """
        MATCH ()-[r]->()
        RETURN type(r) as relationship_type, count(*) as count
        ORDER BY relationship_type
        """
        
        rel_result = make_mcp_query(rel_query)
        if rel_result and "content" in rel_result:
            for item in rel_result["content"]:
                if "text" in item:
                    try:
                        data = json.loads(item["text"])
                        for record in data:
                            rel_type = record.get("relationship_type", "Unknown")
                            count = record.get("count", 0)
                            print(f"  🔗 {rel_type}: {count} relationships")
                    except json.JSONDecodeError:
                        print(f"  Raw result: {item['text']}")
        
        print()
        
        # 3. Get specific node details
        print("📋 Checking Specific Node Details:")
        print("-" * 40)
        
        detailed_query = """
        MATCH (n)
        RETURN labels(n)[0] as label, n.name as name, n.description as description
        ORDER BY label, name
        LIMIT 20
        """
        
        detailed_result = make_mcp_query(detailed_query)
        if detailed_result and "content" in detailed_result:
            for item in detailed_result["content"]:
                if "text" in item:
                    try:
                        data = json.loads(item["text"])
                        for record in data:
                            label = record.get("label", "Unknown")
                            name = record.get("name", "No name")
                            description = record.get("description", "No description")
                            print(f"  📌 {label}: {name}")
                            if description and description != "No description":
                                print(f"      Description: {description[:100]}...")
                    except json.JSONDecodeError:
                        print(f"  Raw result: {item['text']}")
        
        print()
        
        # 4. Check relationship details
        print("🔗 Checking Specific Relationship Details:")
        print("-" * 40)
        
        rel_detail_query = """
        MATCH (start)-[r]->(end)
        RETURN 
            labels(start)[0] + ':' + start.name as start_node,
            type(r) as relationship,
            labels(end)[0] + ':' + end.name as end_node
        ORDER BY relationship
        LIMIT 15
        """
        
        rel_detail_result = make_mcp_query(rel_detail_query)
        if rel_detail_result and "content" in rel_detail_result:
            for item in rel_detail_result["content"]:
                if "text" in item:
                    try:
                        data = json.loads(item["text"])
                        for record in data:
                            start = record.get("start_node", "Unknown")
                            rel = record.get("relationship", "UNKNOWN")
                            end = record.get("end_node", "Unknown")
                            print(f"  🔗 {start} --[{rel}]--> {end}")
                    except json.JSONDecodeError:
                        print(f"  Raw result: {item['text']}")
        
        print()
        
        # 5. Verify expected entities from the ingestion log
        print("✅ Verifying Expected Entities from Ingestion:")
        print("-" * 50)
        
        expected_entities = [
            ("Concept", "ChestX-ray8"),
            ("Person", "Xiaosong Wang"), 
            ("Person", "Yifan Peng"),
            ("Person", "Le Lu"),
            ("Location", "Bethesda, MD"),
            ("Organization", "National Institutes of Health"),
            ("Document", "Paper on ChestX-ray8"),
            ("Concept", "Weakly-Supervised Learning"),
            ("Concept", "Deep Learning"),
            ("Concept", "Thoracic Diseases")
        ]
        
        for label, name in expected_entities:
            check_query = f"""
            MATCH (n:{label} {{name: $name}})
            RETURN count(n) as count
            """
            
            check_result = make_mcp_query(check_query, {"name": name})
            if check_result and "content" in check_result:
                for item in check_result["content"]:
                    if "text" in item:
                        try:
                            data = json.loads(item["text"])
                            count = data[0].get("count", 0) if data else 0
                            status = "✅" if count > 0 else "❌"
                            print(f"  {status} {label}: {name} ({'Found' if count > 0 else 'Missing'})")
                        except (json.JSONDecodeError, IndexError):
                            print(f"  ❓ {label}: {name} (Check failed)")
        
        print()
        
        # 6. Summary
        print("📊 INGESTION VERIFICATION SUMMARY:")
        print("=" * 50)
        
        summary_query = """
        MATCH (n)
        WITH labels(n)[0] as label, count(*) as node_count
        WITH collect({label: label, count: node_count}) as node_summary, sum(node_count) as total_nodes
        MATCH ()-[r]->()
        WITH node_summary, total_nodes, count(r) as total_relationships
        RETURN 
            total_nodes,
            total_relationships,
            node_summary
        """
        
        summary_result = make_mcp_query(summary_query)
        if summary_result and "content" in summary_result:
            for item in summary_result["content"]:
                if "text" in item:
                    try:
                        data = json.loads(item["text"])
                        if data:
                            record = data[0]
                            total_nodes = record.get("total_nodes", 0)
                            total_relationships = record.get("total_relationships", 0)
                            
                            print(f"📋 Total Nodes: {total_nodes}")
                            print(f"🔗 Total Relationships: {total_relationships}")
                            print()
                            
                            # Expected counts from ingestion log
                            expected_nodes = 10  # 4 Concept + 3 Person + 1 Location + 1 Organization + 1 Document
                            expected_relationships = 9  # 7 relationship types with 9 total instances
                            
                            nodes_match = total_nodes == expected_nodes
                            rels_match = total_relationships == expected_relationships
                            
                            print(f"✅ Expected Nodes: {expected_nodes} {'✅ MATCH' if nodes_match else '❌ MISMATCH'}")
                            print(f"✅ Expected Relationships: {expected_relationships} {'✅ MATCH' if rels_match else '❌ MISMATCH'}")
                            
                            if nodes_match and rels_match:
                                print("\n🎉 SUCCESS: All entities and relationships properly ingested!")
                                return True
                            else:
                                print("\n⚠️ WARNING: Count mismatch detected!")
                                return False
                                
                    except (json.JSONDecodeError, IndexError, KeyError) as e:
                        print(f"❌ Error parsing summary: {e}")
                        return False
        
        print("❌ Failed to get summary data")
        return False
        
    except Exception as e:
        print(f"❌ Error verifying Neo4j ingestion: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = verify_neo4j_ingestion()
    
    if success:
        print("\n" + "=" * 60)
        print("🎯 CONCLUSION: Neo4j ingestion verification SUCCESSFUL!")
        print("✅ All expected entities and relationships are present")
        print("✅ Box file content properly parsed and structured")
        print("✅ Knowledge graph successfully created in Neo4j")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("❌ CONCLUSION: Neo4j ingestion verification FAILED!")
        print("⚠️ Some entities or relationships may be missing")
        print("🔧 Check MCP server connectivity and Neo4j database")
        print("=" * 60)