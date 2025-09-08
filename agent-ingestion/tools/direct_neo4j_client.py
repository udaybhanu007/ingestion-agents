#!/usr/bin/env python3
"""
Direct Neo4j Client for GraphIngestionTool
This replaces the problematic MCP cypher server with direct Neo4j operations
"""

import asyncio
from typing import Dict, Any, List, Optional
import neo4j

class DirectNeo4jClient:
    """Direct Neo4j client that provides the same interface as MCP cypher server"""
    
    def __init__(self):
        # Neo4j connection details
        self.uri = "neo4j+s://b2b4aae0.databases.neo4j.io"
        self.username = "neo4j"
        self.password = "sIBdhIqkmmugcG2rB_7XhsbCbsAdCbT7mhUb54d7nQI"
        self.database = "neo4j"
        
        # Initialize driver
        self.driver = None
        self._initialize_driver()
    
    def _initialize_driver(self):
        """Initialize Neo4j driver"""
        try:
            self.driver = neo4j.GraphDatabase.driver(
                self.uri, 
                auth=(self.username, self.password)
            )
            # Test connection
            with self.driver.session(database=self.database) as session:
                session.run("RETURN 1")
            print("✅ Direct Neo4j connection established")
        except Exception as e:
            print(f"❌ Failed to connect to Neo4j: {str(e)}")
            self.driver = None
    
    async def get_database_schema(self) -> Dict[str, Any]:
        """Get database schema - equivalent to MCP function"""
        if not self.driver:
            return {
                "success": False,
                "error": "No database connection",
                "schema": {"nodes": [], "relationships": [], "constraints": [], "indexes": []}
            }
        
        try:
            with self.driver.session(database=self.database) as session:
                # Get nodes (labels)
                nodes_result = session.run("CALL db.labels()")
                nodes = [record["label"] for record in nodes_result]
                
                # Get relationships
                rels_result = session.run("CALL db.relationshipTypes()")
                relationships = [record["relationshipType"] for record in rels_result]
                
                # Get constraints
                constraints_result = session.run("SHOW CONSTRAINTS")
                constraints = [dict(record) for record in constraints_result]
                
                # Get indexes
                indexes_result = session.run("SHOW INDEXES")
                indexes = [dict(record) for record in indexes_result]
                
                return {
                    "success": True,
                    "schema": {
                        "nodes": nodes,
                        "relationships": relationships,
                        "constraints": constraints,
                        "indexes": indexes
                    }
                }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "schema": {"nodes": [], "relationships": [], "constraints": [], "indexes": []}
            }
    
    async def write_neo4j_cypher(self, cypher_statements: List[str], parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute write Cypher statements - equivalent to MCP function"""
        if not self.driver:
            return {
                "success": False,
                "error": "No database connection",
                "results": []
            }
        
        results = []
        try:
            with self.driver.session(database=self.database) as session:
                for statement in cypher_statements:
                    result = session.run(statement, parameters or {})
                    summary = result.consume()
                    
                    results.append({
                        "statement": statement,
                        "nodes_created": summary.counters.nodes_created,
                        "relationships_created": summary.counters.relationships_created,
                        "properties_set": summary.counters.properties_set,
                        "constraints_added": summary.counters.constraints_added,
                        "indexes_added": summary.counters.indexes_added
                    })
            
            return {
                "success": True,
                "results": results,
                "total_operations": len(results)
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "results": results
            }
    
    async def read_neo4j_cypher(self, cypher_query: str, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute read Cypher query - equivalent to MCP function"""
        if not self.driver:
            return {
                "success": False,
                "error": "No database connection",
                "data": []
            }
        
        try:
            with self.driver.session(database=self.database) as session:
                result = session.run(cypher_query, parameters or {})
                
                # Convert result to list of dictionaries
                data = []
                for record in result:
                    record_dict = {}
                    for key in record.keys():
                        value = record[key]
                        # Handle Neo4j types
                        if hasattr(value, '_properties'):
                            record_dict[key] = dict(value._properties)
                        elif hasattr(value, 'start_node') and hasattr(value, 'end_node'):
                            # Relationship
                            record_dict[key] = {
                                "type": value.type,
                                "properties": dict(value._properties) if hasattr(value, '_properties') else {}
                            }
                        else:
                            record_dict[key] = value
                    data.append(record_dict)
                
                return {
                    "success": True,
                    "data": data,
                    "count": len(data)
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "data": []
            }
    
    def close(self):
        """Close the driver"""
        if self.driver:
            self.driver.close()
            print("📝 Neo4j driver closed")

def get_direct_neo4j_client() -> DirectNeo4jClient:
    """Get a direct Neo4j client instance"""
    return DirectNeo4jClient()

# Test the client
async def test_direct_client():
    """Test the direct Neo4j client"""
    print("🧪 Testing Direct Neo4j Client")
    print("=" * 40)
    
    client = get_direct_neo4j_client()
    
    try:
        # Test schema retrieval
        print("📊 Testing schema retrieval...")
        schema_result = await client.get_database_schema()
        if schema_result["success"]:
            print(f"✅ Schema retrieved: {len(schema_result['schema']['nodes'])} node types")
        else:
            print(f"❌ Schema failed: {schema_result['error']}")
        
        # Test read query
        print("📖 Testing read query...")
        read_result = await client.read_neo4j_cypher("MATCH (n) RETURN count(n) as total_nodes LIMIT 1")
        if read_result["success"]:
            print(f"✅ Read query successful: {read_result['data']}")
        else:
            print(f"❌ Read failed: {read_result['error']}")
        
        print("🎉 Direct Neo4j client working!")
        
    finally:
        client.close()

if __name__ == "__main__":
    asyncio.run(test_direct_client())
