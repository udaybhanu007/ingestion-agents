#!/usr/bin/env python3
"""
Verify Neo4j Database Contents
"""

from neo4j import GraphDatabase

def verify_database():
    """Verify Neo4j database contents"""
    
    print("🔍 Verifying Neo4j Database Contents")
    print("="*40)
    
    # Neo4j connection details
    uri = "neo4j+s://b2b4aae0.databases.neo4j.io"
    username = "neo4j"
    password = "sIBdhIqkmmugcG2rB_7XhsbCbsAdCbT7mhUb54d7nQI"
    
    try:
        # Connect to Neo4j
        driver = GraphDatabase.driver(uri, auth=(username, password))
        
        with driver.session() as session:
            # Count all nodes
            result = session.run("MATCH (n) RETURN count(n) as total_nodes")
            record = result.single()
            total_nodes = record['total_nodes'] if record else 0
            print(f"✅ Total nodes in database: {total_nodes}")
            
            if total_nodes > 0:
                # Count by label
                result = session.run("""
                    MATCH (n)
                    RETURN labels(n) as nodeType, count(n) as count
                    ORDER BY count DESC
                """)
                
                print(f"\n📊 Node counts by type:")
                for record in result:
                    print(f"   {record['nodeType']}: {record['count']}")
                
                # Sample medical images
                result = session.run("""
                    MATCH (img:MedicalImage)-[:HAS_FINDING]->(f:Finding)
                    RETURN img.image_index, img.finding_label, f.label
                    LIMIT 5
                """)
                
                print(f"\n📋 Sample medical records:")
                for record in result:
                    print(f"   Image: {record['img.image_index']}")
                    print(f"   Finding: {record['img.finding_label']} -> {record['f.label']}")
                    print()
                    
                # Count findings
                result = session.run("""
                    MATCH (f:Finding)
                    RETURN f.label as finding, count{(img:MedicalImage)-[:HAS_FINDING]->(f)} as image_count
                    ORDER BY image_count DESC
                    LIMIT 10
                """)
                
                print(f"📈 Top findings by frequency:")
                for record in result:
                    print(f"   {record['finding']}: {record['image_count']} images")
            
            else:
                print("⚠️ Database is empty - no nodes found")
        
        driver.close()
        return total_nodes > 0
        
    except Exception as e:
        print(f"❌ Database verification error: {e}")
        return False

if __name__ == "__main__":
    success = verify_database()
    print(f"\nDatabase is {'populated' if success else 'empty'}!")
