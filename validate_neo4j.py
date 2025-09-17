#!/usr/bin/env python3
"""
Neo4j Validation Script - Check saved nodes and relationships
"""

import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment variables
load_dotenv('.env.dev')

def connect_to_neo4j():
    """Connect to Neo4j using configuration from .env.dev"""
    
    uri = os.getenv('NEO4J_URI')
    username = os.getenv('NEO4J_USERNAME')
    password = os.getenv('NEO4J_PASSWORD')
    
    print(f"Connecting to Neo4j at: {uri}")
    print(f"Username: {username}")
    print(f"Password: {'*' * len(password) if password else 'Not set'}")
    
    try:
        # Validate required credentials
        if not all([uri, username, password]):
            print("❌ Missing required Neo4j credentials in .env.dev")
            return None
            
        # For neo4j+s URI, don't specify additional SSL params
        print(f"🔌 Attempting connection to {uri}...")
        driver = GraphDatabase.driver(uri, auth=(username, password))
        
        # Test connection
        with driver.session() as session:
            result = session.run("RETURN 1 as test")
            record = result.single()
            if record and record["test"] == 1:
                print("✅ Successfully connected to Neo4j!")
                return driver
            else:
                print("❌ Connection test failed")
                return None
                
    except Exception as e:
        print(f"❌ Failed to connect to Neo4j: {e}")
        
        # Try with bolt+s scheme if neo4j+s fails
        if "neo4j+s" in uri:
            print("🔄 Trying with bolt+s scheme...")
            try:
                bolt_uri = uri.replace("neo4j+s", "bolt+s") 
                driver = GraphDatabase.driver(bolt_uri, auth=(username, password))
                
                with driver.session() as session:
                    result = session.run("RETURN 1 as test")
                    record = result.single()
                    if record and record["test"] == 1:
                        print("✅ Successfully connected to Neo4j with bolt+s!")
                        return driver
                        
            except Exception as e2:
                print(f"❌ Bolt+s connection also failed: {e2}")
            
        return None

def validate_nodes_and_relationships(driver):
    """Query Neo4j to validate saved nodes and relationships"""
    
    with driver.session() as session:
        print("\n" + "="*60)
        print("📊 NEO4J DATABASE VALIDATION")
        print("="*60)
        
        # Count all nodes
        result = session.run("MATCH (n) RETURN count(n) as node_count")
        node_count = result.single()["node_count"]
        print(f"📈 Total nodes in database: {node_count}")
        
        # Count all relationships
        result = session.run("MATCH ()-[r]->() RETURN count(r) as rel_count")
        rel_count = result.single()["rel_count"]
        print(f"🔗 Total relationships in database: {rel_count}")
        
        # Get node labels and their counts
        print(f"\n🏷️  Node Labels:")
        result = session.run("MATCH (n) RETURN labels(n) as labels, count(n) as count ORDER BY count DESC")
        for record in result:
            labels = record["labels"]
            count = record["count"]
            label_str = ":".join(labels) if labels else "No Label"
            print(f"   - {label_str}: {count} nodes")
        
        # Get relationship types and their counts
        print(f"\n🔗 Relationship Types:")
        result = session.run("MATCH ()-[r]->() RETURN type(r) as rel_type, count(r) as count ORDER BY count DESC")
        for record in result:
            rel_type = record["rel_type"]
            count = record["count"]
            print(f"   - {rel_type}: {count} relationships")
        
        # Show recent nodes (likely from our test)
        print(f"\n📋 Recent Nodes (last 10):")
        result = session.run("""
            MATCH (n) 
            RETURN labels(n) as labels, properties(n) as props
            ORDER BY id(n) DESC 
            LIMIT 10
        """)
        
        for i, record in enumerate(result, 1):
            labels = record["labels"]
            props = record["props"]
            label_str = ":".join(labels) if labels else "No Label"
            print(f"   {i}. [{label_str}] {props}")
        
        # Show recent relationships
        print(f"\n🔗 Recent Relationships (last 10):")
        result = session.run("""
            MATCH (a)-[r]->(b) 
            RETURN labels(a) as start_labels, properties(a) as start_props,
                   type(r) as rel_type, properties(r) as rel_props,
                   labels(b) as end_labels, properties(b) as end_props
            ORDER BY id(r) DESC 
            LIMIT 10
        """)
        
        for i, record in enumerate(result, 1):
            start_labels = ":".join(record["start_labels"]) if record["start_labels"] else "NoLabel"
            end_labels = ":".join(record["end_labels"]) if record["end_labels"] else "NoLabel"
            rel_type = record["rel_type"]
            start_props = record["start_props"]
            end_props = record["end_props"]
            rel_props = record["rel_props"]
            
            print(f"   {i}. [{start_labels}]{start_props} -[{rel_type}]-> [{end_labels}]{end_props}")
            if rel_props:
                print(f"      Relationship props: {rel_props}")
        
        # Search for nodes from our test content
        print(f"\n🔍 Searching for test content nodes:")
        test_names = ["John Smith", "TechCorp", "Mary Johnson", "San Francisco"]
        
        for name in test_names:
            result = session.run("""
                MATCH (n) 
                WHERE any(prop in keys(n) WHERE toString(n[prop]) CONTAINS $name)
                RETURN labels(n) as labels, properties(n) as props
                LIMIT 3
            """, name=name)
            
            found_nodes = list(result)
            if found_nodes:
                print(f"   ✅ Found nodes containing '{name}':")
                for record in found_nodes:
                    labels = ":".join(record["labels"]) if record["labels"] else "NoLabel"
                    props = record["props"]
                    print(f"      [{labels}] {props}")
            else:
                print(f"   ❌ No nodes found containing '{name}'")

def main():
    print("🚀 Starting Neo4j validation...")
    
    # Connect to Neo4j
    driver = connect_to_neo4j()
    if not driver:
        print("❌ Cannot proceed without Neo4j connection")
        return
    
    try:
        # Validate data
        validate_nodes_and_relationships(driver)
        
        print(f"\n" + "="*60)
        print("✅ Neo4j validation completed successfully!")
        print("="*60)
        
    except Exception as e:
        print(f"❌ Error during validation: {e}")
        
    finally:
        driver.close()

if __name__ == "__main__":
    main()