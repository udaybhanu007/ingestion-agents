#!/usr/bin/env python3
"""
Simple Neo4j data verification to understand the 10 nodes and 8 relationships
"""

from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

def verify_neo4j_data():
    """Verify what data was actually saved in Neo4j"""
    
    load_dotenv('.env.dev')
    
    # Neo4j connection
    uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    username = os.getenv('NEO4J_USERNAME', 'neo4j')
    password = os.getenv('NEO4J_PASSWORD', 'password')
    
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    try:
        with driver.session() as session:
            print("📊 Detailed Neo4j Data Analysis")
            print("=" * 50)
            
            # Get node counts by label
            result = session.run('MATCH (n) RETURN labels(n) as labels, count(n) as count ORDER BY count DESC')
            print('\n📋 Node Counts by Label:')
            total_nodes = 0
            for record in result:
                label = record["labels"][0] if record["labels"] else "No Label"
                count = record["count"]
                total_nodes += count
                print(f'  📌 {label}: {count} nodes')
            
            # Get relationship counts  
            result = session.run('MATCH ()-[r]->() RETURN type(r) as rel_type, count(r) as count ORDER BY count DESC')
            print('\n🔗 Relationship Counts by Type:')
            total_rels = 0
            for record in result:
                rel_type = record["rel_type"]
                count = record["count"]
                total_rels += count
                print(f'  🔗 {rel_type}: {count} relationships')
            
            # Sample actual data with properties
            print('\n📋 Sample Node Data (with properties):')
            result = session.run('''
                MATCH (n) 
                RETURN labels(n)[0] as label, properties(n) as props 
                LIMIT 15
            ''')
            for record in result:
                label = record["label"]
                props = record["props"]
                print(f'  📌 {label}: {props}')
                
            # Sample relationships with properties
            print('\n🔗 Sample Relationship Data:')
            result = session.run('''
                MATCH (a)-[r]->(b) 
                RETURN labels(a)[0] as start_label, a.id as start_id, 
                       type(r) as rel_type, properties(r) as rel_props,
                       labels(b)[0] as end_label, b.id as end_id 
                LIMIT 10
            ''')
            for record in result:
                start_label = record["start_label"]
                start_id = record["start_id"]
                rel_type = record["rel_type"]
                rel_props = record["rel_props"]
                end_label = record["end_label"]
                end_id = record["end_id"]
                print(f'  🔗 {start_label}({start_id}) --[{rel_type}]{rel_props}--> {end_label}({end_id})')
            
            print(f'\n📊 SUMMARY:')
            print(f'  📋 Total Nodes: {total_nodes}')
            print(f'  🔗 Total Relationships: {total_rels}')
            
    except Exception as e:
        print(f"❌ Error connecting to Neo4j: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    verify_neo4j_data()
