"""
Test Neo4j database connection using credentials from .env.dev
"""
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
import sys

# Load environment variables
load_dotenv('.env.dev')

def test_neo4j_connection():
    """Test Neo4j database connection"""
    try:
        # Get Neo4j credentials from environment
        uri = os.getenv('NEO4J_URI')
        username = os.getenv('NEO4J_USERNAME')
        password = os.getenv('NEO4J_PASSWORD')
        
        print(f"Testing Neo4j connection...")
        print(f"URI: {uri}")
        print(f"Username: {username}")
        print(f"Password: {'*' * len(password) if password else 'None'}")
        
        if not all([uri, username, password]):
            print("❌ Missing Neo4j credentials in environment")
            return False
        
        # Create driver
        driver = GraphDatabase.driver(uri, auth=(username, password))
        
        # Test connection with a simple query
        with driver.session() as session:
            result = session.run("RETURN 1 as test")
            record = result.single()
            
            if record and record["test"] == 1:
                print("✅ Neo4j connection successful!")
                
                # Get database info
                db_result = session.run("CALL dbms.components() YIELD name, versions")
                for record in db_result:
                    print(f"Database: {record['name']} - Version: {record['versions'][0]}")
                
                # Check if any data exists
                count_result = session.run("MATCH (n) RETURN count(n) as node_count")
                node_count = count_result.single()["node_count"]
                print(f"Current nodes in database: {node_count}")
                
                return True
            else:
                print("❌ Neo4j connection test failed")
                return False
                
    except Exception as e:
        print(f"❌ Neo4j connection error: {str(e)}")
        return False
    finally:
        if 'driver' in locals():
            driver.close()

if __name__ == "__main__":
    success = test_neo4j_connection()
    sys.exit(0 if success else 1)
