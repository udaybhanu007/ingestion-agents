#!/usr/bin/env python3

import os
from neo4j import GraphDatabase
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_neo4j_directly():
    """Check Neo4j data directly using neo4j driver"""
    
    # Neo4j connection details
    uri = "neo4j+s://b72be05b.databases.neo4j.io:7687"
    username = "neo4j"
    password = "3FiBzqAmruVOcJZKhS8KL0CfJcEW-EhFrAOwSgR61oQ"
    
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    try:
        with driver.session() as session:
            logger.info("=== CHECKING NEO4J DATA AFTER LATEST INGESTION ===")
            
            # 1. Total node and relationship counts
            logger.info("\n1. TOTAL COUNTS:")
            result = session.run("MATCH (n) RETURN count(n) as node_count")
            node_count = result.single()["node_count"]
            logger.info(f"  Total Nodes: {node_count}")
            
            result = session.run("MATCH ()-[r]->() RETURN count(r) as rel_count")
            rel_count = result.single()["rel_count"]
            logger.info(f"  Total Relationships: {rel_count}")
            
            # 2. Node counts by label
            logger.info("\n2. NODE COUNTS BY LABEL:")
            result = session.run("""
                MATCH (n)
                RETURN labels(n)[0] as label, count(n) as count
                ORDER BY count DESC
            """)
            for record in result:
                logger.info(f"  {record['label']}: {record['count']} nodes")
            
            # 3. Relationship counts by type
            logger.info("\n3. RELATIONSHIP COUNTS BY TYPE:")
            result = session.run("""
                MATCH ()-[r]->()
                RETURN type(r) as rel_type, count(r) as count
                ORDER BY count DESC
            """)
            for record in result:
                logger.info(f"  {record['rel_type']}: {record['count']} relationships")
            
            # 4. Sample ImageRecord entities
            logger.info("\n4. SAMPLE IMAGERECORD ENTITIES:")
            result = session.run("""
                MATCH (img:ImageRecord)
                RETURN img.id as image_id, img.patientId as patient_id, img.imageIndex as image_index
                LIMIT 5
            """)
            for record in result:
                logger.info(f"  ImageRecord {record['image_id']}: Patient={record['patient_id']}, Index={record['image_index']}")
            
            # 5. Sample Patient entities  
            logger.info("\n5. SAMPLE PATIENT ENTITIES:")
            result = session.run("""
                MATCH (p:Patient)
                RETURN p.id as patient_id, p.patientId as original_patient_id, p.age as age, p.gender as gender
                LIMIT 5
            """)
            for record in result:
                logger.info(f"  Patient {record['patient_id']}: Original={record['original_patient_id']}, Age={record['age']}, Gender={record['gender']}")
            
            # 6. Sample relationships
            logger.info("\n6. SAMPLE RELATIONSHIPS:")
            result = session.run("""
                MATCH (start)-[r]->(end)
                RETURN labels(start)[0] as start_type, start.id as start_id,
                       type(r) as rel_type,
                       labels(end)[0] as end_type, end.id as end_id
                LIMIT 10
            """)
            for record in result:
                logger.info(f"  {record['start_type']} {record['start_id']} --{record['rel_type']}--> {record['end_type']} {record['end_id']}")
            
            # 7. Check for ImageRecord relationships specifically
            logger.info("\n7. IMAGERECORD RELATIONSHIP ANALYSIS:")
            result = session.run("""
                MATCH (img:ImageRecord)
                OPTIONAL MATCH (img)-[r]->()
                RETURN count(img) as total_images, count(r) as total_img_rels
            """)
            record = result.single()
            total_images = record["total_images"]
            total_img_rels = record["total_img_rels"]
            logger.info(f"  Total ImageRecords: {total_images}")
            logger.info(f"  Total ImageRecord relationships: {total_img_rels}")
            if total_images > 0:
                ratio = total_img_rels / total_images
                logger.info(f"  Relationship ratio: {ratio:.2f} (relationships per ImageRecord)")
            
            logger.info("\n=== NEO4J DATA CHECK COMPLETE ===")
            
    except Exception as e:
        logger.error(f"Error checking Neo4j data: {e}")
        raise
    finally:
        driver.close()

if __name__ == "__main__":
    check_neo4j_directly()
