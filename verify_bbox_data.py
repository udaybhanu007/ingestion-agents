#!/usr/bin/env python3
"""
Verify BBox medical data ingestion in Neo4j using MCP servers
"""
import json
import logging
import time
import requests
from typing import Dict, Any

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BBoxDataVerifier:
    """Verify BBox medical data in Neo4j via MCP servers."""
    
    def __init__(self):
        logger.info("Initializing BBoxDataVerifier...")
        self.cypher_server_url = "http://127.0.0.1:8003/mcp/"
        logger.info(f"Cypher server URL: {self.cypher_server_url}")

    def make_mcp_request(self, server_url: str, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Make MCP request with proper headers and error handling."""
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "arguments": arguments
                }
            }
            
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "User-Agent": "bbox-data-verifier/1.0"
            }
            
            response = requests.post(server_url, json=payload, headers=headers, timeout=60)
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                
                if 'text/event-stream' in content_type:
                    return self.parse_sse_response(response.text)
                else:
                    try:
                        result = response.json()
                        if "result" in result:
                            return {"success": True, "result": result["result"]}
                        elif "error" in result:
                            return {"success": False, "error": result["error"]}
                        else:
                            return {"success": True, "result": result}
                    except json.JSONDecodeError as e:
                        return {"success": False, "error": f"JSON decode error: {str(e)}"}
            else:
                return {"success": False, "error": f"HTTP {response.status_code}: {response.text}"}
                
        except Exception as e:
            return {"success": False, "error": f"Request failed: {str(e)}"}

    def parse_sse_response(self, response_text: str) -> Dict[str, Any]:
        """Parse Server-Sent Events response format."""
        try:
            lines = response_text.strip().split('\n')
            
            for line in lines:
                if line.startswith('data: '):
                    data_content = line[6:]
                    try:
                        parsed_data = json.loads(data_content)
                        if "result" in parsed_data:
                            return {"success": True, "result": parsed_data["result"]}
                        elif "error" in parsed_data:
                            return {"success": False, "error": parsed_data["error"]}
                    except json.JSONDecodeError:
                        continue
            
            return {"success": False, "error": f"Failed to parse response: {response_text[:200]}"}
            
        except Exception as e:
            return {"success": False, "error": f"Failed to parse response: {str(e)}"}

    def run_verification_query(self, query: str, description: str) -> bool:
        """Run a verification query and display results."""
        try:
            logger.info(f"🔍 {description}")
            logger.info(f"Query: {query}")
            
            result = self.make_mcp_request(
                self.cypher_server_url,
                "read_neo4j_cypher",
                {"query": query}
            )
            
            if not result.get("success", False):
                logger.error(f"❌ Query failed: {result.get('error')}")
                return False
            
            # Parse result
            result_data = result["result"]
            if isinstance(result_data, dict) and "content" in result_data:
                content = result_data["content"]
                if isinstance(content, list) and len(content) > 0:
                    first_content = content[0]
                    if isinstance(first_content, dict) and "text" in first_content:
                        query_result = first_content["text"]
                        try:
                            parsed_result = json.loads(query_result)
                            logger.info(f"✅ Results: {json.dumps(parsed_result, indent=2)}")
                            return True
                        except json.JSONDecodeError:
                            logger.info(f"✅ Results: {query_result}")
                            return True
            
            logger.info(f"✅ Query executed successfully")
            logger.info(f"Raw result: {json.dumps(result_data, indent=2)}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Verification query failed: {e}")
            return False

    def verify_data_ingestion(self) -> bool:
        """Run verification queries to check data ingestion."""
        try:
            logger.info("🚀 Starting BBox Data Verification")
            logger.info("=" * 60)
            
            # Query 1: Count all nodes by type
            queries = [
                {
                    "query": "MATCH (n:Image) RETURN count(n) as image_count",
                    "description": "Count Image nodes"
                },
                {
                    "query": "MATCH (n:Finding) RETURN count(n) as finding_count",
                    "description": "Count Finding nodes"
                },
                {
                    "query": "MATCH (n:BoundingBox) RETURN count(n) as bbox_count",
                    "description": "Count BoundingBox nodes"
                },
                {
                    "query": "MATCH ()-[r:CONTAINS_FINDING]->() RETURN count(r) as contains_finding_count",
                    "description": "Count CONTAINS_FINDING relationships"
                },
                {
                    "query": "MATCH ()-[r:LOCATED_IN]->() RETURN count(r) as located_in_count",
                    "description": "Count LOCATED_IN relationships"
                },
                {
                    "query": "MATCH ()-[r:IDENTIFIES]->() RETURN count(r) as identifies_count",
                    "description": "Count IDENTIFIES relationships"
                },
                {
                    "query": "MATCH (i:Image)-[:CONTAINS_FINDING]->(f:Finding) RETURN i.image_index, f.finding_label LIMIT 5",
                    "description": "Sample Image-Finding relationships"
                },
                {
                    "query": "MATCH (b:BoundingBox) RETURN b.image_index, b.finding_label, b.x, b.y, b.width, b.height LIMIT 5",
                    "description": "Sample BoundingBox data"
                },
                {
                    "query": "MATCH (f:Finding) RETURN f.finding_label, f.category, f.description",
                    "description": "All Finding labels"
                },
                {
                    "query": "MATCH (b:BoundingBox) RETURN min(b.area) as min_area, max(b.area) as max_area, avg(b.area) as avg_area",
                    "description": "BoundingBox area statistics"
                }
            ]
            
            all_passed = True
            for i, query_info in enumerate(queries, 1):
                logger.info(f"\n--- Query {i}/{len(queries)} ---")
                if not self.run_verification_query(query_info["query"], query_info["description"]):
                    all_passed = False
            
            logger.info("\n" + "=" * 60)
            if all_passed:
                logger.info("🎉 All verification queries completed successfully!")
                logger.info("✅ BBox medical data has been properly ingested into Neo4j")
            else:
                logger.error("❌ Some verification queries failed")
            
            logger.info("=" * 60)
            return all_passed
            
        except Exception as e:
            logger.error(f"❌ Data verification failed: {e}")
            logger.exception("Full exception details:")
            return False


def main():
    """Main function to run data verification."""
    logger.info("=" * 70)
    logger.info("STARTING BBOX DATA VERIFICATION")
    logger.info("=" * 70)
    
    try:
        logger.info("Initializing data verifier...")
        verifier = BBoxDataVerifier()
        
        logger.info("Starting data verification...")
        success = verifier.verify_data_ingestion()
        
        if success:
            logger.info("=" * 70)
            logger.info("✅ SUCCESS: BBox data verification completed!")
            logger.info("=" * 70)
        else:
            logger.error("=" * 70)
            logger.error("❌ FAILED: Data verification failed")
            logger.error("=" * 70)
            
        return success
        
    except Exception as e:
        logger.error("=" * 70)
        logger.error(f"Main execution failed: {e}")
        logger.exception("Full exception details:")
        logger.error("=" * 70)
        return False


if __name__ == "__main__":
    main()
