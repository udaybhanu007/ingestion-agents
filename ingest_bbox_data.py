#!/usr/bin/env python3
"""
Ingest BBox medical image data from CSV into Neo4j using MCP servers
"""
import csv
import json
import logging
import time
import requests
from typing import Dict, Any, List

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BBoxDataIngester:
    """Ingest BBox medical image data into Neo4j via MCP servers."""
    
    def __init__(self):
        logger.info("Initializing BBoxDataIngester...")
        self.cypher_server_url = "http://127.0.0.1:8003/mcp/"
        self.data_modeling_server_url = "http://127.0.0.1:8004/mcp/"
        
        logger.info(f"Cypher server URL: {self.cypher_server_url}")
        logger.info(f"Data modeling server URL: {self.data_modeling_server_url}")
        
        # Data containers
        self.images = []
        self.findings = []
        self.bounding_boxes = []
        
        logger.info("BBoxDataIngester initialization completed")

    def load_csv_data(self, csv_file_path: str) -> bool:
        """Load and parse CSV data into structured format."""
        try:
            logger.info(f"Loading CSV data from: {csv_file_path}")
            
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                # Read raw lines and parse manually since the CSV structure is complex
                lines = file.readlines()
                
                # Skip header
                if len(lines) < 2:
                    logger.error("CSV file is empty or has no data rows")
                    return False
                
                header = lines[0].strip()
                logger.info(f"CSV header: {header}")
                
                processed_images = set()
                processed_findings = set()
                
                row_count = 0
                for line in lines[1:]:  # Skip header
                    row_count += 1
                    
                    # Split by semicolon
                    parts = line.strip().split(';')
                    
                    if len(parts) < 5:  # Need at least image, finding, x, y, w, h
                        logger.warning(f"Invalid row format in line {row_count}: {parts}")
                        continue
                    
                    # Extract data from parts
                    try:
                        image_index = parts[0].strip()
                        finding_label = parts[1].strip()
                        x = float(parts[2])
                        y = float(parts[3])
                        w = float(parts[4])
                        h = float(parts[5]) if len(parts) > 5 else 0.0
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Error parsing row {row_count}: {e}")
                        continue
                    
                    # Create Image entity if not already processed
                    if image_index not in processed_images:
                        image_entity = {
                            "image_index": image_index,
                            "file_name": image_index,
                            "image_type": "medical_xray"
                        }
                        self.images.append(image_entity)
                        processed_images.add(image_index)
                        logger.debug(f"Added image: {image_index}")
                    
                    # Create Finding entity if not already processed
                    if finding_label not in processed_findings:
                        finding_entity = {
                            "finding_label": finding_label,
                            "category": "medical_condition",
                            "description": f"Medical finding: {finding_label}"
                        }
                        self.findings.append(finding_entity)
                        processed_findings.add(finding_label)
                        logger.debug(f"Added finding: {finding_label}")
                    
                    # Create BoundingBox entity
                    bbox_entity = {
                        "bbox_id": f"{image_index}_{finding_label}_{row_count}",
                        "image_index": image_index,
                        "finding_label": finding_label,
                        "x": x,
                        "y": y,
                        "width": w,
                        "height": h,
                        "area": w * h
                    }
                    self.bounding_boxes.append(bbox_entity)
                    logger.debug(f"Added bounding box for {image_index}")
                
                logger.info(f"Successfully loaded {row_count} rows from CSV")
                logger.info(f"Extracted {len(self.images)} unique images")
                logger.info(f"Extracted {len(self.findings)} unique findings")
                logger.info(f"Extracted {len(self.bounding_boxes)} bounding boxes")
                
                return True
                
        except FileNotFoundError:
            logger.error(f"CSV file not found: {csv_file_path}")
            return False
        except Exception as e:
            logger.error(f"Error loading CSV data: {e}")
            logger.exception("Full exception details:")
            return False

    def make_mcp_request(self, server_url: str, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Make MCP request with proper headers and error handling."""
        logger.debug(f"Starting MCP request to {tool_name} at {server_url}")
        start_time = time.time()
        
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
            
            logger.info(f"Making MCP request to {tool_name}")
            logger.debug(f"Request payload: {json.dumps(payload, indent=2)}")
            
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "User-Agent": "bbox-data-ingester/1.0"
            }
            
            response = requests.post(server_url, json=payload, headers=headers, timeout=60)
            
            elapsed_time = time.time() - start_time
            logger.info(f"Response status: {response.status_code} (took {elapsed_time:.2f}s)")
            
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
                        logger.error(f"Failed to parse JSON response: {e}")
                        return {"success": False, "error": f"JSON decode error: {str(e)}"}
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                logger.error(f"HTTP error response: {error_msg}")
                return {"success": False, "error": error_msg}
                
        except requests.exceptions.Timeout as e:
            error_msg = f"Request timeout: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Connection error: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(error_msg)
            logger.exception("Full exception details:")
            return {"success": False, "error": error_msg}

    def parse_sse_response(self, response_text: str) -> Dict[str, Any]:
        """Parse Server-Sent Events response format from MCP servers."""
        try:
            lines = response_text.strip().split('\n')
            
            for line in lines:
                if line.startswith('data: '):
                    data_content = line[6:]  # Remove 'data: ' prefix
                    try:
                        parsed_data = json.loads(data_content)
                        if "result" in parsed_data:
                            return {"success": True, "result": parsed_data["result"]}
                        elif "error" in parsed_data:
                            return {"success": False, "error": parsed_data["error"]}
                    except json.JSONDecodeError:
                        continue
            
            # Try direct JSON parse as fallback
            try:
                parsed_data = json.loads(response_text)
                if "result" in parsed_data:
                    return {"success": True, "result": parsed_data["result"]}
                elif "error" in parsed_data:
                    return {"success": False, "error": parsed_data["error"]}
            except json.JSONDecodeError:
                pass
            
            return {"success": False, "error": f"Failed to parse response: {response_text[:200]}"}
            
        except Exception as e:
            logger.error(f"Error parsing SSE response: {e}")
            return {"success": False, "error": f"Failed to parse response: {str(e)}"}

    def build_data_model(self) -> Dict[str, Any]:
        """Build Neo4j data model for medical image data."""
        logger.info("Building data model for medical image data")
        
        try:
            # Define nodes
            nodes = [
                {
                    "label": "Image",
                    "key_property": {
                        "name": "image_index",
                        "type": "STRING",
                        "description": "Unique identifier for the medical image"
                    },
                    "properties": [
                        {"name": "image_index", "type": "STRING", "description": "Unique identifier for the medical image"},
                        {"name": "file_name", "type": "STRING", "description": "File name of the image"},
                        {"name": "image_type", "type": "STRING", "description": "Type of medical image"}
                    ]
                },
                {
                    "label": "Finding",
                    "key_property": {
                        "name": "finding_label",
                        "type": "STRING",
                        "description": "Medical finding or condition label"
                    },
                    "properties": [
                        {"name": "finding_label", "type": "STRING", "description": "Medical finding or condition label"},
                        {"name": "category", "type": "STRING", "description": "Category of the medical finding"},
                        {"name": "description", "type": "STRING", "description": "Description of the medical finding"}
                    ]
                },
                {
                    "label": "BoundingBox",
                    "key_property": {
                        "name": "bbox_id",
                        "type": "STRING",
                        "description": "Unique identifier for the bounding box"
                    },
                    "properties": [
                        {"name": "bbox_id", "type": "STRING", "description": "Unique identifier for the bounding box"},
                        {"name": "image_index", "type": "STRING", "description": "Reference to the image"},
                        {"name": "finding_label", "type": "STRING", "description": "Reference to the finding"},
                        {"name": "x", "type": "FLOAT", "description": "X coordinate of bounding box"},
                        {"name": "y", "type": "FLOAT", "description": "Y coordinate of bounding box"},
                        {"name": "width", "type": "FLOAT", "description": "Width of bounding box"},
                        {"name": "height", "type": "FLOAT", "description": "Height of bounding box"},
                        {"name": "area", "type": "FLOAT", "description": "Area of bounding box"}
                    ]
                }
            ]
            
            # Define relationships
            relationships = [
                {
                    "type": "CONTAINS_FINDING",
                    "start_node_label": "Image",
                    "end_node_label": "Finding",
                    "key_property": {
                        "name": "confidence",
                        "type": "FLOAT",
                        "description": "Confidence level of finding detection"
                    },
                    "properties": [
                        {"name": "confidence", "type": "FLOAT", "description": "Confidence level of finding detection"}
                    ]
                },
                {
                    "type": "LOCATED_IN",
                    "start_node_label": "BoundingBox",
                    "end_node_label": "Image",
                    "key_property": {
                        "name": "region_type",
                        "type": "STRING",
                        "description": "Type of image region"
                    },
                    "properties": [
                        {"name": "region_type", "type": "STRING", "description": "Type of image region"}
                    ]
                },
                {
                    "type": "IDENTIFIES",
                    "start_node_label": "BoundingBox",
                    "end_node_label": "Finding",
                    "key_property": {
                        "name": "detection_method",
                        "type": "STRING",
                        "description": "Method used for detection"
                    },
                    "properties": [
                        {"name": "detection_method", "type": "STRING", "description": "Method used for detection"}
                    ]
                }
            ]
            
            data_model = {
                "nodes": nodes,
                "relationships": relationships
            }
            
            logger.info(f"Built data model with {len(nodes)} node types and {len(relationships)} relationship types")
            return data_model
            
        except Exception as e:
            logger.error(f"Failed to build data model: {e}")
            return None

    def extract_cypher_query(self, result_data: Any) -> str:
        """Extract Cypher query from MCP response."""
        cypher_query = None
        
        if isinstance(result_data, dict) and "structuredContent" in result_data:
            structured_content = result_data["structuredContent"]
            if isinstance(structured_content, dict) and "result" in structured_content:
                cypher_query = structured_content["result"]
        elif isinstance(result_data, str):
            cypher_query = result_data
        elif isinstance(result_data, dict) and "content" in result_data:
            content = result_data["content"]
            if isinstance(content, list) and len(content) > 0:
                first_content = content[0]
                if isinstance(first_content, dict) and "text" in first_content:
                    cypher_query = first_content["text"]
        
        return cypher_query

    def create_nodes(self) -> bool:
        """Create all nodes in Neo4j."""
        try:
            logger.info("=== Creating Nodes in Neo4j ===")
            
            data_model = self.build_data_model()
            if not data_model:
                logger.error("Failed to build data model")
                return False
            
            # Create Image nodes
            logger.info("Creating Image nodes...")
            image_node = data_model["nodes"][0]
            
            query_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "get_node_cypher_ingest_query",
                {"node": image_node}
            )
            
            if not query_result.get("success", False):
                logger.error(f"Failed to generate Image node query: {query_result.get('error')}")
                return False
            
            cypher_query = self.extract_cypher_query(query_result["result"])
            if not cypher_query:
                logger.error("Could not extract Image Cypher query")
                return False
            
            logger.info(f"Image Cypher query: {cypher_query}")
            
            # Execute Image node creation
            result = self.make_mcp_request(
                self.cypher_server_url,
                "write_neo4j_cypher",
                {
                    "query": cypher_query,
                    "params": {"records": self.images}
                }
            )
            
            if not result.get("success", False):
                logger.error(f"Failed to create Image nodes: {result.get('error')}")
                return False
            
            logger.info(f"✅ Successfully created {len(self.images)} Image nodes")
            
            # Create Finding nodes
            logger.info("Creating Finding nodes...")
            finding_node = data_model["nodes"][1]
            
            query_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "get_node_cypher_ingest_query",
                {"node": finding_node}
            )
            
            if not query_result.get("success", False):
                logger.error(f"Failed to generate Finding node query: {query_result.get('error')}")
                return False
            
            cypher_query = self.extract_cypher_query(query_result["result"])
            if not cypher_query:
                logger.error("Could not extract Finding Cypher query")
                return False
            
            logger.info(f"Finding Cypher query: {cypher_query}")
            
            # Execute Finding node creation
            result = self.make_mcp_request(
                self.cypher_server_url,
                "write_neo4j_cypher",
                {
                    "query": cypher_query,
                    "params": {"records": self.findings}
                }
            )
            
            if not result.get("success", False):
                logger.error(f"Failed to create Finding nodes: {result.get('error')}")
                return False
            
            logger.info(f"✅ Successfully created {len(self.findings)} Finding nodes")
            
            # Create BoundingBox nodes
            logger.info("Creating BoundingBox nodes...")
            bbox_node = data_model["nodes"][2]
            
            query_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "get_node_cypher_ingest_query",
                {"node": bbox_node}
            )
            
            if not query_result.get("success", False):
                logger.error(f"Failed to generate BoundingBox node query: {query_result.get('error')}")
                return False
            
            cypher_query = self.extract_cypher_query(query_result["result"])
            if not cypher_query:
                logger.error("Could not extract BoundingBox Cypher query")
                return False
            
            logger.info(f"BoundingBox Cypher query: {cypher_query}")
            
            # Execute BoundingBox node creation
            result = self.make_mcp_request(
                self.cypher_server_url,
                "write_neo4j_cypher",
                {
                    "query": cypher_query,
                    "params": {"records": self.bounding_boxes}
                }
            )
            
            if not result.get("success", False):
                logger.error(f"Failed to create BoundingBox nodes: {result.get('error')}")
                return False
            
            logger.info(f"✅ Successfully created {len(self.bounding_boxes)} BoundingBox nodes")
            
            return True
            
        except Exception as e:
            logger.error(f"Node creation failed: {e}")
            logger.exception("Full exception details:")
            return False

    def create_relationships(self) -> bool:
        """Create relationships between nodes."""
        try:
            logger.info("=== Creating Relationships in Neo4j ===")
            
            data_model = self.build_data_model()
            if not data_model:
                logger.error("Failed to build data model")
                return False
            
            # Create CONTAINS_FINDING relationships (Image -> Finding)
            logger.info("Creating CONTAINS_FINDING relationships...")
            
            # Prepare relationship records for Image -> Finding
            contains_finding_records = []
            for bbox in self.bounding_boxes:
                record = {
                    "sourceId": bbox["image_index"],
                    "targetId": bbox["finding_label"],
                    "confidence": 1.0  # Default confidence
                }
                # Avoid duplicates
                if record not in contains_finding_records:
                    contains_finding_records.append(record)
            
            query_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "get_relationship_cypher_ingest_query",
                {
                    "data_model": data_model,
                    "relationship_type": "CONTAINS_FINDING",
                    "relationship_start_node_label": "Image",
                    "relationship_end_node_label": "Finding"
                }
            )
            
            if not query_result.get("success", False):
                logger.error(f"Failed to generate CONTAINS_FINDING query: {query_result.get('error')}")
                return False
            
            cypher_query = self.extract_cypher_query(query_result["result"])
            if not cypher_query:
                logger.error("Could not extract CONTAINS_FINDING Cypher query")
                return False
            
            result = self.make_mcp_request(
                self.cypher_server_url,
                "write_neo4j_cypher",
                {
                    "query": cypher_query,
                    "params": {"records": contains_finding_records}
                }
            )
            
            if not result.get("success", False):
                logger.error(f"Failed to create CONTAINS_FINDING relationships: {result.get('error')}")
                return False
            
            logger.info(f"✅ Successfully created {len(contains_finding_records)} CONTAINS_FINDING relationships")
            
            # Create LOCATED_IN relationships (BoundingBox -> Image)
            logger.info("Creating LOCATED_IN relationships...")
            
            located_in_records = []
            for bbox in self.bounding_boxes:
                record = {
                    "sourceId": bbox["bbox_id"],
                    "targetId": bbox["image_index"],
                    "region_type": "detection_region"
                }
                located_in_records.append(record)
            
            query_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "get_relationship_cypher_ingest_query",
                {
                    "data_model": data_model,
                    "relationship_type": "LOCATED_IN",
                    "relationship_start_node_label": "BoundingBox",
                    "relationship_end_node_label": "Image"
                }
            )
            
            if not query_result.get("success", False):
                logger.error(f"Failed to generate LOCATED_IN query: {query_result.get('error')}")
                return False
            
            cypher_query = self.extract_cypher_query(query_result["result"])
            if not cypher_query:
                logger.error("Could not extract LOCATED_IN Cypher query")
                return False
            
            result = self.make_mcp_request(
                self.cypher_server_url,
                "write_neo4j_cypher",
                {
                    "query": cypher_query,
                    "params": {"records": located_in_records}
                }
            )
            
            if not result.get("success", False):
                logger.error(f"Failed to create LOCATED_IN relationships: {result.get('error')}")
                return False
            
            logger.info(f"✅ Successfully created {len(located_in_records)} LOCATED_IN relationships")
            
            # Create IDENTIFIES relationships (BoundingBox -> Finding)
            logger.info("Creating IDENTIFIES relationships...")
            
            identifies_records = []
            for bbox in self.bounding_boxes:
                record = {
                    "sourceId": bbox["bbox_id"],
                    "targetId": bbox["finding_label"],
                    "detection_method": "manual_annotation"
                }
                identifies_records.append(record)
            
            query_result = self.make_mcp_request(
                self.data_modeling_server_url,
                "get_relationship_cypher_ingest_query",
                {
                    "data_model": data_model,
                    "relationship_type": "IDENTIFIES",
                    "relationship_start_node_label": "BoundingBox",
                    "relationship_end_node_label": "Finding"
                }
            )
            
            if not query_result.get("success", False):
                logger.error(f"Failed to generate IDENTIFIES query: {query_result.get('error')}")
                return False
            
            cypher_query = self.extract_cypher_query(query_result["result"])
            if not cypher_query:
                logger.error("Could not extract IDENTIFIES Cypher query")
                return False
            
            result = self.make_mcp_request(
                self.cypher_server_url,
                "write_neo4j_cypher",
                {
                    "query": cypher_query,
                    "params": {"records": identifies_records}
                }
            )
            
            if not result.get("success", False):
                logger.error(f"Failed to create IDENTIFIES relationships: {result.get('error')}")
                return False
            
            logger.info(f"✅ Successfully created {len(identifies_records)} IDENTIFIES relationships")
            
            return True
            
        except Exception as e:
            logger.error(f"Relationship creation failed: {e}")
            logger.exception("Full exception details:")
            return False

    def run_ingestion(self, csv_file_path: str) -> bool:
        """Run the complete data ingestion process."""
        start_time = time.time()
        
        try:
            logger.info("🚀 Starting BBox Medical Data Ingestion")
            logger.info("=" * 60)
            
            # Step 1: Load CSV data
            logger.info("STEP 1: Loading CSV data...")
            if not self.load_csv_data(csv_file_path):
                logger.error("❌ Failed to load CSV data")
                return False
            logger.info("✅ CSV data loaded successfully")
            
            # Step 2: Create nodes
            logger.info("STEP 2: Creating nodes in Neo4j...")
            if not self.create_nodes():
                logger.error("❌ Failed to create nodes")
                return False
            logger.info("✅ Nodes created successfully")
            
            # Step 3: Create relationships
            logger.info("STEP 3: Creating relationships in Neo4j...")
            if not self.create_relationships():
                logger.error("❌ Failed to create relationships")
                return False
            logger.info("✅ Relationships created successfully")
            
            total_elapsed = time.time() - start_time
            logger.info("=" * 60)
            logger.info(f"🎉 Data ingestion completed successfully in {total_elapsed:.2f} seconds!")
            logger.info(f"📊 Summary:")
            logger.info(f"  - {len(self.images)} images ingested")
            logger.info(f"  - {len(self.findings)} findings ingested")
            logger.info(f"  - {len(self.bounding_boxes)} bounding boxes ingested")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            total_elapsed = time.time() - start_time
            logger.error(f"❌ Data ingestion failed after {total_elapsed:.2f} seconds: {e}")
            logger.exception("Full exception details:")
            return False


def main():
    """Main function to run the data ingestion."""
    logger.info("=" * 70)
    logger.info("STARTING BBOX MEDICAL DATA INGESTION")
    logger.info("=" * 70)
    
    try:
        # Path to the CSV file
        csv_file_path = r"c:\Users\kulbir\Desktop\Project\GenAI\Agent-Ingestion\ingestion-agents\doc\BBox_List_2017.csv"
        
        logger.info("Initializing data ingester...")
        ingester = BBoxDataIngester()
        
        logger.info("Starting data ingestion...")
        success = ingester.run_ingestion(csv_file_path)
        
        if success:
            logger.info("=" * 70)
            logger.info("✅ SUCCESS: BBox medical data ingested successfully!")
            logger.info("=" * 70)
        else:
            logger.error("=" * 70)
            logger.error("❌ FAILED: Data ingestion failed")
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
