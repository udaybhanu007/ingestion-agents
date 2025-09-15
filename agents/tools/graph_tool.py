"""
Graph Ingestion Tool for Neo4j using MCP

This tool handles ingestion of content into Neo4j graph database using MCP (Model Context Protocol).
Based on the successful dual-server approach from ingest_working.py.
"""

import logging
import json
import sys
import os
import requests
import re
import math
import time
from typing import Dict, Any, List, Optional, Union
from langchain_openai import AzureChatOpenAI
from pydantic import SecretStr

# Add parent directory to path for config import
parent_dir = os.path.dirname(os.path.dirname(__file__))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from config.config_manager import get_config
    config_manager = get_config()
    def get_graph_config():
        return config_manager
except ImportError:
    # Fallback config
    class MockConfig:
        def get_config(self, section, key=None):
            configs = {
                "neo4j": {
                    "uri": "bolt://localhost:7687",
                    "username": "neo4j",
                    "password": "password"
                },
                "openai": {
                    "deployment_name": "gpt-4o-mini",
                    "azure_api_version": "2024-08-01-preview",
                    "azure_endpoint": os.getenv("AZURE_OPENAI_ENDPOINT", ""),
                    "azure_api_key": os.getenv("AZURE_OPENAI_API_KEY", "")
                }
            }
            if key:
                return configs.get(section, {}).get(key)
            return configs.get(section, {})
        
        @property
        def max_content_length(self):
            return 50000
    
    def get_graph_config():
        return MockConfig()

# Import structured logging
try:
    from config.logger_config import get_tool_logger
    STRUCTURED_LOGGING_AVAILABLE = True
except ImportError:
    STRUCTURED_LOGGING_AVAILABLE = False

logger = logging.getLogger(__name__)


class GraphIngestionTool:
    """Tool for ingesting content into Neo4j graph database using MCP."""
    
    def __init__(self):
        self.config = get_graph_config()
        
        # Initialize structured logging
        if STRUCTURED_LOGGING_AVAILABLE:
            self.logger = get_tool_logger("graph_tool")
        else:
            self.logger = logging.getLogger(__name__)
        
        # Initialize Azure OpenAI client using correct config structure
        openai_config = self.config.get_config('openai')
        self.llm = AzureChatOpenAI(
            azure_deployment=openai_config.get('deployment_name'),
            api_version=openai_config.get('azure_api_version'),
            azure_endpoint=openai_config.get('azure_endpoint'),
            api_key=SecretStr(openai_config.get('azure_api_key', ''))
        )
        
        # MCP server configurations - using working dual server approach
        self.cypher_server_url = "http://127.0.0.1:8003/mcp/"
        self.data_modeling_server_url = "http://127.0.0.1:8004/mcp/" 
        
        self.logger.info("GraphIngestionTool initialized",
                        component="graph_tool",
                        cypher_server_url=self.cypher_server_url,
                        data_modeling_server_url=self.data_modeling_server_url,
                        openai_deployment=openai_config.get('deployment_name'))
        
        # Track processing state
        self.processing_stats = {
            "documents_processed": 0,
            "schemas_generated": 0,
            "constraints_created": 0,
            "entities_ingested": 0,
            "relationships_created": 0,
            "errors": []
        }
        
        self.logger.info("GraphIngestionTool initialized with dual MCP server configuration")
    
    @property
    def name(self) -> str:
        return "graph_ingest"
    
    @property
    def description(self) -> str:
        return "Ingests structured and unstructured content into Neo4j graph database using MCP"
    
    @property
    def priority(self) -> int:
        return 10
    
    @property 
    def enabled(self) -> bool:
        return True
    
    def parse_sse_response(self, response_text: str) -> Dict[str, Any]:
        """Parse Server-Sent Events response format."""
        try:
            # Split by lines and find data lines
            lines = response_text.strip().split('\n')
            
            for line in lines:
                if line.startswith('data: '):
                    json_str = line[6:]  # Remove 'data: ' prefix
                    if json_str.strip() == '[DONE]':
                        continue
                    try:
                        return json.loads(json_str)
                    except json.JSONDecodeError:
                        continue
            
            # If no valid SSE format found, try parsing as direct JSON
            return json.loads(response_text)
            
        except Exception as e:
            self.logger.error(f"Error parsing SSE response: {e}")
            return {"error": f"Failed to parse response: {str(e)}"}

    def make_mcp_request(self, server_url: str, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Make a request to an MCP server."""
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
            
            response = requests.post(
                server_url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream"
                },
                timeout=30
            )
            
            if response.status_code == 200:
                return self.parse_sse_response(response.text)
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                self.logger.error(f"MCP request failed: {error_msg}")
                return {"error": error_msg}
                
        except Exception as e:
            error_msg = f"Request failed: {str(e)}"
            self.logger.error(f"MCP request error: {error_msg}")
            return {"error": error_msg}

    def initialize_servers(self) -> bool:
        """Initialize and verify both MCP servers are available."""
        try:
            # Test cypher server
            cypher_response = self.make_mcp_request(
                self.cypher_server_url,
                "execute_cypher",
                {"query": "RETURN 1 as test"}
            )
            
            if "error" in cypher_response:
                self.logger.error(f"Cypher server not available: {cypher_response['error']}")
                return False
            
            # Test data modeling server  
            modeling_response = self.make_mcp_request(
                self.data_modeling_server_url,
                "analyze_document_structure", 
                {"content": "test content", "document_type": "test"}
            )
            
            if "error" in modeling_response:
                self.logger.error(f"Data modeling server not available: {modeling_response['error']}")
                return False
                
            self.logger.info("Both MCP servers initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Server initialization failed: {e}")
            return False

    def ingest_content_with_schema_validation(self, content: str, document_type: str = "document") -> Dict[str, Any]:
        """
        Main ingestion method using schema-driven approach with dual MCP servers.
        
        Workflow:
        1. Analyze document structure using data modeling server
        2. Generate and validate schema 
        3. Create constraints in Neo4j via cypher server
        4. Ingest data using validated schema
        """
        try:
            # Initialize servers
            if not self.initialize_servers():
                return {
                    "success": False,
                    "error": "Failed to initialize MCP servers",
                    "stats": self.processing_stats
                }
            
            self.logger.info(f"Starting schema-driven ingestion for {document_type}")
            
            # Step 1: Analyze document structure
            self.logger.info("Step 1: Analyzing document structure...")
            structure_response = self.make_mcp_request(
                self.data_modeling_server_url,
                "analyze_document_structure",
                {
                    "content": content,
                    "document_type": document_type
                }
            )
            
            if "error" in structure_response:
                self.processing_stats["errors"].append(f"Structure analysis failed: {structure_response['error']}")
                return {
                    "success": False,
                    "error": f"Structure analysis failed: {structure_response['error']}",
                    "stats": self.processing_stats
                }
            
            # Step 2: Generate schema from structure
            self.logger.info("Step 2: Generating graph schema...")
            schema_response = self.make_mcp_request(
                self.data_modeling_server_url,
                "generate_graph_schema",
                {
                    "document_structure": structure_response.get("result", structure_response),
                    "content": content[:2000]  # First 2k chars for context
                }
            )
            
            if "error" in schema_response:
                self.processing_stats["errors"].append(f"Schema generation failed: {schema_response['error']}")
                return {
                    "success": False,
                    "error": f"Schema generation failed: {schema_response['error']}",
                    "stats": self.processing_stats
                }
            
            schema = schema_response.get("result", schema_response)
            self.processing_stats["schemas_generated"] += 1
            
            # Step 3: Validate and create constraints
            self.logger.info("Step 3: Creating database constraints...")
            constraints_result = self._create_schema_constraints(schema)
            
            if not constraints_result["success"]:
                return {
                    "success": False,
                    "error": f"Constraint creation failed: {constraints_result['error']}",
                    "stats": self.processing_stats
                }
            
            # Step 4: Ingest data using schema
            self.logger.info("Step 4: Ingesting data with schema validation...")
            ingestion_result = self._ingest_with_schema(content, schema, document_type)
            
            if ingestion_result["success"]:
                self.processing_stats["documents_processed"] += 1
                self.logger.info("Schema-driven ingestion completed successfully")
            
            return ingestion_result
            
        except Exception as e:
            error_msg = f"Schema-driven ingestion failed: {str(e)}"
            self.logger.error(error_msg)
            self.processing_stats["errors"].append(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "stats": self.processing_stats
            }
    
    def _create_schema_constraints(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Create database constraints based on generated schema."""
        try:
            constraints_created = 0
            
            # Extract node types and their properties
            node_types = schema.get("nodes", {})
            for node_type, properties in node_types.items():
                # Create uniqueness constraints for ID properties
                if "id" in properties or "name" in properties:
                    id_property = "id" if "id" in properties else "name"
                    constraint_query = f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{node_type}) REQUIRE n.{id_property} IS UNIQUE"
                    
                    constraint_response = self.make_mcp_request(
                        self.cypher_server_url,
                        "execute_cypher",
                        {"query": constraint_query}
                    )
                    
                    if "error" not in constraint_response:
                        constraints_created += 1
                        self.logger.info(f"Created constraint for {node_type}.{id_property}")
                    else:
                        self.logger.warning(f"Failed to create constraint for {node_type}: {constraint_response['error']}")
            
            self.processing_stats["constraints_created"] += constraints_created
            
            return {
                "success": True,
                "constraints_created": constraints_created
            }
            
        except Exception as e:
            error_msg = f"Constraint creation failed: {str(e)}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg
            }
    
    def _ingest_with_schema(self, content: str, schema: Dict[str, Any], document_type: str) -> Dict[str, Any]:
        """Ingest content using the validated schema."""
        try:
            # Use data modeling server to extract structured data
            extraction_response = self.make_mcp_request(
                self.data_modeling_server_url,
                "extract_structured_data",
                {
                    "content": content,
                    "schema": schema,
                    "document_type": document_type
                }
            )
            
            if "error" in extraction_response:
                return {
                    "success": False,
                    "error": f"Data extraction failed: {extraction_response['error']}"
                }
            
            extracted_data = extraction_response.get("result", extraction_response)
            
            # Generate Cypher queries for ingestion
            cypher_response = self.make_mcp_request(
                self.data_modeling_server_url,
                "generate_cypher_queries",
                {
                    "extracted_data": extracted_data,
                    "schema": schema
                }
            )
            
            if "error" in cypher_response:
                return {
                    "success": False,
                    "error": f"Cypher generation failed: {cypher_response['error']}"
                }
            
            queries = cypher_response.get("result", {}).get("queries", [])
            
            # Execute queries via cypher server
            entities_created = 0
            relationships_created = 0
            
            for query_info in queries:
                query = query_info.get("query", "")
                query_type = query_info.get("type", "unknown")
                
                if query:
                    execution_response = self.make_mcp_request(
                        self.cypher_server_url,
                        "execute_cypher",
                        {"query": query}
                    )
                    
                    if "error" not in execution_response:
                        if query_type == "node":
                            entities_created += 1
                        elif query_type == "relationship":
                            relationships_created += 1
                        self.logger.debug(f"Executed {query_type} query successfully")
                    else:
                        self.logger.warning(f"Query execution failed: {execution_response['error']}")
            
            self.processing_stats["entities_ingested"] += entities_created
            self.processing_stats["relationships_created"] += relationships_created
            
            return {
                "success": True,
                "entities_created": entities_created,
                "relationships_created": relationships_created,
                "stats": self.processing_stats
            }
            
        except Exception as e:
            error_msg = f"Schema-based ingestion failed: {str(e)}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg
            }

    def ingest_content(self, content: str, source_name: str = "document", content_type: str = "text", metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Main entry point for content ingestion - delegates to schema-driven approach.
        
        Args:
            content: Text content to ingest
            source_name: Name/identifier for the source document 
            content_type: Type of content (text, json, etc.)
            metadata: Additional metadata about the content
            
        Returns:
            Dict with success status and ingestion results
        """
        try:
            # Determine document type from content_type and metadata
            document_type = content_type
            if metadata and "document_type" in metadata:
                document_type = metadata["document_type"]
            
            self.logger.info(f"Starting content ingestion for '{source_name}' (type: {document_type})")
            
            # Use schema-driven ingestion approach
            result = self.ingest_content_with_schema_validation(content, document_type)
            
            # Add source information to result
            if result.get("success"):
                result["source_name"] = source_name
                result["content_type"] = content_type
                if metadata:
                    result["metadata"] = metadata
            
            return result
            
        except Exception as e:
            error_msg = f"Content ingestion failed: {str(e)}"
            self.logger.error(error_msg)
            self.processing_stats["errors"].append(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "stats": self.processing_stats
            }

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get current processing statistics."""
        return self.processing_stats.copy()

    def reset_stats(self):
        """Reset processing statistics."""
        self.processing_stats = {
            "documents_processed": 0,
            "schemas_generated": 0,
            "constraints_created": 0,
            "entities_ingested": 0,
            "relationships_created": 0,
            "errors": []
        }
        self.logger.info("Processing statistics reset")


# For backward compatibility - export the main class
__all__ = ["GraphIngestionTool"]
