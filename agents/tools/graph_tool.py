"""
Graph Ingestion Tool for Neo4j using MCP

This tool handles ingestion of content into Neo4j graph database using MCP (Model Context Protocol).
"""

import logging
import asyncio
import json
import sys
import os
import httpx
import re
import math
from typing import Dict, Any, List, Optional, Union
from langchain_openai import AzureChatOpenAI
from pydantic import SecretStr

# Add parent directory to path for config import
parent_dir = os.path.dirname(os.path.dirname(__file__))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from config import get_config
except ImportError:
    # Try importing from the current package
    import sys
    current_dir = os.path.dirname(os.path.dirname(__file__))
    sys.path.insert(0, current_dir)
    from config import get_config
from .direct_neo4j_client import get_direct_neo4j_client

logger = logging.getLogger(__name__)

# Import the new MCP client
try:
    sys.path.append(os.path.dirname(__file__))
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from mcp_neo4j_client import MCPNeo4jClient
    MCP_CLIENT_AVAILABLE = True
except ImportError as e:
    logger.warning(f"MCP client not available: {e}")
    MCP_CLIENT_AVAILABLE = False
    MCPNeo4jClient = None


class GraphIngestionTool:
    def _clean_record(self, record):
        """Recursively clean a record, replacing NaN/inf/-inf with None for JSON compliance."""
        if isinstance(record, dict):
            return {k: self._clean_record(v) for k, v in record.items()}
        elif isinstance(record, list):
            return [self._clean_record(v) for v in record]
        elif isinstance(record, float):
            if math.isnan(record) or math.isinf(record):
                return None
            return record
        else:
            return record

    def __init__(self):
        self.config = get_config()
        self._llm: Optional[AzureChatOpenAI] = None
        
        # MCP Server endpoints (fixed URLs based on PyPI docs)
        self.data_modeling_url = "http://127.0.0.1:8001/mcp/"
        self.cypher_url = "http://127.0.0.1:8002/mcp/"
        
        # HTTP client for MCP calls
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # MCP client for enhanced functionality  
        self.mcp_client = None
    
    def _clean_property_name(self, name: str) -> str:
        """Convert property names to valid Neo4j identifiers"""
        if not name:
            return "unknown_property"
        
        # Replace spaces with underscores
        name = name.replace(' ', '_')
        
        # Remove or replace special characters with underscores
        name = re.sub(r'[^\w]', '_', name)
        
        # Remove multiple consecutive underscores
        name = re.sub(r'_+', '_', name)
        
        # Remove leading/trailing underscores
        name = name.strip('_')
        
        # Ensure it starts with a letter or underscore
        if name and name[0].isdigit():
            name = 'prop_' + name
            
        return name or 'unknown_property'
    
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
    
    @property
    def llm(self) -> AzureChatOpenAI:
        """Get or create LLM client"""
        if self._llm is None:
            self._llm = AzureChatOpenAI(
                azure_deployment=self.config.azure_openai_deployment,
                api_version=self.config.azure_openai_api_version,
                azure_endpoint=self.config.azure_openai_endpoint,
                api_key=SecretStr(self.config.azure_openai_api_key)
            )
        return self._llm

    async def _call_mcp_function(self, server_url: str, function_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Call MCP server function via HTTP with SSE response parsing"""
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": function_name,
                    "arguments": params
                }
            }
            
            response = await self.http_client.post(
                server_url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream"
                }
            )
            
            if response.status_code == 200:
                # Parse SSE response
                lines = response.text.strip().split('\n')
                for line in lines:
                    if line.startswith('data: '):
                        data_part = line[6:]  # Remove 'data: '
                        try:
                            json_data = json.loads(data_part)
                            if "result" in json_data:
                                return json_data["result"]
                            return json_data
                        except json.JSONDecodeError:
                            logger.error(f"Failed to parse SSE data: {data_part}")
                            return {"status": "error", "error": "Invalid JSON in SSE response"}
                
                return {"status": "error", "error": "No data in SSE response"}
            else:
                logger.error(f"MCP call failed: {response.status_code} - {response.text}")
                return {"status": "error", "error": f"HTTP {response.status_code}"}
                
        except Exception as e:
            logger.error(f"Error calling MCP function {function_name}: {e}")
            return {"status": "error", "error": str(e)}
    
    async def validate(self, content: Any) -> bool:
        """Validate content before ingestion"""
        try:
            if not content:
                return False
            
            # Check content length
            content_str = str(content)
            if len(content_str) > self.config.max_content_length:
                logger.warning(f"Content length {len(content_str)} exceeds maximum {self.config.max_content_length}")
                return False
            
            return True
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False

    async def get_schema_for_data_model(self, data_sample: Dict[str, Any]) -> Dict[str, Any]:
        """Get schema recommendations for a data model using MCP Data Modeling server"""
        try:
            logger.info("Creating data model from sample using MCP Data Modeling server")
            
            # Use create_data_model tool with sample data
            result = await self._call_mcp_function(
                self.data_modeling_url,
                "create_data_model",
                {
                    "data_sample": json.dumps(data_sample),
                    "model_name": "generated_model"
                }
            )
            
            return {
                "status": "success",
                "schema_suggestion": result,
                "data_sample": data_sample
            }
            
        except Exception as e:
            logger.error(f"Error getting schema for data model: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

    async def validate_data_model(self, data: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data against a schema using MCP Data Modeling server"""
        try:
            logger.info("Validating data model against schema")
            
            result = await self._call_mcp_function(
                self.data_modeling_url,
                "validate_data_model",
                {
                    "data": data,
                    "schema": schema,
                    "strict_validation": True
                }
            )
            
            return {
                "status": "success",
                "validation_result": result,
                "is_valid": result.get("valid", False) if isinstance(result, dict) else False
            }
            
        except Exception as e:
            logger.error(f"Error validating data model: {e}")
            return {
                "status": "error",
                "error": str(e),
                "is_valid": False
            }

    async def generate_cypher_constraints(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Generate and create Cypher constraints using MCP Data Modeling server"""
        try:
            logger.info("Generating constraints from schema")
            
            # Use get_constraints_cypher_queries tool
            result = await self._call_mcp_function(
                self.data_modeling_url,
                "get_constraints_cypher_queries",
                {
                    "data_model": schema
                }
            )
            
            return {
                "status": "success",
                "constraints": result
            }
            
        except Exception as e:
            logger.error(f"Error generating cypher constraints: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

    async def create_indexes(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Create database indexes using MCP Data Modeling server"""
        try:
            logger.info("Creating indexes from schema")
            
            result = await self._call_mcp_function(
                self.data_modeling_url,
                "create_indexes",
                {
                    "schema": schema,
                    "index_types": ["btree", "text", "point"]
                }
            )
            
            return {
                "status": "success",
                "indexes": result
            }
            
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

    async def get_database_schema(self) -> Dict[str, Any]:
        """Get current database schema using MCP Cypher server with fallback"""
        try:
            logger.info("Getting current database schema")
            
            result = await self._call_mcp_function(
                self.cypher_url,
                "get_neo4j_schema",
                {}
            )
            
            return {
                "status": "success",
                "current_schema": result
            }
            
        except Exception as e:
            logger.error(f"Error calling MCP function get_schema: {e}")
            logger.info("Falling back to direct Neo4j connection")
            
            # Fallback to direct Neo4j
            try:
                direct_client = get_direct_neo4j_client()
                schema = await direct_client.get_database_schema()
                direct_client.close()
                
                logger.info("Retrieved current database schema via direct client")
                return {
                    "status": "success",
                    "current_schema": schema,
                    "source": "direct_neo4j"
                }
                
            except Exception as direct_error:
                logger.error(f"Direct Neo4j also failed: {direct_error}")
                return {
                    "status": "error",
                    "error": str(e),
                    "direct_error": str(direct_error),
                    "message": "Both MCP and direct Neo4j connections failed"
                }

    async def generate_cypher_node_ingest(self, data: Dict[str, Any], node_type: str) -> Dict[str, Any]:
        """Generate Cypher for node ingestion"""
        try:
            logger.info(f"Generating node ingestion Cypher for {node_type}")
            
            # Create MERGE statement for the node
            properties = []
            params = {}
            
            for key, value in data.items():
                if key != "id" and value is not None:
                    # Clean the property name for Neo4j compatibility
                    clean_key = self._clean_property_name(key)
                    param_name = f"{clean_key}_value"
                    properties.append(f"{clean_key}: ${param_name}")
                    params[param_name] = value
            
            # Use ID if available, otherwise create unique identifier
            if "id" in data:
                merge_clause = f"MERGE (n:{node_type} {{id: $id_value}})"
                params["id_value"] = data["id"]
            else:
                # Use a combination of properties for uniqueness
                unique_props = []
                unique_keys = ["name", "title", "patient_id", "document_id", "email", "phone"]
                
                for key in unique_keys:
                    if key in data and data[key] is not None:
                        # Clean the property name for Neo4j compatibility
                        clean_key = self._clean_property_name(key)
                        unique_props.append(f"{clean_key}: ${clean_key}_value")
                        params[f"{clean_key}_value"] = data[key]
                
                if unique_props:
                    merge_clause = f"MERGE (n:{node_type} {{{', '.join(unique_props)}}})"
                else:
                    merge_clause = f"CREATE (n:{node_type})"
            
            set_clause = f"SET n += {{{', '.join(properties)}}}" if properties else ""
            
            cypher_statement = f"{merge_clause}\n{set_clause}\nRETURN n"
            
            return {
                "status": "success",
                "cypher_statements": [cypher_statement],
                "parameters": params,
                "node_type": node_type
            }
            
        except Exception as e:
            logger.error(f"Error generating cypher node ingest: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

    async def generate_cypher_relationship_ingest(
        self, 
        source_data: Dict[str, Any], 
        target_data: Dict[str, Any], 
        relationship_type: str
    ) -> Dict[str, Any]:
        """Generate Cypher for relationship ingestion"""
        try:
            logger.info(f"Generating relationship ingestion Cypher for {relationship_type}")
            
            # Build MATCH clauses for source and target nodes
            source_match = self._build_match_clause(source_data, "source")
            target_match = self._build_match_clause(target_data, "target")
            
            # Build relationship creation
            relationship_clause = f"MERGE (source)-[r:{relationship_type}]->(target)"
            
            cypher_statement = f"""
            {source_match}
            {target_match}
            {relationship_clause}
            RETURN source, r, target
            """.strip()
            
            return {
                "status": "success",
                "cypher_statements": [cypher_statement],
                "relationship_type": relationship_type
            }
            
        except Exception as e:
            logger.error(f"Error generating cypher relationship ingest: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

    def _build_match_clause(self, data: Dict[str, Any], alias: str) -> str:
        """Build MATCH clause for node identification"""
        node_type = data.get("node_type", "Node")
        
        # Try to find unique identifiers in order of preference
        unique_keys = ["id", "patient_id", "document_id", "name", "title", "email", "phone"]
        conditions = []
        
        for key in unique_keys:
            if key in data and data[key] is not None:
                conditions.append(f"{key}: '{data[key]}'")
                break  # Use only the first found unique identifier
        
        if conditions:
            where_clause = "{" + ", ".join(conditions) + "}"
            return f"MATCH ({alias}:{node_type} {where_clause})"
        else:
            return f"MATCH ({alias}:{node_type})"

    async def write_neo4j_cypher(self, cypher_statements: List[str], parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute Cypher statements in Neo4j using MCP Cypher server"""
        try:
            logger.info(f"Executing {len(cypher_statements)} Cypher statements")
            
            results = []
            
            for i, statement in enumerate(cypher_statements):
                logger.info(f"Executing statement {i+1}/{len(cypher_statements)}")
                
                result = await self._call_mcp_function(
                    self.cypher_url,
                    "write_neo4j_cypher",
                    {
                        "query": statement,
                        "params": parameters or {}
                    }
                )
                
                results.append({
                    "statement_index": i,
                    "statement": statement,
                    "result": result,
                    "success": result.get("status") != "error"
                })
            
            successful_executions = sum(1 for r in results if r["success"])
            
            return {
                "status": "success" if successful_executions > 0 else "error",
                "execution_results": results,
                "executed_statements": len(cypher_statements),
                "successful_executions": successful_executions
            }
            
        except Exception as e:
            logger.error(f"Error calling MCP function write_query: {e}")
            logger.info("Falling back to direct Neo4j connection")
            
            # Fallback to direct Neo4j
            try:
                direct_client = get_direct_neo4j_client()
                result = await direct_client.write_neo4j_cypher(cypher_statements, parameters)
                direct_client.close()
                
                logger.info(f"Cypher execution completed via direct Neo4j: {result}")
                return {
                    "status": "success" if result.get("success") else "error",
                    "execution_results": result.get("results", []),
                    "executed_statements": len(cypher_statements),
                    "successful_executions": len(result.get("results", [])),
                    "source": "direct_neo4j"
                }
                
            except Exception as direct_error:
                logger.error(f"Direct Neo4j also failed: {direct_error}")
                return {
                    "status": "error",
                    "error": str(e),
                    "direct_error": str(direct_error),
                    "message": "Both MCP and direct Neo4j connections failed",
                    "executed_statements": 0
                }

    async def read_neo4j_cypher(self, cypher_query: str, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute read-only Cypher query using MCP Cypher server"""
        try:
            logger.info("Executing read query")
            
            result = await self._call_mcp_function(
                self.cypher_url,
                "read_neo4j_cypher",
                {
                    "query": cypher_query,
                    "params": parameters or {}
                }
            )
            
            return {
                "status": "success",
                "query_result": result
            }
            
        except Exception as e:
            logger.error(f"Error calling MCP function read_query: {e}")
            logger.info("Falling back to direct Neo4j connection")
            
            # Fallback to direct Neo4j
            try:
                direct_client = get_direct_neo4j_client()
                result = await direct_client.read_neo4j_cypher(cypher_query, parameters)
                direct_client.close()
                
                return {
                    "status": "success",
                    "query_result": result,
                    "source": "direct_neo4j"
                }
                
            except Exception as direct_error:
                logger.error(f"Direct Neo4j also failed: {direct_error}")
                return {
                    "status": "error",
                    "error": str(e),
                    "direct_error": str(direct_error),
                    "message": "Both MCP and direct Neo4j connections failed"
                }
    
    async def ingest(self, content: Any, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Main ingestion method that orchestrates the full process"""
        try:
            logger.info("Starting Neo4j ingestion process")
            # Clean content for JSON compliance
            cleaned_content = self._clean_record(content)
            # Validate content
            if not await self.validate(cleaned_content):
                raise ValueError("Content validation failed")
            # Convert content to structured data
            if isinstance(cleaned_content, str):
                try:
                    structured_data = json.loads(cleaned_content)
                except json.JSONDecodeError:
                    structured_data = {
                        "content": cleaned_content,
                        "type": "text_document",
                        "created_at": metadata.get("created_at"),
                        **metadata
                    }
            else:
                structured_data = cleaned_content
            # Step 1: Get current database schema
            current_schema = await self.get_database_schema()
            logger.info("Retrieved current database schema")
            # Step 2: Get schema recommendations for data model
            schema_result = await self.get_schema_for_data_model(structured_data)
            if schema_result["status"] != "success":
                logger.warning(f"Schema generation failed: {schema_result.get('error')}")
            # Step 3: Validate data model against recommended schema
            if schema_result["status"] == "success":
                validation_result = await self.validate_data_model(structured_data, schema_result["schema_suggestion"])
                logger.info(f"Data validation result: {validation_result.get('is_valid')}")
            else:
                validation_result = {"status": "skipped", "is_valid": True}
            # Step 4: Create constraints and indexes
            constraints_result = {"status": "skipped"}
            indexes_result = {"status": "skipped"}
            
            if schema_result["status"] == "success":
                constraints_result = await self.generate_cypher_constraints(schema_result["schema_suggestion"])
                indexes_result = await self.create_indexes(schema_result["schema_suggestion"])
                logger.info("Created constraints and indexes")
            
            # Step 5: Generate and execute node ingestion
            node_type = metadata.get("node_type", "Document")
            node_result = await self.generate_cypher_node_ingest(structured_data, node_type)
            
            execution_result = {"status": "skipped"}
            if node_result["status"] == "success":
                execution_result = await self.write_neo4j_cypher(
                    node_result["cypher_statements"], 
                    node_result.get("parameters", {})
                )
                logger.info(f"Node ingestion completed: {execution_result.get('successful_executions', 0)} statements executed")
            
            # Step 6: Auto-detect and handle relationships
            relationship_results = []
            
            # First check for explicitly specified relationships
            if "relationships" in metadata:
                for rel in metadata["relationships"]:
                    rel_result = await self.generate_cypher_relationship_ingest(
                        structured_data,
                        rel["target"],
                        rel["type"]
                    )
                    if rel_result["status"] == "success":
                        rel_execution = await self.write_neo4j_cypher(rel_result["cypher_statements"])
                        relationship_results.append(rel_execution)
            
            # Auto-detect relationships from data structure
            auto_relationships = []
            if isinstance(structured_data, list) and len(structured_data) > 0:
                auto_relationships = await self._auto_detect_relationships(structured_data, metadata)
                if auto_relationships:
                    logger.info(f"Auto-detected {len(auto_relationships)} relationship types")
                    for rel_config in auto_relationships:
                        try:
                            rel_result = await self._create_auto_detected_relationships(structured_data, rel_config)
                            if rel_result["status"] == "success":
                                relationship_results.extend(rel_result.get("executions", []))
                        except Exception as rel_error:
                            logger.warning(f"Failed to create auto-detected relationship {rel_config.get('type')}: {rel_error}")
            
            return {
                "status": "success",
                "ingested_nodes": execution_result.get("successful_executions", 0),
                "ingested_relationships": len(relationship_results),
                "schema_result": schema_result,
                "validation_result": validation_result,
                "constraints_result": constraints_result,
                "indexes_result": indexes_result,
                "execution_result": execution_result,
                "relationship_results": relationship_results,
                "current_schema": current_schema
            }
            
        except Exception as e:
            logger.error(f"Error ingesting content into Neo4j: {e}")
            return {
                "status": "error",
                "error": str(e),
                "ingested_nodes": 0,
                "ingested_relationships": 0
            }

    async def _auto_detect_relationships(self, data: List[Dict], metadata: Optional[Dict] = None) -> List[Dict]:
        """Auto-detect potential relationships from data structure using LLM analysis."""
        
        if not data or not isinstance(data, list) or len(data) == 0:
            return []
        
        try:
            # Use LLM to analyze the data structure and suggest relationships
            llm_relationships = await self._llm_analyze_relationships(data, metadata)
            
            # Combine LLM suggestions with pattern-based detection
            pattern_relationships = await self._pattern_based_relationship_detection(data)
            
            # Merge and deduplicate relationships
            all_relationships = llm_relationships + pattern_relationships
            unique_relationships = self._deduplicate_relationships(all_relationships)
            
            logger.info(f"Detected {len(unique_relationships)} unique relationships from {len(all_relationships)} suggestions")
            return unique_relationships
            
        except Exception as e:
            logger.error(f"Error in auto-detecting relationships: {e}")
            # Fallback to pattern-based detection only
            return await self._pattern_based_relationship_detection(data)

    async def _llm_analyze_relationships(self, data: List[Dict], metadata: Optional[Dict] = None) -> List[Dict]:
        """Use LLM to analyze data and suggest meaningful relationships."""
        
        try:
            # Take a sample of data for analysis
            sample_size = min(5, len(data))
            sample_data = data[:sample_size]
            field_names = list(data[0].keys()) if data else []
            
            # Analyze data values to understand field semantics
            field_analysis = {}
            for field in field_names:
                values = [str(record.get(field, '')) for record in sample_data if record.get(field)]
                field_analysis[field] = {
                    "sample_values": values[:3],  # First 3 values
                    "unique_count": len(set(values)),
                    "data_type": self._infer_field_type(values)
                }
            
            # Create LLM prompt for relationship analysis
            prompt = f"""
            Analyze this CSV data structure and suggest meaningful relationships for a graph database.

            FIELD STRUCTURE:
            {json.dumps(field_analysis, indent=2)}

            SAMPLE RECORDS:
            {json.dumps(sample_data, indent=2)}

            CONTEXT: {metadata.get('context', 'General data analysis') if metadata else 'General data analysis'}

            Please suggest relationships between fields that would create a meaningful graph structure. 
            Consider:
            1. Entity relationships (e.g., Person->Company, Product->Category)
            2. Hierarchical relationships (e.g., Parent->Child, Manager->Employee)
            3. Temporal relationships (e.g., Event->Date, Order->DeliveryDate)
            4. Spatial relationships (e.g., Address->City, Store->Location)
            5. Reference relationships (e.g., Customer->Order, User->Account)

            Return ONLY a JSON array of relationship objects with this format:
            [
              {{
                "type": "RELATIONSHIP_NAME",
                "from_field": "source_field_name",
                "to_field": "target_field_name", 
                "from_label": "SourceNodeType",
                "to_label": "TargetNodeType",
                "description": "Brief description of the relationship",
                "confidence": "high|medium|low",
                "reasoning": "Why this relationship makes sense"
              }}
            ]

            Focus on relationships that would be most valuable for querying and understanding the data.
            """

            # Call LLM
            response = await self.llm.ainvoke(prompt)
            
            # Parse LLM response
            try:
                # Extract JSON from response
                response_text = response.content.strip()
                
                # Try to find JSON array in response
                import re
                json_match = re.search(r'\[.*\]', response_text, re.DOTALL)
                if json_match:
                    json_text = json_match.group(0)
                    relationships = json.loads(json_text)
                    
                    # Validate and clean relationships
                    valid_relationships = []
                    for rel in relationships:
                        if self._validate_relationship_config(rel, field_names):
                            valid_relationships.append(rel)
                    
                    logger.info(f"LLM suggested {len(valid_relationships)} valid relationships")
                    return valid_relationships
                else:
                    logger.warning("No valid JSON found in LLM response")
                    return []
                    
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse LLM relationship suggestions: {e}")
                return []
                
        except Exception as e:
            logger.error(f"Error in LLM relationship analysis: {e}")
            return []

    def _infer_field_type(self, values: List[str]) -> str:
        """Infer the semantic type of a field based on its values."""
        
        if not values:
            return "unknown"
        
        # Check for IDs
        if any(pattern in str(values[0]).lower() for pattern in ['id', 'key', 'ref']):
            return "identifier"
        
        # Check for names/titles
        if any(pattern in str(values[0]).lower() for pattern in ['name', 'title', 'label']):
            return "name"
        
        # Check for dates
        if any(pattern in str(values[0]).lower() for pattern in ['date', 'time', 'created', 'updated']):
            return "temporal"
        
        # Check for locations
        if any(pattern in str(values[0]).lower() for pattern in ['address', 'city', 'country', 'location', 'lat', 'lng']):
            return "location"
        
        # Check for categories
        if len(set(values)) < len(values) * 0.5:  # Many duplicates = categorical
            return "categorical"
        
        # Check for numeric
        try:
            float(values[0])
            return "numeric"
        except:
            pass
        
        return "text"

    def _validate_relationship_config(self, rel: Dict, field_names: List[str]) -> bool:
        """Validate that a relationship configuration is valid."""
        
        # Check basic required fields
        if not rel.get("type") or not isinstance(rel["type"], str):
            return False
        
        if not rel.get("from_field") or not rel.get("from_label"):
            return False
        
        # Check from_field exists
        if rel["from_field"] not in field_names:
            return False
        
        # Handle spatial relationships (they use coordinate_fields instead of to_field)
        if rel["type"] == "LOCATED_AT" and "coordinate_fields" in rel:
            # Validate coordinate fields exist
            coord_fields = rel.get("coordinate_fields", [])
            if not coord_fields or not all(cf in field_names for cf in coord_fields):
                return False
            return True
        
        # Handle regular relationships
        required_fields = ["to_field", "to_label"]
        if not all(field in rel for field in required_fields):
            return False
        
        # Check that referenced fields exist in data
        if rel["to_field"] not in field_names:
            return False
        
        # Check that relationship type is valid
        if not rel["type"] or not isinstance(rel["type"], str):
            return False
        
        return True

    async def _pattern_based_relationship_detection(self, data: List[Dict]) -> List[Dict]:
        """Fallback pattern-based relationship detection."""
        
        relationships = []
        sample_record = data[0] if isinstance(data[0], dict) else {}
        field_names = list(sample_record.keys()) if sample_record else []
        
        # Look for ID/reference patterns
        id_fields = [f for f in field_names if any(pattern in f.lower() for pattern in ['id', 'index', 'ref', 'key'])]
        
        # Look for categorical fields
        categorical_fields = []
        for field in field_names:
            if any(keyword in field.lower() for keyword in ['label', 'type', 'category', 'class', 'status', 'group']):
                categorical_fields.append(field)
        
        # Look for coordinate/location fields
        coordinate_fields = []
        for field in field_names:
            if any(coord in field.lower() for coord in ['x', 'y', 'w', 'h', 'bbox', 'coord', 'lat', 'lng', 'location']):
                coordinate_fields.append(field)
        
        # Create generic relationships based on patterns
        if id_fields and categorical_fields:
            for id_field in id_fields[:1]:  # Limit to first ID field
                for cat_field in categorical_fields[:2]:  # Limit to first 2 categorical fields
                    relationships.append({
                        "type": "BELONGS_TO",
                        "from_field": id_field,
                        "to_field": cat_field,
                        "from_label": self._clean_field_name(id_field),
                        "to_label": self._clean_field_name(cat_field),
                        "description": f"{id_field} belongs to {cat_field}",
                        "confidence": "medium",
                        "reasoning": "Pattern-based: ID field related to categorical field"
                    })
        
        # Skip spatial relationships for now as they need special handling
        # TODO: Improve spatial relationship handling
        
        return relationships

    def _deduplicate_relationships(self, relationships: List[Dict]) -> List[Dict]:
        """Remove duplicate relationships based on field combinations."""
        
        seen = set()
        unique_relationships = []
        
        for rel in relationships:
            # Create a key for deduplication
            key = (rel.get("from_field"), rel.get("to_field"), rel.get("type"))
            
            if key not in seen:
                seen.add(key)
                unique_relationships.append(rel)
        
        return unique_relationships

    def _clean_field_name(self, field_name: str) -> str:
        """Convert field name to a clean node label."""
        # Remove common suffixes and clean up
        clean_name = field_name.replace('_', ' ').replace('-', ' ')
        clean_name = ''.join(word.capitalize() for word in clean_name.split())
        
        # Remove common suffixes
        for suffix in ['Id', 'Index', 'Key', 'Ref']:
            if clean_name.endswith(suffix):
                clean_name = clean_name[:-len(suffix)]
        
        return clean_name or 'Entity'

    async def _create_auto_detected_relationships(self, data: List[Dict], rel_config: Dict) -> Dict[str, Any]:
        """Create relationships based on auto-detected configuration (both LLM and pattern-based)."""
        
        try:
            executions = []
            rel_type = rel_config["type"]
            from_field = rel_config.get("from_field")
            to_field = rel_config.get("to_field")
            from_label = rel_config.get("from_label", "Entity")
            to_label = rel_config.get("to_label", "Entity")
            
            # Log the relationship being created
            if rel_type == "LOCATED_AT" and rel_config.get("coordinate_fields"):
                logger.info(f"Creating spatial relationships: {from_label}({from_field}) -[{rel_type}]-> {to_label}(coordinates)")
            else:
                logger.info(f"Creating relationships: {from_label}({from_field}) -[{rel_type}]-> {to_label}({to_field})")
            
            # Handle different relationship patterns
            if rel_type in ["HAS_FINDING", "HAS", "CONTAINS", "BELONGS_TO", "RELATED_TO"] and from_field and to_field:
                # Generic entity relationships
                cypher_statements = []
                unique_pairs = set()  # Prevent duplicate relationships
                
                for record in data:
                    from_value = record.get(from_field)
                    to_value = record.get(to_field)
                    
                    if from_value and to_value:
                        # Create unique pair to avoid duplicates
                        pair_key = (str(from_value), str(to_value))
                        if pair_key not in unique_pairs:
                            unique_pairs.add(pair_key)
                            
                            cypher = f"""
                            MERGE (from_node:{from_label} {{value: $from_value}})
                            MERGE (to_node:{to_label} {{value: $to_value}})
                            MERGE (from_node)-[r:{rel_type}]->(to_node)
                            RETURN from_node, to_node, r
                            """
                            cypher_statements.append({
                                "query": cypher,
                                "params": {
                                    "from_value": str(from_value), 
                                    "to_value": str(to_value)
                                }
                            })
                
                # Execute relationship creation in batches
                if cypher_statements:
                    batch_size = 50
                    for i in range(0, min(len(cypher_statements), batch_size)):
                        stmt = cypher_statements[i]
                        result = await self.write_neo4j_cypher([stmt["query"]], stmt["params"])
                        executions.append(result)
                        
                    logger.info(f"Created {len(executions)} {rel_type} relationships")
            
            elif rel_type == "LOCATED_AT" and from_field and rel_config.get("coordinate_fields"):
                # Handle spatial relationships
                coord_fields = rel_config["coordinate_fields"]
                cypher_statements = []
                unique_locations = set()
                
                for record in data:
                    entity_value = record.get(from_field)
                    
                    # Extract coordinates
                    coords = {}
                    for coord_field in coord_fields:
                        if coord_field in record and record[coord_field] is not None:
                            coords[coord_field] = record[coord_field]
                    
                    if entity_value and coords:
                        # Create a unique location identifier
                        coord_str = ', '.join(f"{k}:{v}" for k, v in coords.items())
                        location_key = (str(entity_value), coord_str)
                        
                        if location_key not in unique_locations:
                            unique_locations.add(location_key)
                            
                            cypher = f"""
                            MERGE (entity:{from_label} {{value: $entity_value}})
                            MERGE (loc:{to_label} {{coordinates: $coordinates}})
                            SET loc += $coord_props
                            MERGE (entity)-[r:{rel_type}]->(loc)
                            RETURN entity, loc, r
                            """
                            
                            # Include individual coordinate properties
                            coord_props = {self._clean_property_name(k): v for k, v in coords.items()}
                            
                            cypher_statements.append({
                                "query": cypher,
                                "params": {
                                    "entity_value": str(entity_value), 
                                    "coordinates": coord_str,
                                    "coord_props": coord_props
                                }
                            })
                
                # Execute spatial relationships
                if cypher_statements:
                    batch_size = 50
                    for i in range(0, min(len(cypher_statements), batch_size)):
                        stmt = cypher_statements[i]
                        result = await self.write_neo4j_cypher([stmt["query"]], stmt["params"])
                        executions.append(result)
                        
                    logger.info(f"Created {len(executions)} spatial relationships")
            
            # Handle time-based relationships
            elif rel_type in ["OCCURRED_ON", "CREATED_ON", "HAPPENED_AT"] and from_field and to_field:
                cypher_statements = []
                unique_time_pairs = set()
                
                for record in data:
                    entity_value = record.get(from_field)
                    time_value = record.get(to_field)
                    
                    if entity_value and time_value:
                        time_key = (str(entity_value), str(time_value))
                        if time_key not in unique_time_pairs:
                            unique_time_pairs.add(time_key)
                            
                            cypher = f"""
                            MERGE (entity:{from_label} {{value: $entity_value}})
                            MERGE (time_node:{to_label} {{value: $time_value}})
                            MERGE (entity)-[r:{rel_type}]->(time_node)
                            RETURN entity, time_node, r
                            """
                            cypher_statements.append({
                                "query": cypher,
                                "params": {
                                    "entity_value": str(entity_value),
                                    "time_value": str(time_value)
                                }
                            })
                
                # Execute temporal relationships
                if cypher_statements:
                    for i in range(0, min(len(cypher_statements), 50)):
                        stmt = cypher_statements[i]
                        result = await self.write_neo4j_cypher([stmt["query"]], stmt["params"])
                        executions.append(result)
            
            # Handle hierarchical relationships
            elif rel_type in ["PARENT_OF", "CHILD_OF", "MANAGES", "REPORTS_TO"] and from_field and to_field:
                cypher_statements = []
                unique_hierarchy_pairs = set()
                
                for record in data:
                    parent_value = record.get(from_field)
                    child_value = record.get(to_field)
                    
                    if parent_value and child_value and parent_value != child_value:
                        hierarchy_key = (str(parent_value), str(child_value))
                        if hierarchy_key not in unique_hierarchy_pairs:
                            unique_hierarchy_pairs.add(hierarchy_key)
                            
                            cypher = f"""
                            MERGE (parent:{from_label} {{value: $parent_value}})
                            MERGE (child:{to_label} {{value: $child_value}})
                            MERGE (parent)-[r:{rel_type}]->(child)
                            RETURN parent, child, r
                            """
                            cypher_statements.append({
                                "query": cypher,
                                "params": {
                                    "parent_value": str(parent_value),
                                    "child_value": str(child_value)
                                }
                            })
                
                # Execute hierarchical relationships
                if cypher_statements:
                    for i in range(0, min(len(cypher_statements), 50)):
                        stmt = cypher_statements[i]
                        result = await self.write_neo4j_cypher([stmt["query"]], stmt["params"])
                        executions.append(result)
            
            return {
                "status": "success",
                "relationship_type": rel_type,
                "executions": executions,
                "created_relationships": len(executions),
                "from_label": from_label,
                "to_label": to_label
            }
            
        except Exception as e:
            logger.error(f"Error creating auto-detected relationships: {e}")
            return {
                "status": "error",
                "error": str(e),
                "executions": []
            }

    # ==========================================
    # ENHANCED MCP FUNCTIONALITY
    # ==========================================
    
    async def mcp_get_schema_for_data_model(self, data_model: Dict[str, Any]) -> Dict[str, Any]:
    # Get the overall schema structure using MCP Neo4j data modeling server
        if not MCP_CLIENT_AVAILABLE or MCPNeo4jClient is None:
            logger.warning("MCP client not available, using fallback")
            return {
                "status": "fallback",
                "error": "MCP client not available",
                "schema": {
                    "data_model": data_model,
                    "is_valid": True,  # Assume valid for fallback
                    "node_count": len(data_model.get("nodes", [])),
                    "relationship_count": len(data_model.get("relationships", []))
                }
            }
        
        try:
            async with MCPNeo4jClient() as client:
                return await client.get_schema_for_data_model(data_model)
        except Exception as e:
            logger.error(f"Error getting schema for data model: {e}")
            return {"status": "error", "error": str(e)}
    
    async def mcp_validate_data_model(self, data_model: Dict[str, Any]) -> Dict[str, Any]:
        # Validate your complete schema using MCP Neo4j data modeling server
        # Uses the actual 'validate_data_model' MCP tool
        if not MCP_CLIENT_AVAILABLE or MCPNeo4jClient is None:
            logger.warning("MCP client not available, using fallback validation")
            return {
                "status": "fallback",
                "is_valid": True,  # Assume valid for fallback
                "validation_result": {"message": "Fallback validation - MCP not available"}
            }
        
        try:
            result = await self._call_mcp_function(
                self.data_modeling_url,
                "validate_data_model",
                {"data_model": data_model}
            )
            result["source"] = "mcp_data_modeling_server"
            return result
        except Exception as e:
            logger.error(f"Error validating data model: {e}")
            return {"status": "error", "is_valid": False, "error": str(e)}
    
    async def mcp_generate_cypher_constraints(self, data_model: Dict[str, Any]) -> Dict[str, Any]:
        # Generate constraint queries using MCP Neo4j data modeling server
        # Uses the actual 'get_constraints_cypher_queries' MCP tool
        if not MCP_CLIENT_AVAILABLE or MCPNeo4jClient is None:
            logger.warning("MCP client not available, generating basic constraints")
            # Generate basic constraints as fallback
            constraints = []
            for node in data_model.get("nodes", []):
                if node.get("key_property"):
                    label = node.get("label")
                    key_prop = node.get("key_property")
                    constraints.append(
                        f"CREATE CONSTRAINT {label.lower()}_{key_prop}_unique IF NOT EXISTS "
                        f"FOR (n:{label}) REQUIRE n.{key_prop} IS UNIQUE"
                    )
            
            return {
                "status": "fallback",
                "constraint_queries": constraints,
                "count": len(constraints)
            }
        
        try:
            result = await self._call_mcp_function(
                self.data_modeling_url,
                "get_constraints_cypher_queries",
                {"data_model": data_model}
            )
            result["source"] = "mcp_data_modeling_server"
            return result
        except Exception as e:
            logger.error(f"Error generating constraints: {e}")
            return {"status": "error", "error": str(e)}
    
    async def mcp_generate_cypher_node_ingest(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate node ingestion queries using MCP Neo4j data modeling server
        Uses the actual 'get_node_cypher_ingest_query' MCP tool
        """
        if not MCP_CLIENT_AVAILABLE or MCPNeo4jClient is None:
            logger.warning("MCP client not available, generating basic node ingest query")
            # Generate basic node ingest query as fallback
            label = node.get("label", "Node")
            key_prop = node.get("key_property", "id")
            
            # Build property list for SET clause
            properties = node.get("properties", [])
            prop_names = [prop.get("name") for prop in properties if prop.get("name")]
            
            if key_prop not in prop_names:
                prop_names.append(key_prop)
            
            query = f"UNWIND $records AS record MERGE (n:{label} {{{key_prop}: record.{key_prop}}}) SET n += record RETURN n"
            
            return {
                "status": "fallback",
                "cypher_query": query,
                "node_label": label,
                "uses_parameters": True
            }
        
        try:
            result = await self._call_mcp_function(
                self.data_modeling_url,
                "get_node_cypher_ingest_query",
                {"node": node}
            )
            result["source"] = "mcp_data_modeling_server"
            return result
        except Exception as e:
            logger.error(f"Error generating node ingest query: {e}")
            return {"status": "error", "error": str(e)}
    
    # (Removed accidental/duplicated code block that caused syntax error)
        
        try:
            result = await self._call_mcp_function(
                self.data_modeling_url,
                "get_relationship_cypher_ingest_query",
                {
                    "data_model": data_model,
                    "relationship_type": relationship_type,
                    "relationship_start_node_label": start_node_label,
                    "relationship_end_node_label": end_node_label
                }
            )
            result["source"] = "mcp_data_modeling_server"
            return result
        except Exception as e:
            logger.error(f"Error generating relationship ingest query: {e}")
            return {"status": "error", "error": str(e)}
    
    async def mcp_write_neo4j_cypher(self, query: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute updating Cypher queries using MCP Neo4j cypher server with fallback
        """
        if not MCP_CLIENT_AVAILABLE or MCPNeo4jClient is None:
            logger.warning("MCP client not available, using fallback cypher execution")
            # Try direct Neo4j client as fallback
            try:
                direct_client = get_direct_neo4j_client()
                result = await direct_client.write_neo4j_cypher([query], params)
                result["source"] = "direct_neo4j_fallback"
                return result
            except Exception as e:
                logger.error(f"Direct Neo4j fallback failed: {e}")
                return {"status": "error", "error": str(e), "source": "fallback_failed"}

        try:
            async with MCPNeo4jClient() as client:
                result = await client.write_neo4j_cypher(query, params)
                result["source"] = "mcp_cypher_server"
                return result
        except Exception as e:
            logger.error(f"MCP cypher server failed: {e}")
            # Try direct Neo4j client as fallback
            try:
                direct_client = get_direct_neo4j_client()
                result = await direct_client.write_neo4j_cypher([query], params)
                result["source"] = "direct_neo4j_fallback"
                return result
            except Exception as fallback_error:
                logger.error(f"All write methods failed: {fallback_error}")
                return {
                    "status": "error", 
                    "error": f"MCP failed: {str(e)}, Fallback failed: {str(fallback_error)}",
                    "source": "all_failed"
                }

    # ==========================================
    # ==========================================
    # DYNAMIC AUTO-DETECTION CAPABILITIES
    # ==========================================
    
    async def auto_analyze_and_ingest(self, data: Union[str, Dict, List], metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Automatically analyze any data structure and dynamically ingest using MCP
        
        This method can handle:
        - JSON strings and objects
        - CSV-like strings
        - Lists of objects
        - Raw text content
        
        Returns comprehensive analysis and ingestion results
        """
        try:
            logger.info("Starting auto-analysis and ingestion")
            
            # Step 1: Auto-detect data structure
            structure_analysis = await self._auto_detect_structure(data)
            logger.info(f"Detected data type: {structure_analysis['type']}")
            
            # Step 2: Extract representative sample
            data_sample = await self._extract_representative_sample(data, structure_analysis)
            logger.info(f"Extracted sample with {data_sample['count']} records")
            
            # Step 3: Get schema recommendation
            schema_result = await self.get_schema_for_data_model(data_sample.get('sample', {}))
            logger.info(f"Schema generation: {schema_result.get('status')}")
            
            # Step 4: Process and ingest all data
            ingestion_result = await self._process_and_ingest_all(data, structure_analysis, schema_result, metadata)
            logger.info(f"Ingestion completed: {ingestion_result.get('total_ingested', 0)} records")
            
            return {
                "status": "success",
                "analysis": structure_analysis,
                "schema": schema_result,
                "ingestion": ingestion_result,
                "summary": {
                    "data_type": structure_analysis['type'],
                    "records_analyzed": data_sample['count'],
                    "records_ingested": ingestion_result.get('total_ingested', 0),
                    "success_rate": self._calculate_success_rate(data_sample['count'], ingestion_result.get('total_ingested', 0))
                }
            }
            
        except Exception as e:
            logger.error(f"Auto-analysis and ingestion failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "analysis": None,
            }
    
    async def _auto_detect_structure(self, data: Union[str, Dict, List]) -> Dict[str, Any]:
        """Automatically detect data structure and characteristics."""
        
        analysis = {
            "type": "unknown",
            "detected_types": [],
            "complexity": "simple",
            "characteristics": {}
        }
        
        if isinstance(data, str):
            # Try to parse as JSON first
            try:
                parsed = json.loads(data)
                analysis["type"] = "json_string"
                analysis["parsed_type"] = type(parsed).__name__
                analysis["characteristics"]["parseable"] = True
                analysis["characteristics"]["original_format"] = "string"
                # Recursively analyze parsed content
                sub_analysis = await self._auto_detect_structure(parsed)
                analysis["sub_analysis"] = sub_analysis
                return analysis
            except json.JSONDecodeError:
                pass
            
            # Check if it's CSV-like
            lines = data.strip().split('\n')
            if len(lines) > 1:
                delimiter_candidates = [',', '\t', ';', '|']
                for delimiter in delimiter_candidates:
                    if delimiter in lines[0]:
                        # Check consistency across lines
                        first_count = lines[0].count(delimiter)
                        if all(line.count(delimiter) == first_count for line in lines[1:3]):
                            analysis["type"] = "csv_string"
                            analysis["characteristics"]["delimiter"] = delimiter
                            analysis["characteristics"]["estimated_columns"] = first_count + 1
                            analysis["characteristics"]["estimated_rows"] = len(lines)
                            return analysis
            
            # Default to text content
            analysis["type"] = "text"
            analysis["characteristics"]["length"] = len(data)
            analysis["characteristics"]["lines"] = len(lines)
            analysis["characteristics"]["words"] = len(data.split())
            
        elif isinstance(data, dict):
            analysis["type"] = "object"
            analysis["characteristics"]["keys"] = list(data.keys())
            analysis["characteristics"]["key_count"] = len(data.keys())
            
            # Analyze value types
            value_types = set()
            nested_objects = 0
            nested_arrays = 0
            
            for key, value in data.items():
                value_type = type(value).__name__
                value_types.add(value_type)
                
                if isinstance(value, dict):
                    nested_objects += 1
                elif isinstance(value, list):
                    nested_arrays += 1
            
            analysis["detected_types"] = list(value_types)
            analysis["characteristics"]["nested_objects"] = nested_objects
            analysis["characteristics"]["nested_arrays"] = nested_arrays
            
            # Determine complexity
            if nested_objects > 0 or nested_arrays > 0:
                analysis["complexity"] = "complex"
            elif len(data.keys()) > 10:
                analysis["complexity"] = "moderate"
        
        elif isinstance(data, list):
            analysis["type"] = "array"
            analysis["characteristics"]["length"] = len(data)
            
            if data:
                # Analyze first few items to understand structure
                sample_items = data[:5]
                item_types = set()
                
                for item in sample_items:
                    item_types.add(type(item).__name__)
                
                analysis["detected_types"] = list(item_types)
                analysis["characteristics"]["homogeneous"] = len(item_types) == 1
                
                # If it's an array of objects, analyze object structure
                if all(isinstance(item, dict) for item in sample_items):
                    all_keys = set()
                    for item in sample_items:
                        all_keys.update(item.keys())
                    
                    analysis["characteristics"]["common_keys"] = list(all_keys)
                    analysis["complexity"] = "moderate" if len(all_keys) > 5 else "simple"
        
        return analysis
    
    async def _extract_representative_sample(self, data: Union[str, Dict, List], structure_analysis: Dict) -> Dict[str, Any]:
        """Extract a representative sample for schema generation."""
        
        data_type = structure_analysis["type"]
        
        if data_type == "json_string" and isinstance(data, str):
            # Parse and handle the sub-structure
            parsed_data = json.loads(data)
            return await self._extract_representative_sample(parsed_data, structure_analysis.get("sub_analysis", {}))
        
        elif data_type == "csv_string" and isinstance(data, str):
            # Convert CSV string to structured data
            lines = data.strip().split('\n')
            delimiter = structure_analysis["characteristics"]["delimiter"]
            
            if len(lines) < 2:
                return {"sample": {}, "count": 0}
            
            headers = lines[0].split(delimiter)
            sample_records = []
            
            for line in lines[1:min(51, len(lines))]:  # Sample up to 50 records
                values = line.split(delimiter)
                if len(values) == len(headers):
                    record = dict(zip(headers, values))
                    sample_records.append(record)
            
            return {
                "sample": sample_records[0] if sample_records else {},
                "all_samples": sample_records,
                "count": len(sample_records)
            }
        
        elif data_type == "object" and isinstance(data, dict):
            return {
                "sample": data,
                "count": 1
            }
        
        elif data_type == "array" and isinstance(data, list):
            if data and isinstance(data[0], dict):
                return {
                    "sample": data[0],
                    "all_samples": data[:50],  # First 50 for analysis
                    "count": len(data)
                }
            else:
                # Convert array elements to object format
                sample_records = []
                for i, item in enumerate(data[:50]):
                    record = {
                        "value": item,
                        "index": i,
                        "type": type(item).__name__
                    }
                    sample_records.append(record)
                
                return {
                    "sample": sample_records[0] if sample_records else {},
                    "all_samples": sample_records,
                    "count": len(sample_records)
                }
        
        elif data_type == "text" and isinstance(data, str):
            # Convert text to a document-like structure
            return {
                "sample": {
                    "content": data[:1000],  # First 1000 characters
                    "type": "text_document",
                    "length": len(data),
                    "word_count": len(data.split())
                },
                "count": 1
            }
        
        return {"sample": {}, "count": 0}
    
    async def _process_and_ingest_all(
        self, 
        data: Union[str, Dict, List], 
        structure_analysis: Dict, 
        schema_result: Dict,
        metadata: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Process and ingest all data based on detected structure."""
        
        try:
            data_type = structure_analysis["type"]
            records_to_ingest = []
            
            # Convert all data to ingestable records
            if data_type == "json_string" and isinstance(data, str):
                parsed_data = json.loads(data)
                records_to_ingest = await self._convert_to_records(parsed_data, structure_analysis.get("sub_analysis", {}))
            
            elif data_type == "csv_string" and isinstance(data, str):
                lines = data.strip().split('\n')
                delimiter = structure_analysis["characteristics"]["delimiter"]
                headers = lines[0].split(delimiter)
                
                for line in lines[1:]:
                    values = line.split(delimiter)
                    if len(values) == len(headers):
                        record = dict(zip(headers, values))
                        records_to_ingest.append(record)
            
            elif data_type == "object" and isinstance(data, dict):
                records_to_ingest = [data]
            
            elif data_type == "array" and isinstance(data, list):
                records_to_ingest = await self._convert_to_records(data, structure_analysis)
            
            elif data_type == "text" and isinstance(data, str):
                records_to_ingest = [{
                    "content": data,
                    "type": "text_document",
                    "word_count": len(data.split()),
                    "character_count": len(data)
                }]
            
            # Ingest records in batches
            total_ingested = 0
            batch_size = 100
            ingestion_results = []
            
            for i in range(0, len(records_to_ingest), batch_size):
                batch = records_to_ingest[i:i+batch_size]
                
                for record in batch:
                    try:
                        # Ensure record is a dictionary
                        if not isinstance(record, dict):
                            record = {"value": str(record), "type": type(record).__name__}
                        
                        # Determine node type from schema or metadata
                        node_type = self._determine_node_type(record, schema_result, metadata)
                        
                        # Create ingestion metadata
                        ingest_metadata = {
                            "node_type": node_type,
                            "source": "auto_detection",
                            "batch_number": i // batch_size,
                            **(metadata or {})
                        }
                        
                        result = await self.ingest(record, ingest_metadata)
                        
                        if result.get("status") == "success":
                            total_ingested += result.get("ingested_nodes", 0)
                        
                        ingestion_results.append({
                            "record_index": len(ingestion_results),
                            "status": result.get("status"),
                            "nodes_created": result.get("ingested_nodes", 0)
                        })
                        
                    except Exception as e:
                        logger.warning(f"Failed to ingest record {len(ingestion_results)}: {e}")
                        ingestion_results.append({
                            "record_index": len(ingestion_results),
                            "status": "error",
                            "error": str(e),
                            "nodes_created": 0
                        })
                
                logger.info(f"Processed batch {i//batch_size + 1}, total ingested: {total_ingested}")
            
            # After all nodes are ingested, detect and create relationships
            relationship_results = []
            if isinstance(data, list) and len(data) > 0 and total_ingested > 0:
                try:
                    logger.info("Starting relationship detection for ingested data...")
                    
                    # Use the original data for relationship detection
                    auto_relationships = await self._auto_detect_relationships(data, metadata)
                    
                    if auto_relationships:
                        logger.info(f"Auto-detected {len(auto_relationships)} relationship types")
                        
                        for rel_config in auto_relationships:
                            try:
                                rel_result = await self._create_auto_detected_relationships(data, rel_config)
                                if rel_result["status"] == "success":
                                    relationship_results.extend(rel_result.get("executions", []))
                                    logger.info(f"Created {rel_result.get('created_relationships', 0)} relationships of type {rel_config.get('type')}")
                            except Exception as rel_error:
                                logger.warning(f"Failed to create auto-detected relationship {rel_config.get('type')}: {rel_error}")
                    else:
                        logger.info("No relationships detected for this dataset")
                        
                except Exception as e:
                    logger.error(f"Error during relationship detection: {e}")
            
            successful_ingestions = sum(1 for r in ingestion_results if r["status"] == "success")
            total_relationships = len(relationship_results)
            
            return {
                "status": "success",
                "total_records": len(records_to_ingest),
                "total_ingested": total_ingested,
                "total_relationships": total_relationships,
                "successful_ingestions": successful_ingestions,
                "relationship_results": relationship_results,
                "batch_results": ingestion_results,
                "success_rate": self._calculate_success_rate(len(records_to_ingest), successful_ingestions)
            }
            
        except Exception as e:
            logger.error(f"Error processing and ingesting data: {e}")
            return {
                "status": "error",
                "error": str(e),
                "total_ingested": 0
            }
    
    async def _convert_to_records(self, data: Union[Dict, List], structure_analysis: Dict) -> List[Dict]:
        """Convert data to a list of records for ingestion."""
        
        if isinstance(data, list):
            if all(isinstance(item, dict) for item in data):
                return data  # Already list of dicts
            else:
                # Convert other types to dict format
                return [
                    {
                        "value": item,
                        "index": i,
                        "data_type": type(item).__name__
                    }
                    for i, item in enumerate(data)
                ]
        
        elif isinstance(data, dict):
            return [data]  # Single object becomes single record
        
        else:
            return [{"content": str(data), "data_type": type(data).__name__}]
    
    def _determine_node_type(self, record: Dict, schema_result: Dict, metadata: Optional[Dict]) -> str:
        # Determine appropriate node type for a record
        
        # Check metadata first
        if metadata and "node_type" in metadata:
            return metadata["node_type"]
        
        # Check schema suggestion
        if schema_result.get("status") == "success":
            schema_suggestion = schema_result.get("schema_suggestion", {})
            nodes = schema_suggestion.get("nodes", [])
            if nodes:
                return nodes[0].get("label", "DataRecord")
        
        # Analyze record content to guess type
        if "patient" in str(record).lower() or "patient_id" in record:
            return "Patient"
        elif "doctor" in str(record).lower() or "physician" in str(record).lower():
            return "Doctor"
        elif "image" in str(record).lower() or "bbox" in str(record).lower():
            return "MedicalImage"
        elif "content" in record and len(str(record.get("content", ""))) > 100:
            return "Document"
        else:
            return "DataRecord"
    
    def _calculate_success_rate(self, total: int, successful: int) -> float:
        # Calculate success percentage
        if total == 0:
            return 0.0
        return round((successful / total) * 100, 2)

    async def llm_mcp_smart_ingest(self, data: Union[str, Dict, List], context: Optional[str] = None) -> Dict[str, Any]:
        # Use LLM reasoning + MCP functions for intelligent data ingestion.
        # This method:
        # 1. Uses LLM to understand data semantics and structure
        # 2. Uses MCP to generate optimal schema
        # 3. Uses LLM to optimize the MCP schema
        # 4. Executes intelligent ingestion
        # Args:
        #     data: Any data structure (string, dict, list)
        #     context: Optional context about the data domain
        # Returns:
        #     Complete analysis and ingestion results
        try:
            logger.info("Starting LLM + MCP smart ingestion")
            
            # Step 1: LLM analysis of data structure and semantics
            llm_analysis = await self._llm_analyze_data_semantics(data, context)
            logger.info(f"LLM analysis: {llm_analysis.get('confidence', 'unknown')} confidence")
            
            # Step 2: Prepare enhanced sample for MCP
            enhanced_sample = await self._prepare_llm_enhanced_sample(data, llm_analysis)
            logger.info(f"Enhanced sample prepared with {len(enhanced_sample.get('records', []))} records")
            
            # Step 3: MCP schema generation with LLM context
            mcp_schema = await self.get_schema_for_data_model(enhanced_sample.get('sample', {}))
            logger.info(f"MCP schema generation: {mcp_schema.get('status')}")
            
            # Step 4: LLM optimization of MCP schema
            optimized_schema = await self._llm_optimize_schema(mcp_schema, llm_analysis)
            logger.info(f"Schema optimization: {optimized_schema.get('status')}")
            
            # Step 5: Execute intelligent ingestion
            ingestion_metadata = {
                "node_type": llm_analysis.get('primary_node_type', 'DataRecord'),
                "domain": llm_analysis.get('domain', 'general'),
                "llm_confidence": llm_analysis.get('confidence'),
                "mcp_enhanced": True,
                "smart_ingestion": True
            }
            
            ingestion_result = await self.auto_analyze_and_ingest(data, ingestion_metadata)
            logger.info(f"Smart ingestion completed: {ingestion_result.get('summary', {}).get('records_ingested', 0)} records")
            
            return {
                "status": "success",
                "method": "llm_mcp_smart_ingest",
                "llm_analysis": llm_analysis,
                "mcp_schema": mcp_schema,
                "optimized_schema": optimized_schema,
                "ingestion_result": ingestion_result,
                "intelligence_metrics": {
                    "llm_confidence": llm_analysis.get('confidence'),
                    "schema_quality": optimized_schema.get('quality_score', 'unknown'),
                    "total_ingested": ingestion_result.get('summary', {}).get('records_ingested', 0),
                    "approach": "hybrid_llm_mcp"
                }
            }
            
        except Exception as e:
            logger.error(f"LLM+MCP smart ingestion failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "method": "llm_mcp_smart_ingest",
                "fallback_attempted": False
            }
    
    async def _llm_analyze_data_semantics(self, data: Union[str, Dict, List], context: Optional[str] = None) -> Dict[str, Any]:
        """Use LLM to analyze data semantics and suggest optimal modeling"""
        
        # Create analysis prompt
        data_preview = str(data)[:2000] if len(str(data)) > 2000 else str(data)
        
        prompt = f"""
        Analyze this data for Neo4j graph database modeling. Provide semantic insights and recommendations.

        DATA PREVIEW:
        {data_preview}

        DATA TYPE: {type(data).__name__}
        CONTEXT: {context or 'No specific context provided'}

        Provide analysis in JSON format:
        {{
            "domain": "data domain (medical/financial/social/technical/other)",
            "primary_node_type": "best Neo4j node label for this data",
            "entity_types": ["entities found in the data"],
            "relationships": ["relationships between entities"],
            "key_properties": ["most important properties"],
            "semantic_meaning": "what this data represents",
            "complexity": "simple/moderate/complex",
            "confidence": "high/medium/low",
            "modeling_strategy": "recommended graph modeling approach",
            "optimization_tips": ["specific tips for this data type"]
        }}

        Focus on semantic understanding and graph modeling best practices.
        """
        
        try:
            response = await self.llm.ainvoke(prompt)
            
            # Get string content from response
            response_text = response.content if hasattr(response, 'content') else str(response)
            if isinstance(response_text, list):
                response_text = ' '.join(str(item) for item in response_text)
            
            # Try to parse JSON response
            try:
                analysis = json.loads(response_text)
                analysis["llm_processing"] = "successful"
                return analysis
            except json.JSONDecodeError:
                # Extract key insights from text response
                content = response_text.lower()
                
                # Basic domain detection
                domain = "general"
                if any(word in content for word in ["medical", "patient", "doctor", "hospital"]):
                    domain = "medical"
                elif any(word in content for word in ["financial", "payment", "transaction"]):
                    domain = "financial"
                elif any(word in content for word in ["social", "user", "friend", "message"]):
                    domain = "social"
                
                return {
                    "domain": domain,
                    "primary_node_type": "DataRecord",
                    "entity_types": ["Record"],
                    "relationships": ["CONTAINS"],
                    "key_properties": ["id", "type"],
                    "semantic_meaning": response_text[:300],
                    "complexity": "moderate",
                    "confidence": "medium",
                    "modeling_strategy": "standard_ingestion",
                    "optimization_tips": [],
                    "llm_processing": "text_parsing",
                    "raw_response": response_text[:500]
                }
        
        except Exception as e:
            logger.error(f"LLM semantic analysis failed: {e}")
            return {
                "domain": "unknown",
                "primary_node_type": "Node",
                "entity_types": ["Unknown"],
                "relationships": ["RELATED"],
                "key_properties": [],
                "semantic_meaning": f"Analysis failed: {str(e)}",
                "complexity": "simple",
                "confidence": "low",
                "modeling_strategy": "fallback",
                "optimization_tips": [],
                "llm_processing": "failed",
                "error": str(e)
            }
    
    async def _prepare_llm_enhanced_sample(self, data: Union[str, Dict, List], llm_analysis: Dict) -> Dict[str, Any]:
        """Prepare data sample enhanced with LLM insights for MCP analysis"""
        
        try:
            # Extract or create sample based on data type
            if isinstance(data, dict):
                sample_record = data.copy()
            elif isinstance(data, list) and data:
                sample_record = data[0] if isinstance(data[0], dict) else {"value": data[0], "index": 0}
            elif isinstance(data, str):
                sample_record = {"content": data[:1000], "type": "text"}
            else:
                sample_record = {"data": str(data), "type": type(data).__name__}
            
            # Enhance with LLM insights
            if isinstance(sample_record, dict):
                sample_record["_llm_domain"] = llm_analysis.get('domain', 'unknown')
                sample_record["_llm_node_type"] = llm_analysis.get('primary_node_type', 'DataRecord')
                sample_record["_llm_entities"] = llm_analysis.get('entity_types', [])
                sample_record["_llm_semantic_meaning"] = llm_analysis.get('semantic_meaning', '')
                sample_record["_llm_confidence"] = llm_analysis.get('confidence', 'medium')
            
            # Create records collection
            if isinstance(data, list):
                all_records = []
                for i, item in enumerate(data[:20]):  # Sample first 20
                    if isinstance(item, dict):
                        record = item.copy()
                    else:
                        record = {"value": item, "index": i}
                    
                    record.update({
                        "_llm_domain": llm_analysis.get('domain'),
                        "_llm_node_type": llm_analysis.get('primary_node_type')
                    })
                    all_records.append(record)
            else:
                all_records = [sample_record]
            
            return {
                "sample": sample_record,
                "records": all_records,
                "enhancement_method": "llm_guided",
                "llm_context": llm_analysis
            }
        
        except Exception as e:
            logger.error(f"LLM-enhanced sample preparation failed: {e}")
            return {
                "sample": {"error": str(e)},
                "records": [],
                "enhancement_method": "failed"
            }
    
    async def _llm_optimize_schema(self, mcp_schema: Dict, llm_analysis: Dict) -> Dict[str, Any]:
        """Use LLM to review and optimize MCP-generated schema"""
        
        if mcp_schema.get('status') != 'success':
            return {"status": "skipped", "reason": "No valid MCP schema to optimize"}
        
        schema_suggestion = mcp_schema.get('schema_suggestion', {})
        
        optimization_prompt = f"""
        Review this Neo4j schema and optimize it based on the data analysis:

        CURRENT SCHEMA:
        {json.dumps(schema_suggestion, indent=2)[:1500]}

        DATA ANALYSIS:
        - Domain: {llm_analysis.get('domain')}
        - Primary Entity: {llm_analysis.get('primary_node_type')}
        - Complexity: {llm_analysis.get('complexity')}
        - Key Properties: {llm_analysis.get('key_properties', [])}

        Provide optimized schema in JSON:
        {{
            "nodes": [
                {{
                    "label": "NodeLabel",
                    "properties": ["property1", "property2"],
                    "key_property": "unique_identifier",
                    "indexes": ["indexed_properties"]
                }}
            ],
            "relationships": [
                {{
                    "type": "RELATIONSHIP_TYPE",
                    "from_node": "SourceNode",
                    "to_node": "TargetNode"
                }}
            ],
            "optimization_changes": ["changes made and why"],
            "quality_score": 8,
            "performance_notes": ["performance considerations"]
        }}

        Focus on semantic correctness and performance.
        """
        
        try:
            response = await self.llm.ainvoke(optimization_prompt)
            
            # Get string content from response
            response_text = response.content if hasattr(response, 'content') else str(response)
            if isinstance(response_text, list):
                response_text = ' '.join(str(item) for item in response_text)
            
            try:
                optimized = json.loads(response_text)
                optimized.update({
                    "status": "success",
                    "optimization_method": "llm_review",
                    "original_schema": schema_suggestion
                })
                return optimized
            except json.JSONDecodeError:
                # Use original schema with LLM feedback
                return {
                    "status": "partial",
                    "nodes": schema_suggestion.get('nodes', []),
                    "relationships": schema_suggestion.get('relationships', []),
                    "optimization_changes": ["LLM provided text feedback"],
                    "quality_score": 6,
                    "performance_notes": ["Review LLM feedback"],
                    "llm_feedback": response_text[:800],
                    "optimization_method": "text_feedback"
                }
        
        except Exception as e:
            logger.error(f"LLM schema optimization failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "fallback_schema": schema_suggestion
            }

    # Additional MCP functions using actual available tools
    async def mcp_get_mermaid_visualization(self, data_model: Dict[str, Any]) -> Dict[str, Any]:
    # Generate Mermaid diagram visualization using MCP Neo4j data modeling server
    # Uses the actual 'get_mermaid_config_str' MCP tool
        if not MCP_CLIENT_AVAILABLE or MCPNeo4jClient is None:
            logger.warning("MCP client not available, generating basic mermaid")
            return {
                "status": "fallback",
                "mermaid_config": "graph TD\n  A[Node] --> B[Node]",
                "visualization_type": "mermaid"
            }
        
        try:
            result = await self._call_mcp_function(
                self.data_modeling_url,
                "get_mermaid_config_str", 
                {"data_model": data_model}
            )
            result["source"] = "mcp_data_modeling_server"
            return result
        except Exception as e:
            logger.error(f"Error generating mermaid visualization: {e}")
            return {"status": "error", "error": str(e)}

    async def mcp_validate_node(self, node: Dict[str, Any]) -> Dict[str, Any]:
    # Validate a single node structure using MCP Neo4j data modeling server
    # Uses the actual 'validate_node' MCP tool
        if not MCP_CLIENT_AVAILABLE or MCPNeo4jClient is None:
            logger.warning("MCP client not available, using fallback node validation")
            return {
                "status": "fallback",
                "is_valid": True,
                "validation_result": {"message": "Fallback node validation - MCP not available"}
            }
        
        try:
            result = await self._call_mcp_function(
                self.data_modeling_url,
                "validate_node",
                {"node": node}
            )
            result["source"] = "mcp_data_modeling_server"
            return result
        except Exception as e:
            logger.error(f"Error validating node: {e}")
            return {"status": "error", "is_valid": False, "error": str(e)}

    async def mcp_validate_relationship(self, relationship: Dict[str, Any]) -> Dict[str, Any]:
    # Validate a single relationship structure using MCP Neo4j data modeling server
    # Uses the actual 'validate_relationship' MCP tool
        if not MCP_CLIENT_AVAILABLE or MCPNeo4jClient is None:
            logger.warning("MCP client not available, using fallback relationship validation")
            return {
                "status": "fallback",
                "is_valid": True,
                "validation_result": {"message": "Fallback relationship validation - MCP not available"}
            }
        
        try:
            result = await self._call_mcp_function(
                self.data_modeling_url,
                "validate_relationship",
                {"relationship": relationship}
            )
            result["source"] = "mcp_data_modeling_server"
            return result
        except Exception as e:
            logger.error(f"Error validating relationship: {e}")
            return {"status": "error", "is_valid": False, "error": str(e)}

    async def mcp_get_example_data_model(self, example_name: str) -> Dict[str, Any]:
    # Get an example graph data model from available templates using MCP Neo4j data modeling server
    # Uses the actual 'get_example_data_model' MCP tool
    # Available examples: 'patient_journey', 'supply_chain', 'software_dependency', 
    #                   'oil_gas_monitoring', 'customer_360', 'fraud_aml', 'health_insurance_fraud'
        if not MCP_CLIENT_AVAILABLE or MCPNeo4jClient is None:
            logger.warning("MCP client not available, using fallback example model")
            return {
                "status": "fallback",
                "example_name": example_name,
                "data_model": {
                    "nodes": [{"label": "ExampleNode", "key_property": "id", "properties": ["name"]}],
                    "relationships": [{"type": "RELATES_TO", "source": "ExampleNode", "target": "ExampleNode"}]
                }
            }
        
        try:
            result = await self._call_mcp_function(
                self.data_modeling_url,
                "get_example_data_model",
                {"example_name": example_name}
            )
            result["source"] = "mcp_data_modeling_server"
            return result
        except Exception as e:
            logger.error(f"Error getting example data model: {e}")
            return {"status": "error", "error": str(e)}

    async def mcp_list_example_data_models(self) -> Dict[str, Any]:
    # List all available example data models with descriptions using MCP Neo4j data modeling server
    # Uses the actual 'list_example_data_models' MCP tool
        if not MCP_CLIENT_AVAILABLE or MCPNeo4jClient is None:
            logger.warning("MCP client not available, using fallback examples list")
            return {
                "status": "fallback",
                "examples": {
                    "patient_journey": "Healthcare patient journey model",
                    "supply_chain": "Supply chain management model",
                    "software_dependency": "Software dependency tracking model"
                }
            }
        
        try:
            result = await self._call_mcp_function(
                self.data_modeling_url,
                "list_example_data_models",
                {}
            )
            result["source"] = "mcp_data_modeling_server"
            return result
        except Exception as e:
            logger.error(f"Error listing example data models: {e}")
            return {"status": "error", "error": str(e)}

    async def mcp_export_to_arrows_json(self, data_model: Dict[str, Any]) -> Dict[str, Any]:
    # Export a data model to Arrows app JSON format using MCP Neo4j data modeling server
    # Uses the actual 'export_to_arrows_json' MCP tool
        if not MCP_CLIENT_AVAILABLE or MCPNeo4jClient is None:
            logger.warning("MCP client not available, using fallback arrows export")
            return {
                "status": "fallback",
                "arrows_json": '{"style": {}, "nodes": [], "relationships": []}',
                "export_format": "arrows_app"
            }
        
        try:
            result = await self._call_mcp_function(
                self.data_modeling_url,
                "export_to_arrows_json",
                {"data_model": data_model}
            )
            result["source"] = "mcp_data_modeling_server"
            return result
        except Exception as e:
            logger.error(f"Error exporting to Arrows JSON: {e}")
            return {"status": "error", "error": str(e)}

    async def mcp_load_from_arrows_json(self, arrows_data_model_dict: Dict[str, Any]) -> Dict[str, Any]:
    # Load a data model from Arrows app JSON format using MCP Neo4j data modeling server
    # Uses the actual 'load_from_arrows_json' MCP tool
        if not MCP_CLIENT_AVAILABLE or MCPNeo4jClient is None:
            logger.warning("MCP client not available, using fallback arrows import")
            return {
                "status": "fallback",
                "data_model": {
                    "nodes": [],
                    "relationships": []
                },
                "import_format": "arrows_app"
            }
        
        try:
            result = await self._call_mcp_function(
                self.data_modeling_url,
                "load_from_arrows_json",
                {"arrows_data_model_dict": arrows_data_model_dict}
            )
            result["source"] = "mcp_data_modeling_server"
            return result
        except Exception as e:
            logger.error(f"Error loading from Arrows JSON: {e}")
            return {"status": "error", "error": str(e)}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.http_client.aclose()
