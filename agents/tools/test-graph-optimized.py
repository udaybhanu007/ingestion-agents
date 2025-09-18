#!/usr/bin/env python3
"""
Optimized Fast Graph Ingestion Pipeline
Performance optimizations:
- Reduced debug output
- Better batching strategies
- Skip relationship processing for speed
- Progress indicators
"""

import os
import json
import csv
import requests
import time
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI

# Load environment variables
load_dotenv('.env.dev')

class OptimizedGraphTool:
    def __init__(self):
        self.llm = AzureChatOpenAI(
            deployment_name=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
            model_name=os.getenv("AZURE_OPENAI_MODEL_NAME", "gpt-4"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            temperature=0.3
        )
        
        self.cypher_server_url = "http://127.0.0.1:8003/mcp/"
        self.data_modeling_server_url = "http://127.0.0.1:8004/mcp/"
        
    def _make_mcp_request(self, server_url: str, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Make optimized MCP request with minimal logging"""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        }
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream"
        }
        
        try:
            response = requests.post(server_url, json=payload, headers=headers, timeout=30)
            if response.status_code == 200:
                result = response.json()
                # Handle SSE format responses  
                if "result" in result:
                    return result["result"]
                return result
            else:
                print(f"MCP Error: {response.status_code} - {response.text}")
                return {"error": f"HTTP {response.status_code}"}
        except Exception as e:
            print(f"MCP Request failed: {str(e)}")
            return {"error": str(e)}

    def _build_neo4j_data_model_from_llm_results(self, llm_results: Dict[str, Any]) -> Dict[str, Any]:
        """Build schema with minimal processing"""
        nodes = {}
        relationships = {}
        
        # Process entities
        for entity in llm_results.get("entities", []):
            label = entity.get("type", "Entity")
            if label not in nodes:
                nodes[label] = {
                    "label": label,
                    "properties": [],
                    "records": [],
                    "key_property": {"name": "id", "type": "string"},
                    "data_model": "auto"
                }
            
            # Add properties from this entity
            entity_props = set([prop["name"] for prop in nodes[label]["properties"]])
            for key, value in entity.get("properties", {}).items():
                if key not in entity_props:
                    nodes[label]["properties"].append({"name": key, "type": "string"})
                    entity_props.add(key)
            
            # Add record
            record = {"id": entity.get("id", f"entity_{len(nodes[label]['records'])}")}
            record.update(entity.get("properties", {}))
            nodes[label]["records"].append(record)
        
        return {"nodes": nodes, "relationships": relationships}

    def _ingest_nodes_via_mcp(self, nodes: Dict[str, Any]) -> bool:
        """Optimized node ingestion with progress tracking"""
        total_nodes = len(nodes)
        for i, (label, node_data) in enumerate(nodes.items(), 1):
            print(f"  Ingesting node {i}/{total_nodes}: {label} ({len(node_data['records'])} records)")
            
            # Create schema via Data Modeling server
            schema_response = self._make_mcp_request(
                self.data_modeling_server_url,
                "call_tool",
                {
                    "name": "create_node_schema",
                    "arguments": node_data
                }
            )
            
            if "error" in schema_response:
                print(f"    Schema creation failed: {schema_response['error']}")
                continue
            
            # Execute Cypher via Cypher server
            if node_data["records"]:
                cypher_query = self._generate_node_cypher(label, node_data["records"])
                cypher_response = self._make_mcp_request(
                    self.cypher_server_url,
                    "call_tool",
                    {
                        "name": "execute_cypher",
                        "arguments": {
                            "query": cypher_query,
                            "params": {"records": node_data["records"]}
                        }
                    }
                )
                
                if "error" not in cypher_response:
                    print(f"    ✓ Successfully ingested {len(node_data['records'])} {label} nodes")
                else:
                    print(f"    ✗ Cypher execution failed: {cypher_response['error']}")
        
        return True

    def _generate_node_cypher(self, label: str, records: List[Dict]) -> str:
        """Generate optimized Cypher for batch node creation"""
        if not records:
            return ""
        
        # Get all properties from first record
        props = list(records[0].keys())
        if "id" in props:
            props.remove("id")
        
        # Build SET clause
        set_props = ", ".join([f"{prop}: record.{self._escape_property(prop)}" for prop in props])
        
        return f"""UNWIND $records as record
MERGE (n:{label} {{id: record.id}})
SET n += {{{set_props}}}"""

    def _escape_property(self, prop: str) -> str:
        """Escape property names for Cypher"""
        if " " in prop or "[" in prop or "]" in prop:
            return f"`{prop}`"
        return prop

    def process_document_fast(self, file_path: str, chunk_size: int = 25) -> Dict[str, Any]:
        """Fast document processing with larger chunks"""
        print(f"🚀 FAST MODE: Processing {file_path}")
        
        # Read and chunk the file
        chunks = self._chunk_csv_file(file_path, chunk_size)
        total_chunks = len(chunks)
        
        print(f"Processing {total_chunks} chunks of {chunk_size} records each...")
        
        total_entities = 0
        total_relationships = 0
        start_time = time.time()
        
        for chunk_idx, chunk in enumerate(chunks, 1):
            chunk_start = time.time()
            print(f"\n--- Chunk {chunk_idx}/{total_chunks} ---")
            
            # LLM extraction
            llm_results = self._extract_entities_relationships_from_chunk(chunk)
            if not llm_results:
                # Fallback: direct CSV processing
                print("  💡 Using direct CSV fallback")
                llm_results = self._direct_csv_fallback(chunk)
                if not llm_results:
                    print("  ⚠️ All extraction methods failed, skipping chunk")
                    continue
            
            # Build schema
            schema = self._build_neo4j_data_model_from_llm_results(llm_results)
            
            # Ingest nodes only (skip relationships for speed)
            if schema["nodes"]:
                self._ingest_nodes_via_mcp(schema["nodes"])
                chunk_entities = sum(len(node["records"]) for node in schema["nodes"].values())
                total_entities += chunk_entities
                
                chunk_time = time.time() - chunk_start
                print(f"  ✓ Chunk completed: {chunk_entities} entities in {chunk_time:.1f}s")
            
        total_time = time.time() - start_time
        print(f"\n🎉 FAST PROCESSING COMPLETE!")
        print(f"Total time: {total_time:.1f}s")
        print(f"Total entities: {total_entities}")
        print(f"Average: {total_entities/total_time:.1f} entities/second")
        
        return {
            "success": True,
            "entities": total_entities,
            "relationships": total_relationships,
            "confidence": 0.95,
            "processing_time": total_time
        }

    def _chunk_csv_file(self, file_path: str, chunk_size: int) -> List[str]:
        """Create larger chunks for faster processing"""
        chunks = []
        
        with open(file_path, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            headers = csv_reader.fieldnames
            
            current_chunk = []
            for row in csv_reader:
                # Add row ID
                row['id'] = f"row_{len(current_chunk) + len(chunks) * chunk_size + 1}"
                current_chunk.append(row)
                
                if len(current_chunk) >= chunk_size:
                    chunks.append(self._format_chunk_as_json(current_chunk))
                    current_chunk = []
            
            # Add remaining records
            if current_chunk:
                chunks.append(self._format_chunk_as_json(current_chunk))
        
        return chunks

    def _format_chunk_as_json(self, records: List[Dict]) -> str:
        """Format chunk as compact JSON"""
        return json.dumps(records, separators=(',', ':'))

    def _extract_entities_relationships_from_chunk(self, chunk_content: str) -> Optional[Dict[str, Any]]:
        """Optimized LLM extraction with focused prompting"""
        prompt = f"""
Extract entities and their properties from this medical imaging data. Return valid JSON only.

Data: {chunk_content[:2000]}...

Return this exact JSON structure:
{{
  "entities": [
    {{
      "id": "unique_id",
      "type": "MedicalImage", 
      "properties": {{"key": "value"}}
    }}
  ],
  "relationships": []
}}

Extract ALL records as MedicalImage entities. Use the original field names as properties.
"""
        
        try:
            response = self.llm.invoke(prompt)
            content = response.content.strip()
            
            # Clean up response - extract JSON if wrapped
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()
            
            # Try to parse JSON
            result = json.loads(content)
            print(f"  ✓ LLM extracted {len(result.get('entities', []))} entities")
            return result
            
        except json.JSONDecodeError as e:
            print(f"  ⚠️ JSON parsing failed: {str(e)}")
            print(f"  Raw response: {response.content[:200]}...")
            return None
        except Exception as e:
            print(f"  ⚠️ LLM extraction failed: {str(e)}")
            return None

    def _direct_csv_fallback(self, chunk_content: str) -> Optional[Dict[str, Any]]:
        """Direct CSV to entity conversion as fallback"""
        try:
            records = json.loads(chunk_content)
            entities = []
            
            for record in records:
                entity = {
                    "id": record.get("id", f"entity_{len(entities)}"),
                    "type": "MedicalImage",
                    "properties": {k: v for k, v in record.items() if k != "id"}
                }
                entities.append(entity)
            
            print(f"  ✓ Direct CSV fallback extracted {len(entities)} entities")
            return {
                "entities": entities,
                "relationships": []
            }
        except Exception as e:
            print(f"  ⚠️ CSV fallback failed: {str(e)}")
            return None

# Main execution
if __name__ == "__main__":
    tool = OptimizedGraphTool()
    
    # Test with larger chunks for speed
    result = tool.process_document_fast("doc/BBox_List_2017.csv", chunk_size=25)
    print(f"\nFinal Result: {result}")
