"""
Simplified Planner Agent for Enterprise RAG Ingestion Pipeline

This agent uses Azure OpenAI LLM to analyze documents and create intelligent ingestion plans.
It acts as an expert Document Classifier & Ingest Planner that:
1. Retrieves documents from Azure, Box, and Confluence connectors
2. Uses LLM-powered analysis to classify documents based on content structure and entity relationships
3. Generates JSON ingestion plans following the specified schema
"""

import asyncio
import json
import os
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

# Add the parent directory to the path to import local modules
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

# Azure OpenAI imports
from openai import AzureOpenAI

from dotenv import load_dotenv

# Import connectors
from connector.azure import AzureConnector
from connector.box import BoxConnector
from connector.confluence_mcp import ConfluenceMCPConnector

# Import common functions
from utils.common_function import (
    generate_uuid, clean_text, extract_entities,
    analyze_document_structure, extract_metadata_from_uri,
    validate_json_plan, save_json, create_ingestion_plan_schema,
    get_ingestion_plan_schema_example, create_ingestion_step, format_classification_result
)

# Import simple logging
from config.simple_logger import get_agent_logger, get_react_logger


class PlannerAgent:
    """
    LLM-powered Planner Agent for document classification and ingestion planning.
    """
    
    def __init__(self, env_file: str = ".env.dev"):
        """
        Initialize the Planner Agent with Azure OpenAI client.
        Connectors are lazy-loaded based on doc_uri.
        
        Args:
            env_file: Path to environment file for credentials
        """
        # Initialize simple logging
        self.logger = get_agent_logger("planner")
        self.react_logger = None  # Will be initialized per execution
            
        self.env_file = env_file
        
        # Load environment variables
        self._load_environment(env_file)
        
        # Initialize Azure OpenAI client
        self._initialize_llm()
        
        # Lazy-loaded connectors (initialized only when needed)
        self._connectors = {}
        
        # Load guidelines
        self.guidelines = self._load_guidelines()
        
        self.logger.info("PlannerAgent initialized with Azure OpenAI")
    
    def _load_environment(self, env_file: str):
        """Load environment variables from specified file only (no fallback)."""
        if os.path.exists(env_file):
            load_dotenv(env_file)
            self.logger.info(f"Environment file loaded: {env_file}")
        else:
            raise FileNotFoundError(f"Required environment file not found: {env_file}")
    
    def _initialize_llm(self):
        """Initialize Azure OpenAI client."""
        # Get Azure OpenAI credentials from environment
        api_key = os.getenv('AZURE_OPENAI_API_KEY')
        endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
        api_version = os.getenv('AZURE_OPENAI_API_VERSION')
        
        if not all([api_key, endpoint, api_version]):
            missing = []
            if not api_key:
                missing.append('AZURE_OPENAI_API_KEY')
            if not endpoint:
                missing.append('AZURE_OPENAI_ENDPOINT')
            if not api_version:
                missing.append('AZURE_OPENAI_API_VERSION')
            raise ValueError(f"Missing required Azure OpenAI environment variables: {', '.join(missing)}")
        
        # Initialize Azure OpenAI client
        self.azure_openai = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version=api_version
        )
        
        self.logger.info("Azure OpenAI client initialized successfully")
    
    def _load_guidelines(self) -> str:
        """Load processing guidelines from doc/guidelines.md."""
        try:
            guidelines_path = Path(__file__).parent.parent / "doc" / "guidelines.md"
            if guidelines_path.exists():
                with open(guidelines_path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                self.logger.warning(f"Guidelines file not found at {guidelines_path}")
                return ""
        except Exception as e:
            self.logger.error(f"Error loading guidelines: {e}")
            return ""
    
    def _get_connector(self, connector_type: str):
        """Get or create connector instance."""
        if connector_type not in self._connectors:
            try:
                if connector_type == "azure":
                    self._connectors["azure"] = AzureConnector()
                    self.logger.info("Azure connector initialized successfully")
                elif connector_type == "box":
                    self._connectors["box"] = BoxConnector()  
                    self.logger.info("Box connector initialized successfully")
                elif connector_type == "confluence":
                    self._connectors["confluence"] = ConfluenceMCPConnector()
                    self.logger.info("Confluence MCP connector initialized successfully")
                else:
                    self.logger.error(f"Unknown connector type: {connector_type}")
                    return None
                    
            except Exception as e:
                self.logger.error(f"Failed to initialize {connector_type} connector: {e}")
                return None
        
        return self._connectors.get(connector_type)
    
    async def create_ingestion_plan_async(self, doc_uri: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Asynchronous method to create an ingestion plan for a document.
        
        Args:
            doc_uri: Document URI (e.g., azure://container/filename)
            metadata: Document metadata
            
        Returns:
            Ingestion plan dictionary
        """
        try:
            # Initialize ReAct logger for this execution
            execution_id = generate_uuid()
            self.react_logger = get_react_logger(f"planner-{execution_id}")
            
            self.react_logger.reasoning(
                f"Starting ingestion plan creation for document: {doc_uri}"
            )
            
            # Parse document URI to determine connector type
            if doc_uri.startswith("azure://"):
                connector_type = "azure"
            elif doc_uri.startswith("box://"):
                connector_type = "box"
            elif doc_uri.startswith("confluence://"):
                connector_type = "confluence"
            else:
                raise ValueError(f"Unsupported document URI scheme: {doc_uri}")
            
            self.react_logger.reasoning(
                f"Determined connector type: {connector_type} for URI: {doc_uri}"
            )
            
            # Get appropriate connector
            connector = self._get_connector(connector_type)
            if not connector:
                raise Exception(f"Failed to initialize {connector_type} connector")
            
            self.react_logger.action(
                f"Initialized {connector_type} connector, retrieving document content"
            )
            
            # Get document content
            try:
                content = await connector.get_document_content(doc_uri)
                if content and 'content' in content:
                    content_text = content['content']
                    self.react_logger.observation(
                        f"Retrieved document content, length: {len(content_text)} characters"
                    )
                else:
                    content_text = ""
                    self.react_logger.observation("Document retrieval returned empty content")
            except Exception as e:
                self.logger.error(f"Failed to retrieve document content: {e}")
                self.react_logger.observation(f"Document retrieval failed: {e}")
                # Continue with empty content
                content_text = ""
            
            # Extract document metadata
            try:
                extracted_metadata = extract_metadata_from_uri(doc_uri)
                self.react_logger.observation(
                    f"Extracted metadata: {extracted_metadata}"
                )
            except Exception as e:
                self.logger.error(f"Failed to extract metadata: {e}")
                extracted_metadata = {}
            
            # Combine metadata
            combined_metadata = {**metadata, **extracted_metadata}
            
            # Analyze document structure  
            try:
                structure_analysis = analyze_document_structure(content_text)
                self.react_logger.observation(
                    f"Document structure analysis: {structure_analysis}"
                )
            except Exception as e:
                self.logger.error(f"Document structure analysis failed: {e}")
                structure_analysis = {"error": str(e)}
            
            # Extract entities
            try:
                entities = extract_entities(content_text)
                self.react_logger.observation(
                    f"Extracted {len(entities)} entities from document"
                )
            except Exception as e:
                self.logger.error(f"Entity extraction failed: {e}")
                entities = []
            
            # Create plan using LLM
            plan = await self._create_plan_with_llm(
                doc_uri, content, combined_metadata, structure_analysis, entities
            )
            # Always set a valid UUID for plan_id
            plan['plan_id'] = generate_uuid()
            
            # Add context with the document content for execution
            plan['context'] = {
                'content': content_text,
                'document_uri': doc_uri,
                'metadata': combined_metadata
            }
            
            # Patch: Add step_type to each step based on tool
            for step in plan.get('steps', []):
                tool = step.get('tool')
                if tool == 'vector_ingestion':
                    step['step_type'] = 'vector_generation'
                elif tool == 'graph_ingestion':
                    step['step_type'] = 'graph_ingestion'
                else:
                    step['step_type'] = 'unknown'
            # Validate plan
            try:
                validation_result = validate_json_plan(plan)
                if not validation_result["valid"]:
                    self.logger.warning(f"Plan validation failed: {validation_result['errors']}")
                    # Continue with the plan anyway
                self.react_logger.observation(
                    f"Plan validation: {validation_result['valid']}"
                )
            except Exception as e:
                self.logger.error(f"Plan validation error: {e}")
            self.react_logger.observation(
                f"Ingestion plan created successfully with {len(plan.get('steps', []))} steps"
            )
            return plan
            
        except Exception as e:
            self.logger.error(f"Error creating ingestion plan: {e}")
            if self.react_logger:
                self.react_logger.observation(f"Plan creation failed: {e}")
            
            # Return a simple error plan instead of fallback
            raise Exception(f"Error creating ingestion plan: {e}")
    
    async def _create_plan_with_llm(self, doc_uri: str, content: str, metadata: Dict, 
                                  structure_analysis: Dict, entities: List) -> Dict[str, Any]:
        """Create ingestion plan using Azure OpenAI LLM."""
        
        try:
            # Prepare prompt
            prompt = self._build_classification_prompt(
                doc_uri, content, metadata, structure_analysis, entities
            )
            
            self.react_logger.action("Sending classification request to Azure OpenAI")
            
            # Make LLM call - Use deployment name, not model name for Azure OpenAI
            deployment_name = os.getenv('AZURE_OPENAI_DEPLOYMENT')
            if not deployment_name:
                raise ValueError("AZURE_OPENAI_DEPLOYMENT environment variable is required")
                
            self.logger.info(f"Making Azure OpenAI call with deployment: {deployment_name}")
            self.logger.info(f"Azure OpenAI endpoint: {os.getenv('AZURE_OPENAI_ENDPOINT', 'NOT_SET')}")
            self.logger.info(f"Azure OpenAI API version: {os.getenv('AZURE_OPENAI_API_VERSION', 'NOT_SET')}")
            
            # Test Azure OpenAI connection with a simple call first
            try:
                test_response = self.azure_openai.chat.completions.create(
                    model=deployment_name,
                    messages=[{"role": "user", "content": "Say 'test'"}],
                    max_tokens=10,
                    temperature=0
                )
                test_content = test_response.choices[0].message.content
                self.logger.info(f"Azure OpenAI connection test successful: {test_content}")
            except Exception as test_e:
                self.logger.error(f"Azure OpenAI connection test failed: {test_e}")
                raise Exception(f"Azure OpenAI connection failed: {test_e}")
            
            # Log prompt for debugging
            self.logger.debug(f"Sending prompt to LLM (length: {len(prompt)})")
            
            response = self.azure_openai.chat.completions.create(
                model=deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert document classifier and ingestion planner. Always respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=2000,
                temperature=0.1
            )

            response_text = response.choices[0].message.content
            self.logger.info(f"Received LLM response, length: {len(response_text) if response_text else 0}")
            self.react_logger.observation(f"Received LLM response, length: {len(response_text) if response_text else 0}")
            
            # Log the full response object for debugging
            self.logger.debug(f"Full response object: {response}")
            
            # Check if response is empty
            if not response_text or response_text.strip() == "":
                self.logger.error("LLM returned empty response")
                self.logger.error(f"Response object: {response}")
                self.react_logger.observation("LLM returned empty response")
                raise Exception("LLM returned empty response")
            
            # Log the actual response for debugging
            self.logger.debug(f"LLM response content: {response_text}")

            # Extract JSON from markdown code blocks if present
            clean_response = self._extract_json_from_response(response_text)
            self.logger.debug(f"Cleaned response: {clean_response}")

            # Parse JSON response
            try:
                plan = json.loads(clean_response)
                self.react_logger.observation("Successfully parsed LLM response as JSON")
                return plan
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse LLM response as JSON: {e}")
                self.logger.error(f"Response content: '{response_text}'")
                self.logger.error(f"Cleaned content: '{clean_response}'")
                self.react_logger.observation(f"JSON parsing failed: {e}")
                raise Exception(f"Invalid JSON response from LLM: {e}")
                
        except Exception as e:
            self.logger.error(f"LLM classification failed: {e}")
            self.react_logger.observation(f"LLM classification failed: {e}")
            raise Exception(f"LLM plan creation failed: {e}")
    
    def _extract_json_from_response(self, response_text: str) -> str:
        """Extract JSON content from markdown code blocks or return as-is."""
        if not response_text:
            return response_text
            
        # Remove leading/trailing whitespace
        response_text = response_text.strip()
        
        # Check if response is wrapped in markdown code blocks
        if response_text.startswith('```json'):
            # Extract content between ```json and ```
            start = response_text.find('```json') + 7  # 7 = len('```json')
            end = response_text.find('```', start)
            if end != -1:
                json_content = response_text[start:end].strip()
                return json_content
        elif response_text.startswith('```'):
            # Extract content between ``` and ```
            start = response_text.find('```') + 3
            end = response_text.find('```', start)
            if end != -1:
                json_content = response_text[start:end].strip()
                return json_content
        
        # If no markdown blocks found, return original response
        return response_text
    
    def _build_classification_prompt(self, doc_uri: str, content: str, metadata: Dict,
                                   structure_analysis: Dict, entities: List) -> str:
        """Build prompt for document classification."""
        
        # Truncate content if too long
        max_content_length = 3000
        if len(content) > max_content_length:
            content = content[:max_content_length] + "... [truncated]"
        
        prompt = f"""
        Analyze this document and create an ingestion plan.
        
        Document URI: {doc_uri}
        Document Type: {metadata.get('document_type', 'unknown')}
        Content Type: {metadata.get('content_type', 'unknown')}
        
        Document Content:
        {content}
        
        Structure Analysis: {structure_analysis}
        Entities: {entities}
        
        Guidelines:
        {self.guidelines}
        
        Based on your analysis, classify the document as:
        - VECTOR_STORE_ONLY: Use "vector_ingestion" tool (single step with task_id "1")
        - KNOWLEDGE_GRAPH_ONLY: Use "graph_ingestion" tool (single step with task_id "1")  
        - DUAL_INGESTION: Use both tools (two steps: task_id "1" for vector_ingestion, task_id "2" for graph_ingestion with depends_on ["1"])
        
        Create a JSON ingestion plan with this schema:
        {get_ingestion_plan_schema_example()}
        
        Examples:
        
        For VECTOR_STORE_ONLY:
        {{
          "plan_id": "uuid",
          "steps": [
            {{
              "task_id": "1",
              "tool": "vector_ingestion",
              "args": {{ "doc_uri": "{doc_uri}" }},
              "depends_on": []
            }}
          ]
        }}
        
        For KNOWLEDGE_GRAPH_ONLY:
        {{
          "plan_id": "uuid",
          "steps": [
            {{
              "task_id": "1",
              "tool": "graph_ingestion",
              "args": {{ "doc_uri": "{doc_uri}" }},
              "depends_on": []
            }}
          ]
        }}
        
        For DUAL_INGESTION:
        {{
          "plan_id": "uuid", 
          "steps": [
            {{
              "task_id": "1",
              "tool": "vector_ingestion",
              "args": {{ "doc_uri": "{doc_uri}" }},
              "depends_on": []
            }},
            {{
              "task_id": "2",
              "tool": "graph_ingestion", 
              "args": {{ "doc_uri": "{doc_uri}" }},
              "depends_on": ["1"]
            }}
          ]
        }}
        
        Important: 
        - Use EXACTLY "vector_ingestion" or "graph_ingestion" as tool names (no other tool names allowed)
        - For DUAL_INGESTION, create TWO separate steps: task_id "1" for vector_ingestion, task_id "2" for graph_ingestion  
        - graph_ingestion must depend on vector_ingestion (depends_on: ["1"])
        - Do NOT include document content in the args
        - Only include doc_uri in the args
        - Use simple task_id values: "1", "2", etc.
        - Return only valid JSON without any explanation or markdown formatting
        """
        
        return prompt
    
# Export the class
__all__ = ['PlannerAgent']
