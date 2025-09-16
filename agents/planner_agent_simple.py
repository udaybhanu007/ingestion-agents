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
try:
    from openai import AzureOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    AzureOpenAI = None

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
    create_ingestion_step, format_classification_result
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
        
        self.logger.info(f"PlannerAgent initialized - OpenAI: {OPENAI_AVAILABLE}")
    
    def _load_environment(self, env_file: str):
        """Load environment variables from file."""
        env_files_to_try = [env_file, ".env.dev", ".env"]
        env_file_loaded = None
        
        for env_path in env_files_to_try:
            if os.path.exists(env_path):
                load_dotenv(env_path)
                env_file_loaded = env_path
                self.logger.info(f"Environment file loaded: {env_path}")
                break
        
        if not env_file_loaded:
            self.logger.warning("No environment file found, using system environment")
    
    def _initialize_llm(self):
        """Initialize Azure OpenAI client."""
        if not OPENAI_AVAILABLE:
            self.logger.error("OpenAI package not available. Please install: pip install openai")
            self.azure_openai = None
            return
        
        try:
            # Get Azure OpenAI credentials from environment
            api_key = os.getenv('AZURE_OPENAI_API_KEY')
            endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
            api_version = os.getenv('AZURE_OPENAI_API_VERSION', '2024-02-15-preview')
            
            if not all([api_key, endpoint]):
                self.logger.error("Azure OpenAI credentials not found in environment variables")
                self.azure_openai = None
                return
            
            # Initialize Azure OpenAI client
            self.azure_openai = AzureOpenAI(
                api_key=api_key,
                azure_endpoint=endpoint,
                api_version=api_version
            )
            
            self.logger.info("Azure OpenAI client initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Azure OpenAI: {e}")
            self.azure_openai = None
    
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
                content = await connector.get_content_async(doc_uri)
                self.react_logger.observation(
                    f"Retrieved document content, length: {len(content)} characters"
                )
            except Exception as e:
                self.logger.error(f"Failed to retrieve document content: {e}")
                self.react_logger.observation(f"Document retrieval failed: {e}")
                # Continue with empty content
                content = ""
            
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
                structure_analysis = analyze_document_structure(content)
                self.react_logger.observation(
                    f"Document structure analysis: {structure_analysis}"
                )
            except Exception as e:
                self.logger.error(f"Document structure analysis failed: {e}")
                structure_analysis = {"error": str(e)}
            
            # Extract entities
            try:
                entities = extract_entities(content)
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
            
            # Return a fallback plan
            return self._create_fallback_plan(doc_uri, metadata)
    
    async def _create_plan_with_llm(self, doc_uri: str, content: str, metadata: Dict, 
                                  structure_analysis: Dict, entities: List) -> Dict[str, Any]:
        """Create ingestion plan using Azure OpenAI LLM."""
        
        if not self.azure_openai:
            self.logger.warning("Azure OpenAI not available, creating basic plan")
            return self._create_fallback_plan(doc_uri, metadata)
        
        try:
            # Prepare prompt
            prompt = self._build_classification_prompt(
                doc_uri, content, metadata, structure_analysis, entities
            )
            
            self.react_logger.action("Sending classification request to Azure OpenAI")
            
            # Make LLM call
            response = self.azure_openai.chat.completions.create(
                model=os.getenv('AZURE_OPENAI_MODEL_NAME', 'gpt-4'),
                messages=[
                    {"role": "system", "content": "You are an expert document classifier and ingestion planner."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=2000,
                temperature=0.1
            )
            
            response_text = response.choices[0].message.content
            self.react_logger.observation(f"Received LLM response, length: {len(response_text)}")
            
            # Parse JSON response
            try:
                plan = json.loads(response_text)
                self.react_logger.observation("Successfully parsed LLM response as JSON")
                return plan
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse LLM response as JSON: {e}")
                self.react_logger.observation(f"JSON parsing failed: {e}")
                return self._create_fallback_plan(doc_uri, metadata)
                
        except Exception as e:
            self.logger.error(f"LLM classification failed: {e}")
            self.react_logger.observation(f"LLM classification failed: {e}")
            return self._create_fallback_plan(doc_uri, metadata)
    
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
        
        Create a JSON ingestion plan with this schema:
        {create_ingestion_plan_schema()}
        
        Classify the document and determine appropriate processing steps.
        Return only valid JSON.
        """
        
        return prompt
    
    def _create_fallback_plan(self, doc_uri: str, metadata: Dict) -> Dict[str, Any]:
        """Create a basic fallback ingestion plan."""
        plan_id = generate_uuid()
        
        # Determine document type
        doc_type = metadata.get('document_type', 'unknown')
        
        # Create basic steps
        steps = []
        
        # Step 1: Content extraction
        steps.append(create_ingestion_step(
            step_id=f"extract-{generate_uuid()[:8]}",
            step_type="content_extraction",
            description=f"Extract content from {doc_type} document",
            config={
                "extraction_method": "default",
                "preserve_formatting": True
            }
        ))
        
        # Step 2: Text chunking
        chunk_size = metadata.get('processing_options', {}).get('chunk_size', 1024)
        steps.append(create_ingestion_step(
            step_id=f"chunk-{generate_uuid()[:8]}",
            step_type="text_chunking", 
            description="Split document into semantic chunks",
            config={
                "chunk_size": chunk_size,
                "overlap": 200,
                "method": "semantic"
            }
        ))
        
        # Step 3: Vector generation
        steps.append(create_ingestion_step(
            step_id=f"vector-{generate_uuid()[:8]}",
            step_type="vector_generation",
            description="Generate embeddings for document chunks",
            config={
                "embedding_model": "text-embedding-ada-002",
                "batch_size": 100
            }
        ))
        
        # Create plan
        plan = {
            "plan_id": plan_id,
            "document_uri": doc_uri,
            "document_classification": {
                "primary_type": doc_type,
                "content_structure": "unknown",
                "estimated_complexity": "medium",
                "confidence_score": 0.5
            },
            "processing_strategy": "default",
            "steps": steps,
            "estimated_duration": len(steps) * 30,
            "resource_requirements": {
                "memory_mb": 512,
                "cpu_cores": 1,
                "storage_mb": 100
            },
            "metadata": metadata,
            "created_at": datetime.utcnow().isoformat(),
            "created_by": "planner_agent_fallback"
        }
        
        self.logger.info(f"Created fallback plan with {len(steps)} steps")
        return plan


# Export the class
__all__ = ['PlannerAgent']
