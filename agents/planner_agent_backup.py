"""
Planner Agent for Enterprise RAG Ingestion Pipeline

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
    generate_uuid, setup_logging, clean_text, extract_entities,
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
                if STRUCTURED_LOGGING_AVAILABLE:
                    self.logger.info("Environment file loaded", 
                                   file_path=env_path,
                                   component="planner_agent")
                else:
                    self.logger.info(f"Environment file loaded: {env_path}")
                break
        
        if not env_file_loaded:
            self.logger.warning("No environment file found, using system environment",
                              component="planner_agent")
    
    def _initialize_llm(self):
        """Initialize Azure OpenAI client."""
        if not OPENAI_AVAILABLE:
            self.logger.error("OpenAI package not available. Please install: pip install openai")
            self.azure_openai = None
            return
        
        try:
            # Get Azure OpenAI configuration
            api_key = os.getenv("AZURE_OPENAI_API_KEY")
            endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
            api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01")
            
            if not api_key or not endpoint:
                self.logger.error("Azure OpenAI credentials not found in environment variables")
                self.azure_openai = None
                return
            
            self.azure_openai = AzureOpenAI(
                api_key=api_key,
                api_version=api_version,
                azure_endpoint=endpoint
            )
            self.deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4")
            self.logger.info("Azure OpenAI client initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Azure OpenAI: {e}")
            self.azure_openai = None
    
    def _get_connector_type_from_uri(self, doc_uri: str) -> str:
        """Determine connector type from document URI."""
        if doc_uri.startswith("azure://"):
            return "azure"
        elif doc_uri.startswith("box://"):
            return "box"
        elif doc_uri.startswith("confluence://"):
            return "confluence"
        elif doc_uri.startswith("test://"):
            return "test"
        elif doc_uri.startswith("local://"):
            return "local"
        elif doc_uri.startswith("file://"):
            return "file"
        else:
            # Default fallback or determine from other patterns
            if "blob.core.windows.net" in doc_uri:
                return "azure"
            elif "box.com" in doc_uri:
                return "box"
            elif "confluence" in doc_uri.lower():
                return "confluence"
            else:
                raise ValueError(f"Unable to determine connector type for URI: {doc_uri}")
    
    async def _get_connector(self, doc_uri: str):
        """Get or initialize connector based on document URI (lazy loading)."""
        connector_type = self._get_connector_type_from_uri(doc_uri)
        
        # Handle test and local URIs with mock content
        if connector_type in ["test", "local", "file"]:
            return self._create_mock_connector(connector_type, doc_uri)
        
        # Return existing connector if already initialized
        if connector_type in self._connectors and self._connectors[connector_type] is not None:
            return self._connectors[connector_type]
        
        # Initialize connector based on type
        try:
            if connector_type == "azure":
                self._connectors['azure'] = AzureConnector(env_file=self.env_file)
                if hasattr(self._connectors['azure'], 'blob_service_client') and self._connectors['azure'].blob_service_client:
                    self.logger.info("Azure connector initialized successfully")
                    return self._connectors['azure']
                else:
                    self.logger.warning("Azure connector initialization failed")
                    self._connectors['azure'] = None
                    
            elif connector_type == "box":
                self._connectors['box'] = BoxConnector(env_file=self.env_file)
                if hasattr(self._connectors['box'], 'is_available') and self._connectors['box'].is_available():
                    self.logger.info("Box connector initialized successfully")
                    return self._connectors['box']
                else:
                    self.logger.warning("Box connector initialization failed")
                    self._connectors['box'] = None
                    
            elif connector_type == "confluence":
                self._connectors['confluence'] = ConfluenceMCPConnector(env_file=self.env_file)
                self.logger.info("Confluence MCP connector initialized successfully")
                return self._connectors['confluence']
                
        except Exception as e:
            self.logger.error(f"Failed to initialize {connector_type} connector: {e}")
            self._connectors[connector_type] = None
        
        return None

    def _create_mock_connector(self, connector_type: str, doc_uri: str):
        """Create a mock connector for testing purposes."""
        class MockConnector:
            def __init__(self, connector_type, doc_uri):
                self.connector_type = connector_type
                self.doc_uri = doc_uri
            
            async def get_document_content(self, uri):
                """Return mock content for testing."""
                return {
                    "content": f"Mock content for {self.connector_type} connector testing.\n"
                             f"URI: {uri}\n"
                             f"This is sample content used for testing the ingestion pipeline.\n"
                             f"It includes multiple sentences for proper chunking and processing.\n"
                             f"Created at: {datetime.now().isoformat()}",
                    "metadata": {
                        "connector_type": self.connector_type,
                        "doc_uri": uri,
                        "content_type": "text/plain",
                        "size": 250,
                        "is_mock": True,
                        "created_at": datetime.now().isoformat()
                    }
                }
        
        return MockConnector(connector_type, doc_uri)

    async def create_ingestion_plan_async(self, doc_uri: str, metadata: Optional[Dict[str, Any]] = None, 
                                        content: Optional[str] = None) -> Dict[str, Any]:
        """
        Create an ingestion plan for a given document URI asynchronously.
        This is the main async API entry point for creating ingestion plans.
        
        Args:
            doc_uri: Document URI to create plan for
            metadata: Optional metadata for the document
            content: Optional content if already fetched
            
        Returns:
            Dict containing the ingestion plan
        """
        # Initialize ReAct logger for this execution
        execution_id = generate_uuid()
        if STRUCTURED_LOGGING_AVAILABLE:
            self.react_logger = get_react_logger("planner", execution_id)
            self.react_logger.reasoning(
                "Starting ingestion plan creation", 
                {
                    "doc_uri": doc_uri,
                    "has_metadata": metadata is not None,
                    "has_content": content is not None
                }
            )
        
        try:
            fetched_content = content
            
            # REASONING: Determine content acquisition strategy
            if STRUCTURED_LOGGING_AVAILABLE:
                self.react_logger.reasoning(
                    "Analyzing content availability and acquisition strategy",
                    {"content_provided": content is not None}
                )
            
            # Content should always be provided or fetched
            if fetched_content:
                if isinstance(fetched_content, bytes):
                    fetched_content = fetched_content.decode('utf-8', errors='ignore')
                
                # ACTION: Use LLM classification
                if STRUCTURED_LOGGING_AVAILABLE:
                    self.react_logger.action(
                        "classify_document_with_llm",
                        {"doc_uri": doc_uri, "content_length": len(fetched_content)}
                    )
                
                classification_result = await self.classify_document_with_llm(fetched_content, doc_uri, metadata)
                
                # OBSERVATION: Document classification completed
                if STRUCTURED_LOGGING_AVAILABLE:
                    self.react_logger.observation(
                        classification_result,
                        success=True
                    )
            else:
                # ACTION: Fetch content from connector
                if STRUCTURED_LOGGING_AVAILABLE:
                    self.react_logger.action("fetch_content_from_connector", {"doc_uri": doc_uri})
                
                connector = await self._get_connector(doc_uri)
                if connector:
                    fetched_content = await self._fetch_content_async(connector, doc_uri)
                    if fetched_content:
                        # OBSERVATION: Content fetched successfully
                        if STRUCTURED_LOGGING_AVAILABLE:
                            self.react_logger.observation(
                                {"content_length": len(fetched_content), "connector_type": type(connector).__name__},
                                success=True
                            )
                        
                        classification_result = await self.classify_document_with_llm(fetched_content, doc_uri, metadata)
                    else:
                        error_msg = f"Unable to fetch content for {doc_uri}"
                        if STRUCTURED_LOGGING_AVAILABLE:
                            self.react_logger.observation(None, success=False, error=error_msg)
                        raise ValueError(error_msg)
                else:
                    error_msg = f"No connector available for {doc_uri}"
                    if STRUCTURED_LOGGING_AVAILABLE:
                        self.react_logger.observation(None, success=False, error=error_msg)
                    raise ValueError(error_msg)
            
            # PLANNING: Generate ingestion plan based on classification
            if STRUCTURED_LOGGING_AVAILABLE:
                self.react_logger.planning(
                    classification_result,
                    "Generating ingestion plan based on document classification and analysis"
                )
            
            # ACTION: Generate the ingestion plan with content
            if STRUCTURED_LOGGING_AVAILABLE:
                self.react_logger.action("generate_ingestion_plan", {"classification": classification_result})
            
            plan = self.generate_ingestion_plan(classification_result, fetched_content)
            
            # OBSERVATION: Plan generation completed
            if STRUCTURED_LOGGING_AVAILABLE:
                self.react_logger.observation(
                    {"plan_id": plan.get("plan_id"), "steps_count": len(plan.get("steps", []))},
                    success=True
                )
            
            if STRUCTURED_LOGGING_AVAILABLE:
                self.logger.info("Ingestion plan created successfully",
                               doc_uri=doc_uri,
                               execution_id=execution_id,
                               plan_id=plan.get("plan_id"))
            else:
                self.logger.info(f"Ingestion plan created successfully - URI: {doc_uri}, Plan ID: {plan.get('plan_id')}")
            return plan
            
        except Exception as e:
            # ERROR: Log the error with full context
            if STRUCTURED_LOGGING_AVAILABLE and self.react_logger:
                self.react_logger.error(
                    e, 
                    {
                        "doc_uri": doc_uri,
                        "operation": "create_ingestion_plan",
                        "execution_id": execution_id
                    }
                )
            
            self.logger.error("Ingestion plan creation failed",
                            doc_uri=doc_uri,
                            error=str(e),
                            error_type=type(e).__name__,
                            execution_id=execution_id)
            # Re-raise the exception since content is mandatory
            raise ValueError(f"Ingestion plan creation failed for {doc_uri}: {str(e)}")

    async def _fetch_content_async(self, connector, doc_uri: str) -> Optional[str]:
        """Fetch content from connector asynchronously."""
        try:
            connector_type = self._get_connector_type_from_uri(doc_uri)
            
            # Handle mock connectors for testing
            if hasattr(connector, 'get_document_content'):
                result = await connector.get_document_content(doc_uri)
                if isinstance(result, dict) and 'content' in result:
                    return result['content']
                return str(result) if result else None
            
            if connector_type == "azure":
                # Use the unified get_document_content interface
                if hasattr(connector, 'get_document_content'):
                    result = await connector.get_document_content(doc_uri)
                    if isinstance(result, dict) and 'content' in result:
                        return result['content']
                else:
                    # Fallback to direct download_blob method
                    parts = doc_uri.replace("azure://", "").split("/", 1)
                    if len(parts) == 2:
                        container_name, blob_name = parts
                        content = connector.download_blob(container_name, blob_name)
                        return content if isinstance(content, str) else content.decode('utf-8', errors='ignore')
                    
            elif connector_type == "box":
                # Parse Box URI: box://file/file_id
                parts = doc_uri.replace("box://", "").split("/")
                if len(parts) >= 2 and parts[0] == "file":
                    file_id = parts[1]
                    download_result = connector.download_file(file_id)
                    if download_result and isinstance(download_result, str):
                        with open(download_result, 'r', encoding='utf-8', errors='ignore') as f:
                            return f.read()
                            
            elif connector_type == "confluence":
                # Parse Confluence URI: confluence://page/page_title
                parts = doc_uri.replace("confluence://", "").split("/", 1)
                if len(parts) == 2 and parts[0] == "page":
                    page_title = parts[1]
                    result = await connector.download_page_content(page_title)
                    if result.get('success'):
                        return result.get('content', '')
                        
        except Exception as e:
            self.logger.error(f"Failed to fetch content for {doc_uri}: {e}")
            
        return None

    def create_ingestion_plan(self, doc_uri: str, metadata: Optional[Dict[str, Any]] = None, 
                             content: Optional[str] = None) -> Dict[str, Any]:
        """
        Create an ingestion plan for a given document URI.
        This is a sync wrapper for the async method.
        
        Args:
            doc_uri: Document URI to create plan for
            metadata: Optional metadata for the document
            content: Optional content if already fetched
            
        Returns:
            Dict containing the ingestion plan
        """
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        return loop.run_until_complete(
            self.create_ingestion_plan_async(doc_uri, metadata, content)
        )
    
    def _load_guidelines(self) -> str:
        """Load classification guidelines from file."""
        try:
            guidelines_path = "doc/guidelines.md"
            with open(guidelines_path, 'r', encoding='utf-8') as f:
                guidelines = f.read()
            self.logger.info("Guidelines loaded successfully")
            return guidelines
        except Exception as e:
            self.logger.error(f"Failed to load guidelines: {e}")
            return ""
    
    def _log_classification_details(self, classification_result: Dict[str, Any], doc_uri: str) -> None:
        """
        Log detailed classification information to console only.
        
        Args:
            classification_result: Result from classify_document_with_llm()
            doc_uri: Document URI
        """
        # Log to console with detailed formatting
        self.logger.info("=" * 80)
        self.logger.info("DOCUMENT CLASSIFICATION COMPLETE")
        self.logger.info("=" * 80)
        self.logger.info(f"Document URI: {doc_uri}")
        self.logger.info(f"Classification: {classification_result['classification']}")
        self.logger.info(f"Document Type: {classification_result['document_type']}")
        self.logger.info(f"Reasoning: {classification_result['reasoning']}")
        if classification_result.get('key_indicators'):
            self.logger.info(f"Key Indicators: {', '.join(classification_result['key_indicators'])}")
        self.logger.info("=" * 80)
    
    def _log_ingestion_plan(self, plan: Dict[str, Any], doc_uri: str) -> None:
        """
        Log ingestion plan details to console only.
        
        Args:
            plan: Generated ingestion plan
            doc_uri: Document URI
        """
        plan_id = plan.get('plan_id', 'unknown')
        
        self.logger.info("=" * 60)
        self.logger.info("INGESTION PLAN GENERATED")
        self.logger.info("=" * 60)
        self.logger.info(f"Plan ID: {plan_id}")
        self.logger.info(f"Document URI: {doc_uri}")
        self.logger.info(f"Number of Steps: {len(plan.get('steps', []))}")
        
        # Log each step
        for i, step in enumerate(plan.get('steps', []), 1):
            self.logger.info(f"Step {i}: {step.get('tool', 'unknown')} (ID: {step.get('task_id', 'unknown')})")
            if step.get('depends_on'):
                self.logger.info(f"  Depends on: {', '.join(step['depends_on'])}")
        
        self.logger.info("=" * 60)    
      
    def get_available_connectors(self) -> List[str]:
        """Get list of available and initialized connectors."""
        return [name for name, connector in self._connectors.items() if connector is not None]
    
    def is_llm_available(self) -> bool:
        """Check if Azure OpenAI LLM is available."""
        return self.azure_openai is not None
    
    async def classify_document_with_llm(self, content: str, doc_uri: str, 
                                         metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Use Azure OpenAI LLM to classify document and determine ingestion strategy.
        
        Args:
            content: Document content text
            doc_uri: Document URI
            metadata: Additional document metadata
            
        Returns:
            Dict containing classification results and reasoning
        """
        if not self.is_llm_available():
            raise ValueError("Azure OpenAI LLM is not available. Please check your configuration.")
        
        # Clean and prepare content
        clean_content = clean_text(content)
        
        # Extract basic analysis
        structure_analysis = analyze_document_structure(clean_content)
        entities = extract_entities(clean_content)
        uri_metadata = extract_metadata_from_uri(doc_uri)
        
        # Prepare prompt for LLM
        prompt = self._create_classification_prompt(
            clean_content, doc_uri, structure_analysis, entities, uri_metadata, metadata
        )
        
        try:
            # Call Azure OpenAI
            response = self.azure_openai.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert Document Classifier for an enterprise RAG ingestion pipeline. Analyze documents based on the provided guidelines and return a JSON response with classification and reasoning."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,  # Low temperature for consistent classification
                max_tokens=1500,
                response_format={"type": "json_object"}
            )
            
            # Parse LLM response
            llm_result = json.loads(response.choices[0].message.content)
            
            # Validate and structure result
            classification_result = self._process_llm_classification(
                llm_result, doc_uri, structure_analysis, entities
            )
            
            # Log detailed classification information and save to file
            self._log_classification_details(classification_result, doc_uri)
            
            return classification_result
            
        except Exception as e:
            self.logger.error(f"LLM classification failed for {doc_uri}: {e}")
            # Re-raise the exception since we don't have fallback anymore
            raise ValueError(f"Document classification failed for {doc_uri}: {str(e)}")
    
    def _create_classification_prompt(self, content: str, doc_uri: str, 
                                      structure_analysis: Dict, entities: List[str],
                                      uri_metadata: Dict, metadata: Optional[Dict] = None) -> str:
        """Create detailed prompt for LLM classification."""
        
        # Truncate content if too long
        content_preview = content[:3000] + "..." if len(content) > 3000 else content
        
        prompt = f"""
DOCUMENT CLASSIFICATION TASK

Please analyze the following document and classify it according to the provided guidelines.

CLASSIFICATION GUIDELINES:
{self.guidelines}

DOCUMENT INFORMATION:
- URI: {doc_uri}
- Source: {uri_metadata.get('source', 'unknown')}
- File Type: {uri_metadata.get('file_type', 'unknown')}
- Word Count: {structure_analysis.get('word_count', 0)}
- Has Headers: {structure_analysis.get('has_headers', False)}
- Has Numbered Sections: {structure_analysis.get('has_numbered_sections', False)}
- Has Tables: {structure_analysis.get('has_tables', False)}
- Has Links: {structure_analysis.get('has_links', False)}
- Detected Entities: {entities[:10]}  # First 10 entities

DOCUMENT CONTENT:
{content_preview}

ANALYSIS REQUIREMENTS:
1. Analyze the content structure (structured vs unstructured indicators)
2. Assess entity relationship density (high vs low relationship density)
3. Predict likely query patterns (graph-optimized vs vector-optimized)
4. Determine document type based on content patterns
5. Make final classification decision

CLASSIFICATION OPTIONS:
- VECTOR_STORE_ONLY: Unstructured content with rich semantics, minimal explicit relationships
- KNOWLEDGE_GRAPH_ONLY: Highly structured with explicit entity relationships, formal connections
- DUAL_INGESTION: Hybrid content with both rich semantics AND explicit relationships

Please respond with a JSON object containing:
{{
    "classification": "VECTOR_STORE_ONLY|KNOWLEDGE_GRAPH_ONLY|DUAL_INGESTION",
    "document_type": "specific document type from guidelines",
    "reasoning": "explanation of why this classification was chosen based on structure, relationships, and expected query patterns",
    "key_indicators": [
        "list of key indicators that influenced the decision"
    ]
}}
"""
        return prompt
    
    def _process_llm_classification(self, llm_result: Dict, doc_uri: str,
                                  structure_analysis: Dict, entities: List[str]) -> Dict[str, Any]:
        """Process and validate LLM classification result."""
        
        # Validate required fields are present in LLM result
        required_fields = ['classification', 'document_type', 'reasoning']
        missing_fields = [field for field in required_fields if field not in llm_result]
        
        if missing_fields:
            raise ValueError(f"LLM result missing required fields: {missing_fields}. "
                           f"LLM must provide: {required_fields}")
        
        # Extract classification
        classification = llm_result['classification']
        document_type = llm_result['document_type']
        reasoning = llm_result['reasoning']
        key_indicators = llm_result.get('key_indicators', [])  # Optional field
        
        # Validate classification
        valid_classifications = ['VECTOR_STORE_ONLY', 'KNOWLEDGE_GRAPH_ONLY', 'DUAL_INGESTION']
        if classification not in valid_classifications:
            raise ValueError(f"Invalid classification '{classification}'. "
                           f"Must be one of: {valid_classifications}")
        
        return {
            'doc_uri': doc_uri,
            'classification': classification,
            'document_type': document_type,
            'reasoning': reasoning,
        }
    
    def generate_ingestion_plan(self, classification_result: Dict[str, Any], content: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate ingestion plan based on classification result.
        
        Args:
            classification_result: Result from classify_document_with_llm()
            content: Document content to include in plan steps
            
        Returns:
            JSON plan following the specified schema
        """
        doc_uri = classification_result['doc_uri']
        classification = classification_result['classification']
        
        plan_id = generate_uuid()
        
        if classification == 'VECTOR_STORE_ONLY':
            steps = [create_ingestion_step("1", "vector_ingestion", doc_uri, content=content)]
        elif classification == 'KNOWLEDGE_GRAPH_ONLY':
            steps = [create_ingestion_step("1", "graph_ingestion", doc_uri, content=content)]
        elif classification == 'DUAL_INGESTION':
            steps = [
                create_ingestion_step("1", "vector_ingestion", doc_uri, content=content),
                create_ingestion_step("2", "graph_ingestion", doc_uri, depends_on=["1"], content=content)
            ]
        else:
            # Default to vector ingestion
            self.logger.warning(f"Unknown classification {classification}, defaulting to vector ingestion")
            steps = [create_ingestion_step("1", "vector_ingestion", doc_uri, content=content)]
        
        plan = create_ingestion_plan_schema(plan_id, steps)
        
        # Validate plan
        if not validate_json_plan(plan):
            self.logger.error(f"Generated invalid plan for {doc_uri}")
            raise ValueError("Generated plan does not follow required schema")
        
        # Log plan details to console
        self._log_ingestion_plan(plan, doc_uri)
        
        return plan
    
    async def retrieve_documents_from_azure(self, container_name: str, 
                                          prefix: str = None, max_docs: int = 10) -> List[Dict[str, Any]]:
        """Retrieve documents from Azure Blob Storage."""
        if not self.connectors.get('azure'):
            raise ValueError("Azure connector not available")
        
        connector = self.connectors['azure']
        documents = []
        
        try:
            blobs = connector.list_blobs(container_name, prefix)[:max_docs]
            
            for blob_info in blobs:
                blob_name = blob_info['name']
                content = connector.download_blob(container_name, blob_name)
                
                if content:
                    doc_uri = f"azure://{container_name}/{blob_name}"
                    documents.append({
                        'uri': doc_uri,
                        'content': content if isinstance(content, str) else content.decode('utf-8', errors='ignore'),
                        'metadata': blob_info,
                        'source': 'azure'
                    })
                    
        except Exception as e:
            self.logger.error(f"Error retrieving Azure documents: {e}")
            
        return documents
    
    def retrieve_documents_from_box(self, folder_id: str = "0", max_docs: int = 10) -> List[Dict[str, Any]]:
        """Retrieve documents from Box."""
        if not self.connectors.get('box'):
            raise ValueError("Box connector not available")
        
        connector = self.connectors['box']
        documents = []
        
        try:
            # First try to find the documents-ingest folder if we're starting from root
            target_folder_id = folder_id
            if folder_id == "0":
                # Look for documents-ingest folder
                ingest_folder_id = connector.find_folder_by_name("documents-ingest", "0")
                if ingest_folder_id:
                    self.logger.info(f"Found documents-ingest folder with ID: {ingest_folder_id}")
                    target_folder_id = ingest_folder_id
                else:
                    self.logger.info("documents-ingest folder not found, searching recursively from root")
            
            # Try recursive search first to find all files
            files = connector.list_files_recursive(target_folder_id, max_docs)
            
            # If no files found recursively, try direct folder listing
            if not files:
                files = connector.list_files(target_folder_id)[:max_docs]
            
            self.logger.info(f"Found {len(files)} files to process from folder {target_folder_id}")
            
            for file_info in files:
                if file_info['type'] == 'file':
                    file_id = file_info['id']
                    download_result = connector.download_file(file_id, file_name=file_info['name'])
                    
                    if download_result and isinstance(download_result, str):
                        try:
                            with open(download_result, 'r', encoding='utf-8', errors='ignore') as f:
                                content = f.read()
                            
                            doc_uri = f"box://file/{file_id}?v={file_info.get('etag', 'latest')}"
                            documents.append({
                                'uri': doc_uri,
                                'content': content,
                                'metadata': file_info,
                                'source': 'box'
                            })
                            
                            self.logger.info(f"Successfully processed Box file: {file_info['name']}")
                            
                        except Exception as e:
                            self.logger.error(f"Error reading downloaded file {download_result}: {e}")
                            
        except Exception as e:
            self.logger.error(f"Error retrieving Box documents: {e}")
            
        return documents
    
    async def retrieve_documents_from_confluence(self, page_titles: List[str]) -> List[Dict[str, Any]]:
        """Retrieve documents from Confluence."""
        if not self.connectors.get('confluence'):
            raise ValueError("Confluence connector not available")
        
        connector = self.connectors['confluence']
        documents = []
        
        try:
            for page_title in page_titles:
                result = await connector.download_page_content(page_title)
                
                if result.get('success'):
                    content = result.get('content', '')
                    
                    doc_uri = f"confluence://page/{page_title}"
                    documents.append({
                        'uri': doc_uri,
                        'content': content,
                        'metadata': result,
                        'source': 'confluence'
                    })
                    
        except Exception as e:
            self.logger.error(f"Error retrieving Confluence documents: {e}")
            
        return documents
    
    async def classify_and_plan_documents(self, documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Classify documents and generate ingestion plans using LLM.
        
        Args:
            documents: List of document dicts with 'uri', 'content', 'metadata', 'source'
            
        Returns:
            List of classification results with ingestion plans
        """
        results = []
        
        for doc in documents:
            try:
                # Classify document using LLM
                classification_result = await self.classify_document_with_llm(
                    content=doc['content'],
                    doc_uri=doc['uri'],
                    metadata=doc.get('metadata', {})
                )
                
                # Generate ingestion plan
                ingestion_plan = self.generate_ingestion_plan(classification_result, doc['content'])
                
                # Combine results
                result = {
                    **classification_result,
                    'ingestion_plan': ingestion_plan,
                    'source': doc['source']
                }
                
                results.append(result)
                
                # Log result
                self.logger.info(format_classification_result(
                    classification_result['classification'],
                    0.85,  # Default confidence since removed from structure
                    classification_result['document_type'],
                    doc['uri']
                ))
                
            except Exception as e:
                self.logger.error(f"Error processing document {doc['uri']}: {e}")
                
        return results
    
    async def process_azure_documents(self, container_name: str, prefix: str = None, 
                                    max_docs: int = 10) -> List[Dict[str, Any]]:
        """End-to-end processing for Azure documents."""
        documents = await self.retrieve_documents_from_azure(container_name, prefix, max_docs)
        return await self.classify_and_plan_documents(documents)
    
    async def process_box_documents(self, folder_id: str = "0", max_docs: int = 10) -> List[Dict[str, Any]]:
        """End-to-end processing for Box documents."""
        documents = self.retrieve_documents_from_box(folder_id, max_docs)
        return await self.classify_and_plan_documents(documents)
    
    async def process_confluence_documents(self, page_titles: List[str]) -> List[Dict[str, Any]]:
        """End-to-end processing for Confluence documents."""
        documents = await self.retrieve_documents_from_confluence(page_titles)
        return await self.classify_and_plan_documents(documents)
    
    def save_results(self, results: List[Dict[str, Any]], output_file: str = "ingestion_plans.json") -> bool:
        """Save classification and planning results to JSON file."""
        return save_json(results, output_file)
