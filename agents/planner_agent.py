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


class PlannerAgent:
    """
    LLM-powered Planner Agent for document classification and ingestion planning.
    """
    
    def __init__(self, env_file: str = ".env.dev"):
        """
        Initialize the Planner Agent with Azure OpenAI and connectors.
        
        Args:
            env_file: Path to environment file for credentials
        """
        self.logger = setup_logging("INFO")
        self.env_file = env_file
        
        # Load environment variables
        self._load_environment(env_file)
        
        # Initialize Azure OpenAI client
        self._initialize_llm()
        
        # Initialize connectors
        self._initialize_connectors()
        
        # Load guidelines
        self.guidelines = self._load_guidelines()
    
    def _load_environment(self, env_file: str):
        """Load environment variables from file."""
        env_files_to_try = [env_file, ".env.dev", ".env"]
        env_file_loaded = None
        
        for env_path in env_files_to_try:
            if os.path.exists(env_path):
                load_dotenv(env_path)
                env_file_loaded = env_path
                self.logger.info(f"Loaded environment from {env_path}")
                break
        
        if not env_file_loaded:
            self.logger.warning("No environment file found. Using system environment variables.")
    
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
    
    def _initialize_connectors(self):
        """Initialize all available connectors."""
        self.connectors = {}
        
        # Azure connector
        try:
            self.connectors['azure'] = AzureConnector(env_file=self.env_file)
            if hasattr(self.connectors['azure'], 'blob_service_client') and self.connectors['azure'].blob_service_client:
                self.logger.info("Azure connector initialized successfully")
            else:
                self.logger.warning("Azure connector initialization failed")
        except Exception as e:
            self.logger.error(f"Failed to initialize Azure connector: {e}")
            self.connectors['azure'] = None
        
        # Box connector
        try:
            self.connectors['box'] = BoxConnector(env_file=self.env_file)
            if hasattr(self.connectors['box'], 'is_available') and self.connectors['box'].is_available():
                self.logger.info("Box connector initialized successfully")
            else:
                self.logger.warning("Box connector initialization failed")
        except Exception as e:
            self.logger.error(f"Failed to initialize Box connector: {e}")
            self.connectors['box'] = None
        
        # Confluence connector
        try:
            self.connectors['confluence'] = ConfluenceMCPConnector(env_file=self.env_file)
            self.logger.info("Confluence MCP connector initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Confluence connector: {e}")
            self.connectors['confluence'] = None
    
    def create_ingestion_plan(self, doc_uri: str, metadata: Optional[Dict[str, Any]] = None, 
                             content: Optional[str] = None) -> Dict[str, Any]:
        """
        Create an ingestion plan for a given document URI.
        This is the main API entry point for creating ingestion plans.
        
        Args:
            doc_uri: Document URI to create plan for
            metadata: Optional metadata for the document
            content: Optional content if already fetched
            
        Returns:
            Dict containing the ingestion plan
        """
        try:
            # If content is provided, use it for classification
            if content:
                if isinstance(content, bytes):
                    content = content.decode('utf-8', errors='ignore')
                
                # Use LLM classification if available
                if self.is_llm_available():
                    # Run async classification in sync context
                    import asyncio
                    try:
                        loop = asyncio.get_event_loop()
                    except RuntimeError:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                    
                    classification_result = loop.run_until_complete(
                        self.classify_document_with_llm(content, doc_uri, metadata)
                    )
                else:
                    # Use fallback classification
                    classification_result = self._fallback_classification(content, doc_uri, metadata)
            else:
                # No content provided, use fallback based on URI and metadata
                classification_result = self._fallback_classification("", doc_uri, metadata)
            
            # Generate the ingestion plan
            plan = self.generate_ingestion_plan(classification_result)
            
            self.logger.info(f"Created ingestion plan for {doc_uri}")
            return plan
            
        except Exception as e:
            self.logger.error(f"Failed to create ingestion plan for {doc_uri}: {e}")
            # Return a basic fallback plan
            plan_id = generate_uuid()
            steps = [create_ingestion_step("1", "vector_ingestion", doc_uri)]
            plan = create_ingestion_plan_schema(plan_id, steps)
            return plan
    
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
    
    def get_available_connectors(self) -> List[str]:
        """Get list of available and initialized connectors."""
        return [name for name, connector in self.connectors.items() if connector is not None]
    
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
            self.logger.warning("LLM not available, falling back to rule-based classification")
            return self._fallback_classification(content, doc_uri, metadata)
        
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
            
            self.logger.info(f"Document {doc_uri} classified as {classification_result['classification']}")
            return classification_result
            
        except Exception as e:
            self.logger.error(f"LLM classification failed for {doc_uri}: {e}")
            # Fallback to rule-based classification
            return self._fallback_classification(clean_content, doc_uri, metadata)
    
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
        
        # Extract classification
        classification = llm_result.get('classification', 'VECTOR_STORE_ONLY')
        document_type = llm_result.get('document_type', 'unknown')
        reasoning = llm_result.get('reasoning', {})
        key_indicators = llm_result.get('key_indicators', [])
        
        # Validate classification
        valid_classifications = ['VECTOR_STORE_ONLY', 'KNOWLEDGE_GRAPH_ONLY', 'DUAL_INGESTION']
        if classification not in valid_classifications:
            self.logger.warning(f"Invalid classification {classification}, defaulting to VECTOR_STORE_ONLY")
            classification = 'VECTOR_STORE_ONLY'
        
        return {
            'doc_uri': doc_uri,
            'classification': classification,
            'document_type': document_type,
            'reasoning': reasoning,
            'key_indicators': key_indicators
        }
    
    def _fallback_classification(self, content: str, doc_uri: str, 
                                 metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Fallback rule-based classification when LLM fails."""
        self.logger.info(f"Using fallback classification for {doc_uri}")
        
        content_lower = content.lower()
        structure_analysis = analyze_document_structure(content)
        
        # Simple rule-based classification
        if any(keyword in content_lower for keyword in ['api', 'endpoint', 'schema', 'database', 'medical', 'treatment', 'protocol']):
            classification = 'DUAL_INGESTION'
            document_type = 'technical_documentation'
        elif any(keyword in content_lower for keyword in ['org chart', 'hierarchy', 'workflow', 'reports to', '├──', '└──']):
            classification = 'KNOWLEDGE_GRAPH_ONLY'
            document_type = 'organizational_chart'
        else:
            classification = 'VECTOR_STORE_ONLY'
            document_type = 'general_document'
        
        return {
            'doc_uri': doc_uri,
            'classification': classification,
            'document_type': document_type,
            'reasoning': {'fallback': 'Used rule-based classification due to LLM unavailability'},
            'key_indicators': ['fallback_classification']
        }
    
    def generate_ingestion_plan(self, classification_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate ingestion plan based on classification result.
        
        Args:
            classification_result: Result from classify_document_with_llm()
            
        Returns:
            JSON plan following the specified schema
        """
        doc_uri = classification_result['doc_uri']
        classification = classification_result['classification']
        
        plan_id = generate_uuid()
        
        if classification == 'VECTOR_STORE_ONLY':
            steps = [create_ingestion_step("1", "vector_ingestion", doc_uri)]
        elif classification == 'KNOWLEDGE_GRAPH_ONLY':
            steps = [create_ingestion_step("1", "graph_ingestion", doc_uri)]
        elif classification == 'DUAL_INGESTION':
            steps = [
                create_ingestion_step("1", "vector_ingestion", doc_uri),
                create_ingestion_step("2", "graph_ingestion", doc_uri, depends_on=["1"])
            ]
        else:
            # Default to vector ingestion
            self.logger.warning(f"Unknown classification {classification}, defaulting to vector ingestion")
            steps = [create_ingestion_step("1", "vector_ingestion", doc_uri)]
        
        plan = create_ingestion_plan_schema(plan_id, steps)
        
        # Validate plan
        if not validate_json_plan(plan):
            self.logger.error(f"Generated invalid plan for {doc_uri}")
            raise ValueError("Generated plan does not follow required schema")
        
        self.logger.info(f"Generated ingestion plan {plan_id} for {doc_uri}")
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
                ingestion_plan = self.generate_ingestion_plan(classification_result)
                
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


# Example usage
async def main():
    """Example usage of the Planner Agent with real connectors."""
    
    # Initialize the Planner Agent
    planner = PlannerAgent()
    
    print("Available connectors:", planner.get_available_connectors())
    print("LLM available:", planner.is_llm_available())
    
    results = []
    
    # Process documents from Azure Blob Storage (if available)
    if 'azure' in planner.get_available_connectors():
        print("\n--- Processing Azure Documents ---")
        try:
            # Replace with your actual container name
            container_name = os.getenv("AZURE_CONTAINER_NAME", "documents")
            prefix = os.getenv("AZURE_BLOB_PREFIX", None)  # Optional prefix filter
            max_docs = int(os.getenv("MAX_DOCS_PER_SOURCE", "5"))
            
            azure_results = await planner.process_azure_documents(
                container_name=container_name,
                prefix=prefix,
                max_docs=max_docs
            )
            results.extend(azure_results)
            print(f"Processed {len(azure_results)} Azure documents")
            
        except Exception as e:
            print(f"Error processing Azure documents: {e}")
    
    # Process documents from Box (if available)
    if 'box' in planner.get_available_connectors():
        print("\n--- Processing Box Documents ---")
        try:
            # Replace with your actual folder ID (0 = root folder)
            folder_id = os.getenv("BOX_FOLDER_ID", "0")
            max_docs = int(os.getenv("MAX_DOCS_PER_SOURCE", "5"))
            
            box_results = await planner.process_box_documents(
                folder_id=folder_id,
                max_docs=max_docs
            )
            results.extend(box_results)
            print(f"Processed {len(box_results)} Box documents")
            
        except Exception as e:
            print(f"Error processing Box documents: {e}")
    
    # Process documents from Confluence (if available)
    if 'confluence' in planner.get_available_connectors():
        print("\n--- Processing Confluence Documents ---")
        try:
            # Get configured Confluence pages from environment
            if planner.connectors.get('confluence') and planner.connectors['confluence'].is_available():
                page_titles = planner.connectors['confluence'].get_configured_pages()
                if not page_titles:
                    # Fallback to environment variable if no pages configured
                    page_titles_env = os.getenv("CONFLUENCE_PAGE_TITLES", "")
                    if page_titles_env:
                        page_titles = [title.strip() for title in page_titles_env.split(",")]
                    else:
                        # Default page titles for testing
                        page_titles = [
                            "API Documentation",
                            "Project Overview", 
                            "User Guide",
                            "Technical Specifications"
                        ]
            else:
                page_titles = []
            
            if page_titles:
                confluence_results = await planner.process_confluence_documents(page_titles)
                results.extend(confluence_results)
                print(f"Processed {len(confluence_results)} Confluence documents")
            else:
                print("No Confluence pages configured or connector not available")
                
        except Exception as e:
            print(f"Error processing Confluence documents: {e}")
    
    # If no connectors are available, provide sample data for testing
    if not results and not planner.get_available_connectors():
        print("\n--- No connectors available, using sample document for testing ---")
        sample_documents = [
            {
                'uri': 'example://sample/medical_protocol.txt',
                'content': '''
                Medical Treatment Protocol for Diabetes Management
                
                1. Patient Assessment
                   1.1 Blood glucose monitoring
                   1.2 HbA1c testing
                   1.3 Complication screening
                
                2. Treatment Plan
                   2.1 Medication Management
                       - Metformin is first-line treatment
                       - Insulin therapy depends on blood glucose levels
                       - Drug interactions must be monitored
                   
                   2.2 Lifestyle Interventions
                       - Diet modification
                       - Exercise program
                       - Weight management
                
                3. Monitoring Protocol
                   - Daily glucose checks
                   - Quarterly HbA1c
                   - Annual eye examination
                
                Drug Interactions:
                - Metformin interacts with contrast agents
                - Insulin dosage affects other medications
                - Monitor for hypoglycemia with combination therapy
                ''',
                'metadata': {'type': 'medical_document'},
                'source': 'example'
            }
        ]
        
        # Classify and plan sample documents
        results = await planner.classify_and_plan_documents(sample_documents)
    
    # Display results
    if results:
        print(f"\n=== PROCESSING COMPLETE ===")
        print(f"Total documents processed: {len(results)}")
        
        # Classification summary
        vector_only = [r for r in results if r['classification'] == 'VECTOR_STORE_ONLY']
        graph_only = [r for r in results if r['classification'] == 'KNOWLEDGE_GRAPH_ONLY']
        dual = [r for r in results if r['classification'] == 'DUAL_INGESTION']
        
        print(f"\nClassification Summary:")
        print(f"  Vector Store Only: {len(vector_only)}")
        print(f"  Knowledge Graph Only: {len(graph_only)}")
        print(f"  Dual Ingestion: {len(dual)}")
        
        for i, result in enumerate(results, 1):
            print(f"\n--- Classification Result {i} ---")
            print(f"Document: {result['doc_uri']}")
            print(f"Source: {result['source']}")
            print(f"Classification: {result['classification']}")
            print(f"Document Type: {result['document_type']}")
            print(f"Reasoning: {result['reasoning']}")
            
            # Show key indicators
            if result.get('key_indicators'):
                print(f"Key Indicators: {', '.join(result['key_indicators'])}")
            
            # Show ingestion plan summary
            plan = result.get('ingestion_plan', {})
            steps = plan.get('steps', [])
            print(f"Ingestion Steps: {len(steps)} step(s)")
            for step in steps:
                task_id = step.get('task_id', 'unknown')
                tool = step.get('tool', 'unknown')
                print(f"  - Task {task_id}: {tool}")
        
        # Save results with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"planner_agent_results_{timestamp}.json"
        
        if planner.save_results(results, output_file):
            print(f"\nResults saved to: {output_file}")
        else:
            print("\nFailed to save results")
    else:
        print("\nNo documents were processed")


async def process_specific_sources():
    """Example of processing specific document sources."""
    planner = PlannerAgent()
    
    # Process only Azure documents
    if 'azure' in planner.get_available_connectors():
        print("Processing Azure documents only...")
        results = await planner.process_azure_documents(
            container_name="my-documents",
            prefix="reports/",
            max_docs=20
        )
        
        # Filter results by classification
        vector_only = [r for r in results if r['classification'] == 'VECTOR_STORE_ONLY']
        graph_only = [r for r in results if r['classification'] == 'KNOWLEDGE_GRAPH_ONLY']
        dual = [r for r in results if r['classification'] == 'DUAL_INGESTION']
        
        print(f"Classification Summary:")
        print(f"  Vector Store Only: {len(vector_only)}")
        print(f"  Knowledge Graph Only: {len(graph_only)}")
        print(f"  Dual Ingestion: {len(dual)}")
        
        return results
    else:
        print("Azure connector not available")
        return []


async def batch_process_with_config():
    """Example of batch processing with configuration."""
    planner = PlannerAgent()
    
    # Configuration for batch processing
    config = {
        'azure': {
            'enabled': True,
            'container_name': os.getenv('AZURE_CONTAINER_NAME', 'documents'),
            'prefix': os.getenv('AZURE_BLOB_PREFIX'),
            'max_docs': 10
        },
        'box': {
            'enabled': True,
            'folder_id': os.getenv('BOX_FOLDER_ID', '0'),
            'max_docs': 10
        },
        'confluence': {
            'enabled': True,
            'page_titles': ['API Documentation', 'User Guide', 'Architecture Overview'],
            'max_docs': 5
        }
    }
    
    all_results = []
    
    # Process each configured source
    for source, source_config in config.items():
        if not source_config.get('enabled', False):
            continue
            
        if source not in planner.get_available_connectors():
            print(f"Skipping {source}: connector not available")
            continue
            
        print(f"\nProcessing {source} documents...")
        
        try:
            if source == 'azure':
                results = await planner.process_azure_documents(
                    container_name=source_config['container_name'],
                    prefix=source_config.get('prefix'),
                    max_docs=source_config['max_docs']
                )
            elif source == 'box':
                results = await planner.process_box_documents(
                    folder_id=source_config['folder_id'],
                    max_docs=source_config['max_docs']
                )
            elif source == 'confluence':
                results = await planner.process_confluence_documents(
                    source_config['page_titles']
                )
            
            all_results.extend(results)
            print(f"  Processed {len(results)} documents from {source}")
            
        except Exception as e:
            print(f"  Error processing {source}: {e}")
    
    return all_results


if __name__ == "__main__":
    asyncio.run(main())
