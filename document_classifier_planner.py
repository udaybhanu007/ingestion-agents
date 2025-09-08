"""
Document Classifier & Ingest Planner for Enterprise RAG Ingestion Pipeline

This module acts as an expert Document Classifier & Ingest Planner that:
1. Retrieves documents from Azure, Box, and Confluence connectors
2. Classifies documents based on content structure and entity relationships
3. Generates JSON ingestion plans following the specified schema
"""

import asyncio
import json
import re
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

# Import connectors
from connector.azure import AzureConnector
from connector.box import BoxConnector
from connector.confluence_mcp import ConfluenceMCPConnector

# Import common utility functions
from utils.common_function import (
    generate_uuid, setup_logging, clean_text, extract_entities,
    analyze_document_structure, extract_metadata_from_uri,
    validate_json_plan, save_json, create_ingestion_plan_schema,
    create_ingestion_step, format_classification_result
)

# Setup logging
logger = setup_logging("INFO")


class DocumentClassifier:
    """
    Expert Document Classifier following the guidelines.md specification.
    Analyzes documents for structure, entity relationships, and query patterns.
    """
    
    def __init__(self):
        self.logger = logger
    
    def analyze_content_structure(self, content: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyze document's organizational pattern for structured vs unstructured indicators.
        Uses common utility function for structure analysis.
        
        Args:
            content: Document content text
            metadata: Additional document metadata
            
        Returns:
            Dict containing structure analysis results
        """
        # Use common utility function
        basic_structure = analyze_document_structure(content)
        
        # Enhanced analysis specific to classification
        structured_indicators = {
            'hierarchical_relationships': 0,
            'explicit_entity_connections': 0,
            'taxonomies_classifications': 0,
            'cross_references': 0,
            'network_architecture': 0
        }
        
        unstructured_indicators = {
            'narrative_text': 0,
            'sequential_flow': 0,
            'descriptive_passages': 0,
            'opinion_based_content': 0
        }
        
        # Check for hierarchical relationships
        hierarchy_patterns = [
            r'\d+\.\d+\.\d+',  # Numbered sections (1.1.1)
            r'#{2,6}\s',       # Markdown headers
            r'Chapter \d+',     # Chapter references
            r'Section \d+',     # Section references
            r'Part [A-Z]',      # Part references
        ]
        
        for pattern in hierarchy_patterns:
            structured_indicators['hierarchical_relationships'] += len(re.findall(pattern, content, re.IGNORECASE))
        
        # Check for explicit entity connections
        connection_patterns = [
            r'is\s+a\s+',
            r'part\s+of\s+',
            r'related\s+to\s+',
            r'depends\s+on\s+',
            r'connected\s+to\s+',
            r'associated\s+with\s+',
            r'linked\s+to\s+',
            r'references?\s+',
            r'see\s+also\s+',
            r'→|->|←|<-',  # Arrow symbols
        ]
        
        for pattern in connection_patterns:
            structured_indicators['explicit_entity_connections'] += len(re.findall(pattern, content, re.IGNORECASE))
        
        # Check for taxonomies and classifications
        taxonomy_patterns = [
            r'category:\s*',
            r'type:\s*',
            r'class:\s*',
            r'classification:\s*',
            r'taxonomy:\s*',
            r'belongs\s+to\s+',
        ]
        
        for pattern in taxonomy_patterns:
            structured_indicators['taxonomies_classifications'] += len(re.findall(pattern, content, re.IGNORECASE))
        
        # Check for cross-references
        reference_patterns = [
            r'\[.*?\]\(.*?\)',  # Markdown links
            r'http[s]?://[^\s]+',  # URLs
            r'see\s+page\s+\d+',
            r'figure\s+\d+',
            r'table\s+\d+',
            r'appendix\s+[A-Z]',
        ]
        
        for pattern in reference_patterns:
            structured_indicators['cross_references'] += len(re.findall(pattern, content, re.IGNORECASE))
        
        # Check for narrative text patterns
        narrative_patterns = [
            r'\b(however|therefore|furthermore|moreover|nevertheless|consequently)\b',
            r'\b(in\s+conclusion|to\s+summarize|in\s+summary)\b',
            r'\b(first|second|third|finally|lastly)\b',
            r'\b(once\s+upon\s+a\s+time|in\s+the\s+beginning)\b',
        ]
        
        for pattern in narrative_patterns:
            unstructured_indicators['narrative_text'] += len(re.findall(pattern, content, re.IGNORECASE))
        
        # Check for opinion-based content
        opinion_patterns = [
            r'\b(I\s+think|I\s+believe|in\s+my\s+opinion|personally)\b',
            r'\b(arguably|presumably|supposedly|allegedly)\b',
            r'\b(might|could|may|probably|likely)\b',
        ]
        
        for pattern in opinion_patterns:
            unstructured_indicators['opinion_based_content'] += len(re.findall(pattern, content, re.IGNORECASE))
        
        # Calculate scores
        structured_score = sum(structured_indicators.values())
        unstructured_score = sum(unstructured_indicators.values())
        
        return {
            'structured_indicators': structured_indicators,
            'unstructured_indicators': unstructured_indicators,
            'structured_score': structured_score,
            'unstructured_score': unstructured_score,
            'structure_ratio': structured_score / (structured_score + unstructured_score + 1)  # +1 to avoid division by zero
        }
    
    def assess_entity_relationships(self, content: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Count and evaluate entity interconnections for relationship density assessment.
        Uses common utility function for entity extraction.
        """
        # Use common utility function for entity extraction
        entities = extract_entities(content)
        
        high_density_indicators = {
            'multiple_entities_connected': 0,
            'complex_interdependencies': 0,
            'formal_relationships': 0,
            'network_effects': 0
        }
        
        low_density_indicators = {
            'standalone_chunks': 0,
            'minimal_cross_references': 0,
            'self_contained_passages': 0,
            'linear_presentation': 0
        }
        
        # Count entities (proper nouns, technical terms)
        entity_patterns = [
            r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b',  # Proper nouns
            r'\b[A-Z]{2,}\b',  # Acronyms
            r'\b\w+(?:_\w+)+\b',  # Technical terms with underscores
        ]
        
        entities = set()
        for pattern in entity_patterns:
            entities.update(re.findall(pattern, content))
        
        # Count formal relationship definitions
        formal_relationship_patterns = [
            r'(\w+)\s+is\s+a\s+(\w+)',
            r'(\w+)\s+part\s+of\s+(\w+)',
            r'(\w+)\s+extends\s+(\w+)',
            r'(\w+)\s+implements\s+(\w+)',
            r'(\w+)\s+inherits\s+from\s+(\w+)',
        ]
        
        relationship_count = 0
        for pattern in formal_relationship_patterns:
            relationship_count += len(re.findall(pattern, content, re.IGNORECASE))
        
        high_density_indicators['formal_relationships'] = relationship_count
        high_density_indicators['multiple_entities_connected'] = len(entities)
        
        # Check for complex interdependencies
        dependency_patterns = [
            r'depends\s+on',
            r'requires',
            r'prerequisite',
            r'conditional\s+on',
            r'if\s+and\s+only\s+if',
        ]
        
        for pattern in dependency_patterns:
            high_density_indicators['complex_interdependencies'] += len(re.findall(pattern, content, re.IGNORECASE))
        
        # Calculate relationship density
        total_entities = len(entities)
        relationship_density = relationship_count / (total_entities + 1) if total_entities > 0 else 0
        
        return {
            'high_density_indicators': high_density_indicators,
            'low_density_indicators': low_density_indicators,
            'total_entities': total_entities,
            'total_relationships': relationship_count,
            'relationship_density': relationship_density,
            'entities_found': list(entities)[:20]  # Limit for readability
        }
    
    def predict_query_patterns(self, content: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Anticipate likely user query types to determine optimal retrieval method.
        """
        graph_optimized_patterns = {
            'relationship_queries': 0,
            'connection_queries': 0,
            'multi_hop_reasoning': 0,
            'exploratory_discovery': 0
        }
        
        vector_optimized_patterns = {
            'similarity_search': 0,
            'semantic_concepts': 0,
            'contextual_understanding': 0,
            'content_description': 0
        }
        
        # Patterns that suggest graph queries would be useful
        if re.search(r'workflow|process|procedure|steps', content, re.IGNORECASE):
            graph_optimized_patterns['relationship_queries'] += 1
        
        if re.search(r'dependency|prerequisite|sequence|order', content, re.IGNORECASE):
            graph_optimized_patterns['multi_hop_reasoning'] += 1
        
        # Patterns that suggest vector queries would be useful
        if re.search(r'description|explanation|overview|introduction', content, re.IGNORECASE):
            vector_optimized_patterns['contextual_understanding'] += 1
        
        if re.search(r'example|illustration|case\s+study|scenario', content, re.IGNORECASE):
            vector_optimized_patterns['similarity_search'] += 1
        
        graph_score = sum(graph_optimized_patterns.values())
        vector_score = sum(vector_optimized_patterns.values())
        
        return {
            'graph_optimized_patterns': graph_optimized_patterns,
            'vector_optimized_patterns': vector_optimized_patterns,
            'graph_score': graph_score,
            'vector_score': vector_score,
            'query_pattern_ratio': graph_score / (graph_score + vector_score + 1)
        }
    
    def classify_document(self, content: str, doc_uri: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Main classification method that determines ingestion strategy.
        
        Returns:
            Dict containing classification results and ingestion recommendation
        """
        self.logger.info(f"Classifying document: {doc_uri}")
        
        # Perform all three analyses
        structure_analysis = self.analyze_content_structure(content, metadata)
        relationship_analysis = self.assess_entity_relationships(content, metadata)
        query_analysis = self.predict_query_patterns(content, metadata)
        
        # Determine document type based on content patterns
        doc_type = self._determine_document_type(content, metadata)
        
        # Make classification decision
        classification = self._make_classification_decision(
            structure_analysis, relationship_analysis, query_analysis, doc_type
        )
        
        return {
            'doc_uri': doc_uri,
            'classification': classification,
            'document_type': doc_type,
            'structure_analysis': structure_analysis,
            'relationship_analysis': relationship_analysis,
            'query_analysis': query_analysis,
            'confidence_score': self._calculate_confidence(structure_analysis, relationship_analysis, query_analysis)
        }
    
    def _determine_document_type(self, content: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Determine the specific document type based on content patterns."""
        content_lower = content.lower()
        
        # VECTOR_STORE_ONLY types
        if any(pattern in content_lower for pattern in ['research', 'methodology', 'results', 'discussion']):
            return 'research_paper'
        elif any(pattern in content_lower for pattern in ['news', 'article', 'journalism', 'breaking']):
            return 'news_article'
        elif any(pattern in content_lower for pattern in ['product', 'features', 'benefits', 'marketing']):
            return 'product_description'
        elif any(pattern in content_lower for pattern in ['manual', 'guide', 'how to', 'instructions']):
            return 'user_manual'
        elif any(pattern in content_lower for pattern in ['meeting', 'notes', 'minutes', 'transcript']):
            return 'meeting_notes'
        
        # KNOWLEDGE_GRAPH_ONLY types
        elif any(pattern in content_lower for pattern in ['organizational chart', 'hierarchy', 'org chart']):
            return 'organizational_chart'
        elif any(pattern in content_lower for pattern in ['specification', 'technical spec', 'component']):
            return 'technical_specification'
        elif any(pattern in content_lower for pattern in ['curriculum', 'course', 'prerequisite', 'academic']):
            return 'academic_curriculum'
        elif any(pattern in content_lower for pattern in ['regulation', 'compliance', 'framework', 'policy']):
            return 'regulatory_framework'
        elif any(pattern in content_lower for pattern in ['taxonomy', 'classification', 'schema', 'ontology']):
            return 'scientific_taxonomy'
        
        # DUAL_INGESTION types
        elif any(pattern in content_lower for pattern in ['medical', 'drug', 'treatment', 'protocol']):
            return 'medical_literature'
        elif any(pattern in content_lower for pattern in ['legal', 'case law', 'citation', 'court']):
            return 'legal_case_law'
        elif any(pattern in content_lower for pattern in ['technical documentation', 'api', 'component']):
            return 'technical_documentation'
        elif any(pattern in content_lower for pattern in ['textbook', 'academic', 'concept', 'explanation']):
            return 'academic_textbook'
        elif any(pattern in content_lower for pattern in ['business process', 'workflow', 'procedure']):
            return 'business_process'
        elif any(pattern in content_lower for pattern in ['scientific journal', 'research', 'data']):
            return 'scientific_journal'
        
        return 'unknown'
    
    def _make_classification_decision(self, structure_analysis: Dict, relationship_analysis: Dict, 
                                    query_analysis: Dict, doc_type: str) -> str:
        """Make the final classification decision based on all analyses."""
        
        # Get scores
        structure_ratio = structure_analysis['structure_ratio']
        relationship_density = relationship_analysis['relationship_density']
        query_ratio = query_analysis['query_pattern_ratio']
        
        # Document type mappings from guidelines
        vector_only_types = {
            'research_paper', 'news_article', 'product_description', 
            'user_manual', 'meeting_notes', 'blog_post', 'literary_work'
        }
        
        graph_only_types = {
            'organizational_chart', 'technical_specification', 'academic_curriculum',
            'regulatory_framework', 'scientific_taxonomy', 'database_schema',
            'legal_document', 'supply_chain'
        }
        
        dual_ingestion_types = {
            'medical_literature', 'legal_case_law', 'technical_documentation',
            'academic_textbook', 'business_process', 'scientific_journal'
        }
        
        # Check document type first
        if doc_type in dual_ingestion_types:
            return 'DUAL_INGESTION'
        elif doc_type in vector_only_types:
            return 'VECTOR_STORE_ONLY'
        elif doc_type in graph_only_types:
            return 'KNOWLEDGE_GRAPH_ONLY'
        
        # Fallback to score-based decision
        # High structure + high relationships = likely graph or dual
        if structure_ratio > 0.6 and relationship_density > 0.3:
            if query_ratio > 0.5:  # High graph query potential
                return 'KNOWLEDGE_GRAPH_ONLY'
            else:
                return 'DUAL_INGESTION'  # Both structured and semantic value
        
        # High structure but low relationships = likely vector with some structure
        elif structure_ratio > 0.4 and relationship_density < 0.2:
            return 'VECTOR_STORE_ONLY'
        
        # Low structure but high relationships = unusual, probably dual
        elif structure_ratio < 0.3 and relationship_density > 0.4:
            return 'DUAL_INGESTION'
        
        # Default to vector for unstructured content
        return 'VECTOR_STORE_ONLY'
    
    def _calculate_confidence(self, structure_analysis: Dict, relationship_analysis: Dict, 
                            query_analysis: Dict) -> float:
        """Calculate confidence score for the classification."""
        # Base confidence on how clear the indicators are
        total_indicators = (
            structure_analysis['structured_score'] + 
            structure_analysis['unstructured_score'] +
            relationship_analysis['total_relationships'] +
            query_analysis['graph_score'] +
            query_analysis['vector_score']
        )
        
        # Normalize to 0-1 range
        confidence = min(total_indicators / 100.0, 1.0)
        return round(confidence, 2)


class IngestPlanGenerator:
    """
    Generates JSON ingestion plans following the specified schema.
    """
    
    def __init__(self):
        self.logger = logger
    
    def generate_plan(self, classification_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate ingestion plan based on classification result.
        Uses common utility functions for plan generation.
        
        Args:
            classification_result: Result from DocumentClassifier.classify_document()
            
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
        
        # Validate plan using common utility
        if not validate_json_plan(plan):
            self.logger.error(f"Generated invalid plan for {doc_uri}")
            raise ValueError("Generated plan does not follow required schema")
        
        return plan
    
    def _generate_vector_only_plan(self, plan_id: str, doc_uri: str) -> Dict[str, Any]:
        """Generate plan for vector ingestion only."""
        return {
            "plan_id": plan_id,
            "steps": [
                {
                    "task_id": "1",
                    "tool": "vector_ingestion",
                    "args": {
                        "doc_uri": doc_uri
                    },
                    "depends_on": []
                }
            ]
        }
    
    def _generate_graph_only_plan(self, plan_id: str, doc_uri: str) -> Dict[str, Any]:
        """Generate plan for graph ingestion only."""
        return {
            "plan_id": plan_id,
            "steps": [
                {
                    "task_id": "1",
                    "tool": "graph_ingestion",
                    "args": {
                        "doc_uri": doc_uri
                    },
                    "depends_on": []
                }
            ]
        }
    
    def _generate_dual_ingestion_plan(self, plan_id: str, doc_uri: str) -> Dict[str, Any]:
        """Generate plan for both vector and graph ingestion."""
        return {
            "plan_id": plan_id,
            "steps": [
                {
                    "task_id": "1",
                    "tool": "vector_ingestion",
                    "args": {
                        "doc_uri": doc_uri
                    },
                    "depends_on": []
                },
                {
                    "task_id": "2",
                    "tool": "graph_ingestion",
                    "args": {
                        "doc_uri": doc_uri
                    },
                    "depends_on": ["1"]
                }
            ]
        }


class DocumentIngestionPlanner:
    """
    Main orchestrator class that combines document retrieval, classification, and plan generation.
    """
    
    def __init__(self, env_file: str = ".env.dev"):
        """
        Initialize the Document Ingestion Planner with all connectors.
        
        Args:
            env_file: Path to environment file for connector credentials
        """
        self.logger = logger
        self.env_file = env_file
        
        # Initialize components
        self.classifier = DocumentClassifier()
        self.plan_generator = IngestPlanGenerator()
        
        # Initialize connectors
        self._initialize_connectors()
    
    def _initialize_connectors(self):
        """Initialize all available connectors."""
        self.connectors = {}
        
        # Azure connector
        try:
            self.connectors['azure'] = AzureConnector(env_file=self.env_file)
            if self.connectors['azure'].blob_service_client:
                self.logger.info("Azure connector initialized successfully")
            else:
                self.logger.warning("Azure connector initialization failed")
        except Exception as e:
            self.logger.error(f"Failed to initialize Azure connector: {e}")
            self.connectors['azure'] = None
        
        # Box connector
        try:
            self.connectors['box'] = BoxConnector(env_file=self.env_file)
            if self.connectors['box'].is_available():
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
    
    def get_available_connectors(self) -> List[str]:
        """Get list of available and initialized connectors."""
        return [name for name, connector in self.connectors.items() if connector is not None]
    
    async def retrieve_documents_from_azure(self, container_name: str, prefix: Optional[str] = None, max_docs: int = 10) -> List[Dict[str, Any]]:
        """Retrieve documents from Azure Blob Storage."""
        if not self.connectors.get('azure'):
            raise ValueError("Azure connector not available")
        
        connector = self.connectors['azure']
        documents = []
        
        try:
            blobs = connector.list_blobs(container_name, prefix)[:max_docs]
            
            for blob_info in blobs:
                blob_name = blob_info['name']
                
                # Download content
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
            files = connector.list_files(folder_id)[:max_docs]
            
            for file_info in files:
                if file_info['type'] == 'file':
                    file_id = file_info['id']
                    
                    # Download file content
                    download_result = connector.download_file(file_id)
                    
                    if download_result and isinstance(download_result, str):
                        # Read the downloaded file
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
                    page_data = result['data']
                    content = page_data.get('content', '')
                    
                    doc_uri = f"confluence://page/{page_data.get('id', page_title)}"
                    documents.append({
                        'uri': doc_uri,
                        'content': content,
                        'metadata': page_data,
                        'source': 'confluence'
                    })
                    
        except Exception as e:
            self.logger.error(f"Error retrieving Confluence documents: {e}")
            
        return documents
    
    async def classify_and_plan_documents(self, documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Classify documents and generate ingestion plans.
        
        Args:
            documents: List of document dicts with 'uri', 'content', 'metadata', 'source'
            
        Returns:
            List of classification results with ingestion plans
        """
        results = []
        
        for doc in documents:
            try:
                # Classify document
                classification_result = self.classifier.classify_document(
                    content=doc['content'],
                    doc_uri=doc['uri'],
                    metadata=doc.get('metadata', {})
                )
                
                # Generate ingestion plan
                ingestion_plan = self.plan_generator.generate_plan(classification_result)
                
                # Combine results
                result = {
                    **classification_result,
                    'ingestion_plan': ingestion_plan,
                    'source': doc['source']
                }
                
                results.append(result)
                
                self.logger.info(f"Classified {doc['uri']} as {classification_result['classification']}")
                
            except Exception as e:
                self.logger.error(f"Error processing document {doc['uri']}: {e}")
                
        return results
    
    async def process_azure_documents(self, container_name: str, prefix: Optional[str] = None, max_docs: int = 10) -> List[Dict[str, Any]]:
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
    
    def save_results(self, results: List[Dict[str, Any]], output_file: str = "ingestion_plans.json"):
        """Save classification and planning results to JSON file."""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Results saved to {output_file}")
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")


# Example usage functions
async def example_usage():
    """Example of how to use the Document Ingestion Planner."""
    
    # Initialize planner
    planner = DocumentIngestionPlanner()
    
    print("Available connectors:", planner.get_available_connectors())
    
    # Example 1: Process Box documents
    if 'box' in planner.get_available_connectors():
        print("\n--- Processing Box Documents ---")
        box_results = await planner.process_box_documents(folder_id="0", max_docs=5)
        
        for result in box_results:
            print(f"\nDocument: {result['doc_uri']}")
            print(f"Classification: {result['classification']}")
            print(f"Document Type: {result['document_type']}")
            print(f"Confidence: {result['confidence_score']}")
            print("Ingestion Plan:")
            print(json.dumps(result['ingestion_plan'], indent=2))
    
    # Example 2: Process Confluence documents
    if 'confluence' in planner.get_available_connectors():
        print("\n--- Processing Confluence Documents ---")
        confluence_results = await planner.process_confluence_documents([
            "Project Documentation",
            "API Reference",
            "User Guide"
        ])
        
        for result in confluence_results:
            print(f"\nDocument: {result['doc_uri']}")
            print(f"Classification: {result['classification']}")
            print("Ingestion Plan:")
            print(json.dumps(result['ingestion_plan'], indent=2))
    
    # Example 3: Process Azure documents
    if 'azure' in planner.get_available_connectors():
        print("\n--- Processing Azure Documents ---")
        azure_results = await planner.process_azure_documents(
            container_name="documents", 
            prefix="public/",
            max_docs=3
        )
        
        for result in azure_results:
            print(f"\nDocument: {result['doc_uri']}")
            print(f"Classification: {result['classification']}")
            print("Ingestion Plan:")
            print(json.dumps(result['ingestion_plan'], indent=2))


if __name__ == "__main__":
    # Run example
    asyncio.run(example_usage())
