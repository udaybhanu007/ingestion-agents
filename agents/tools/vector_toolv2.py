"""
Vector Ingestion Tool V2 - LlamaIndex Implementation

This tool focuses purely on document ingestion using LlamaIndex SentenceSplitter 
with Qdrant vector database. Implements minimal required logic for document chunking,
embedding generation, and vector storage.

Key Features:
- LlamaIndex SentenceSplitter for intelligent chunking
- Azure OpenAI embeddings integration
- Direct Qdrant storage with metadata
- Simple, focused ingestion workflow
"""

import asyncio
import logging
import os
import sys
import uuid
from typing import Dict, List, Any, Optional
from datetime import datetime
import hashlib

# Add parent directory to path for config import
parent_dir = os.path.dirname(os.path.dirname(__file__))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# LlamaIndex imports
try:
    from llama_index.core import Document
    from llama_index.core.node_parser import SentenceSplitter
    from llama_index.embeddings.azure_openai import AzureOpenAIEmbedding
    LLAMAINDEX_AVAILABLE = True
except ImportError:
    logging.warning("LlamaIndex not available - install with: pip install llama-index llama-index-embeddings-azure-openai")
    LLAMAINDEX_AVAILABLE = False

# Qdrant imports
try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
    QDRANT_AVAILABLE = True
except ImportError:
    logging.warning("Qdrant client not available - install with: pip install qdrant-client")
    QDRANT_AVAILABLE = False

# Configuration management
try:
    from config.config_manager import get_config
    config_manager = get_config()
    def get_tool_config():
        return config_manager
except ImportError:
    # Fallback configuration using environment variables
    class MockConfig:
        def get_config(self, section, key=None):
            configs = {
                "qdrant": {
                    "url": os.getenv("QDRANT_URL", "http://localhost:6333"),
                    "api_key": os.getenv("QDRANT_API_KEY"),
                    "collection_name": os.getenv("QDRANT_COLLECTION", "documents_v2")
                },
                "openai": {
                    "azure_endpoint": os.getenv("AZURE_OPENAI_ENDPOINT", ""),
                    "azure_api_key": os.getenv("AZURE_OPENAI_API_KEY", ""),
                    "api_version": os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
                    "embedding_deployment": os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "text-embedding-ada-002")
                }
            }
            if key:
                return configs.get(section, {}).get(key)
            return configs.get(section, {})
    
    def get_tool_config():
        return MockConfig()

# Structured logging
try:
    from config.logger_config import get_tool_logger
    STRUCTURED_LOGGING_AVAILABLE = True
except ImportError:
    STRUCTURED_LOGGING_AVAILABLE = False

logger = logging.getLogger(__name__)


class VectorToolV2:
    """
    LlamaIndex-based vector ingestion tool for Qdrant.
    
    This tool implements a streamlined document ingestion pipeline:
    1. Document parsing with LlamaIndex SentenceSplitter
    2. Embedding generation with Azure OpenAI
    3. Vector storage in Qdrant with metadata
    """
    
    def __init__(self):
        """Initialize the Vector Tool V2 with LlamaIndex components."""
        self.config = get_tool_config()
        
        # Initialize structured logging if available
        if STRUCTURED_LOGGING_AVAILABLE:
            self.logger = get_tool_logger("vector_toolv2")
        else:
            self.logger = logging.getLogger(__name__)
        
        # Check dependencies
        if not LLAMAINDEX_AVAILABLE:
            raise ImportError("LlamaIndex is required for VectorToolV2")
        if not QDRANT_AVAILABLE:
            raise ImportError("Qdrant client is required for VectorToolV2")
        
        # Initialize LlamaIndex components
        self._init_llamaindex_components()
        
        # Initialize Qdrant client
        self._init_qdrant_client()
        
        # Processing statistics
        self.stats = {
            "documents_processed": 0,
            "chunks_created": 0,
            "embeddings_generated": 0,
            "vectors_stored": 0,
            "errors": []
        }
        
        self.logger.info("VectorToolV2 initialized with LlamaIndex SentenceSplitter")
    
    def _init_llamaindex_components(self):
        """Initialize LlamaIndex components for chunking and embeddings."""
        
        # Get OpenAI configuration
        openai_config = self.config.get_config('openai')
        
        # Initialize Azure OpenAI embedding model
        self.embedding_model = AzureOpenAIEmbedding(
            model=openai_config.get('embedding_deployment', 'text-embedding-ada-002'),
            azure_endpoint=openai_config.get('azure_endpoint'),
            api_key=openai_config.get('azure_api_key'),
            api_version=openai_config.get('api_version'),
            azure_deployment=openai_config.get('embedding_deployment')
        )
        
        # Initialize SentenceSplitter for intelligent document chunking
        # Optimized for academic/research documents with overlap for context preservation
        self.sentence_splitter = SentenceSplitter(
            chunk_size=1024,              # Good balance for semantic chunks
            chunk_overlap=20,             # Preserve context between chunks
            separator=" ",                # Split on spaces primarily
            paragraph_separator="\n\n\n", # Recognize paragraph boundaries
            secondary_chunking_regex="[^,.;。]+[,.;。]?",  # Respect sentence boundaries
        )
        
        self.logger.info("LlamaIndex components initialized", 
                        component="vector_toolv2",
                        chunk_size=1024,
                        chunk_overlap=20)
    
    def _init_qdrant_client(self):
        """Initialize Qdrant client and ensure collection exists."""
        
        # Get Qdrant configuration
        qdrant_config = self.config.get_config('qdrant')
        
        # Initialize Qdrant client
        self.qdrant_client = QdrantClient(
            url=qdrant_config.get('url', 'http://localhost:6333'),
            api_key=qdrant_config.get('api_key')
        )
        
        self.collection_name = qdrant_config.get('collection_name', 'documents_v2')
        
        # Ensure collection exists with proper vector configuration
        self._ensure_collection_exists()
        
        self.logger.info("Qdrant client initialized",
                        component="vector_toolv2", 
                        collection=self.collection_name)
    
    def _ensure_collection_exists(self):
        """Ensure Qdrant collection exists with proper vector configuration."""
        try:
            # Check if collection exists
            collections = self.qdrant_client.get_collections()
            collection_names = [col.name for col in collections.collections]
            
            if self.collection_name not in collection_names:
                # Create collection with vector configuration for Azure OpenAI embeddings (1536 dimensions)
                self.qdrant_client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=1536,  # Azure OpenAI text-embedding-ada-002 dimension
                        distance=Distance.COSINE
                    )
                )
                self.logger.info(f"Created Qdrant collection: {self.collection_name}")
            else:
                self.logger.info(f"Qdrant collection exists: {self.collection_name}")
                
        except Exception as e:
            self.logger.error(f"Failed to ensure collection exists: {e}")
            raise
    
    def _generate_content_hash(self, content: str) -> str:
        """Generate a hash for content deduplication."""
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def _create_document_metadata(self, file_path: str, content_hash: str, 
                                 additional_metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """Create comprehensive metadata for the document."""
        metadata = {
            "file_path": file_path,
            "content_hash": content_hash,
            "ingestion_date": datetime.now().isoformat(),
            "tool_version": "vector_toolv2",
            "chunking_method": "llamaindex_sentence_splitter"
        }
        
        # Add additional metadata if provided
        if additional_metadata:
            metadata.update(additional_metadata)
        
        return metadata
    
    async def ingest_document(self, content: str, file_path: str, 
                            metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Ingest a document using LlamaIndex SentenceSplitter and store in Qdrant.
        
        Args:
            content: Text content to ingest
            file_path: Path/identifier for the document
            metadata: Additional metadata to store with the document
            
        Returns:
            Dictionary with ingestion results and statistics
        """
        try:
            self.logger.info(f"Starting document ingestion for: {file_path}")
            
            # Generate content hash for deduplication
            content_hash = self._generate_content_hash(content)
            
            # Create document metadata
            doc_metadata = self._create_document_metadata(file_path, content_hash, metadata)
            
            # Step 1: Create LlamaIndex Document
            document = Document(
                text=content,
                metadata=doc_metadata
            )
            
            # Step 2: Parse document into chunks using SentenceSplitter
            self.logger.info("Parsing document with SentenceSplitter...")
            nodes = self.sentence_splitter.get_nodes_from_documents([document])
            
            if not nodes:
                return {
                    "success": False,
                    "error": "No chunks created from document",
                    "stats": self.stats
                }
            
            self.stats["chunks_created"] += len(nodes)
            self.logger.info(f"Created {len(nodes)} chunks from document")
            
            # Step 3: Generate embeddings for all chunks
            self.logger.info("Generating embeddings...")
            embedded_nodes = []
            
            for node in nodes:
                # Generate embedding for the chunk
                embedding = await self.embedding_model.aget_text_embedding(node.text)
                
                # Create enhanced metadata for the chunk
                chunk_metadata = node.metadata.copy()
                chunk_metadata.update({
                    "chunk_id": node.node_id,
                    "chunk_length": len(node.text),
                    "chunk_word_count": len(node.text.split())
                })
                
                embedded_nodes.append({
                    "node": node,
                    "embedding": embedding,
                    "metadata": chunk_metadata
                })
            
            self.stats["embeddings_generated"] += len(embedded_nodes)
            self.logger.info(f"Generated {len(embedded_nodes)} embeddings")
            
            # Step 4: Store vectors in Qdrant
            self.logger.info("Storing vectors in Qdrant...")
            points = []
            
            for idx, embedded_node in enumerate(embedded_nodes):
                # Create unique point ID
                point_id = str(uuid.uuid4())
                
                # Create Qdrant point
                point = PointStruct(
                    id=point_id,
                    vector=embedded_node["embedding"],
                    payload={
                        "text": embedded_node["node"].text,
                        "metadata": embedded_node["metadata"]
                    }
                )
                points.append(point)
            
            # Batch upsert to Qdrant
            self.qdrant_client.upsert(
                collection_name=self.collection_name,
                points=points
            )
            
            self.stats["vectors_stored"] += len(points)
            self.stats["documents_processed"] += 1
            
            self.logger.info(f"Successfully ingested document: {file_path}",
                           chunks_created=len(nodes),
                           vectors_stored=len(points))
            
            return {
                "success": True,
                "file_path": file_path,
                "content_hash": content_hash,
                "chunks_created": len(nodes),
                "embeddings_generated": len(embedded_nodes),
                "vectors_stored": len(points),
                "stats": self.stats.copy()
            }
            
        except Exception as e:
            error_msg = f"Document ingestion failed for {file_path}: {str(e)}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
            
            return {
                "success": False,
                "error": error_msg,
                "file_path": file_path,
                "stats": self.stats.copy()
            }
    
    async def ingest(self, content: Any, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compatibility method for existing execution agent integration.
        
        Args:
            content: Text content to ingest
            metadata: Document metadata (must include doc_uri)
            
        Returns:
            Dictionary with ingestion results
        """
        # Extract file path from metadata
        file_path = metadata.get('doc_uri', f"unknown_document_{uuid.uuid4()}")
        
        # Convert content to string if needed
        if not isinstance(content, str):
            content = str(content)
        
        # Call the main ingestion method
        return await self.ingest_document(content, file_path, metadata)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current processing statistics."""
        return self.stats.copy()
    
    def reset_stats(self):
        """Reset processing statistics."""
        self.stats = {
            "documents_processed": 0,
            "chunks_created": 0,
            "embeddings_generated": 0,
            "vectors_stored": 0,
            "errors": []
        }
        self.logger.info("Processing statistics reset")
    
    @property
    def name(self) -> str:
        return "vector_ingestion_v2"
    
    @property
    def description(self) -> str:
        return "LlamaIndex-based vector ingestion tool using SentenceSplitter for intelligent document chunking and Qdrant for vector storage"
    
    @property
    def enabled(self) -> bool:
        return LLAMAINDEX_AVAILABLE and QDRANT_AVAILABLE


# Example usage and testing
async def main():
    """Test the VectorToolV2 implementation."""
    
    # Sample document content for testing
    sample_content = """
    Artificial Intelligence and Machine Learning in Healthcare
    
    The integration of artificial intelligence (AI) and machine learning (ML) technologies 
    in healthcare has revolutionized medical diagnosis and treatment. These advanced 
    computational methods enable healthcare professionals to analyze vast amounts of 
    medical data with unprecedented accuracy and speed.
    
    Machine learning algorithms can identify patterns in medical images, such as X-rays, 
    MRIs, and CT scans, often detecting abnormalities that might be missed by human 
    radiologists. This capability is particularly valuable in early disease detection, 
    where timely intervention can significantly improve patient outcomes.
    
    Natural language processing, a subset of AI, allows for the automated analysis of 
    clinical notes and medical literature. This technology helps in extracting relevant 
    information from unstructured text data, supporting evidence-based medical decisions.
    """
    
    try:
        # Initialize the tool
        tool = VectorToolV2()
        
        print("🚀 Testing VectorToolV2 - LlamaIndex Implementation")
        print(f"Tool: {tool.description}")
        print(f"Enabled: {tool.enabled}")
        
        if tool.enabled:
            # Test document ingestion
            result = await tool.ingest_document(
                content=sample_content,
                file_path="test_documents/ai_healthcare.md",
                metadata={
                    "source": "test",
                    "category": "healthcare_ai",
                    "author": "test_user"
                }
            )
            
            print(f"\n📊 Ingestion Result:")
            print(f"Success: {result['success']}")
            if result['success']:
                print(f"Chunks Created: {result['chunks_created']}")
                print(f"Embeddings Generated: {result['embeddings_generated']}")
                print(f"Vectors Stored: {result['vectors_stored']}")
            else:
                print(f"Error: {result['error']}")
            
            # Show statistics
            stats = tool.get_stats()
            print(f"\n📈 Tool Statistics:")
            for key, value in stats.items():
                print(f"  {key}: {value}")
        
        else:
            print("❌ Tool is not enabled - check dependencies")
    
    except Exception as e:
        print(f"❌ Test failed: {e}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
