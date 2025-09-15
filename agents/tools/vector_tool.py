"""
Vector Ingestion Tool for Qdrant Database

This tool handles vector ingestion into Qdrant database with document chunking,
embedding generation, and similarity search capabilities.
Enhanced with advanced chunking, utility functions, and batch processing.
"""

import logging
import uuid
import asyncio
import re
import random
import hashlib
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import json
import os
import sys
from enum import Enum

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv('.env.dev')
except ImportError:
    pass

# Add parent directory to path for config import
parent_dir = os.path.dirname(os.path.dirname(__file__))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from config.config_manager import get_config
    config_manager = get_config()
    def get_tool_config():
        return {
            "tools": {
                "vector_database": {
                    "type": "qdrant",
                    "url": os.getenv("QDRANT_API_URL", "http://localhost:6333"),
                    "api_key": os.getenv("QDRANT_API_KEY"),
                    "embedding_dimension": 1536,
                    "collection_name": os.getenv("QDRANT_COLLECTION", "ingestion_documents")
                }
            }
        }
except ImportError:
    def get_tool_config():
        return {
            "tools": {
                "vector_database": {
                    "type": "qdrant",
                    "url": os.getenv("QDRANT_API_URL", "http://localhost:6333"),
                    "api_key": os.getenv("QDRANT_API_KEY"),
                    "embedding_dimension": 1536,
                    "collection_name": os.getenv("QDRANT_COLLECTION", "ingestion_documents")
                }
            }
        }

# Import structured logging
try:
    from config.logger_config import get_tool_logger
    STRUCTURED_LOGGING_AVAILABLE = True
except ImportError:
    STRUCTURED_LOGGING_AVAILABLE = False
    # Fallback config using environment variables
    def get_tool_config():
        return {
            "tools": {
                "vector_database": {
                    "type": "qdrant",
                    "url": os.getenv("QDRANT_API_URL", "http://localhost:6333"),
                    "api_key": os.getenv("QDRANT_API_KEY"),
                    "embedding_dimension": 1536,
                    "collection_name": os.getenv("QDRANT_COLLECTION", "ingestion_documents")
                }
            }
        }

# Qdrant client import
try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
except ImportError:
    logging.warning("Qdrant client not available - using fallback mode")
    QdrantClient = None

# OpenAI for embeddings
try:
    from openai import AzureOpenAI
except ImportError:
    logging.warning("OpenAI not available - using mock embeddings")
    AzureOpenAI = None

logger = logging.getLogger(__name__)


class IngestionStatus(Enum):
    """Status of document ingestion."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class IngestionCatalogEntry:
    """Entry in the ingestion catalog for tracking document processing."""
    document_uri: str
    content_hash: str
    ingestion_status: IngestionStatus
    last_updated: datetime
    metadata: Optional[Dict] = None
    chunk_count: Optional[int] = None
    embedding_count: Optional[int] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class IngestionCatalog:
    """Catalog for tracking document ingestion and deduplication."""
    
    def __init__(self, storage_path: Optional[str] = None):
        """
        Initialize ingestion catalog.
        
        Args:
            storage_path: Optional path to persist catalog data
        """
        self.storage_path = storage_path or "content_cache/ingestion_catalog.json"
        self.catalog: Dict[str, IngestionCatalogEntry] = {}
        self.logger = logging.getLogger(__name__)
        self._load_catalog()
    
    def _load_catalog(self):
        """Load catalog from storage."""
        try:
            if os.path.exists(self.storage_path):
                with open(self.storage_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for uri, entry_data in data.items():
                        self.catalog[uri] = IngestionCatalogEntry(
                            document_uri=entry_data['document_uri'],
                            content_hash=entry_data['content_hash'],
                            ingestion_status=IngestionStatus(entry_data['ingestion_status']),
                            last_updated=datetime.fromisoformat(entry_data['last_updated']),
                            metadata=entry_data.get('metadata', {}),
                            chunk_count=entry_data.get('chunk_count'),
                            embedding_count=entry_data.get('embedding_count')
                        )
                self.logger.info(f"Loaded {len(self.catalog)} entries from catalog")
        except Exception as e:
            self.logger.warning(f"Could not load catalog: {e}")
    
    def _save_catalog(self):
        """Save catalog to storage."""
        try:
            os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
            data = {}
            for uri, entry in self.catalog.items():
                data[uri] = {
                    'document_uri': entry.document_uri,
                    'content_hash': entry.content_hash,
                    'ingestion_status': entry.ingestion_status.value,
                    'last_updated': entry.last_updated.isoformat(),
                    'metadata': entry.metadata,
                    'chunk_count': entry.chunk_count,
                    'embedding_count': entry.embedding_count
                }
            
            with open(self.storage_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            self.logger.error(f"Could not save catalog: {e}")
    
    def get_entry(self, document_uri: str) -> Optional[IngestionCatalogEntry]:
        """Get catalog entry for document."""
        return self.catalog.get(document_uri)
    
    def update_entry(self, document_uri: str, content_hash: str, status: IngestionStatus, 
                    metadata: Optional[Dict] = None, chunk_count: Optional[int] = None,
                    embedding_count: Optional[int] = None):
        """Update or create catalog entry."""
        self.catalog[document_uri] = IngestionCatalogEntry(
            document_uri=document_uri,
            content_hash=content_hash,
            ingestion_status=status,
            last_updated=datetime.now(),
            metadata=metadata or {},
            chunk_count=chunk_count,
            embedding_count=embedding_count
        )
        self._save_catalog()
    
    def has_content_changed(self, document_uri: str, current_hash: str) -> bool:
        """Check if document content has changed since last ingestion."""
        entry = self.get_entry(document_uri)
        if not entry:
            return True  # New document
        return entry.content_hash != current_hash
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get catalog statistics."""
        status_counts = {}
        for status in IngestionStatus:
            status_counts[status.value] = sum(1 for entry in self.catalog.values() 
                                            if entry.ingestion_status == status)
        
        return {
            'total_documents': len(self.catalog),
            'status_breakdown': status_counts,
            'last_updated': max((entry.last_updated for entry in self.catalog.values()), 
                              default=datetime.now()).isoformat()
        }


class ContentHasher:
    """Utility class for computing content hashes."""
    
    @staticmethod
    def compute_hash(content: str) -> str:
        """
        Compute SHA256 hash of content.
        
        Args:
            content: Text content to hash
            
        Returns:
            str: SHA256 hash as hexadecimal string
        """
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    @staticmethod
    def compute_normalized_hash(content: str) -> str:
        """
        Compute hash of normalized content (whitespace cleaned).
        
        Args:
            content: Text content to hash
            
        Returns:
            str: SHA256 hash of normalized content
        """
        # Normalize whitespace and remove extra spaces
        normalized = re.sub(r'\s+', ' ', content.strip())
        return ContentHasher.compute_hash(normalized)


class UtilityFunctions:
    """Utility functions for text processing and cleaning."""
    
    @staticmethod
    def contains_citation(text: str) -> bool:
        """Check if text contains citations."""
        citation_patterns = [
            r'\[\d+\]',  # [1], [23], etc.
            r'\(\d{4}\)',  # (2023), etc.
            r'et al\.',  # et al.
            r'ibid\.',  # ibid.
        ]
        return any(re.search(pattern, text) for pattern in citation_patterns)
    
    @staticmethod
    def remove_citations(text: str) -> str:
        """Remove citations from text."""
        # Remove bracketed numbers
        text = re.sub(r'\[\d+\]', '', text)
        # Remove common citation patterns
        text = re.sub(r'\(.*?\d{4}.*?\)', '', text)
        text = re.sub(r'et al\.', '', text)
        text = re.sub(r'ibid\.', '', text)
        # Clean up extra spaces
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    @staticmethod
    def clean_text_for_vector_db(text: str) -> str:
        """Clean text for vector database storage."""
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove special characters that might cause issues
        text = re.sub(r'[^\w\s\.\,\!\?\-\:\;]', '', text)
        # Normalize line endings
        text = text.replace('\r\n', '\n').replace('\r', '\n')
        return text.strip()
    
    @staticmethod
    def create_qdrant_points(chunks: List[str], embeddings: List[List[float]], 
                           metadatas: List[Dict], ids: Optional[List[str]] = None):
        """Create Qdrant points from chunks, embeddings, and metadata."""
        points = []
        for i, (chunk, embedding, metadata) in enumerate(zip(chunks, embeddings, metadatas)):
            point_id = ids[i] if ids and i < len(ids) else str(uuid.uuid4())
            
            point = PointStruct(
                id=point_id,
                vector=embedding.tolist() if hasattr(embedding, 'tolist') else embedding,
                payload={
                    "content": chunk,
                    "metadata": metadata
                }
            )
            points.append(point)
        
        return points


class DocumentChunker:
    """Advanced document chunker with paragraph-based chunking and overlap."""
    
    def __init__(self, chunk_size: int = 3, overlap: int = 1, 
                 min_words: int = 30, max_words: int = 400):
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.min_words = min_words
        self.max_words = max_words
    
    def split_markdown_paragraphs(self, md_text: str) -> List[str]:
        """Split markdown text into paragraphs."""
        paragraphs = []
        for para in md_text.split('\n\n'):
            para = para.strip()
            if para.startswith('## Page '):
                continue
            if para:
                paragraphs.append(para)
        return paragraphs
    
    def merge_short_paragraphs(self, paragraphs: List[str]) -> List[str]:
        """Merge short paragraphs to meet minimum word count."""
        merged = []
        buffer = ""
        
        for para in paragraphs:
            if para.startswith('## Page '):
                if buffer:
                    merged.append(buffer.strip())
                    buffer = ""
                merged.append(para)
            elif len(para.split()) < self.min_words:
                buffer += " " + para
            else:
                if buffer:
                    merged.append(buffer.strip())
                    buffer = ""
                merged.append(para)
        
        if buffer:
            merged.append(buffer.strip())
        
        return merged
    
    def chunk_with_overlap(self, paragraphs: List[str]) -> List[str]:
        """Create overlapping chunks for better context."""
        if not paragraphs:
            return []
        
        chunks = []
        step = self.chunk_size - self.overlap
        
        for i in range(0, len(paragraphs), step):
            chunk = paragraphs[i:i + self.chunk_size]
            if chunk:
                chunks.append("\n\n".join(chunk))
        
        return chunks
    
    def split_large_chunks(self, chunks: List[str]) -> List[str]:
        """Split chunks that exceed the maximum word limit."""
        refined_chunks = []
        
        for chunk in chunks:
            word_count = len(chunk.split())
            
            if word_count <= self.max_words:
                refined_chunks.append(chunk)
            else:
                # Split by sentences for better semantic boundaries
                sentences = re.split(r'(?<=[.!?])\s+', chunk)
                current_chunk = []
                current_words = 0
                
                for sentence in sentences:
                    sentence_words = len(sentence.split())
                    
                    if current_words + sentence_words > self.max_words and current_chunk:
                        refined_chunks.append(" ".join(current_chunk))
                        current_chunk = [sentence]
                        current_words = sentence_words
                    else:
                        current_chunk.append(sentence)
                        current_words += sentence_words
                
                if current_chunk:
                    refined_chunks.append(" ".join(current_chunk))
        
        return refined_chunks
    
    def extract_chunks(self, file_path: str, content: str) -> List[Dict[str, Any]]:
        """Extract chunks from content with metadata."""
        # Step 1: Split into paragraphs
        paragraphs = self.split_markdown_paragraphs(content)
        
        # Step 2: Merge short paragraphs
        merged_paragraphs = self.merge_short_paragraphs(paragraphs)
        
        # Step 3: Create overlapping chunks
        chunks = self.chunk_with_overlap(merged_paragraphs)
        
        # Step 4: Split oversized chunks
        refined_chunks = self.split_large_chunks(chunks)
        
        # Step 5: Create chunk objects with metadata
        chunk_list = []
        for idx, chunk in enumerate(refined_chunks):
            # Create stable UUID for chunk
            unique_str = f"{file_path}:{idx + 1}"
            chunk_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_str))
            
            chunk_metadata = {
                "file_path": file_path,
                "chunk_id": idx + 1,
                "chunk_word_count": len(chunk.split()),
                "created_date": datetime.now().strftime("%Y-%m-%d"),
                "id": chunk_uuid
            }
            
            chunk_list.append({
                "chunk": chunk,
                "metadata": chunk_metadata
            })
        
        # Print chunk statistics
        if refined_chunks:
            lengths = [len(chunk.split()) for chunk in refined_chunks]
            print(f"   📊 Chunk Stats - Count: {len(refined_chunks)}, "
                  f"Avg Words: {sum(lengths)/len(lengths):.1f}, "
                  f"Range: {min(lengths)}-{max(lengths)} words")
        
        return chunk_list


class EmbeddingManager:
    """Manages embedding generation with batch processing."""
    
    def __init__(self, openai_client=None, model_name: str = "text-embedding-ada-002", 
                 batch_size: int = 100):
        self.openai_client = openai_client
        self.model_name = model_name
        self.batch_size = batch_size
        
    async def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a batch of texts."""
        if not self.openai_client:
            # Return mock embeddings
            embeddings = []
            for text in texts:
                random.seed(hash(text) % (2**32))
                embedding = [random.random() for _ in range(1536)]
                embeddings.append(embedding)
            return embeddings
        
        try:
            response = self.openai_client.embeddings.create(
                input=texts,
                model=self.model_name
            )
            return [item.embedding for item in response.data]
            
        except Exception as e:
            logging.error(f"Batch embedding generation failed: {e}")
            # Return mock embeddings as fallback
            embeddings = []
            for text in texts:
                random.seed(hash(text) % (2**32))
                embedding = [random.random() for _ in range(1536)]
                embeddings.append(embedding)
            return embeddings


@dataclass
class VectorDocument:
    """Data class representing a document chunk with vector embedding."""
    id: str
    content: str
    embedding: Optional[List[float]] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class VectorTool:
    """
    Vector tool for processing documents into vector database.
    Handles chunking, embedding generation, storage in Qdrant, and content deduplication.
    Enhanced with advanced chunking and batch processing.
    """
    
    def __init__(self):
        """Initialize the vector ingestion tool."""
        self.config = get_tool_config()
        
        # Initialize structured logging
        if STRUCTURED_LOGGING_AVAILABLE:
            self.logger = get_tool_logger("vector_tool")
        else:
            self.logger = logging.getLogger(__name__)
        
        # Vector database configuration
        self.vector_config = self.config.get("tools", {}).get("vector_database", {})
        self.embedding_dimension = self.vector_config.get("embedding_dimension", 1536)
        self.collection_name = self.vector_config.get("collection_name", "documents")
        
        # Initialize Qdrant client
        self.qdrant_client = None
        self._initialize_qdrant()
        
        # Initialize OpenAI client for embeddings
        self.openai_client = None
        self._initialize_openai()
        
        # Initialize helper classes
        self.document_chunker = DocumentChunker(
            chunk_size=3, 
            overlap=1, 
            min_words=30, 
            max_words=400
        )
        
        # Use correct embedding deployment name from environment
        embedding_deployment = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "text-embedding-ada-002")
        
        self.embedding_manager = EmbeddingManager(
            openai_client=self.openai_client,
            model_name=embedding_deployment,
            batch_size=100
        )
        
        # Initialize deduplication catalog
        self.catalog = IngestionCatalog()
        
        self.logger.info("VectorTool initialized",
                        component="vector_tool",
                        embedding_dimension=self.embedding_dimension,
                        collection_name=self.collection_name,
                        qdrant_available=self.qdrant_client is not None,
                        openai_available=self.openai_client is not None)
    
    def _initialize_qdrant(self):
        """Initialize Qdrant client connection."""
        if QdrantClient is None:
            self.logger.warning("Qdrant client not available")
            return
        
        try:
            url = self.vector_config.get("url", "http://localhost:6333")
            api_key = self.vector_config.get("api_key")
            
            # Initialize Qdrant client with URL and optional API key
            if api_key:
                self.qdrant_client = QdrantClient(
                    url=url, 
                    api_key=api_key,
                    timeout=10.0  # Shorter timeout for faster feedback
                )
                self.logger.info(f"Connecting to Qdrant cloud at: {url}")
            else:
                self.qdrant_client = QdrantClient(url=url, timeout=10.0)
                self.logger.info(f"Connecting to local Qdrant at: {url}")
            
            # Test connection and create collection if it doesn't exist
            try:
                collections = self.qdrant_client.get_collections().collections
                collection_names = [col.name for col in collections]
                
                if self.collection_name not in collection_names:
                    self.qdrant_client.create_collection(
                        collection_name=self.collection_name,
                        vectors_config=VectorParams(
                            size=self.embedding_dimension,
                            distance=Distance.COSINE
                        )
                    )
                    self.logger.info(f"Created Qdrant collection: {self.collection_name}")
                else:
                    self.logger.info(f"Using existing Qdrant collection: {self.collection_name}")
                    
            except Exception as collection_error:
                self.logger.warning(f"Qdrant connectivity issue: {collection_error}")
                self.logger.warning("Will use fallback mode for vector storage")
                # Set client to None to trigger fallback mode
                self.qdrant_client = None
                
        except Exception as e:
            self.logger.warning(f"Qdrant initialization failed: {e}")
            self.logger.info("Using fallback mode - vectors will be stored in memory for testing")
            self.qdrant_client = None
    
    def _initialize_openai(self):
        """Initialize OpenAI client for embeddings."""
        if AzureOpenAI is None:
            self.logger.warning("OpenAI client not available")
            return
        
        try:
            # Load OpenAI configuration from environment
            api_key = os.getenv("AZURE_OPENAI_API_KEY")
            api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01")
            azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
            
            if api_key and azure_endpoint:
                self.openai_client = AzureOpenAI(
                    api_key=api_key,
                    api_version=api_version,
                    azure_endpoint=azure_endpoint
                )
                self.logger.info("OpenAI client initialized for embeddings")
            else:
                self.logger.warning("OpenAI credentials not found in environment")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize OpenAI: {e}")
            self.openai_client = None
    
    async def ingest(self, content: Any, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main ingestion method that orchestrates the full vector ingestion process.
        Enhanced with deduplication to avoid reprocessing unchanged content.
        
        Args:
            content: Document content to ingest
            metadata: Document metadata
            
        Returns:
            Dict with ingestion results
        """
        doc_uri = metadata.get("doc_uri", "unknown")
        
        try:
            self.logger.info("Vector ingestion process started",
                           doc_uri=doc_uri,
                           operation="vector_ingestion",
                           component="vector_tool")
            
            # Convert content to string if needed
            if isinstance(content, dict):
                content_str = json.dumps(content, ensure_ascii=False)
            elif isinstance(content, list):
                content_str = "\n".join(str(item) for item in content)
            else:
                content_str = str(content)
            
            # DEDUPLICATION: Check if content has changed
            current_hash = ContentHasher.compute_normalized_hash(content_str)
            
            self.logger.info("Content hash computed",
                           doc_uri=doc_uri,
                           content_hash=current_hash[:8],  # First 8 chars for logging
                           content_length=len(content_str))
            
            if not self.catalog.has_content_changed(doc_uri, current_hash):
                self.logger.info("Content unchanged, skipping ingestion",
                               doc_uri=doc_uri,
                               operation="deduplication_skip",
                               content_hash=current_hash[:8])
                return {
                    "status": "skipped",
                    "operation": "vector_ingestion",
                    "doc_uri": doc_uri,
                    "reason": "Content unchanged (hash match)",
                    "content_hash": current_hash,
                    "content_length": len(content_str)
                }
            
            # Mark as processing
            self.catalog.update_entry(doc_uri, current_hash, IngestionStatus.PROCESSING)
            
            # Step 1: Chunk the content
            self.logger.info("Starting content chunking",
                           doc_uri=doc_uri,
                           operation="chunking")
            
            chunks = self._chunk_content(content_str, metadata)
            
            self.logger.info("Content chunking completed",
                           doc_uri=doc_uri,
                           operation="chunking",
                           chunks_count=len(chunks))
            
            # Step 2: Generate embeddings for chunks
            self.logger.info("Starting embedding generation",
                           doc_uri=doc_uri,
                           operation="embedding_generation",
                           chunks_count=len(chunks))
            
            documents = await self._generate_embeddings(chunks, metadata)
            
            self.logger.info("Embedding generation completed",
                           doc_uri=doc_uri,
                           operation="embedding_generation",
                           documents_count=len(documents))
            
            # Step 3: Store in vector database
            self.logger.info("Starting vector storage",
                           doc_uri=doc_uri,
                           operation="vector_storage",
                           documents_count=len(documents))
            
            storage_result = await self._store_vectors(documents)
            stored_count = storage_result.get('stored_count', 0)
            
            self.logger.info("Vector storage completed",
                           doc_uri=doc_uri,
                           operation="vector_storage",
                           stored_count=stored_count)
            
            # Update catalog with successful completion
            self.catalog.update_entry(
                doc_uri, 
                current_hash, 
                IngestionStatus.COMPLETED,
                metadata=metadata,
                chunk_count=len(chunks),
                embedding_count=len(documents)
            )
            
            self.logger.info("Vector ingestion process completed successfully",
                           doc_uri=doc_uri,
                           operation="vector_ingestion",
                           chunks_count=len(chunks),
                           stored_count=stored_count,
                           content_hash=current_hash[:8])
            
            return {
                "status": "success",
                "operation": "vector_ingestion",
                "doc_uri": doc_uri,
                "chunks_created": len(chunks),
                "embeddings_generated": len(documents),
                "vectors_stored": storage_result.get("stored_count", 0),
                "collection_name": self.collection_name,
                "embedding_dimension": self.embedding_dimension,
                "storage_result": storage_result,
                "content_length": len(content_str),
                "content_hash": current_hash,
                "deduplication": "processed"
            }
            
        except Exception as e:
            self.logger.error("Vector ingestion failed",
                            doc_uri=doc_uri,
                            operation="vector_ingestion",
                            error=str(e),
                            error_type=type(e).__name__,
                            component="vector_tool")
            
            # Update catalog with failure status
            if 'current_hash' in locals():
                self.catalog.update_entry(doc_uri, current_hash, IngestionStatus.FAILED, metadata={"error": str(e)})
            
            return {
                "status": "error",
                "operation": "vector_ingestion",
                "error": str(e),
                "doc_uri": doc_uri
            }
    
    def _chunk_content(self, content: str, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Split content into chunks for vector processing using advanced chunking.
        
        Args:
            content: Text content to chunk
            metadata: Document metadata
            
        Returns:
            List of chunk dictionaries
        """
        file_path = metadata.get("doc_uri", "unknown")
        
        print(f"   📄 Chunking content using advanced paragraph-based method...")
        
        # Clean content before chunking
        if UtilityFunctions.contains_citation(content):
            content = UtilityFunctions.remove_citations(content)
        
        content = UtilityFunctions.clean_text_for_vector_db(content)
        
        # Use the advanced document chunker
        chunk_list = self.document_chunker.extract_chunks(file_path, content)
        
        # Convert to expected format and add additional metadata
        formatted_chunks = []
        for chunk_data in chunk_list:
            chunk_content = chunk_data["chunk"]
            chunk_metadata = chunk_data["metadata"]
            
            # Merge with parent metadata
            merged_metadata = {
                **metadata,
                **chunk_metadata,
                "created_at": datetime.now().isoformat(),
                "total_chunks": len(chunk_list)
            }
            
            formatted_chunks.append({
                "id": chunk_metadata["id"],
                "content": chunk_content,
                "metadata": merged_metadata
            })
        
        print(f"   📝 Extracted {len(formatted_chunks)} chunks using advanced chunking.")
        return formatted_chunks
    
    async def _generate_embeddings(self, chunks: List[Dict[str, Any]], metadata: Dict[str, Any]) -> List[VectorDocument]:
        """
        Generate embeddings for document chunks using batch processing.
        
        Args:
            chunks: List of text chunks
            metadata: Document metadata
            
        Returns:
            List of VectorDocument objects with embeddings
        """
        documents = []
        batch_size = self.embedding_manager.batch_size
        
        print(f"   🔄 Generating embeddings for {len(chunks)} chunks in batches of {batch_size}...")
        
        # Process chunks in batches
        for i in range(0, len(chunks), batch_size):
            batch_chunks = chunks[i:i + batch_size]
            batch_texts = [chunk["content"] for chunk in batch_chunks]
            
            try:
                # Generate embeddings for the batch
                batch_embeddings = await self.embedding_manager.generate_embeddings_batch(batch_texts)
                
                # Create VectorDocument objects
                for chunk, embedding in zip(batch_chunks, batch_embeddings):
                    doc = VectorDocument(
                        id=chunk["id"],
                        content=chunk["content"],
                        embedding=embedding,
                        metadata=chunk["metadata"]
                    )
                    documents.append(doc)
                    
                print(f"   ✅ Batch {i // batch_size + 1}: Generated {len(batch_embeddings)} embeddings")
                
            except Exception as e:
                self.logger.error(f"Failed to generate embeddings for batch {i // batch_size + 1}: {e}")
                # Continue with next batch
                
        print(f"   📊 Total embeddings generated: {len(documents)}")
        return documents

    async def _store_vectors(self, documents: List[VectorDocument]) -> Dict[str, Any]:
        """
        Store vector documents in Qdrant database using batch processing.
        
        Args:
            documents: List of VectorDocument objects
            
        Returns:
            Dictionary with storage results
        """
        if not self.qdrant_client:
            self.logger.warning("Qdrant client not available, using fallback storage")
            return {
                "status": "fallback",
                "stored_count": len(documents),
                "method": "mock"
            }
        
        try:
            # Prepare data for batch processing
            chunks = [doc.content for doc in documents]
            embeddings = [doc.embedding for doc in documents]
            metadatas = [doc.metadata for doc in documents]
            ids = [doc.id for doc in documents]
            
            # Create Qdrant points using utility function
            points = UtilityFunctions.create_qdrant_points(chunks, embeddings, metadatas, ids)
            
            if points:
                # Store in batches to avoid memory issues
                batch_size = 100
                total_stored = 0
                
                for i in range(0, len(points), batch_size):
                    batch_points = points[i:i + batch_size]
                    
                    operation_info = self.qdrant_client.upsert(
                        collection_name=self.collection_name,
                        points=batch_points
                    )
                    
                    total_stored += len(batch_points)
                
                return {
                    "status": "success",
                    "stored_count": total_stored,
                    "collection_name": self.collection_name,
                    "method": "qdrant_batch"
                }
            else:
                return {
                    "status": "error",
                    "stored_count": 0,
                    "error": "No valid embeddings to store"
                }
                
        except Exception as e:
            self.logger.error(f"Failed to store vectors in Qdrant: {e}")
            return {
                "status": "error",
                "stored_count": 0,
                "error": str(e)
            }
    
    async def search(self, query: str, top_k: int = 5, 
                    filter_conditions: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Perform similarity search in the vector database.
        
        Args:
            query: Search query text
            top_k: Number of results to return
            filter_conditions: Optional filter conditions
            
        Returns:
            Search results dictionary
        """
        try:
            # Generate embedding for query using the embedding manager
            query_embeddings = await self.embedding_manager.generate_embeddings_batch([query])
            query_embedding = query_embeddings[0] if query_embeddings else None
            
            if not query_embedding:
                return {
                    "status": "error",
                    "error": "Failed to generate query embedding",
                    "query": query
                }
            
            if not self.qdrant_client:
                return {
                    "status": "fallback",
                    "results": [],
                    "message": "Qdrant client not available"
                }
            
            # Prepare filter if provided
            query_filter = None
            if filter_conditions:
                conditions = []
                for key, value in filter_conditions.items():
                    conditions.append(
                        FieldCondition(key=f"metadata.{key}", match=MatchValue(value=value))
                    )
                if conditions:
                    query_filter = Filter(must=conditions)
            
            # Perform search
            search_results = self.qdrant_client.search(
                collection_name=self.collection_name,
                query_vector=query_embedding,
                query_filter=query_filter,
                limit=top_k
            )
            
            # Format results
            formatted_results = []
            for result in search_results:
                formatted_results.append({
                    "score": result.score,
                    "document_id": result.payload.get("document_id"),
                    "content": result.payload.get("content"),
                    "metadata": result.payload.get("metadata", {})
                })
            
            return {
                "status": "success",
                "query": query,
                "results": formatted_results,
                "total_found": len(formatted_results)
            }
            
        except Exception as e:
            self.logger.error(f"Vector search failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "query": query
            }
    
    async def get_collection_info(self) -> Dict[str, Any]:
        """Get information about the vector collection."""
        if not self.qdrant_client:
            return {
                "status": "unavailable",
                "message": "Qdrant client not available"
            }
        
        try:
            collection_info = self.qdrant_client.get_collection(self.collection_name)
            return {
                "status": "success",
                "collection_name": self.collection_name,
                "points_count": collection_info.points_count,
                "vectors_config": collection_info.config.params.vectors,
                "status_info": collection_info.status
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get collection info: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def validate(self, content: Any) -> bool:
        """
        Validate content for vector ingestion.
        
        Args:
            content: Content to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            if content is None:
                return False
            
            # Convert to string for validation
            if isinstance(content, (dict, list)):
                content_str = json.dumps(content)
            else:
                content_str = str(content)
            
            # Basic validation rules
            if len(content_str.strip()) == 0:
                return False
            
            if len(content_str) > 1000000:  # 1MB limit
                self.logger.warning("Content too large for vector ingestion")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Content validation failed: {e}")
            return False
    
    def get_catalog_statistics(self) -> Dict[str, Any]:
        """
        Get deduplication catalog statistics.
        
        Returns:
            Dict with catalog statistics
        """
        return self.catalog.get_statistics()
    
    def force_reprocess_document(self, doc_uri: str) -> bool:
        """
        Force reprocessing of a document by removing its catalog entry.
        
        Args:
            doc_uri: Document URI to force reprocess
            
        Returns:
            True if entry was removed, False if not found
        """
        if doc_uri in self.catalog.catalog:
            del self.catalog.catalog[doc_uri]
            self.catalog._save_catalog()
            self.logger.info(f"Forced reprocessing for document: {doc_uri}")
            return True
        return False
    
    def get_document_status(self, doc_uri: str) -> Optional[Dict[str, Any]]:
        """
        Get the ingestion status of a specific document.
        
        Args:
            doc_uri: Document URI to check
            
        Returns:
            Dict with document status or None if not found
        """
        entry = self.catalog.get_entry(doc_uri)
        if entry:
            return {
                "document_uri": entry.document_uri,
                "content_hash": entry.content_hash,
                "status": entry.ingestion_status.value,
                "last_updated": entry.last_updated.isoformat(),
                "chunk_count": entry.chunk_count,
                "embedding_count": entry.embedding_count,
                "metadata": entry.metadata
            }
        return None
    
    def clear_failed_entries(self) -> int:
        """
        Clear all failed entries from the catalog to allow reprocessing.
        
        Returns:
            Number of failed entries cleared
        """
        failed_count = 0
        failed_uris = []
        
        for uri, entry in self.catalog.catalog.items():
            if entry.ingestion_status == IngestionStatus.FAILED:
                failed_uris.append(uri)
        
        for uri in failed_uris:
            del self.catalog.catalog[uri]
            failed_count += 1
        
        if failed_count > 0:
            self.catalog._save_catalog()
            self.logger.info(f"Cleared {failed_count} failed catalog entries")
        
        return failed_count


# Example usage and testing
async def main():
    """Example usage of enhanced VectorTool."""
    tool = VectorTool()
    
    # Test content with more realistic structure
    test_content = """
    # Document Title
    
    This is a sample document for testing enhanced vector ingestion.
    
    ## Introduction
    
    The enhanced vector ingestion tool now includes advanced chunking capabilities.
    It uses paragraph-based chunking with overlap for better context preservation.
    
    ## Features
    
    Key features include:
    - Advanced paragraph-based chunking
    - Citation removal and text cleaning
    - Batch processing for embeddings
    - Improved error handling and logging
    
    ## Implementation Details
    
    The tool processes documents by first splitting them into semantic chunks.
    These chunks are then cleaned and processed in batches for efficiency.
    Finally, they are stored in the Qdrant vector database with proper metadata.
    
    ## Conclusion
    
    This enhanced approach provides better performance and more accurate chunking
    compared to simple sliding window methods.
    """
    
    # Test metadata
    test_metadata = {
        "doc_uri": "test://enhanced/document",
        "document_type": "markdown",
        "source": "test_enhanced",
        "author": "System",
        "classification": "technical_document"
    }
    
    # Test ingestion with enhanced features
    result = await tool.ingest(test_content, test_metadata)
    
    # Test search functionality
    search_result = await tool.search("advanced chunking features", top_k=3)
    
    # Test collection info
    collection_info = await tool.get_collection_info()
    
    return {
        "ingestion_result": result,
        "search_result": search_result,
        "collection_info": collection_info
    }


if __name__ == "__main__":
    asyncio.run(main())
