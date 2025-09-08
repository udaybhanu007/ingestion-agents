"""
Vector Ingestion Tool for Qdrant

This tool handles ingestion of content into Qdrant vector database with simplified chunking.
"""

import logging
import asyncio
import sys
import os
from typing import Dict, Any, List, Optional
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from langchain_openai import AzureOpenAIEmbeddings
from pydantic import SecretStr
import uuid

# Add parent directory to path for config import
parent_dir = os.path.dirname(os.path.dirname(__file__))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from config import get_config

# Import the simplified document chunker
try:
    from document_chunker import DocumentChunker, clean_text_for_vector
    CHUNKER_AVAILABLE = True
except ImportError:
    # Fallback if chunker not available
    DocumentChunker = None
    CHUNKER_AVAILABLE = False
    def clean_text_for_vector(text):
        return text

logger = logging.getLogger(__name__)


class VectorIngestionTool:
    """Simplified tool for ingesting content into Qdrant vector database"""
    
    def __init__(self):
        self.config = get_config()
        self._client: Optional[QdrantClient] = None
        self._embeddings: Optional[AzureOpenAIEmbeddings] = None
        self._chunker = None
    
    @property
    def name(self) -> str:
        return "vector_ingest"
    
    @property
    def description(self) -> str:
        return "Ingests structured and unstructured content into Qdrant vector database"
    
    @property
    def client(self) -> QdrantClient:
        """Get or create Qdrant client"""
        if self._client is None:
            self._client = QdrantClient(
                url=self.config.qdrant_api_url,
                api_key=self.config.qdrant_api_key
            )
        return self._client
    
    @property
    def embeddings(self) -> AzureOpenAIEmbeddings:
        """Get or create embeddings client"""
        if self._embeddings is None:
            self._embeddings = AzureOpenAIEmbeddings(
                azure_endpoint=self.config.azure_openai_endpoint,
                api_key=self.config.azure_openai_api_key,
                azure_deployment=self.config.azure_openai_embedding_deployment,
                api_version=self.config.azure_openai_api_version
            )
        return self._embeddings
    
    @property
    def chunker(self):
        """Get or create document chunker"""
        if CHUNKER_AVAILABLE and self._chunker is None and DocumentChunker is not None:
            self._chunker = DocumentChunker(
                chunk_size=3,
                overlap=1,
                min_words=30,
                max_words=400
            )
        return self._chunker
    
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
    
    async def ensure_collection_exists(self) -> None:
        """Ensure the Qdrant collection exists"""
        try:
            collections = self.client.get_collections()
            collection_names = [col.name for col in collections.collections]
            
            if self.config.qdrant_collection not in collection_names:
                logger.info(f"Creating collection: {self.config.qdrant_collection}")
                
                self.client.create_collection(
                    collection_name=self.config.qdrant_collection,
                    vectors_config=VectorParams(
                        size=1536,  # Azure OpenAI text-embedding-3-small dimension
                        distance=Distance.COSINE
                    )
                )
                logger.info(f"Collection {self.config.qdrant_collection} created successfully")
            else:
                # Check if existing collection has correct dimensions
                collection_info = self.client.get_collection(self.config.qdrant_collection)
                existing_size = collection_info.config.params.vectors.size
                
                if existing_size != 1536:
                    logger.info(f"Collection {self.config.qdrant_collection} has {existing_size} dimensions, recreating with 1536")
                    
                    # Delete and recreate with correct dimensions
                    self.client.delete_collection(self.config.qdrant_collection)
                    self.client.create_collection(
                        collection_name=self.config.qdrant_collection,
                        vectors_config=VectorParams(
                            size=1536,
                            distance=Distance.COSINE
                        )
                    )
                    logger.info(f"Collection {self.config.qdrant_collection} recreated with correct dimensions")
                else:
                    logger.info(f"Collection {self.config.qdrant_collection} already exists with correct dimensions")
                
        except Exception as e:
            logger.error(f"Error ensuring collection exists: {e}")
            raise
    
    def _prepare_chunks(self, content: str, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Prepare content chunks using the simplified chunker"""
        try:
            # Use the simplified chunker if available
            if self.chunker:
                source_file = metadata.get('source', metadata.get('file_path', 'unknown'))
                chunks = self.chunker.chunk_document(content, source_file)
                logger.info(f"Created {len(chunks)} chunks using DocumentChunker")
                return chunks
            else:
                # Fallback to simple splitting
                logger.warning("DocumentChunker not available, using simple fallback")
                words = content.split()
                chunk_size = 300  # words
                chunks = []
                
                for i in range(0, len(words), chunk_size):
                    chunk_words = words[i:i + chunk_size]
                    chunk_text = ' '.join(chunk_words)
                    
                    chunk_metadata = {
                        "id": str(uuid.uuid4()),
                        "chunk_index": i // chunk_size,
                        "word_count": len(chunk_words),
                        "source": metadata.get('source', 'unknown')
                    }
                    
                    chunks.append({
                        "content": chunk_text,
                        "metadata": chunk_metadata
                    })
                
                logger.info(f"Created {len(chunks)} chunks using fallback method")
                return chunks
                
        except Exception as e:
            logger.error(f"Error preparing chunks: {e}")
            # Return single chunk as fallback
            return [{
                "content": content,
                "metadata": {
                    "id": str(uuid.uuid4()),
                    "chunk_index": 0,
                    "word_count": len(content.split()),
                    "source": metadata.get('source', 'unknown')
                }
            }]
    
    async def _embed_chunks(self, chunks: List[Dict[str, Any]]) -> List[List[float]]:
        """Generate embeddings for chunks"""
        try:
            texts = [clean_text_for_vector(chunk["content"]) for chunk in chunks]
            embeddings = await asyncio.get_event_loop().run_in_executor(
                None, self.embeddings.embed_documents, texts
            )
            return embeddings
        except Exception as e:
            logger.error(f"Error generating embeddings: {e}")
            raise
    
    async def ingest(self, content: Any, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Ingest content into Qdrant"""
        try:
            # Validate content
            if not await self.validate(content):
                raise ValueError("Content validation failed")
            
            # Ensure collection exists
            await self.ensure_collection_exists()
            
            # Convert content to string
            content_str = str(content)
            
            # Prepare chunks using simplified chunker
            chunks = self._prepare_chunks(content_str, metadata)
            
            if not chunks:
                raise ValueError("No chunks generated from content")
            
            # Generate embeddings
            embeddings = await self._embed_chunks(chunks)
            
            # Prepare points for Qdrant
            points = []
            for chunk, embedding in zip(chunks, embeddings):
                chunk_metadata = chunk["metadata"]
                
                # Prepare payload with all metadata
                payload = {
                    "content": chunk["content"],
                    **chunk_metadata,
                    **metadata  # Include original metadata
                }
                
                point = PointStruct(
                    id=chunk_metadata.get("id", str(uuid.uuid4())),
                    vector=embedding,
                    payload=payload
                )
                points.append(point)
            
            # Upsert points to Qdrant
            operation_info = self.client.upsert(
                collection_name=self.config.qdrant_collection,
                points=points
            )
            
            logger.info(f"Successfully ingested {len(points)} chunks into Qdrant")
            
            return {
                "status": "success",
                "ingested_chunks": len(points),
                "collection": self.config.qdrant_collection,
                "operation_id": operation_info.operation_id if hasattr(operation_info, 'operation_id') else None,
                "chunking_method": "DocumentChunker" if self.chunker else "fallback"
            }
            
        except Exception as e:
            logger.error(f"Error ingesting content into Qdrant: {e}")
            return {
                "status": "error",
                "error": str(e),
                "ingested_chunks": 0
            }
    
    async def search(self, query: str, limit: int = 10, score_threshold: float = 0.7) -> List[Dict[str, Any]]:
        """Search for similar content in Qdrant"""
        try:
            # Generate query embedding
            query_embedding = await asyncio.get_event_loop().run_in_executor(
                None, self.embeddings.embed_query, query
            )
            
            # Search in Qdrant
            search_results = self.client.search(
                collection_name=self.config.qdrant_collection,
                query_vector=query_embedding,
                limit=limit,
                score_threshold=score_threshold
            )
            
            # Format results
            results = []
            for result in search_results:
                if result.payload:
                    results.append({
                        "content": result.payload.get("content", ""),
                        "score": result.score,
                        "metadata": {k: v for k, v in result.payload.items() if k != "content"}
                    })
                else:
                    results.append({
                        "content": "",
                        "score": result.score,
                        "metadata": {}
                    })
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching in Qdrant: {e}")
            return []
