"""
Vector Tool for Ingestion Agent Application

This module provides vector database operations for storing and retrieving
embeddings and performing similarity searches.
"""

import logging
import numpy as np
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass


@dataclass
class VectorDocument:
    """Data class representing a document with vector embedding."""
    id: str
    content: str
    embedding: List[float]
    metadata: Dict[str, Any]


class VectorTool:
    """Tool for vector database operations and similarity search."""
    
    def __init__(self, vector_db_config: Dict[str, Any]):
        """
        Initialize vector tool.
        
        Args:
            vector_db_config (Dict[str, Any]): Vector database configuration
        """
        self.config = vector_db_config
        self.logger = logging.getLogger(__name__)
        self.documents: Dict[str, VectorDocument] = {}
        self.index_built = False
    
    def add_document(self, document: VectorDocument) -> bool:
        """
        Add a document with its vector embedding to the database.
        
        Args:
            document (VectorDocument): Document to add
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.documents[document.id] = document
            self.index_built = False  # Mark index as needing rebuild
            self.logger.info(f"Added document {document.id} to vector database")
            return True
        except Exception as e:
            self.logger.error(f"Failed to add document {document.id}: {str(e)}")
            return False
    
    def add_documents_batch(self, documents: List[VectorDocument]) -> int:
        """
        Add multiple documents in batch.
        
        Args:
            documents (List[VectorDocument]): List of documents to add
            
        Returns:
            int: Number of documents successfully added
        """
        added_count = 0
        for doc in documents:
            if self.add_document(doc):
                added_count += 1
        
        self.logger.info(f"Added {added_count}/{len(documents)} documents to vector database")
        return added_count
    
    def generate_embedding(self, text: str) -> List[float]:
        """
        Generate vector embedding for text.
        
        Args:
            text (str): Text to embed
            
        Returns:
            List[float]: Vector embedding
        """
        # TODO: Implement actual embedding generation using a model
        # For now, return a random embedding for demonstration
        embedding_dim = self.config.get("embedding_dimension", 384)
        embedding = np.random.random(embedding_dim).tolist()
        
        self.logger.debug(f"Generated embedding for text of length {len(text)}")
        return embedding
    
    def similarity_search(self, query_embedding: List[float], top_k: int = 5, 
                         threshold: float = 0.7) -> List[Dict[str, Any]]:
        """
        Perform similarity search using query embedding.
        
        Args:
            query_embedding (List[float]): Query vector embedding
            top_k (int): Number of top results to return
            threshold (float): Similarity threshold
            
        Returns:
            List[Dict[str, Any]]: List of similar documents with scores
        """
        if not self.documents:
            self.logger.warning("No documents in vector database for similarity search")
            return []
        
        results = []
        query_vector = np.array(query_embedding)
        
        for doc_id, document in self.documents.items():
            doc_vector = np.array(document.embedding)
            
            # Calculate cosine similarity
            similarity = self._cosine_similarity(query_vector, doc_vector)
            
            if similarity >= threshold:
                results.append({
                    "document_id": doc_id,
                    "content": document.content,
                    "metadata": document.metadata,
                    "similarity_score": float(similarity)
                })
        
        # Sort by similarity score and return top_k
        results.sort(key=lambda x: x["similarity_score"], reverse=True)
        
        self.logger.info(f"Found {len(results)} similar documents above threshold {threshold}")
        return results[:top_k]
    
    def search_by_text(self, query_text: str, top_k: int = 5, 
                      threshold: float = 0.7) -> List[Dict[str, Any]]:
        """
        Perform similarity search using text query.
        
        Args:
            query_text (str): Query text
            top_k (int): Number of top results to return
            threshold (float): Similarity threshold
            
        Returns:
            List[Dict[str, Any]]: List of similar documents with scores
        """
        query_embedding = self.generate_embedding(query_text)
        return self.similarity_search(query_embedding, top_k, threshold)
    
    def get_document(self, document_id: str) -> Optional[VectorDocument]:
        """
        Retrieve a document by ID.
        
        Args:
            document_id (str): Document identifier
            
        Returns:
            Optional[VectorDocument]: Document if found, None otherwise
        """
        return self.documents.get(document_id)
    
    def delete_document(self, document_id: str) -> bool:
        """
        Delete a document from the vector database.
        
        Args:
            document_id (str): Document identifier
            
        Returns:
            bool: True if deleted, False if not found
        """
        if document_id in self.documents:
            del self.documents[document_id]
            self.logger.info(f"Deleted document {document_id} from vector database")
            return True
        
        self.logger.warning(f"Document {document_id} not found for deletion")
        return False
    
    def update_document(self, document: VectorDocument) -> bool:
        """
        Update an existing document.
        
        Args:
            document (VectorDocument): Updated document
            
        Returns:
            bool: True if updated, False if not found
        """
        if document.id in self.documents:
            self.documents[document.id] = document
            self.logger.info(f"Updated document {document.id}")
            return True
        
        self.logger.warning(f"Document {document.id} not found for update")
        return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the vector database.
        
        Returns:
            Dict[str, Any]: Database statistics
        """
        return {
            "total_documents": len(self.documents),
            "embedding_dimension": self.config.get("embedding_dimension", 384),
            "index_built": self.index_built,
            "memory_usage_mb": self._estimate_memory_usage()
        }
    
    def _cosine_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """
        Calculate cosine similarity between two vectors.
        
        Args:
            vec1 (np.ndarray): First vector
            vec2 (np.ndarray): Second vector
            
        Returns:
            float: Cosine similarity score
        """
        # Handle zero vectors
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return np.dot(vec1, vec2) / (norm1 * norm2)
    
    def _estimate_memory_usage(self) -> float:
        """
        Estimate memory usage in MB.
        
        Returns:
            float: Estimated memory usage in MB
        """
        if not self.documents:
            return 0.0
        
        # Rough estimation based on number of documents and embedding dimension
        embedding_dim = self.config.get("embedding_dimension", 384)
        bytes_per_float = 4  # assuming 32-bit floats
        bytes_per_doc = embedding_dim * bytes_per_float + 1024  # embedding + metadata overhead
        
        total_bytes = len(self.documents) * bytes_per_doc
        return total_bytes / (1024 * 1024)  # Convert to MB
