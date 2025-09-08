"""
Simple Document Chunker for Vector Ingestion

Simplified version of chunking logic focused on core functionality.
"""

import re
import os
from typing import List, Dict, Any, Optional
import hashlib
import uuid
from datetime import datetime


class DocumentChunker:
    """Simple document chunker with paragraph-based splitting"""
    
    def __init__(self, 
                 chunk_size: int = 3, 
                 overlap: int = 1, 
                 min_words: int = 30, 
                 max_words: int = 400):
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.min_words = min_words
        self.max_words = max_words
    
    def split_paragraphs(self, text: str) -> List[str]:
        """Split text into paragraphs, filter out page headers"""
        paragraphs = []
        for para in text.split('\n\n'):
            para = para.strip()
            # Skip page headers and empty paragraphs
            if para.startswith('## Page ') or not para:
                continue
            paragraphs.append(para)
        return paragraphs
    
    def merge_short_paragraphs(self, paragraphs: List[str]) -> List[str]:
        """Merge paragraphs that are too short"""
        merged = []
        buffer = ""
        
        for para in paragraphs:
            word_count = len(para.split())
            
            if word_count < self.min_words:
                buffer = buffer + " " + para if buffer else para
            else:
                if buffer:
                    merged.append(buffer.strip())
                    buffer = ""
                merged.append(para)
        
        if buffer:
            merged.append(buffer.strip())
        
        return merged
    
    def create_overlapping_chunks(self, paragraphs: List[str]) -> List[str]:
        """Create overlapping chunks from paragraphs"""
        if not paragraphs:
            return []
        
        chunks = []
        step = max(1, self.chunk_size - self.overlap)
        
        for i in range(0, len(paragraphs), step):
            chunk_paras = paragraphs[i:i + self.chunk_size]
            if chunk_paras:
                chunk_text = "\n\n".join(chunk_paras)
                chunks.append(chunk_text)
        
        return chunks
    
    def split_large_chunks(self, chunks: List[str]) -> List[str]:
        """Split chunks that exceed maximum word limit"""
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
    
    def chunk_document(self, content: str, source_file: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Main chunking method - converts content to chunks with metadata
        
        Args:
            content: Text content to chunk
            source_file: Optional source file path
            
        Returns:
            List of chunks with metadata
        """
        # Step 1: Split into paragraphs
        paragraphs = self.split_paragraphs(content)
        
        # Step 2: Merge short paragraphs
        merged_paragraphs = self.merge_short_paragraphs(paragraphs)
        
        # Step 3: Create overlapping chunks
        chunks = self.create_overlapping_chunks(merged_paragraphs)
        
        # Step 4: Split large chunks
        refined_chunks = self.split_large_chunks(chunks)
        
        # Step 5: Create chunks with metadata
        chunk_list = []
        source_filename = os.path.basename(source_file) if source_file else "unknown"
        
        for idx, chunk_text in enumerate(refined_chunks):
            chunk_id = idx + 1
            
            # Create stable UUID for chunk
            unique_str = f"{source_filename}:{chunk_id}"
            chunk_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_str))
            
            chunk_metadata = {
                "id": chunk_uuid,
                "source_file": source_filename,
                "chunk_id": chunk_id,
                "chunk_index": idx,
                "total_chunks": len(refined_chunks),
                "word_count": len(chunk_text.split()),
                "created_date": datetime.now().strftime("%Y-%m-%d"),
                "chunk_type": "paragraph_based"
            }
            
            chunk_list.append({
                "content": chunk_text,
                "metadata": chunk_metadata
            })
        
        # Print statistics
        if refined_chunks:
            lengths = [len(chunk.split()) for chunk in refined_chunks]
            print(f"📊 Chunking Stats - Count: {len(refined_chunks)}, "
                  f"Avg Words: {sum(lengths)/len(lengths):.1f}, "
                  f"Range: {min(lengths)}-{max(lengths)} words")
        
        return chunk_list


def clean_text_for_vector(text: str) -> str:
    """Clean text for vector database storage"""
    if not text:
        return ""
    
    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Remove citation patterns like [1], (Smith, 2020), etc.
    text = re.sub(r'\[\d+\]', '', text)
    text = re.sub(r'\([^)]*\d{4}[^)]*\)', '', text)
    
    # Clean up extra spaces
    text = text.strip()
    
    return text


# Simple usage function
def chunk_text_content(content: str, source_file: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
    """Simple function to chunk text content"""
    chunker = DocumentChunker(**kwargs)
    return chunker.chunk_document(content, source_file)
