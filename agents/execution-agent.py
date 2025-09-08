"""
Execution Agent for Ingestion Agent Application

This module contains the execution agent responsible for executing ingestion plans
with content deduplication and catalog management.
"""

import hashlib
import logging
import asyncio
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timedelta


class ExecutionStatus(Enum):
    """Enumeration of execution statuses."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


@dataclass
class IngestionCatalogEntry:
    """Catalog entry for tracking ingested documents."""
    doc_uri: str
    last_ingested_hash: Optional[str]
    last_ingested_timestamp: Optional[datetime]
    ingestion_status: ExecutionStatus
    metadata: Dict[str, Any]


@dataclass
class ExecutionResult:
    """Data class representing execution results."""
    job_id: str
    status: ExecutionStatus
    start_time: datetime
    end_time: Optional[datetime]
    content_hash: Optional[str]
    records_processed: int
    errors: List[str]
    metrics: Dict[str, Any]


class IngestionCatalog:
    """Manages ingestion catalog for deduplication."""
    
    def __init__(self):
        self.catalog: Dict[str, IngestionCatalogEntry] = {}
    
    async def get_entry(self, doc_uri: str) -> Optional[IngestionCatalogEntry]:
        """Get catalog entry for document."""
        return self.catalog.get(doc_uri)
    
    async def update_entry(self, doc_uri: str, content_hash: str, status: ExecutionStatus, metadata: Optional[Dict] = None):
        """Update catalog entry."""
        self.catalog[doc_uri] = IngestionCatalogEntry(
            doc_uri=doc_uri,
            last_ingested_hash=content_hash,
            last_ingested_timestamp=datetime.now(),
            ingestion_status=status,
            metadata=metadata or {}
        )
    
    async def should_ingest(self, doc_uri: str, content_hash: str) -> bool:
        """Determine if document should be ingested based on hash comparison."""
        entry = await self.get_entry(doc_uri)
        
        if entry is None:
            # First time ingestion
            return True
        
        if entry.last_ingested_hash is None:
            # No previous hash recorded
            return True
        
        if entry.last_ingested_hash != content_hash:
            # Content changed
            return True
        
        # Same hash - skip ingestion
        return False


class ExecutionAgent:
    """Agent responsible for executing data ingestion workflows."""
    
    def __init__(self):
        """Initialize the execution agent."""
        self.logger = logging.getLogger(__name__)
        self.catalog = IngestionCatalog()
        self.active_jobs: Dict[str, ExecutionResult] = {}
        self.completed_jobs: List[ExecutionResult] = []
        self.job_counter = 0
    
    async def execute_step(self, step: Dict[str, Any]) -> ExecutionResult:
        """
        Execute an individual ingestion step.
        
        Args:
            step: Step definition with task_id, tool, args, depends_on
            
        Returns:
            ExecutionResult: Results of the execution
        """
        task_id = step["task_id"]
        tool = step["tool"]
        doc_uri = step["args"]["doc_uri"]
        
        self.logger.info(f"Executing step {task_id} with tool {tool} for {doc_uri}")
        
        # Initialize execution result
        result = ExecutionResult(
            job_id=task_id,
            status=ExecutionStatus.PENDING,
            start_time=datetime.now(),
            end_time=None,
            content_hash=None,
            records_processed=0,
            errors=[],
            metrics={}
        )
        
        self.active_jobs[task_id] = result
        
        try:
            # Update status to running
            result.status = ExecutionStatus.RUNNING
            
            # Fetch content and compute hash
            content = await self._fetch_content(doc_uri)
            content_hash = self._compute_hash(content)
            result.content_hash = content_hash
            
            # Check if ingestion is needed
            should_ingest = await self.catalog.should_ingest(doc_uri, content_hash)
            
            if not should_ingest:
                # Skip ingestion - no change
                result.status = ExecutionStatus.SKIPPED
                result.end_time = datetime.now()
                self.logger.info(f"Skipping {doc_uri} - no content change detected")
                
                await self.catalog.update_entry(
                    doc_uri, 
                    content_hash, 
                    ExecutionStatus.SKIPPED,
                    {"reason": "no_change", "last_check": datetime.now().isoformat()}
                )
                
            else:
                # Execute ingestion with appropriate tool
                if tool == "vector_ingestion":
                    ingestion_result = await self._execute_vector_ingestion(content, doc_uri)
                elif tool == "graph_ingestion":
                    ingestion_result = await self._execute_graph_ingestion(content, doc_uri)
                else:
                    raise ValueError(f"Unknown tool: {tool}")
                
                # Update result with ingestion outcome
                if ingestion_result.get("status") == "success":
                    result.status = ExecutionStatus.COMPLETED
                    result.records_processed = ingestion_result.get("records_processed", 0)
                    result.metrics = ingestion_result.get("metrics", {})
                    
                    # Update catalog
                    await self.catalog.update_entry(
                        doc_uri, 
                        content_hash, 
                        ExecutionStatus.COMPLETED,
                        {"tool": tool, "completion_time": datetime.now().isoformat()}
                    )
                    
                else:
                    result.status = ExecutionStatus.FAILED
                    result.errors.append(ingestion_result.get("error", "Unknown error"))
            
            result.end_time = datetime.now()
            self.logger.info(f"Step {task_id} completed with status: {result.status}")
            
        except Exception as e:
            result.status = ExecutionStatus.FAILED
            result.end_time = datetime.now()
            result.errors.append(str(e))
            self.logger.error(f"Step {task_id} failed: {str(e)}")
        
        finally:
            # Move from active to completed
            if task_id in self.active_jobs:
                del self.active_jobs[task_id]
            self.completed_jobs.append(result)
        
        return result
    
    async def _fetch_content(self, doc_uri: str) -> bytes:
        """Fetch content from document URI."""
        # This would integrate with your connector services
        # For now, simulate content fetching
        
        if doc_uri.startswith("box://"):
            return await self._fetch_from_box(doc_uri)
        elif doc_uri.startswith("confluence://"):
            return await self._fetch_from_confluence(doc_uri)
        elif doc_uri.startswith("azure://"):
            return await self._fetch_from_azure(doc_uri)
        else:
            raise ValueError(f"Unsupported URI scheme: {doc_uri}")
    
    async def _fetch_from_box(self, doc_uri: str) -> bytes:
        """Fetch content from Box using the enhanced Box connector."""
        try:
            # Import the enhanced Box connector
            import sys
            import os
            sys.path.append(os.path.dirname(os.path.dirname(__file__)))
            from connector.box import BoxConnector
            
            # Parse Box URI: box://file/FILE_ID or box://folder/FOLDER_NAME/FILE_NAME
            if "file/" in doc_uri:
                # Direct file ID: box://file/123456
                file_id = doc_uri.split("file/")[1].split("?")[0]
                
                box_client = BoxConnector()
                if not box_client.is_authenticated():
                    raise Exception("Box authentication failed")
                
                # Download file and read content
                downloaded_path = box_client.download_file(file_id)
                if not downloaded_path or downloaded_path == False:
                    raise Exception(f"Failed to download Box file {file_id}")
                
                # Read the downloaded file content
                with open(downloaded_path, 'rb') as f:
                    content = f.read()
                
                return content
                
            else:
                # For other Box URI formats, return simulated content
                return b"Box content for " + doc_uri.encode()
                
        except Exception as e:
            self.logger.error(f"Error fetching from Box: {e}")
            # Return simulated content as fallback
            return b"Box content for " + doc_uri.encode()
    
    async def _fetch_from_confluence(self, doc_uri: str) -> bytes:
        """Fetch content from Confluence."""
        # Implementation would use Confluence API
        return b"Confluence content for " + doc_uri.encode()
    
    async def _fetch_from_azure(self, doc_uri: str) -> bytes:
        """Fetch content from Azure Blob."""
        # Implementation would use Azure Blob API
        return b"Azure content for " + doc_uri.encode()
    
    def _compute_hash(self, content: bytes) -> str:
        """Compute SHA256 hash of content."""
        return hashlib.sha256(content).hexdigest()
    
    async def _execute_vector_ingestion(self, content: bytes, doc_uri: str) -> Dict[str, Any]:
        """Execute vector ingestion using existing vector tool."""
        try:
            # Simulate vector ingestion
            # In real implementation, this would use your existing tools
            await asyncio.sleep(0.5)  # Simulate processing time
            
            # Convert bytes to string for text content
            content_str = content.decode('utf-8', errors='ignore')
            
            return {
                "status": "success",
                "records_processed": len(content_str.split('\n')),
                "metrics": {
                    "content_length": len(content_str),
                    "chunks_created": max(1, len(content_str) // 500)
                }
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e),
                "records_processed": 0
            }
    
    async def _execute_graph_ingestion(self, content: bytes, doc_uri: str) -> Dict[str, Any]:
        """Execute graph ingestion using existing graph tool."""
        try:
            # Simulate graph ingestion
            # In real implementation, this would use your existing tools
            await asyncio.sleep(1.0)  # Simulate processing time
            
            # Convert bytes to string for text content
            content_str = content.decode('utf-8', errors='ignore')
            
            return {
                "status": "success",
                "records_processed": max(1, len(content_str.split('.')) // 2),
                "metrics": {
                    "nodes_created": max(1, len(content_str.split()) // 10),
                    "relationships_created": max(1, len(content_str.split()) // 20)
                }
            }
                
        except Exception as e:
            return {
                "status": "failed", 
                "error": str(e),
                "records_processed": 0
            }
    async def execute_ingestion_plan(self, plan: Any, source_config: Dict[str, Any]) -> ExecutionResult:
        """
        Execute an ingestion plan (legacy method for compatibility).
        
        Args:
            plan: The ingestion plan to execute
            source_config (Dict[str, Any]): Source configuration
            
        Returns:
            ExecutionResult: Results of the execution
        """
        job_id = self._generate_job_id()
        self.logger.info(f"Starting execution of job {job_id}")
        
        # Initialize execution result
        result = ExecutionResult(
            job_id=job_id,
            status=ExecutionStatus.PENDING,
            start_time=datetime.now(),
            end_time=None,
            content_hash=None,
            records_processed=0,
            errors=[],
            metrics={}
        )
        
        self.active_jobs[job_id] = result
        
        try:
            # Update status to running
            result.status = ExecutionStatus.RUNNING
            
            # Execute the actual ingestion
            await self._execute_ingestion_workflow(result, plan, source_config)
            
            # Mark as completed
            result.status = ExecutionStatus.COMPLETED
            result.end_time = datetime.now()
            
            self.logger.info(f"Job {job_id} completed successfully")
            
        except Exception as e:
            result.status = ExecutionStatus.FAILED
            result.end_time = datetime.now()
            result.errors.append(str(e))
            self.logger.error(f"Job {job_id} failed: {str(e)}")
        
        finally:
            # Move from active to completed
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]
            self.completed_jobs.append(result)
        
        return result
    
    async def _execute_ingestion_workflow(self, result: ExecutionResult, plan: Any, source_config: Dict[str, Any]):
        """
        Execute the actual ingestion workflow.
        
        Args:
            result (ExecutionResult): Execution result to update
            plan: The ingestion plan
            source_config (Dict[str, Any]): Source configuration
        """
        # TODO: Implement actual ingestion logic based on plan strategy
        
        # Simulate different ingestion strategies
        if hasattr(plan, 'strategy'):
            if plan.strategy.value == "batch":
                await self._execute_batch_ingestion(result, source_config)
            elif plan.strategy.value == "streaming":
                await self._execute_streaming_ingestion(result, source_config)
            elif plan.strategy.value == "incremental":
                await self._execute_incremental_ingestion(result, source_config)
        else:
            # Default to batch processing
            await self._execute_batch_ingestion(result, source_config)
    
    async def _execute_batch_ingestion(self, result: ExecutionResult, source_config: Dict[str, Any]):
        """Execute batch ingestion."""
        self.logger.info(f"Executing batch ingestion for job {result.job_id}")
        
        # TODO: Implement batch ingestion logic
        # Simulate processing
        await asyncio.sleep(2)
        result.records_processed = 1000
        result.metrics["batch_size"] = 1000
        result.metrics["processing_time"] = 2.0
    
    async def _execute_streaming_ingestion(self, result: ExecutionResult, source_config: Dict[str, Any]):
        """Execute streaming ingestion."""
        self.logger.info(f"Executing streaming ingestion for job {result.job_id}")
        
        # TODO: Implement streaming ingestion logic
        # Simulate streaming
        for i in range(10):
            await asyncio.sleep(0.2)
            result.records_processed += 100
        
        result.metrics["stream_duration"] = 2.0
        result.metrics["records_per_second"] = 500
    
    async def _execute_incremental_ingestion(self, result: ExecutionResult, source_config: Dict[str, Any]):
        """Execute incremental ingestion."""
        self.logger.info(f"Executing incremental ingestion for job {result.job_id}")
        
        # TODO: Implement incremental ingestion logic
        # Simulate incremental processing
        await asyncio.sleep(1.5)
        result.records_processed = 250
        result.metrics["incremental_changes"] = 250
        result.metrics["last_checkpoint"] = datetime.now().isoformat()
    
    def get_job_status(self, job_id: str) -> Optional[ExecutionResult]:
        """
        Get the status of a specific job.
        
        Args:
            job_id (str): Job identifier
            
        Returns:
            Optional[ExecutionResult]: Job status if found
        """
        # Check active jobs first
        if job_id in self.active_jobs:
            return self.active_jobs[job_id]
        
        # Check completed jobs
        for job in self.completed_jobs:
            if job.job_id == job_id:
                return job
        
        return None
    
    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel an active job.
        
        Args:
            job_id (str): Job identifier
            
        Returns:
            bool: True if job was cancelled, False if not found or already completed
        """
        if job_id in self.active_jobs:
            result = self.active_jobs[job_id]
            result.status = ExecutionStatus.CANCELLED
            result.end_time = datetime.now()
            
            # Move to completed jobs
            del self.active_jobs[job_id]
            self.completed_jobs.append(result)
            
            self.logger.info(f"Job {job_id} cancelled")
            return True
        
        return False
    
    def _generate_job_id(self) -> str:
        """Generate a unique job ID."""
        self.job_counter += 1
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"job_{timestamp}_{self.job_counter}"
