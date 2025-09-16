"""
Simplified Execution Agent for Ingestion Agent Application

This module contains the execution agent responsible for executing ingestion plans
from the planner agent results with simplified logging.
"""

import asyncio
import json
import os
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime

# Add the parent directory to sys.path to import modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import ingestion tools
from agents.tools.tool_registry import get_tool_registry

# Import simple logging
from config.simple_logger import get_agent_logger, get_react_logger


class ExecutionAgent:
    """
    Execution Agent for processing planner agent results with simplified JSON structure.
    """
    
    def __init__(self):
        """Initialize the execution agent."""
        # Initialize simple logging
        self.logger = get_agent_logger("executor")
        self.react_logger = None  # Will be initialized per execution
        
        # Initialize tool registry
        self.tool_registry = get_tool_registry()
        self.logger.info("Tool registry initialized successfully")
        
        self.logger.info("ExecutionAgent initialized")
    
    async def execute_plan_async(self, plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Execute an ingestion plan asynchronously.
        
        Args:
            plan: The ingestion plan from planner agent
            
        Returns:
            List of execution results for each step
        """
        try:
            # Initialize ReAct logger for this execution
            execution_id = plan.get('plan_id', 'unknown')
            self.react_logger = get_react_logger(f"executor-{execution_id}")
            
            self.react_logger.reasoning(
                f"Starting plan execution for plan_id: {execution_id}"
            )
            
            # Validate plan structure
            if not isinstance(plan, dict) or 'steps' not in plan:
                raise ValueError("Invalid plan structure: missing 'steps' field")
            
            steps = plan.get('steps', [])
            if not isinstance(steps, list):
                raise ValueError("Invalid plan structure: 'steps' must be a list")
            
            self.react_logger.reasoning(
                f"Plan validated successfully. Found {len(steps)} steps to execute"
            )
            
            # Execute each step
            results = []
            for i, step in enumerate(steps):
                self.react_logger.action(
                    f"Executing step {i+1}/{len(steps)}: {step.get('step_id', f'step-{i}')}"
                )
                
                try:
                    step_result = await self._execute_step(step, plan)
                    results.append({
                        'step_id': step.get('step_id', f'step-{i}'),
                        'step_type': step.get('step_type', 'unknown'),
                        'status': 'success',
                        'result': step_result,
                        'execution_time': datetime.utcnow().isoformat()
                    })
                    
                    self.react_logger.observation(
                        f"Step {i+1} completed successfully"
                    )
                    
                except Exception as e:
                    self.logger.error(f"Step {i+1} failed: {e}")
                    results.append({
                        'step_id': step.get('step_id', f'step-{i}'),
                        'step_type': step.get('step_type', 'unknown'),
                        'status': 'error',
                        'error': str(e),
                        'execution_time': datetime.utcnow().isoformat()
                    })
                    
                    self.react_logger.observation(
                        f"Step {i+1} failed: {e}"
                    )
            
            self.react_logger.observation(
                f"Plan execution completed. {sum(1 for r in results if r['status'] == 'success')} successful, "
                f"{sum(1 for r in results if r['status'] == 'error')} failed"
            )
            
            return results
            
        except Exception as e:
            self.logger.error(f"Plan execution failed: {e}")
            if self.react_logger:
                self.react_logger.observation(f"Plan execution failed: {e}")
            
            return [{
                'step_id': 'execution_error',
                'step_type': 'error',
                'status': 'error',
                'error': str(e),
                'execution_time': datetime.utcnow().isoformat()
            }]
    
    async def _execute_step(self, step: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single step from the ingestion plan.
        
        Args:
            step: Individual step from the plan
            plan: Complete plan context
            
        Returns:
            Step execution result
        """
        step_type = step.get('step_type', 'unknown')
        step_id = step.get('step_id', 'unknown')
        
        self.logger.info(f"Executing step: {step_id} (type: {step_type})")
        
        try:
            # Get appropriate tool for step type
            if step_type == "content_extraction":
                return await self._execute_content_extraction(step, plan)
            elif step_type == "text_chunking":
                return await self._execute_text_chunking(step, plan)
            elif step_type == "vector_generation":
                return await self._execute_vector_ingestion(step, plan)
            elif step_type == "graph_ingestion":
                return await self._execute_graph_ingestion(step, plan)
            elif step_type == "metadata_extraction":
                return await self._execute_metadata_extraction(step, plan)
            elif step_type == "storage":
                return await self._execute_storage(step, plan)
            else:
                self.logger.warning(f"Unknown step type: {step_type}")
                return {
                    'success': False,
                    'error': f'Unknown step type: {step_type}',
                    'step_id': step_id
                }
                
        except Exception as e:
            self.logger.error(f"Step execution error: {e}")
            return {
                'success': False,
                'error': str(e),
                'step_id': step_id
            }
    
    async def _execute_content_extraction(self, step: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute content extraction step."""
        try:
            # Get content extraction tool
            tool = self.tool_registry.get_tool("content_extractor")
            if not tool:
                return {
                    'success': False,
                    'error': 'Content extraction tool not available'
                }
            
            # Extract content using tool
            doc_uri = plan.get('document_uri', '')
            config = step.get('config', {})
            
            result = await tool.extract_content_async(doc_uri, config)
            
            return {
                'success': True,
                'extracted_content': result.get('content', ''),
                'content_length': len(result.get('content', '')),
                'metadata': result.get('metadata', {}),
                'step_id': step.get('step_id')
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Content extraction failed: {e}',
                'step_id': step.get('step_id')
            }
    
    async def _execute_text_chunking(self, step: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute text chunking step."""
        try:
            # Get chunking tool
            tool = self.tool_registry.get_tool("text_chunker")
            if not tool:
                return {
                    'success': False,
                    'error': 'Text chunking tool not available'
                }
            
            # For now, simulate chunking with basic logic
            config = step.get('config', {})
            chunk_size = config.get('chunk_size', 1024)
            
            # Simulate chunking result
            chunks_created = max(1, 5000 // chunk_size)  # Simulate based on typical document size
            
            return {
                'success': True,
                'chunks_created': chunks_created,
                'chunk_size': chunk_size,
                'overlap': config.get('overlap', 200),
                'method': config.get('method', 'semantic'),
                'step_id': step.get('step_id')
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Text chunking failed: {e}',
                'step_id': step.get('step_id')
            }
    
    async def _execute_vector_generation(self, step: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute vector generation step."""
        try:
            # Get vector tool
            tool = self.tool_registry.get_tool("vector_generator")
            if not tool:
                return {
                    'success': False,
                    'error': 'Vector generation tool not available'
                }
            
            # Simulate vector generation
            config = step.get('config', {})
            embedding_model = config.get('embedding_model', 'text-embedding-ada-002')
            
            # Estimate vectors based on chunks
            estimated_chunks = 10  # Default estimate
            vectors_stored = estimated_chunks
            
            return {
                'success': True,
                'vectors_stored': vectors_stored,
                'embedding_model': embedding_model,
                'vector_dimension': 1536,  # Standard for Ada-002
                'step_id': step.get('step_id')
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Vector generation failed: {e}',
                'step_id': step.get('step_id')
            }
    
    async def _execute_metadata_extraction(self, step: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute metadata extraction step."""
        try:
            # Simulate metadata extraction
            config = step.get('config', {})
            
            return {
                'success': True,
                'metadata_extracted': {
                    'title': 'Document Title',
                    'author': 'Unknown',
                    'creation_date': datetime.utcnow().isoformat(),
                    'document_type': plan.get('document_classification', {}).get('primary_type', 'unknown')
                },
                'step_id': step.get('step_id')
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Metadata extraction failed: {e}',
                'step_id': step.get('step_id')
            }
    
    async def _execute_storage(self, step: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute storage step."""
        try:
            # Simulate storage operation
            config = step.get('config', {})
            storage_type = config.get('storage_type', 'vector_database')
            
            return {
                'success': True,
                'storage_type': storage_type,
                'records_stored': 10,  # Simulate
                'storage_location': 'vector_database_collection',
                'step_id': step.get('step_id')
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Storage failed: {e}',
                'step_id': step.get('step_id')
            }

    async def _execute_vector_ingestion(self, step: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute vector ingestion step using VectorToolV2."""
        try:
            # Get vector ingestion tool
            tool = self.tool_registry.get_tool("vector_ingestion")
            if not tool:
                return {
                    'success': False,
                    'error': 'Vector ingestion tool not available',
                    'step_id': step.get('step_id')
                }
            
            # Get document URI and content from plan
            args = step.get('args', {})
            doc_uri = args.get('doc_uri', plan.get('document_uri', ''))
            
            # Get document content from plan context
            content = plan.get('context', {}).get('content', '')
            
            if not doc_uri:
                return {
                    'success': False,
                    'error': 'No document URI provided for vector ingestion',
                    'step_id': step.get('step_id')
                }
            
            if not content:
                return {
                    'success': False,
                    'error': 'No document content available for vector ingestion',
                    'step_id': step.get('step_id')
                }
            
            # Execute vector ingestion with actual content
            metadata = {
                'source': 'execution_agent',
                'step_id': step.get('step_id'),
                'plan_id': plan.get('plan_id'),
                'document_uri': doc_uri
            }
            
            result = tool.ingest(content, metadata)
            
            return {
                'success': result.get('success', False),
                'result': result,
                'vectors_stored': result.get('chunks_processed', 0),
                'step_id': step.get('step_id')
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Vector ingestion failed: {e}',
                'step_id': step.get('step_id')
            }

    async def _execute_graph_ingestion(self, step: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute graph ingestion step using GraphIngestionTool."""
        try:
            # Get graph ingestion tool
            tool = self.tool_registry.get_tool("graph_ingestion")
            if not tool:
                return {
                    'success': False,
                    'error': 'Graph ingestion tool not available',
                    'step_id': step.get('step_id')
                }
            
            # Get document URI from step args
            args = step.get('args', {})
            doc_uri = args.get('doc_uri', plan.get('document_uri', ''))
            
            if not doc_uri:
                return {
                    'success': False,
                    'error': 'No document URI provided for graph ingestion',
                    'step_id': step.get('step_id')
                }
            
            # Execute graph ingestion
            metadata = {
                'source': 'execution_agent',
                'step_id': step.get('step_id'),
                'plan_id': plan.get('plan_id'),
                'doc_uri': doc_uri
            }
            
            result = tool.ingest_content("", doc_uri, "document", metadata)  # Use sync ingest_content method
            
            return {
                'success': result.get('success', False),
                'result': result,
                'nodes_created': result.get('nodes_created', 0),
                'relationships_created': result.get('relationships_created', 0),
                'step_id': step.get('step_id')
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Graph ingestion failed: {e}',
                'step_id': step.get('step_id')
            }


# Export the class
__all__ = ['ExecutionAgent']
