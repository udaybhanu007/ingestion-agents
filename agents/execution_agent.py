"""
Execution Agent V2 for Ingestion Agent Application

This module contains the updated execution agent responsible for executing ingestion plans
from the planner agent results with the simplified JSON structure.
"""

import asyncio
import json
import logging
import os
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime

# Add the parent directory to sys.path to import modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Note: Connectors and URI resolver are no longer needed since content 
# is passed from planner agent in plan steps (Approach 2)

# Import ingestion tools
try:
    # Import using tool registry only
    from agents.tools.tool_registry import get_tool_registry
    TOOLS_AVAILABLE = True
except ImportError as e:
    logging.warning(f"Agent ingestion tools import failed: {e}")
    get_tool_registry = None
    TOOLS_AVAILABLE = False


class ExecutionAgent:
    """
    Execution Agent for processing planner agent results with simplified JSON structure.
    """
    
    def __init__(self):
        """Initialize the execution agent."""
        self.logger = logging.getLogger(__name__)
        self.tools = self._initialize_tools()
        
        # Note: Connectors and URI resolver removed - content comes from plan steps
    
    def _initialize_tools(self) -> Dict[str, Any]:
        """Initialize ingestion tools using the tool registry."""
        tools = {}
        
        if TOOLS_AVAILABLE and get_tool_registry:
            try:
                # Use tool registry for centralized tool management
                registry = get_tool_registry()
                tools = registry.get_all_tools()
                self.logger.info(f"Tools initialized via registry: {list(tools.keys())}")
                
            except Exception as e:
                self.logger.error(f"Failed to initialize tools via registry: {e}")
                raise e
        else:
            raise RuntimeError("Tool registry not available")
        
        return tools
    
    async def execute_planner_results(self, results_file: str) -> List[Dict[str, Any]]:
        """
        Execute ingestion plans from planner agent results file.
        
        Args:
            results_file: Path to the JSON file containing planner results
            
        Returns:
            List of execution results
        """
        try:
            # Load planner results
            with open(results_file, 'r', encoding='utf-8') as f:
                planner_results = json.load(f)
            
            self.logger.info(f"Loaded {len(planner_results)} planner results from {results_file}")
            
            execution_results = []
            
            for result in planner_results:
                doc_uri = result.get('doc_uri')
                classification = result.get('classification')
                ingestion_plan = result.get('ingestion_plan', {})
                source = result.get('source')
                
                self.logger.info(f"Processing {doc_uri} with classification {classification}")
                
                # Execute the ingestion plan
                plan_execution = await self.execute_ingestion_plan(
                    ingestion_plan, doc_uri, source, result
                )
                
                execution_results.append({
                    'doc_uri': doc_uri,
                    'classification': classification,
                    'source': source,
                    'plan_id': ingestion_plan.get('plan_id'),
                    'execution_status': plan_execution['status'],
                    'execution_details': plan_execution,
                    'processed_at': datetime.now().isoformat()
                })
            
            return execution_results
            
        except Exception as e:
            self.logger.error(f"Error executing planner results: {e}")
            return []
    
    async def execute_ingestion_plan(self, plan: Dict[str, Any], doc_uri: str, 
                                   source: str, document_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute an individual ingestion plan.
        
        Args:
            plan: The ingestion plan with steps (content included in step args)
            doc_uri: Document URI
            source: Source system (azure, box, confluence)
            document_result: Full document result from planner
            
        Returns:
            Execution result dictionary
        """
        try:
            steps = plan.get('steps', [])
            plan_id = plan.get('plan_id')
            
            self.logger.info(f"Executing plan {plan_id} with {len(steps)} steps for {doc_uri}")
            
            # Extract content from plan steps (Approach 2)
            content = self._extract_content_from_steps(steps)
            if not content:
                return {
                    'status': 'failed',
                    'error': f'No content found in plan steps for {doc_uri}',
                    'steps_executed': 0
                }
            
            self.logger.info(f"Using content from plan steps for {doc_uri} (length: {len(content)} chars)")
            
            # Execute steps with dependency management
            step_results = await self._execute_steps_with_dependencies(steps, content, doc_uri, document_result)
            
            # Determine overall status
            successful_steps = sum(1 for result in step_results if result.get('status') == 'success')
            total_steps = len(steps)
            
            overall_status = 'success' if successful_steps == total_steps else 'partial' if successful_steps > 0 else 'failed'
            
            return {
                'status': overall_status,
                'plan_id': plan_id,
                'steps_total': total_steps,
                'steps_successful': successful_steps,
                'step_results': step_results,
                'content_size': len(content) if isinstance(content, (str, bytes)) else 0
            }
            
        except Exception as e:
            self.logger.error(f"Error executing ingestion plan for {doc_uri}: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'steps_executed': 0
            }
    
    def _extract_content_from_steps(self, steps: List[Dict[str, Any]]) -> Optional[str]:
        """
        Extract document content from plan steps.
        
        Args:
            steps: List of plan steps
            
        Returns:
            Document content if found in any step, None otherwise
        """
        for step in steps:
            args = step.get('args', {})
            content = args.get('content')
            if content:
                self.logger.info(f"Found content in step {step.get('task_id', 'unknown')} (length: {len(content)} chars)")
                return content
        
        self.logger.warning("No content found in any plan steps")
        return None
    
    async def execute_plan_async(self, plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Execute a plan that contains content in its steps (Approach 2).
        
        Args:
            plan: The ingestion plan with steps containing content
            
        Returns:
            List of execution results for each step
        """
        try:
            steps = plan.get('steps', [])
            plan_id = plan.get('plan_id')
            
            self.logger.info(f"Executing plan {plan_id} with {len(steps)} steps using content from plan")
            
            # Extract content from plan steps
            content = self._extract_content_from_steps(steps)
            if not content:
                return [{
                    'status': 'failed',
                    'error': 'No content found in plan steps',
                    'plan_id': plan_id
                }]
            
            # Execute each step
            step_results = []
            completed_tasks = set()
            
            for step in steps:
                # Wait for dependencies
                await self._wait_for_dependencies(step, completed_tasks)
                
                # Execute step with content from plan
                step_result = await self._execute_single_step(step, content)
                step_results.append(step_result)
                completed_tasks.add(step["task_id"])
                
                self.logger.info(f"Completed step {step['task_id']}: {step['tool']} - {step_result.get('status', 'unknown')}")
            
            return step_results
            
        except Exception as e:
            self.logger.error(f"Error executing plan: {str(e)}")
            return [{
                'status': 'failed',
                'error': str(e),
                'plan_id': plan.get('plan_id', 'unknown')
            }]
    
    async def _wait_for_dependencies(self, step: Dict[str, Any], completed_tasks: set):
        """Wait for step dependencies to complete."""
        depends_on = step.get("depends_on", [])
        
        while not all(dep in completed_tasks for dep in depends_on):
            await asyncio.sleep(0.1)  # Small delay to prevent busy waiting
    
    async def _execute_single_step(self, step: Dict[str, Any], content: str) -> Dict[str, Any]:
        """
        Execute a single step with content from plan.
        
        Args:
            step: Step dictionary containing tool, args, and content
            content: Document content
            
        Returns:
            Execution result dictionary
        """
        task_id = step["task_id"]
        tool = step["tool"]
        args = step.get("args", {})
        doc_uri = args.get("doc_uri", "")
        
        self.logger.info(f"Executing step {task_id}: {tool} for {doc_uri}")
        
        try:
            # Route to appropriate tool handler
            if tool == "vector_ingestion":
                result = await self._execute_vector_ingestion_with_content(doc_uri, content, args)
            elif tool == "graph_ingestion":
                result = await self._execute_graph_ingestion_with_content(doc_uri, content, args)
            else:
                raise ValueError(f"Unknown tool: {tool}")
            
            return {
                "task_id": task_id,
                "tool": tool,
                "status": "success",
                "doc_uri": doc_uri,
                "content_processed": len(content),
                "result": result,
                "executed_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                "task_id": task_id,
                "tool": tool,
                "status": "failed",
                "doc_uri": doc_uri,
                "error": str(e),
                "executed_at": datetime.now().isoformat()
            }
    
    async def _execute_vector_ingestion_with_content(self, doc_uri: str, content: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute vector ingestion with document content using actual vector tool."""
        # Prepare metadata for vector ingestion
        metadata = {
            'doc_uri': doc_uri,
            'source': args.get('source', 'unknown'),
            'document_type': args.get('document_type', 'unknown'),
            'ingestion_timestamp': datetime.now().isoformat()
        }
        
        vector_tool = self.tools['vector_ingestion']
        result = await vector_tool.ingest(content, metadata)
        self.logger.info(f"Vector ingestion completed for {doc_uri}")
        return result
    
    async def _execute_graph_ingestion_with_content(self, doc_uri: str, content: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute graph ingestion with document content using actual graph tool."""
        # Prepare metadata for graph ingestion
        metadata = {
            'doc_uri': doc_uri,
            'source': args.get('source', 'unknown'),
            'document_type': args.get('document_type', 'unknown'),
            'node_type': 'Document',
            'ingestion_timestamp': datetime.now().isoformat()
        }
        
        graph_tool = self.tools['graph_ingestion']
        # Use the correct method name and make it synchronous call
        result = graph_tool.ingest_content(
            content=content,
            source_name=doc_uri,
            content_type=args.get('document_type', 'text'),
            metadata=metadata
        )
        self.logger.info(f"Graph ingestion completed for {doc_uri}")
        return result
    
    async def _execute_steps_with_dependencies(self, steps: List[Dict[str, Any]], 
                                             content: Any, doc_uri: str, 
                                             document_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute steps respecting dependencies."""
        step_results = {}
        execution_results = []
        remaining_steps = {step['task_id']: step for step in steps}
        
        while remaining_steps:
            # Find executable steps (no pending dependencies)
            executable_steps = []
            for task_id, step in remaining_steps.items():
                dependencies = step.get('depends_on', [])
                if all(dep_id in step_results for dep_id in dependencies):
                    executable_steps.append(step)
            
            if not executable_steps:
                self.logger.error("Circular dependency detected in ingestion plan")
                break
            
            # Execute steps
            for step in executable_steps:
                try:
                    result = await self._execute_single_step(step, content)
                    step_results[step['task_id']] = result
                    execution_results.append(result)
                    del remaining_steps[step['task_id']]
                    
                except Exception as e:
                    self.logger.error(f"Failed to execute step {step['task_id']}: {e}")
                    failed_result = {
                        'task_id': step['task_id'],
                        'tool': step['tool'],
                        'status': 'failed',
                        'error': str(e),
                        'execution_time': datetime.now().isoformat()
                    }
                    step_results[step['task_id']] = failed_result
                    execution_results.append(failed_result)
                    del remaining_steps[step['task_id']]
        
        return execution_results
    
    def save_execution_results(self, results: List[Dict[str, Any]], 
                             output_file: str = "execution_results.json") -> bool:
        """Save execution results to JSON file."""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Execution results saved to {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving execution results: {e}")
            return False


# Example usage
async def main():
    """Example usage of the Execution Agent V2."""
    import logging
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Initialize the execution agent
    executor = ExecutionAgent()
    
    # Execute planner results
    results_file = "planner_agent_results_20250909_143524.json"
    if os.path.exists(results_file):
        execution_results = await executor.execute_planner_results(results_file)
        
        # Save execution results
        executor.save_execution_results(
            execution_results, 
            f"execution_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        print(f"Executed {len(execution_results)} documents")
        for result in execution_results:
            print(f"- {result['doc_uri']}: {result['execution_status']}")
    else:
        print(f"Planner results file {results_file} not found")


if __name__ == "__main__":
    asyncio.run(main())
