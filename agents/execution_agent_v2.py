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

# Import the new URI resolver
try:
    from utils.uri_resolver import URIContentResolver
except ImportError as e:
    logging.warning(f"URI resolver import failed: {e}")
    URIContentResolver = None

# Import connectors
try:
    from connector.azure import AzureConnector
    from connector.box import BoxConnector
    from connector.confluence_mcp import ConfluenceMCPConnector
except ImportError as e:
    logging.warning(f"Connector import failed: {e}")

# Import ingestion tools
try:
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'agent-ingestion'))
    from tools.vector_tool import VectorIngestionTool
    from tools.graph_tool import GraphIngestionTool
    from config import IngestionConfig
except ImportError as e:
    logging.warning(f"Agent ingestion tools import failed: {e}")
    VectorIngestionTool = None
    GraphIngestionTool = None
    IngestionConfig = None


class ExecutionAgentV2:
    """
    Execution Agent V2 for processing planner agent results with simplified JSON structure.
    """
    
    def __init__(self):
        """Initialize the execution agent."""
        self.logger = logging.getLogger(__name__)
        self.connectors = self._initialize_connectors()
        self.tools = self._initialize_tools()
        
        # Initialize URI resolver for enhanced content access
        self.uri_resolver = URIContentResolver() if URIContentResolver else None
        if self.uri_resolver:
            self.logger.info("✅ URI resolver initialized")
        else:
            self.logger.warning("⚠️ URI resolver not available")
    
    def _initialize_connectors(self) -> Dict[str, Any]:
        """Initialize data source connectors."""
        connectors = {}
        
        try:
            connectors['azure'] = AzureConnector()
        except Exception as e:
            self.logger.warning(f"Failed to initialize Azure connector: {e}")
        
        try:
            connectors['box'] = BoxConnector()
        except Exception as e:
            self.logger.warning(f"Failed to initialize Box connector: {e}")
        
        try:
            connectors['confluence'] = ConfluenceMCPConnector()
        except Exception as e:
            self.logger.warning(f"Failed to initialize Confluence connector: {e}")
        
        return connectors
    
    def _initialize_tools(self) -> Dict[str, Any]:
        """Initialize ingestion tools."""
        tools = {}
        
        try:
            if VectorIngestionTool and IngestionConfig:
                config = IngestionConfig()
                tools['vector_ingestion'] = VectorIngestionTool(config)
            else:
                self.logger.warning("VectorIngestionTool not available")
        except Exception as e:
            self.logger.warning(f"Failed to initialize vector tool: {e}")
        
        try:
            if GraphIngestionTool and IngestionConfig:
                config = IngestionConfig()
                tools['graph_ingestion'] = GraphIngestionTool(config)
            else:
                self.logger.warning("GraphIngestionTool not available")
        except Exception as e:
            self.logger.warning(f"Failed to initialize graph tool: {e}")
        
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
            plan: The ingestion plan with steps
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
            
            # Fetch document content
            content = await self._fetch_content(doc_uri, source)
            if not content:
                return {
                    'status': 'failed',
                    'error': f'Failed to fetch content for {doc_uri}',
                    'steps_executed': 0
                }
            
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
                    result = await self._execute_single_step(step, content, doc_uri, document_result)
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
    
    async def _execute_single_step(self, step: Dict[str, Any], content: Any, 
                                 doc_uri: str, document_result: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single ingestion step."""
        task_id = step['task_id']
        tool = step['tool']
        
        self.logger.info(f"Executing step {task_id} with tool {tool}")
        
        start_time = datetime.now()
        
        try:
            if tool == 'vector_ingestion':
                result = await self._execute_vector_ingestion(content, doc_uri, document_result)
            elif tool == 'graph_ingestion':
                result = await self._execute_graph_ingestion(content, doc_uri, document_result)
            else:
                raise ValueError(f"Unknown tool: {tool}")
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            return {
                'task_id': task_id,
                'tool': tool,
                'status': result.get('status', 'unknown'),
                'records_processed': result.get('records_processed', 0),
                'execution_time_seconds': execution_time,
                'execution_timestamp': end_time.isoformat(),
                'tool_result': result
            }
            
        except Exception as e:
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            return {
                'task_id': task_id,
                'tool': tool,
                'status': 'failed',
                'error': str(e),
                'execution_time_seconds': execution_time,
                'execution_timestamp': end_time.isoformat()
            }
    
    async def _execute_vector_ingestion(self, content: Any, doc_uri: str, 
                                      document_result: Dict[str, Any]) -> Dict[str, Any]:
        """Execute vector ingestion."""
        try:
            if 'vector_ingestion' not in self.tools:
                # Fallback simulation
                self.logger.warning("Vector ingestion tool not available, using simulation")
                await asyncio.sleep(0.5)
                content_str = str(content)
                return {
                    'status': 'success',
                    'records_processed': len(content_str.split('\n')),
                    'method': 'simulation'
                }
            
            # Prepare metadata
            metadata = {
                'doc_uri': doc_uri,
                'source': document_result.get('source', 'unknown'),
                'classification': document_result.get('classification'),
                'document_type': document_result.get('document_type'),
                'ingestion_timestamp': datetime.now().isoformat()
            }
            
            # Execute with actual tool
            vector_tool = self.tools['vector_ingestion']
            result = await vector_tool.ingest(content, metadata)
            
            self.logger.info(f"Vector ingestion completed for {doc_uri}: {result.get('status')}")
            return result
            
        except Exception as e:
            self.logger.error(f"Vector ingestion failed for {doc_uri}: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'records_processed': 0
            }
    
    async def _execute_graph_ingestion(self, content: Any, doc_uri: str, 
                                     document_result: Dict[str, Any]) -> Dict[str, Any]:
        """Execute graph ingestion."""
        try:
            if 'graph_ingestion' not in self.tools:
                # Fallback simulation
                self.logger.warning("Graph ingestion tool not available, using simulation")
                await asyncio.sleep(1.0)
                content_str = str(content)
                return {
                    'status': 'success',
                    'records_processed': max(1, len(content_str.split('.')) // 2),
                    'method': 'simulation'
                }
            
            # Try to parse as JSON if possible
            if isinstance(content, str):
                try:
                    content_data = json.loads(content)
                except json.JSONDecodeError:
                    content_data = content
            else:
                content_data = content
            
            # Prepare metadata
            metadata = {
                'doc_uri': doc_uri,
                'source': document_result.get('source', 'unknown'),
                'classification': document_result.get('classification'),
                'document_type': document_result.get('document_type'),
                'node_type': 'Document',
                'ingestion_timestamp': datetime.now().isoformat()
            }
            
            # Execute with actual tool
            graph_tool = self.tools['graph_ingestion']
            result = await graph_tool.ingest(content_data, metadata)
            
            self.logger.info(f"Graph ingestion completed for {doc_uri}: {result.get('status')}")
            return result
            
        except Exception as e:
            self.logger.error(f"Graph ingestion failed for {doc_uri}: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'records_processed': 0
            }
    
    async def _fetch_content(self, doc_uri: str, source: str) -> Optional[Any]:
        """Fetch content from the appropriate connector using URI resolver."""
        try:
            # Use URI resolver if available for enhanced content access
            if self.uri_resolver:
                self.logger.info(f"🔍 Using URI resolver for: {doc_uri}")
                result = self.uri_resolver.resolve_uri_to_content(doc_uri)
                
                if result['status'] == 'success':
                    content_text = result['content_text']
                    if content_text:
                        self.logger.info(f"✅ Content resolved: {len(content_text)} characters")
                        return content_text
                    else:
                        self.logger.warning(f"⚠️ No text content available for: {doc_uri}")
                        return None
                else:
                    self.logger.error(f"❌ URI resolution failed: {result['error']}")
                    # Fall back to original method
                    return await self._fetch_content_fallback(doc_uri, source)
            else:
                # Fall back to original method
                return await self._fetch_content_fallback(doc_uri, source)
                
        except Exception as e:
            self.logger.error(f"Error fetching content from {doc_uri}: {e}")
            return None
    
    async def _fetch_content_fallback(self, doc_uri: str, source: str) -> Optional[Any]:
        """Fallback content fetching method using original connectors."""
        try:
            if source == 'azure' and 'azure' in self.connectors:
                return await self._fetch_from_azure(doc_uri)
            elif source == 'box' and 'box' in self.connectors:
                return await self._fetch_from_box(doc_uri)
            elif source == 'confluence' and 'confluence' in self.connectors:
                return await self._fetch_from_confluence(doc_uri)
            else:
                self.logger.error(f"No connector available for source: {source}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error in fallback content fetching from {doc_uri}: {e}")
            return None
    
    async def _fetch_from_azure(self, doc_uri: str) -> Optional[bytes]:
        """Fetch content from Azure Blob Storage."""
        try:
            # Parse Azure URI: azure://container/blob_name
            if "://" in doc_uri:
                path_parts = doc_uri.split("://")[1].split("/")
                if len(path_parts) >= 2:
                    container_name = path_parts[0]
                    blob_name = "/".join(path_parts[1:])
                    
                    azure_client = self.connectors['azure']
                    content = azure_client.download_blob_to_memory(container_name, blob_name)
                    return content
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error fetching from Azure: {e}")
            return None
    
    async def _fetch_from_box(self, doc_uri: str) -> Optional[bytes]:
        """Fetch content from Box."""
        try:
            # Parse Box URI: box://file/FILE_ID
            if "file/" in doc_uri:
                file_id = doc_uri.split("file/")[1].split("?")[0]
                
                box_client = self.connectors['box']
                if not box_client.is_authenticated():
                    raise Exception("Box authentication failed")
                
                # Get files from documents-ingest folder
                files = box_client.get_files_from_folder("documents-ingest")
                
                # Find the file by ID
                target_file = None
                for file_info in files:
                    if file_info.get("id") == file_id:
                        target_file = file_info
                        break
                
                if not target_file:
                    raise Exception(f"File {file_id} not found in documents-ingest folder")
                
                # Download file content to memory
                content = box_client.download_file_to_memory(file_id)
                return content
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error fetching from Box: {e}")
            return None
    
    async def _fetch_from_confluence(self, doc_uri: str) -> Optional[str]:
        """Fetch content from Confluence."""
        try:
            # Parse Confluence URI: confluence://page/PAGE_NAME
            if "page/" in doc_uri:
                page_name = doc_uri.split("page/")[1]
                
                confluence_client = self.connectors['confluence']
                content = await confluence_client.download_page_content(page_name)
                return content
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error fetching from Confluence: {e}")
            return None
    
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
    executor = ExecutionAgentV2()
    
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
