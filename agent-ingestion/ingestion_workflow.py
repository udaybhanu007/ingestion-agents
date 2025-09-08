"""
LangGraph-based Ingestion Workflow

This module provides a LangGraph workflow for orchestrating data ingestion
into both vector (Qdrant) and graph (Neo4j) databases using dynamic tool registry.
"""

import os
import logging
import sys
from typing import Dict, Any, List, Literal, Optional
from typing_extensions import TypedDict
from enum import Enum

# LangGraph imports
from langgraph.graph import StateGraph, END
from langchain_openai import AzureChatOpenAI

# Add current directory to path for imports
current_dir = os.path.dirname(__file__)
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from config import get_config
from tool_registry import get_tool_registry, IngestionTool

logger = logging.getLogger(__name__)


class IngestionStatus(Enum):
    """Ingestion status enumeration"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class IngestionState(TypedDict):
    """LangGraph state schema for ingestion workflow"""
    # Input data
    content: Any
    metadata: Dict[str, Any]
    
    # Processing control
    current_step: str
    status: IngestionStatus
    
    # Tool execution
    selected_tools: List[str]
    tool_results: Dict[str, Dict[str, Any]]
    
    # Error handling
    errors: List[str]
    warnings: List[str]
    
    # Results
    vector_ingestion_result: Optional[Dict[str, Any]]
    graph_ingestion_result: Optional[Dict[str, Any]]
    
    # Workflow control
    next_action: Literal["route", "process_graph", "process_vector", "validate", "complete", "error"]


class ContentProcessor:
    """Simple content processor for analyzing input data"""
    
    def __init__(self, config):
        self.config = config
    
    def analyze_content(self, content: Any) -> Dict[str, Any]:
        """Analyze content to determine processing strategy"""
        analysis = {
            "content_type": "unknown",
            "suggested_tools": [],
            "complexity": "simple",
            "use_smart_analysis": False
        }
        
        if isinstance(content, dict):
            analysis["content_type"] = "structured"
            # Default to smart analysis for all structured data
            analysis["use_smart_analysis"] = True
            analysis["suggested_tools"].append("smart_llm_mcp_ingest")
            analysis["complexity"] = "moderate"
                
        elif isinstance(content, list):
            analysis["content_type"] = "structured_list"
            # Default to smart analysis for all list data
            analysis["use_smart_analysis"] = True
            analysis["suggested_tools"].append("smart_llm_mcp_ingest")
            analysis["complexity"] = "moderate"
                
        elif isinstance(content, str):
            # Check if it's a file path
            if os.path.exists(content):
                analysis["content_type"] = "file_path"
                # All file types benefit from smart analysis
                analysis["use_smart_analysis"] = True
                analysis["suggested_tools"].append("smart_llm_mcp_ingest")
                analysis["complexity"] = "moderate"
            else:
                analysis["content_type"] = "text"
                
                # Detect if it's unstructured text (long form content)
                if self._is_unstructured_text(content):
                    analysis["content_type"] = "unstructured_text"
                    analysis["use_smart_analysis"] = False  # Use vector ingestion for unstructured
                    analysis["suggested_tools"].append("vector_ingest")
                    analysis["complexity"] = "simple"
                else:
                    # Short text content benefits from smart analysis
                    analysis["use_smart_analysis"] = True
                    analysis["suggested_tools"].append("smart_llm_mcp_ingest")
                    analysis["complexity"] = "simple"
        else:
            analysis["content_type"] = "binary"
            # Even binary data can be analyzed smartly
            analysis["use_smart_analysis"] = True
            analysis["suggested_tools"].append("smart_llm_mcp_ingest")
            analysis["complexity"] = "simple"
        
        return analysis
    
    def _is_complex_structured_data(self, content: dict) -> bool:
        """Determine if structured data is complex enough to benefit from smart analysis"""
        # Check for medical/scientific data patterns
        medical_indicators = [
            "image_index", "finding_label", "bbox", "patient", "diagnosis", 
            "medical", "clinical", "pathology", "radiology", "x-ray", "imaging"
        ]
        
        # Check for research data patterns
        research_indicators = [
            "experiment", "data_point", "measurement", "analysis", "study",
            "research", "scientific", "laboratory", "observation"
        ]
        
        # Check for complex relational patterns
        relational_indicators = [
            "id", "reference", "link", "relationship", "parent", "child",
            "network", "graph", "node", "edge", "connection"
        ]
        
        content_str = str(content).lower()
        
        # If content contains indicators of complex domain-specific data
        if any(indicator in content_str for indicator in medical_indicators + research_indicators):
            return True
            
        # If content has multiple related fields that could form relationships
        if any(indicator in content_str for indicator in relational_indicators) and len(content) > 3:
            return True
            
        return False
    
    def _is_unstructured_text(self, content: str) -> bool:
        """Determine if text content is unstructured (long-form content suitable for vector ingestion)"""
        # Check content length - longer content is likely unstructured
        if len(content) > 1000:  # More than 1000 characters
            word_count = len(content.split())
            if word_count > 200:  # More than 200 words
                return True
        
        # Check for document-like patterns
        unstructured_indicators = [
            # Document structure indicators
            "\n\n",  # Multiple paragraphs
            "# ",    # Markdown headers
            "## ",   # Markdown subheaders
            "### ",  # Markdown sub-subheaders
            "Abstract", "Introduction", "Conclusion", "References",
            "Chapter", "Section", "Paragraph",
            # Academic/research indicators
            "research", "study", "analysis", "methodology", "results",
            "discussion", "literature", "findings", "conclusion",
            # Documentation indicators
            "documentation", "guide", "manual", "tutorial", "overview",
            "explanation", "description", "background", "summary"
        ]
        
        content_lower = content.lower()
        indicator_count = sum(1 for indicator in unstructured_indicators if indicator.lower() in content_lower)
        
        # If content has multiple indicators of unstructured text
        if indicator_count >= 3:
            return True
        
        # Check paragraph structure (multiple line breaks suggest paragraphs)
        paragraph_breaks = content.count('\n\n')
        if paragraph_breaks >= 3:  # At least 3 paragraph breaks
            return True
        
        return False


class IngestionWorkflow:
    """LangGraph-based ingestion workflow"""
    
    def __init__(self, config: Optional[Any] = None):
        self.config = config or get_config()
        self.tool_registry = get_tool_registry()
        self.content_processor = ContentProcessor(self.config)
        self.workflow = self._create_workflow()
    
    def should_use_smart_processing(self, content: Any, metadata: Dict[str, Any]) -> bool:
        """Determine if content should use smart LLM + MCP processing - now the default for all structured data"""
        # Check if explicitly disabled
        analysis = metadata.get("analysis", {})
        if analysis.get("disable_smart_analysis"):
            return False
            
        # Use smart processing for all structured data types
        if isinstance(content, str):
            # File paths (CSV, JSON, etc.) - use smart processing
            if os.path.exists(content):
                return True
            # Regular text - use smart processing for analysis
            return True
            
        # All structured data (dict, list) - use smart processing
        if isinstance(content, (dict, list)):
            return True
                
        # Default to smart processing for comprehensive analysis
        return True
    
    def _create_workflow(self):
        """Create the LangGraph workflow"""
        workflow = StateGraph(IngestionState)
        
        # Add nodes
        workflow.add_node("analyze_content", self._analyze_content)
        workflow.add_node("route_tools", self._route_tools)
        workflow.add_node("process_graph", self._process_graph)
        workflow.add_node("process_vector", self._process_vector)
        workflow.add_node("validate_results", self._validate_results)
        workflow.add_node("complete", self._complete)
        workflow.add_node("handle_error", self._handle_error)
        
        # Define edges
        workflow.set_entry_point("analyze_content")
        
        # From analyze_content
        workflow.add_edge("analyze_content", "route_tools")
        
        # From route_tools
        workflow.add_conditional_edges(
            "route_tools",
            self._route_decision,
            {
                "process_graph": "process_graph",
                "process_vector": "process_vector",
                "complete": "complete",
                "error": "handle_error"
            }
        )
        
        # From process_graph (graph tool can be standalone)
        workflow.add_conditional_edges(
            "process_graph",
            self._graph_next,
            {
                "validate": "validate_results",
                "error": "handle_error"
            }
        )
        
        # From validate_results
        workflow.add_conditional_edges(
            "validate_results",
            self._validation_next,
            {
                "complete": "complete",
                "error": "handle_error"
            }
        )
        
        # Terminal nodes
        workflow.add_edge("complete", END)
        workflow.add_edge("handle_error", END)
        
        return workflow.compile()
    
    async def _analyze_content(self, state: IngestionState) -> IngestionState:
        """Analyze content and prepare for processing"""
        try:
            logger.info("Analyzing content for ingestion")
            
            content = state["content"]
            metadata = state.get("metadata", {})
            
            # Analyze content type and structure
            analysis = self.content_processor.analyze_content(content)
            
            return {
                **state,
                "current_step": "analyze_content",
                "status": IngestionStatus.PROCESSING,
                "metadata": {**metadata, "analysis": analysis},
                "next_action": "route"
            }
            
        except Exception as e:
            logger.error(f"Error analyzing content: {e}")
            return {
                **state,
                "current_step": "analyze_content",
                "status": IngestionStatus.FAILED,
                "errors": [str(e)],
                "next_action": "error"
            }
    
    async def _route_tools(self, state: IngestionState) -> IngestionState:
        """Route to appropriate tools based on content analysis"""
        try:
            logger.info("Routing to appropriate tools")
            
            content = state["content"]
            metadata = state["metadata"]
            analysis = metadata.get("analysis", {})
            
            # Check if should use smart processing (integrated LLM + MCP)
            if self.should_use_smart_processing(content, metadata):
                logger.info("Routing to smart LLM + MCP processing")
                return {
                    **state,
                    "selected_tools": ["smart_llm_mcp_ingest"],
                    "current_step": "route_tools",
                    "next_action": "process_graph"
                }
            
            # Otherwise use regular tool registry routing
            suggested_tools = analysis.get("suggested_tools", ["vector_ingest"])
            
            # Filter to only available and enabled tools
            available_tools = []
            for tool_name in suggested_tools:
                try:
                    self.tool_registry.get_tool(tool_name)
                    available_tools.append(tool_name)
                except Exception as e:
                    logger.warning(f"Tool {tool_name} not available: {e}")
            
            if not available_tools:
                logger.warning("No regular tools available, falling back to smart processing")
                return {
                    **state,
                    "selected_tools": ["smart_llm_mcp_ingest"],
                    "current_step": "route_tools",
                    "next_action": "process_graph"
                }
            
            selected_tools = available_tools
            logger.info(f"Selected tools: {selected_tools}")
            
            # Determine next action based on tool priority
            next_action = "complete"
            if "vector_ingest" in selected_tools:
                next_action = "process_vector"
            
            return {
                **state,
                "selected_tools": selected_tools,
                "current_step": "route_tools",
                "next_action": next_action
            }
            
        except Exception as e:
            logger.error(f"Error routing tools: {e}")
            return {
                **state,
                "current_step": "route_tools",
                "status": IngestionStatus.FAILED,
                "errors": state.get("errors", []) + [str(e)],
                "next_action": "error"
            }
    
    async def _process_vector(self, state: IngestionState) -> IngestionState:
        """Process content using vector ingestion tool"""
        try:
            logger.info("Processing content with vector ingestion tool")
            
            if "vector_ingest" not in state["selected_tools"]:
                return {
                    **state,
                    "current_step": "process_vector",
                    "next_action": "validate"
                }
            
            # Get vector tool
            vector_tool = self.tool_registry.get_tool("vector_ingest")
            
            # Ingest content
            result = await vector_tool.ingest(state["content"], state["metadata"])
            
            # Update state
            tool_results = state.get("tool_results", {})
            tool_results["vector_ingest"] = result
            
            return {
                **state,
                "current_step": "process_vector",
                "tool_results": tool_results,
                "vector_ingestion_result": result,
                "next_action": "validate"
            }
            
        except Exception as e:
            logger.error(f"Error in vector processing: {e}")
            return {
                **state,
                "current_step": "process_vector",
                "status": IngestionStatus.FAILED,
                "errors": state.get("errors", []) + [str(e)],
                "next_action": "error"
            }
    
    async def _process_graph(self, state: IngestionState) -> IngestionState:
        """Process content using smart LLM + MCP ingestion - main functionality for all structured data"""
        try:
            logger.info("Processing content with smart LLM + MCP ingestion")
            
            if "smart_llm_mcp_ingest" not in state["selected_tools"]:
                return {
                    **state,
                    "current_step": "process_graph",
                    "next_action": "validate"
                }
            
            # Import here to avoid circular imports
            from tools.graph_tool import GraphIngestionTool
            import pandas as pd
            
            content = state["content"]
            metadata = state["metadata"]
            context = metadata.get("context", "Intelligent data analysis and ingestion for structured content")
            sample_size = metadata.get("sample_size", 50)  # Increased default for general use
            
            # Prepare data based on content type - handle all structured data
            if isinstance(content, str) and os.path.exists(content):
                # Handle different file types
                if content.endswith('.csv'):
                    df = pd.read_csv(content)
                    logger.info(f"📊 Loaded CSV data: {len(df)} records")
                    sample_df = df.head(sample_size)
                    data_records = sample_df.to_dict('records')
                    logger.info(f"📦 Processing top {len(data_records)} records from {len(df)} total")
                elif content.endswith('.json'):
                    import json
                    with open(content, 'r') as f:
                        json_data = json.load(f)
                    if isinstance(json_data, list):
                        data_records = json_data[:sample_size]
                    else:
                        data_records = [json_data]
                    logger.info(f"📦 Processing {len(data_records)} JSON records")
                else:
                    # For other file types, read as text and analyze
                    with open(content, 'r', encoding='utf-8', errors='ignore') as f:
                        file_content = f.read()
                    data_records = [{"content": file_content, "file_type": content.split('.')[-1], "source": content}]
                    logger.info(f"📦 Processing file content as single record")
                    
            elif isinstance(content, list):
                data_records = content[:sample_size]
                logger.info(f"📦 Processing {len(data_records)} list records")
                
            elif isinstance(content, dict):
                data_records = [content]
                logger.info(f"📦 Processing single dictionary record")
                
            elif isinstance(content, str):
                # Text content - create a structured record
                data_records = [{"content": content, "content_type": "text", "length": len(content)}]
                logger.info(f"📦 Processing text content as single record")
                
            else:
                # Handle other data types by converting to string representation
                data_records = [{"content": str(content), "content_type": type(content).__name__, "raw_type": str(type(content))}]
                logger.info(f"📦 Processing {type(content).__name__} content as single record")
            
            # Use GraphIngestionTool with smart LLM + MCP analysis for all data types
            async with GraphIngestionTool() as tool:
                logger.info(f"🧠 Starting LLM + MCP smart ingestion for {len(data_records)} records")
                
                # Call the LLM + MCP smart ingestion method
                result = await tool.llm_mcp_smart_ingest(data_records, context)
                
                # Format results for workflow
                formatted_result = {
                    "status": result.get("status", "unknown"),
                    "tool": "smart_llm_mcp_ingest",
                    "records_processed": len(data_records),
                    "source_info": {
                        "content_type": type(content).__name__,
                        "sample_size": len(data_records),
                        "original_size": len(content) if isinstance(content, (list, str)) else 1
                    }
                }
                
                if result.get("status") == "success":
                    formatted_result.update({
                        "llm_analysis": result.get("llm_analysis", {}),
                        "schema_generation": {
                            "mcp_status": result.get("mcp_schema", {}).get("status"),
                            "optimization_status": result.get("optimized_schema", {}).get("status"),
                            "quality_score": result.get("optimized_schema", {}).get("quality_score")
                        },
                        "ingestion_metrics": {
                            "records_ingested": result.get("ingestion_result", {}).get("summary", {}).get("records_ingested", 0),
                            "success_rate": result.get("ingestion_result", {}).get("summary", {}).get("success_rate", 0),
                            "approach": result.get("intelligence_metrics", {}).get("approach"),
                            "llm_confidence": result.get("intelligence_metrics", {}).get("llm_confidence"),
                            "total_ingested": result.get("intelligence_metrics", {}).get("total_ingested", 0)
                        }
                    })
                else:
                    formatted_result["error"] = result.get("error", "Unknown error during smart ingestion")
            
            # Update state
            tool_results = state.get("tool_results", {})
            tool_results["smart_llm_mcp_ingest"] = formatted_result
            
            return {
                **state,
                "current_step": "process_graph",
                "tool_results": tool_results,
                "graph_ingestion_result": formatted_result,  # Smart tool primarily does graph ingestion
                "next_action": "validate"
            }
            
        except Exception as e:
            logger.error(f"Error in graph processing: {e}")
            return {
                **state,
                "current_step": "process_graph",
                "status": IngestionStatus.FAILED,
                "errors": state.get("errors", []) + [str(e)],
                "next_action": "error"
            }
    
    async def _validate_results(self, state: IngestionState) -> IngestionState:
        """Validate ingestion results"""
        try:
            logger.info("Validating ingestion results")
            
            tool_results = state.get("tool_results", {})
            validation_errors = []
            
            # Check each tool result
            for tool_name, result in tool_results.items():
                if result.get("status") != "success":
                    validation_errors.append(f"{tool_name} failed: {result.get('error', 'Unknown error')}")
            
            if validation_errors:
                return {
                    **state,
                    "current_step": "validate_results",
                    "status": IngestionStatus.FAILED,
                    "errors": state.get("errors", []) + validation_errors,
                    "next_action": "error"
                }
            
            return {
                **state,
                "current_step": "validate_results",
                "status": IngestionStatus.COMPLETED,
                "next_action": "complete"
            }
            
        except Exception as e:
            logger.error(f"Error validating results: {e}")
            return {
                **state,
                "current_step": "validate_results",
                "status": IngestionStatus.FAILED,
                "errors": state.get("errors", []) + [str(e)],
                "next_action": "error"
            }
    
    async def _complete(self, state: IngestionState) -> IngestionState:
        """Complete the workflow successfully"""
        logger.info("Ingestion workflow completed successfully")
        return {
            **state,
            "current_step": "complete",
            "status": IngestionStatus.COMPLETED
        }
    
    async def _handle_error(self, state: IngestionState) -> IngestionState:
        """Handle workflow errors"""
        errors = state.get("errors", [])
        logger.error(f"Ingestion workflow failed: {errors}")
        return {
            **state,
            "current_step": "handle_error",
            "status": IngestionStatus.FAILED
        }
    
    # Conditional edge functions
    def _route_decision(self, state: IngestionState) -> str:
        """Decide where to route from route_tools"""
        return state.get("next_action", "error")
    
    def _vector_next(self, state: IngestionState) -> str:
        """Decide next step after vector processing"""
        return state.get("next_action", "error")
    
    def _graph_next(self, state: IngestionState) -> str:
        """Decide next step after graph processing"""
        return state.get("next_action", "error")
    
    def _validation_next(self, state: IngestionState) -> str:
        """Decide next step after validation"""
        return state.get("next_action", "error")
    
    async def run_ingestion(self, content: Any, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Run the complete ingestion workflow"""
        try:
            logger.info("Starting ingestion workflow")
            
            # Prepare initial state
            initial_state = IngestionState(
                content=content,
                metadata=metadata or {},
                current_step="start",
                status=IngestionStatus.PENDING,
                selected_tools=[],
                tool_results={},
                errors=[],
                warnings=[],
                vector_ingestion_result=None,
                graph_ingestion_result=None,
                next_action="route"
            )
            
            # Run workflow
            result = await self.workflow.ainvoke(initial_state)
            
            return {
                "status": result["status"].value,
                "tool_results": result.get("tool_results", {}),
                "vector_result": result.get("vector_ingestion_result"),
                "graph_result": result.get("graph_ingestion_result"),
                "errors": result.get("errors", []),
                "warnings": result.get("warnings", [])
            }
            
        except Exception as e:
            logger.error(f"Error running ingestion workflow: {e}")
            return {
                "status": "failed",
                "tool_results": {},
                "vector_result": None,
                "graph_result": None,
                "errors": [str(e)],
                "warnings": []
            }


def create_ingestion_workflow(config: Optional[Any] = None) -> IngestionWorkflow:
    """Create an ingestion workflow instance"""
    return IngestionWorkflow(config)


async def demo_smart_ingestion_workflow():
    """Demo function showcasing smart LLM + MCP ingestion for all structured data types"""
    
    print("🚀 UNIVERSAL SMART LLM + MCP INGESTION WORKFLOW")
    print("="*60)
    print("Smart LLM + MCP processing for ALL structured data types!")
    print("Supports: CSV, JSON, Lists, Dictionaries, Text, and more")
    print("="*60)
    
    try:
        # Create workflow instance
        workflow = IngestionWorkflow()
        
        # Demo with different data types
        demo_data_sets = [
            {
                "name": "Sample Dictionary Data",
                "content": {
                    "user_id": "123",
                    "name": "John Doe",
                    "email": "john@example.com",
                    "age": 30,
                    "department": "Engineering",
                    "skills": ["Python", "React", "SQL"],
                    "projects": [
                        {"name": "Project A", "status": "completed"},
                        {"name": "Project B", "status": "in_progress"}
                    ]
                },
                "context": "Employee data for organizational analysis and relationship mapping"
            },
            {
                "name": "Sample List Data",
                "content": [
                    {"product_id": "P001", "name": "Laptop", "category": "Electronics", "price": 999.99},
                    {"product_id": "P002", "name": "Mouse", "category": "Electronics", "price": 29.99},
                    {"product_id": "P003", "name": "Keyboard", "category": "Electronics", "price": 79.99}
                ],
                "context": "Product catalog data for e-commerce relationship analysis"
            },
            {
                "name": "Sample Text Data", 
                "content": "This is a research paper about artificial intelligence and machine learning applications in healthcare. The study covers various algorithms including neural networks, decision trees, and support vector machines.",
                "context": "Research document analysis for knowledge extraction and entity relationships"
            }
        ]
        
        for i, dataset in enumerate(demo_data_sets, 1):
            print(f"\n🔄 DEMO {i}: {dataset['name']}")
            print("-" * 40)
            
            # Prepare metadata for smart processing
            metadata = {
                "context": dataset["context"],
                "sample_size": 10,
                "analysis": {
                    "content_type": type(dataset["content"]).__name__,
                    "use_smart_analysis": True,
                    "complexity": "moderate"
                }
            }
            
            print(f"📊 Processing {type(dataset['content']).__name__} data")
            print(f"🧠 Using smart LLM + MCP analysis")
            
            # Run workflow
            result = await workflow.run_ingestion(dataset["content"], metadata)
            
            # Display results
            print(f"\n📊 RESULTS:")
            print(f"   Status: {result.get('status')}")
            
            if result.get('status') == 'completed':
                tool_results = result.get('tool_results', {})
                
                if 'smart_llm_mcp_ingest' in tool_results:
                    smart_result = tool_results['smart_llm_mcp_ingest']
                    
                    print(f"\n🧠 LLM ANALYSIS:")
                    llm_analysis = smart_result.get('llm_analysis', {})
                    print(f"   Domain: {llm_analysis.get('domain', 'unknown')}")
                    print(f"   Confidence: {llm_analysis.get('confidence', 'unknown')}")
                    print(f"   Primary Node: {llm_analysis.get('primary_node_type', 'unknown')}")
                    print(f"   Entities: {', '.join(llm_analysis.get('entity_types', []))}")
                    
                    print(f"\n💾 INGESTION:")
                    metrics = smart_result.get('ingestion_metrics', {})
                    print(f"   Records Processed: {smart_result.get('records_processed', 0)}")
                    print(f"   Records Ingested: {metrics.get('records_ingested', 0)}")
                    print(f"   Success Rate: {metrics.get('success_rate', 0)}%")
                    print(f"   Approach: {metrics.get('approach', 'unknown')}")
                    
            else:
                errors = result.get('errors', [])
                print(f"   Errors: {errors}")
            
            print("-" * 40)
        
        print(f"\n{'='*60}")
        print("🎉 UNIVERSAL SMART INGESTION WORKFLOW COMPLETED!")
        print("="*60)
        print("Key Features:")
        print("✅ Smart LLM + MCP analysis for ANY data type")
        print("✅ Automatic content type detection")
        print("✅ Dynamic schema generation")
        print("✅ Comprehensive relationship mapping")
        print("✅ Universal structured data processing")
        print("✅ Intelligent entity extraction")
        print("="*60)
        
    except Exception as e:
        print(f"❌ Error in workflow demo: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import asyncio
    asyncio.run(demo_smart_ingestion_workflow())
