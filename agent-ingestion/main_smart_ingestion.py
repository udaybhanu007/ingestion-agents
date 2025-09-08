"""
Main entry point for intelligent ingestion using the workflow system

This module demonstrates how to use the ingestion workflow with the smart LLM + MCP tool
for intelligent data analysis and graph ingestion.
"""

import asyncio
import logging
import os
import pandas as pd
from typing import Dict, Any, Optional
from ingestion_workflow import IngestionWorkflow
from tool_registry import get_tool_registry

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def smart_ingest_bbox_data(
    file_path: Optional[str] = None, 
    sample_size: int = 20,
    context: Optional[str] = None
) -> Dict[str, Any]:
    """
    Intelligent ingestion of BBox medical data using workflow system
    
    Args:
        file_path: Path to CSV file (defaults to BBox file)
        sample_size: Number of records to process (default: 20)
        context: Context description for LLM analysis
        
    Returns:
        Workflow execution results
    """
    
    # Default file path and context
    if file_path is None:
        file_path = "c:/_Projects/GitHubProject/RAG-agents/downloaded_content/BBox_List_2017.csv"
    
    if context is None:
        context = "Medical chest X-ray bounding box annotations for pathology detection. Contains image references, finding labels, and spatial coordinates for automated medical image analysis."
    
    logger.info(f"🚀 Starting intelligent ingestion workflow")
    logger.info(f"📁 File: {file_path}")
    logger.info(f"📊 Sample size: {sample_size}")
    logger.info(f"🎯 Context: {context[:100]}...")
    
    try:
        # Create workflow instance
        workflow = IngestionWorkflow()
        
        # Prepare metadata with context and sample size
        metadata = {
            "context": context,
            "sample_size": sample_size,
            "source_file": file_path,
            "ingestion_type": "smart_llm_mcp"
        }
        
        # Run workflow with file path as content
        logger.info("🔄 Executing ingestion workflow...")
        result = await workflow.run_ingestion(file_path, metadata)
        
        # Log results
        logger.info(f"✅ Workflow completed with status: {result.get('status')}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error in intelligent ingestion: {e}")
        return {
            "status": "error",
            "error": str(e),
            "workflow": "smart_ingest_bbox_data"
        }


async def demo_workflow_integration():
    """Demonstrate the integration of demo_llm_mcp_smart with ingestion workflow"""
    
    print("🎯 INTELLIGENT INGESTION WORKFLOW DEMO")
    print("="*60)
    print("This demo shows how the smart LLM + MCP functionality")
    print("is integrated into the LangGraph-based ingestion workflow")
    print("with dynamic tool registry!")
    print("="*60)
    
    # Show registered tools
    registry = get_tool_registry()
    tools = registry.list_tools()
    
    print(f"\n🔧 REGISTERED TOOLS ({len(tools)} total):")
    for tool in tools:
        print(f"   • {tool.name} (priority: {tool.priority})")
        print(f"     {tool.description}")
    
    # Run intelligent ingestion
    print(f"\n🚀 RUNNING INTELLIGENT INGESTION...")
    print("-" * 40)
    
    result = await smart_ingest_bbox_data(
        sample_size=20,
        context="Medical imaging data with bounding box coordinates for chest X-ray abnormalities"
    )
    
    # Display results
    print(f"\n📊 WORKFLOW RESULTS:")
    print(f"   Status: {result.get('status')}")
    
    if result.get('status') == 'success':
        # Show workflow execution details
        state = result.get('final_state', {})
        tools_used = state.get('selected_tools', [])
        tool_results = state.get('tool_results', {})
        
        print(f"   Tools Selected: {', '.join(tools_used)}")
        print(f"   Steps Completed: {state.get('current_step', 'unknown')}")
        
        # Show smart tool results if available
        if 'smart_llm_mcp_ingest' in tool_results:
            smart_result = tool_results['smart_llm_mcp_ingest']
            
            print(f"\n🧠 SMART LLM + MCP RESULTS:")
            print(f"   Records Processed: {smart_result.get('records_processed', 0)}")
            
            if 'llm_analysis' in smart_result:
                llm_analysis = smart_result['llm_analysis']
                print(f"   Domain Detected: {llm_analysis.get('domain', 'unknown')}")
                print(f"   LLM Confidence: {llm_analysis.get('confidence', 'unknown')}")
                print(f"   Primary Node Type: {llm_analysis.get('primary_node_type', 'unknown')}")
            
            if 'ingestion_metrics' in smart_result:
                metrics = smart_result['ingestion_metrics']
                print(f"   Records Ingested: {metrics.get('records_ingested', 0)}")
                print(f"   Success Rate: {metrics.get('success_rate', 0)}%")
                print(f"   Approach Used: {metrics.get('approach', 'unknown')}")
            
            if 'intelligence_summary' in smart_result:
                summary = smart_result['intelligence_summary']
                print(f"   Complexity: {summary.get('complexity', 'unknown')}")
                entities = summary.get('entity_types', [])
                if entities:
                    print(f"   Entity Types: {', '.join(entities)}")
    
    else:
        print(f"   Error: {result.get('error', 'Unknown error')}")
        
        # Show any errors from the workflow
        state = result.get('final_state', {})
        errors = state.get('errors', [])
        if errors:
            print(f"   Workflow Errors:")
            for error in errors:
                print(f"     • {error}")
    
    print(f"\n{'='*60}")
    print("🎉 WORKFLOW INTEGRATION DEMO COMPLETED!")
    print("="*60)
    print("Key Integration Benefits Demonstrated:")
    print("✅ Dynamic tool registry with smart tool")
    print("✅ LangGraph workflow orchestration") 
    print("✅ Intelligent tool selection based on content")
    print("✅ Smart LLM + MCP analysis integration")
    print("✅ Comprehensive error handling and validation")
    print("✅ Structured result reporting")
    print("="*60)


async def simple_smart_ingestion():
    """Simple example of using the smart ingestion via workflow"""
    
    print("\n🔥 SIMPLE SMART INGESTION EXAMPLE")
    print("="*50)
    
    # Just call the smart ingestion function
    result = await smart_ingest_bbox_data(sample_size=10)
    
    print(f"Status: {result.get('status')}")
    if result.get('status') == 'success':
        state = result.get('final_state', {})
        tool_results = state.get('tool_results', {})
        
        if 'smart_llm_mcp_ingest' in tool_results:
            smart_result = tool_results['smart_llm_mcp_ingest']
            metrics = smart_result.get('ingestion_metrics', {})
            print(f"Records ingested: {metrics.get('records_ingested', 0)}")
            print(f"Approach: {metrics.get('approach', 'unknown')}")
            
            llm_analysis = smart_result.get('llm_analysis', {})
            print(f"Domain: {llm_analysis.get('domain', 'unknown')}")
    else:
        print(f"Error: {result.get('error')}")


if __name__ == "__main__":
    # Run the comprehensive workflow demo
    asyncio.run(demo_workflow_integration())
    
    # Show simple usage
    # asyncio.run(simple_smart_ingestion())
