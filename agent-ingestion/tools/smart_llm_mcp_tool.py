"""
Smart LLM + MCP Ingestion Tool

This tool wraps the demo_llm_mcp_smart functionality into a proper tool
that can be registered with the tool registry system.
"""

import asyncio
import logging
import pandas as pd
import os
from typing import Dict, Any, Optional, Union
from .graph_tool import GraphIngestionTool

logger = logging.getLogger(__name__)


class SmartLLMMCPTool:
    """Smart LLM + MCP ingestion tool for intelligent data analysis and ingestion"""
    
    def __init__(self):
        self.name = "smart_llm_mcp_ingest"
        self.description = "Intelligent data ingestion using LLM reasoning and MCP functions for optimal schema generation"
        self.graph_tool = None
    
    @property
    def name(self) -> str:
        return self.name
    
    @property
    def description(self) -> str:
        return self.description
    
    async def validate(self, content: Any) -> bool:
        """Validate content before ingestion"""
        try:
            # Can handle various data types
            if isinstance(content, (dict, list)):
                return True
            elif isinstance(content, str):
                # Try to parse as file path
                if os.path.exists(content) and content.endswith('.csv'):
                    return True
            elif hasattr(content, 'to_dict'):  # DataFrame-like
                return True
            return False
        except Exception as e:
            logger.warning(f"Validation error: {e}")
            return False
    
    async def ingest(self, content: Any, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ingest content using LLM + MCP smart analysis
        
        Args:
            content: Data to ingest (dict, list, DataFrame, or file path)
            metadata: Additional metadata including context
            
        Returns:
            Ingestion results with intelligence metrics
        """
        try:
            logger.info("Starting Smart LLM + MCP ingestion")
            
            # Extract context from metadata
            context = metadata.get('context', 'General data ingestion using intelligent analysis')
            
            # Prepare data for ingestion
            processed_data = await self._prepare_data(content, metadata)
            if not processed_data:
                return {
                    'status': 'error',
                    'error': 'Failed to prepare data for ingestion'
                }
            
            # Use GraphIngestionTool with smart LLM + MCP analysis
            async with GraphIngestionTool() as tool:
                self.graph_tool = tool
                
                logger.info(f"Processing {len(processed_data.get('records', []))} records with smart analysis")
                
                # Call the LLM + MCP smart ingestion
                result = await tool.llm_mcp_smart_ingest(
                    processed_data['records'], 
                    context
                )
                
                # Format results for consistent return structure
                return self._format_results(result, processed_data)
                
        except Exception as e:
            logger.error(f"Smart LLM + MCP ingestion error: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'tool': self.name
            }
    
    async def _prepare_data(self, content: Any, metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Prepare data for ingestion based on content type"""
        try:
            records = []
            data_info = {}
            
            if isinstance(content, dict):
                # Single record
                records = [content]
                data_info = {'type': 'single_record', 'count': 1}
                
            elif isinstance(content, list):
                # Multiple records
                records = content
                data_info = {'type': 'multiple_records', 'count': len(content)}
                
            elif isinstance(content, str) and os.path.exists(content):
                # File path - handle CSV files
                if content.endswith('.csv'):
                    df = pd.read_csv(content)
                    
                    # Use sample size from metadata or default to 20
                    sample_size = metadata.get('sample_size', 20)
                    sample_df = df.head(sample_size)
                    
                    records = sample_df.to_dict('records')
                    data_info = {
                        'type': 'csv_file',
                        'file_path': content,
                        'total_records': len(df),
                        'sample_size': len(records)
                    }
                    
                    logger.info(f"Loaded CSV: {len(df)} total records, processing {len(records)} records")
                else:
                    logger.error(f"Unsupported file type: {content}")
                    return None
                    
            elif hasattr(content, 'to_dict'):
                # DataFrame-like object
                if hasattr(content, 'head'):
                    sample_size = metadata.get('sample_size', 20)
                    sample_content = content.head(sample_size)
                    records = sample_content.to_dict('records')
                else:
                    records = content.to_dict('records')
                data_info = {'type': 'dataframe', 'count': len(records)}
                
            else:
                logger.error(f"Unsupported content type: {type(content)}")
                return None
            
            return {
                'records': records,
                'data_info': data_info
            }
            
        except Exception as e:
            logger.error(f"Error preparing data: {e}")
            return None
    
    def _format_results(self, result: Dict[str, Any], processed_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format results for consistent return structure"""
        try:
            # Base result structure
            formatted_result = {
                'status': result.get('status', 'unknown'),
                'tool': self.name,
                'data_info': processed_data.get('data_info', {}),
                'records_processed': len(processed_data.get('records', [])),
            }
            
            if result.get('status') == 'success':
                # Extract key information from smart ingestion results
                formatted_result.update({
                    'llm_analysis': result.get('llm_analysis', {}),
                    'schema_generation': {
                        'mcp_status': result.get('mcp_schema', {}).get('status'),
                        'optimization_status': result.get('optimized_schema', {}).get('status'),
                        'quality_score': result.get('optimized_schema', {}).get('quality_score')
                    },
                    'ingestion_metrics': {
                        'records_ingested': result.get('ingestion_result', {}).get('summary', {}).get('records_ingested', 0),
                        'success_rate': result.get('ingestion_result', {}).get('summary', {}).get('success_rate', 0),
                        'approach': result.get('intelligence_metrics', {}).get('approach'),
                        'llm_confidence': result.get('intelligence_metrics', {}).get('llm_confidence'),
                        'total_ingested': result.get('intelligence_metrics', {}).get('total_ingested', 0)
                    },
                    'intelligence_summary': {
                        'domain_detected': result.get('llm_analysis', {}).get('domain'),
                        'primary_node_type': result.get('llm_analysis', {}).get('primary_node_type'),
                        'entity_types': result.get('llm_analysis', {}).get('entity_types', []),
                        'complexity': result.get('llm_analysis', {}).get('complexity')
                    }
                })
            else:
                formatted_result['error'] = result.get('error', 'Unknown error during smart ingestion')
            
            return formatted_result
            
        except Exception as e:
            logger.error(f"Error formatting results: {e}")
            return {
                'status': 'error',
                'error': f'Error formatting results: {str(e)}',
                'tool': self.name
            }


# Function to create and register the smart tool
def create_smart_llm_mcp_tool() -> SmartLLMMCPTool:
    """Create a new SmartLLMMCPTool instance"""
    return SmartLLMMCPTool()


# Export for tool registry
__all__ = ['SmartLLMMCPTool', 'create_smart_llm_mcp_tool']
