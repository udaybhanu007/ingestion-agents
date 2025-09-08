"""
Dynamic Tool Registry for Ingestion System

This module provides a dynamic tool registry that can register and manage
different ingestion tools for vector and graph databases.
"""

import logging
from typing import Dict, List, Any, Type, Protocol, Optional
from abc import ABC, abstractmethod
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class IngestionTool(Protocol):
    """Protocol for ingestion tools"""
    
    @property
    def name(self) -> str:
        """Return the tool name"""
        ...
    
    @property
    def description(self) -> str:
        """Return the tool description"""
        ...
    
    async def ingest(self, content: Any, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Ingest content with metadata"""
        ...
    
    async def validate(self, content: Any) -> bool:
        """Validate content before ingestion"""
        ...


@dataclass
class ToolInfo:
    """Information about a registered tool"""
    name: str
    description: str
    tool_class: Type[IngestionTool]
    priority: int = 0
    enabled: bool = True


class ToolRegistry:
    """Dynamic registry for ingestion tools"""
    
    def __init__(self):
        self._tools: Dict[str, ToolInfo] = {}
        self._instances: Dict[str, IngestionTool] = {}
        
    def register_tool(
        self,
        name: str,
        tool_class: Type[IngestionTool],
        description: str = "",
        priority: int = 0,
        enabled: bool = True
    ) -> None:
        """Register a new ingestion tool"""
        
        tool_info = ToolInfo(
            name=name,
            description=description or tool_class.__doc__ or "No description available",
            tool_class=tool_class,
            priority=priority,
            enabled=enabled
        )
        
        self._tools[name] = tool_info
        logger.info(f"Registered tool: {name} (priority: {priority})")
    
    def unregister_tool(self, name: str) -> None:
        """Unregister a tool"""
        if name in self._tools:
            del self._tools[name]
            if name in self._instances:
                del self._instances[name]
            logger.info(f"Unregistered tool: {name}")
    
    def get_tool(self, name: str) -> IngestionTool:
        """Get a tool instance by name"""
        if name not in self._tools:
            raise ValueError(f"Tool '{name}' is not registered")
        
        if not self._tools[name].enabled:
            raise ValueError(f"Tool '{name}' is disabled")
        
        # Create instance if not exists
        if name not in self._instances:
            tool_class = self._tools[name].tool_class
            self._instances[name] = tool_class()
        
        return self._instances[name]
    
    def list_tools(self, enabled_only: bool = True) -> List[ToolInfo]:
        """List all registered tools"""
        tools = list(self._tools.values())
        
        if enabled_only:
            tools = [tool for tool in tools if tool.enabled]
        
        # Sort by priority (higher priority first)
        return sorted(tools, key=lambda x: x.priority, reverse=True)
    
    def enable_tool(self, name: str) -> None:
        """Enable a tool"""
        if name in self._tools:
            self._tools[name].enabled = True
            logger.info(f"Enabled tool: {name}")
    
    def disable_tool(self, name: str) -> None:
        """Disable a tool"""
        if name in self._tools:
            self._tools[name].enabled = False
            logger.info(f"Disabled tool: {name}")
    
    def get_tool_by_type(self, content_type: str) -> List[IngestionTool]:
        """Get tools that can handle a specific content type"""
        suitable_tools = []
        
        for tool_info in self.list_tools():
            try:
                tool = self.get_tool(tool_info.name)
                # Check if tool can handle this content type
                # This would be implemented by each tool's validation method
                suitable_tools.append(tool)
            except Exception as e:
                logger.warning(f"Error getting tool {tool_info.name}: {e}")
        
        return suitable_tools


# Global tool registry instance
_tool_registry: Optional[ToolRegistry] = None


def get_tool_registry() -> ToolRegistry:
    """Get the global tool registry instance"""
    global _tool_registry
    if _tool_registry is None:
        _tool_registry = ToolRegistry()
        _register_default_tools()
    return _tool_registry


def _register_default_tools():
    """Register default tools"""
    # Import here to avoid circular imports
    try:
        import sys
        import os
        
        # Add current directory to path for imports
        current_dir = os.path.dirname(__file__)
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)
        
        from tools.vector_tool import VectorIngestionTool
        from tools.graph_tool import GraphIngestionTool
        
        registry = _tool_registry
        if registry is None:
            return
        
        # Register vector tool
        registry.register_tool(
            name="vector_ingest",
            tool_class=VectorIngestionTool,
            description="Ingests content into Qdrant vector database",
            priority=10
        )
        
        # Register graph tool
        registry.register_tool(
            name="graph_ingest", 
            tool_class=GraphIngestionTool,
            description="Ingests content into Neo4j graph database using MCP",
            priority=10
        )
        
        # Smart LLM + MCP functionality is integrated directly into workflow
        # No separate tool registration needed
    except ImportError as e:
        logger.warning(f"Could not import default tools: {e}")


def register_custom_tool(name: str, tool_class: Type[IngestionTool], **kwargs):
    """Register a custom tool"""
    registry = get_tool_registry()
    registry.register_tool(name, tool_class, **kwargs)
