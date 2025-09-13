"""
Tool Registry for Ingestion Agents

Centralized registry for managing and initializing ingestion tools with proper configuration.
"""

import logging
from typing import Dict, Any, Optional, Type
import sys
import os

# Add parent directory for config import
parent_dir = os.path.dirname(os.path.dirname(__file__))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from config.config_manager import get_config
    config_manager = get_config()
    def get_registry_config():
        return config_manager
except ImportError:
    def get_registry_config():
        return None

logger = logging.getLogger(__name__)


class ToolRegistry:
    """Central registry for managing ingestion tools."""
    
    def __init__(self):
        """Initialize the tool registry."""
        self.config = get_registry_config()
        self.tools: Dict[str, Any] = {}
        self.tool_classes: Dict[str, Type] = {}
        self.logger = logging.getLogger(__name__)
        
        # Register available tool classes
        self._register_tool_classes()
    
    def _register_tool_classes(self):
        """Register available tool classes."""
        try:
            from agents.tools.vector_tool import VectorTool
            self.tool_classes['vector_ingestion'] = VectorTool
            self.logger.info("Registered VectorTool")
        except ImportError as e:
            self.logger.warning(f"Failed to register VectorTool: {e}")
        
        try:
            from agents.tools.graph_tool import GraphIngestionTool
            self.tool_classes['graph_ingestion'] = GraphIngestionTool
            self.logger.info("Registered GraphIngestionTool")
        except ImportError as e:
            self.logger.warning(f"Failed to register GraphIngestionTool: {e}")
    
    def get_tool(self, tool_name: str) -> Optional[Any]:
        """
        Get a tool instance, creating it if necessary.
        
        Args:
            tool_name: Name of the tool to retrieve
            
        Returns:
            Tool instance or None if not available
        """
        if tool_name in self.tools:
            return self.tools[tool_name]
        
        if tool_name in self.tool_classes:
            try:
                tool_instance = self.tool_classes[tool_name]()
                self.tools[tool_name] = tool_instance
                self.logger.info(f"Initialized tool: {tool_name}")
                return tool_instance
            except Exception as e:
                self.logger.error(f"Failed to initialize tool {tool_name}: {e}")
                return None
        
        self.logger.warning(f"Tool not found: {tool_name}")
        return None
    
    def get_all_tools(self) -> Dict[str, Any]:
        """
        Get all available tool instances.
        
        Returns:
            Dictionary of tool names to instances
        """
        tools = {}
        for tool_name in self.tool_classes.keys():
            tool_instance = self.get_tool(tool_name)
            if tool_instance:
                tools[tool_name] = tool_instance
        
        return tools
    
    def is_tool_available(self, tool_name: str) -> bool:
        """
        Check if a tool is available.
        
        Args:
            tool_name: Name of the tool to check
            
        Returns:
            True if available, False otherwise
        """
        return tool_name in self.tool_classes
    
    def get_tool_info(self) -> Dict[str, Any]:
        """
        Get information about registered tools.
        
        Returns:
            Dictionary with tool information
        """
        return {
            "registered_tools": list(self.tool_classes.keys()),
            "initialized_tools": list(self.tools.keys()),
            "total_registered": len(self.tool_classes),
            "total_initialized": len(self.tools)
        }
    
    def validate_tools(self) -> Dict[str, Any]:
        """
        Validate all registered tools.
        
        Returns:
            Validation results for each tool
        """
        results = {}
        
        for tool_name in self.tool_classes.keys():
            try:
                tool_instance = self.get_tool(tool_name)
                if tool_instance and hasattr(tool_instance, 'validate'):
                    # Check if validate is async or sync
                    import asyncio
                    if asyncio.iscoroutinefunction(tool_instance.validate):
                        # For async validate methods, we'll mark as available without validation
                        results[tool_name] = {
                            "status": "available",
                            "validation": "async_method_skipped"
                        }
                    else:
                        # Test with empty content for validation
                        validation_result = tool_instance.validate("")
                        results[tool_name] = {
                            "status": "available",
                            "validation": validation_result
                        }
                else:
                    results[tool_name] = {
                        "status": "available",
                        "validation": "no_validate_method"
                    }
            except Exception as e:
                results[tool_name] = {
                    "status": "error",
                    "error": str(e)
                }
        
        return results


# Global registry instance
_registry = None


def get_tool_registry() -> ToolRegistry:
    """Get the global tool registry instance."""
    global _registry
    if _registry is None:
        _registry = ToolRegistry()
    return _registry


def get_tool(tool_name: str) -> Optional[Any]:
    """Convenience function to get a tool from the global registry."""
    return get_tool_registry().get_tool(tool_name)


def get_all_tools() -> Dict[str, Any]:
    """Convenience function to get all tools from the global registry."""
    return get_tool_registry().get_all_tools()


# Example usage
if __name__ == "__main__":
    import asyncio
    
    async def main():
        """Test the tool registry."""
        registry = get_tool_registry()
        
        print("Tool Registry Information:")
        print(f"Tool info: {registry.get_tool_info()}")
        
        print("\nTool Validation:")
        validation_results = registry.validate_tools()
        for tool_name, result in validation_results.items():
            print(f"  {tool_name}: {result}")
        
        # Test getting tools
        vector_tool = registry.get_tool('vector_ingestion')
        graph_tool = registry.get_tool('graph_ingestion')
        
        print(f"\nVector tool available: {vector_tool is not None}")
        return {
            "graph_tool_available": graph_tool is not None,
            "vector_tool_available": vector_tool is not None
        }
        
        if vector_tool:
            # Test vector tool
            result = await vector_tool.ingest(
                "Test content for vector ingestion",
                {"doc_uri": "test://registry/test", "source": "registry_test"}
            )
            return result
    
    asyncio.run(main())
