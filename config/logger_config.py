"""
Structured Logging Configuration for Ingestion Agents

Configures structured logging using Structlog for consistent logging across the application.
Supports different log levels for development (INFO) and production (ERROR) environments.
"""

import os
import sys
import structlog
import logging
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime


class LoggerConfig:
    """Configuration class for structured logging using Structlog."""
    
    def __init__(self, environment: str = None):
        """
        Initialize logger configuration.
        
        Args:
            environment: Environment type ('dev', 'prod'). If None, reads from ENV variable.
        """
        self.environment = environment or os.getenv('ENVIRONMENT', 'dev').lower()
        self.log_level = self._get_log_level()
        self.log_dir = self._get_log_directory()
        self._setup_logging()
    
    def _get_log_level(self) -> int:
        """Get appropriate log level based on environment."""
        if self.environment in ['production', 'prod']:
            return logging.ERROR
        elif self.environment in ['development', 'dev']:
            return logging.INFO
        else:
            return logging.INFO  # Default to INFO for unknown environments
    
    def _get_log_directory(self) -> Path:
        """Create and return logs directory."""
        project_root = Path(__file__).parent.parent
        log_dir = project_root / "logs"
        log_dir.mkdir(exist_ok=True)
        return log_dir
    
    def _setup_logging(self):
        """Configure structlog with appropriate processors and formatters."""
        
        # Configure standard library logging
        logging.basicConfig(
            format="%(message)s",
            stream=sys.stdout,
            level=self.log_level,
        )
        
        # Define processors based on environment
        processors = [
            # Add timestamp
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="ISO"),
            
            # Add context processors
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            
            # Add custom processors for ReAct pattern
            self._add_react_context,
        ]
        
        # Add environment-specific processors
        if self.environment in ['production', 'prod']:
            # Production: JSON output for log aggregation
            processors.append(structlog.processors.JSONRenderer())
        else:
            # Development: Human-readable output
            processors.extend([
                structlog.processors.CallsiteParameterAdder(
                    parameters=[structlog.processors.CallsiteParameter.FILENAME,
                               structlog.processors.CallsiteParameter.LINENO]
                ),
                structlog.dev.ConsoleRenderer(colors=True)
            ])
        
        # Configure structlog
        structlog.configure(
            processors=processors,
            wrapper_class=structlog.stdlib.BoundLogger,
            logger_factory=structlog.stdlib.LoggerFactory(),
            context_class=dict,
            cache_logger_on_first_use=True,
        )
    
    def _add_react_context(self, logger, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Add ReAct pattern context to log events."""
        
        # Add ReAct phase if specified
        if 'react_phase' in event_dict:
            event_dict['phase'] = event_dict.pop('react_phase')
        
        # Add agent context if specified
        if 'agent_type' in event_dict:
            event_dict['component'] = f"agent.{event_dict.pop('agent_type')}"
        
        # Add execution context
        if 'execution_id' in event_dict:
            event_dict['exec_id'] = event_dict['execution_id']
        
        return event_dict
    
    def get_logger(self, name: str) -> structlog.stdlib.BoundLogger:
        """
        Get a configured logger instance.
        
        Args:
            name: Logger name (typically __name__)
            
        Returns:
            Configured structlog logger
        """
        return structlog.get_logger(name)
    
    def get_agent_logger(self, agent_name: str, execution_id: str = None) -> structlog.stdlib.BoundLogger:
        """
        Get a logger specifically configured for agent operations.
        
        Args:
            agent_name: Name of the agent (e.g., 'planner', 'executor')
            execution_id: Unique execution identifier
            
        Returns:
            Bound logger with agent context
        """
        logger = self.get_logger(f"agents.{agent_name}")
        
        context = {'agent_type': agent_name}
        if execution_id:
            context['execution_id'] = execution_id
            
        return logger.bind(**context)
    
    def get_api_logger(self, endpoint: str = None) -> structlog.stdlib.BoundLogger:
        """
        Get a logger specifically configured for API operations.
        
        Args:
            endpoint: API endpoint name
            
        Returns:
            Bound logger with API context
        """
        logger_name = f"api.{endpoint}" if endpoint else "api"
        return self.get_logger(logger_name)
    
    def get_tool_logger(self, tool_name: str) -> structlog.stdlib.BoundLogger:
        """
        Get a logger specifically configured for tool operations.
        
        Args:
            tool_name: Name of the tool
            
        Returns:
            Bound logger with tool context
        """
        return self.get_logger(f"tools.{tool_name}")


# Global logger configuration instance
_logger_config: Optional[LoggerConfig] = None


def init_logging(environment: str = None) -> LoggerConfig:
    """
    Initialize global logging configuration.
    
    Args:
        environment: Environment type ('dev', 'prod')
        
    Returns:
        LoggerConfig instance
    """
    global _logger_config
    _logger_config = LoggerConfig(environment)
    return _logger_config


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Configured structlog logger
    """
    if _logger_config is None:
        init_logging()
    return _logger_config.get_logger(name)


def get_agent_logger(agent_name: str, execution_id: str = None) -> structlog.stdlib.BoundLogger:
    """
    Get a logger for agent operations with ReAct pattern support.
    
    Args:
        agent_name: Name of the agent
        execution_id: Unique execution identifier
        
    Returns:
        Bound logger with agent context
    """
    if _logger_config is None:
        init_logging()
    return _logger_config.get_agent_logger(agent_name, execution_id)


def get_api_logger(endpoint: str = None) -> structlog.stdlib.BoundLogger:
    """
    Get a logger for API operations.
    
    Args:
        endpoint: API endpoint name
        
    Returns:
        Bound logger with API context
    """
    if _logger_config is None:
        init_logging()
    return _logger_config.get_api_logger(endpoint)


def get_tool_logger(tool_name: str) -> structlog.stdlib.BoundLogger:
    """
    Get a logger for tool operations.
    
    Args:
        tool_name: Name of the tool
        
    Returns:
        Bound logger with tool context
    """
    if _logger_config is None:
        init_logging()
    return _logger_config.get_tool_logger(tool_name)


# ReAct Pattern Logging Helpers
class ReActLogger:
    """Helper class for ReAct pattern logging."""
    
    def __init__(self, logger: structlog.stdlib.BoundLogger, execution_id: str = None):
        self.logger = logger
        self.execution_id = execution_id or self._generate_execution_id()
    
    def _generate_execution_id(self) -> str:
        """Generate a unique execution ID."""
        return f"exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.getpid()}"
    
    def reasoning(self, thought: str, context: Dict[str, Any] = None, **kwargs):
        """Log reasoning phase of ReAct pattern."""
        log_data = {
            'react_phase': 'reasoning',
            'thought': thought,
            'execution_id': self.execution_id
        }
        if context:
            log_data.update(context)
        log_data.update(kwargs)
        
        self.logger.info("Agent reasoning", **log_data)
    
    def planning(self, plan: Dict[str, Any], rationale: str = None, **kwargs):
        """Log planning phase of ReAct pattern."""
        log_data = {
            'react_phase': 'planning',
            'plan': plan,
            'execution_id': self.execution_id
        }
        if rationale:
            log_data['rationale'] = rationale
        log_data.update(kwargs)
        
        self.logger.info("Agent planning", **log_data)
    
    def action(self, action: str, parameters: Dict[str, Any] = None, **kwargs):
        """Log action phase of ReAct pattern."""
        log_data = {
            'react_phase': 'action',
            'action': action,
            'execution_id': self.execution_id
        }
        if parameters:
            log_data['parameters'] = parameters
        log_data.update(kwargs)
        
        self.logger.info("Agent action", **log_data)
    
    def observation(self, result: Any, success: bool = True, error: str = None, **kwargs):
        """Log observation phase of ReAct pattern."""
        log_data = {
            'react_phase': 'observation',
            'success': success,
            'execution_id': self.execution_id
        }
        
        if success:
            log_data['result'] = result
            self.logger.info("Agent observation", **log_data, **kwargs)
        else:
            log_data['error'] = error
            self.logger.error("Agent observation failed", **log_data, **kwargs)
    
    def error(self, error: Exception, context: Dict[str, Any] = None, **kwargs):
        """Log error with full context."""
        log_data = {
            'react_phase': 'error',
            'error_type': type(error).__name__,
            'error_message': str(error),
            'execution_id': self.execution_id
        }
        if context:
            log_data.update(context)
        log_data.update(kwargs)
        
        self.logger.error("Agent error", **log_data, exc_info=True)


def get_react_logger(agent_name: str, execution_id: str = None) -> ReActLogger:
    """
    Get a ReAct pattern logger for agent operations.
    
    Args:
        agent_name: Name of the agent
        execution_id: Unique execution identifier
        
    Returns:
        ReActLogger instance
    """
    base_logger = get_agent_logger(agent_name, execution_id)
    return ReActLogger(base_logger, execution_id)


# Example usage and testing
if __name__ == "__main__":
    # Test the logging configuration
    init_logging('dev')
    
    # Test basic logging
    logger = get_logger(__name__)
    logger.info("Logger configuration test", component="config", status="initialized")
    
    # Test agent logging
    agent_logger = get_agent_logger("test_agent", "test_exec_123")
    agent_logger.info("Agent test message", operation="initialization")
    
    # Test ReAct pattern logging
    react_logger = get_react_logger("test_agent", "react_exec_456")
    react_logger.reasoning("Analyzing document type and content", {"doc_type": "pdf"})
    react_logger.planning({"step1": "extract", "step2": "process"}, "Based on document analysis")
    react_logger.action("extract_content", {"method": "pdf_parser"})
    react_logger.observation({"content_length": 1500, "pages": 5}, success=True)
