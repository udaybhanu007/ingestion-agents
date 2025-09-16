"""
Simple Logging Configuration for Ingestion Agents

A simplified logging setup that eliminates all complex dependencies and issues.
Uses standard Python logging with clear, simple configuration.
"""

import logging
import sys
import os
from pathlib import Path
from datetime import datetime


def setup_simple_logging(name: str = None, level: str = "INFO") -> logging.Logger:
    """
    Setup simple, robust logging that just works.
    
    Args:
        name: Logger name (typically __name__)
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        
    Returns:
        Configured logger instance
    """
    if name is None:
        name = "ingestion-agents"
    
    # Create logger
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Set log level
    log_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    logger.setLevel(log_levels.get(level.upper(), logging.INFO))
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logger.level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(console_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger


def get_logger(name: str = None) -> logging.Logger:
    """
    Get a simple logger instance.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return setup_simple_logging(name)


def get_agent_logger(agent_name: str) -> logging.Logger:
    """
    Get a logger for agent operations.
    
    Args:
        agent_name: Name of the agent
        
    Returns:
        Logger instance
    """
    return setup_simple_logging(f"agents.{agent_name}")


def get_api_logger(endpoint: str = None) -> logging.Logger:
    """
    Get a logger for API operations.
    
    Args:
        endpoint: API endpoint name
        
    Returns:
        Logger instance
    """
    logger_name = f"api.{endpoint}" if endpoint else "api"
    return setup_simple_logging(logger_name)


def get_tool_logger(tool_name: str) -> logging.Logger:
    """
    Get a logger for tool operations.
    
    Args:
        tool_name: Name of the tool
        
    Returns:
        Logger instance
    """
    return setup_simple_logging(f"tools.{tool_name}")


# For backwards compatibility with structured logging imports
def get_react_logger(agent_name: str):
    """
    Simple react logger that uses standard logging.
    
    Args:
        agent_name: Name of the agent
        
    Returns:
        Simple logger wrapper
    """
    logger = get_agent_logger(agent_name)
    
    class SimpleReactLogger:
        def __init__(self, logger):
            self.logger = logger
            
        def reasoning(self, message: str, **kwargs):
            """Log reasoning phase"""
            self.logger.info(f"[REASONING] {message}")
            
        def action(self, message: str, **kwargs):
            """Log action phase"""
            self.logger.info(f"[ACTION] {message}")
            
        def observation(self, message: str, **kwargs):
            """Log observation phase"""
            self.logger.info(f"[OBSERVATION] {message}")
    
    return SimpleReactLogger(logger)


# Initialize default logging
default_logger = setup_simple_logging()
