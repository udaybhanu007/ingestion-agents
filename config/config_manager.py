"""
Configuration Manager for Ingestion Agent Application

This module provides centralized configuration management using environment variables
from .env.dev file or other environment files.
"""

import os
import logging
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Import logger configuration
try:
    from .logger_config import init_logging, get_logger
    LOGGER_CONFIG_AVAILABLE = True
except ImportError:
    LOGGER_CONFIG_AVAILABLE = False


class ConfigManager:
    """Centralized configuration manager for the ingestion agent application."""
    
    def __init__(self, env_file: str = ".env.dev"):
        """
        Initialize configuration manager.
        
        Args:
            env_file (str): Path to environment file (defaults to .env.dev)
        """
        # Initialize structured logging first
        if LOGGER_CONFIG_AVAILABLE:
            environment = os.getenv('ENVIRONMENT', 'dev')
            init_logging(environment)
            self.logger = get_logger(__name__)
        else:
            self.logger = logging.getLogger(__name__)
            
        self.config: Dict[str, Any] = {}
        
        # Try different environment files in order of preference
        env_files_to_try = [env_file, ".env.dev", ".env"]
        env_file_loaded = None
        
        for env_path in env_files_to_try:
            if os.path.exists(env_path):
                load_dotenv(env_path)
                env_file_loaded = env_path
                self.logger.info(f"Environment file loaded: {env_path} [component=config_manager]")
                break
        
        if not env_file_loaded:
            self.logger.warning("No environment file found, using system environment [component=config_manager]")
        
        # Load all configuration sections
        self._load_azure_config()
        self._load_box_config()
        self._load_confluence_config()
        self._load_openai_config()
        self._load_qdrant_config()
        self._load_neo4j_config()
        self._load_langsmith_config()
        self._load_api_config()
        self._load_logging_config()
    
    def _load_azure_config(self):
        """Load Azure Storage configuration."""
        self.config['azure'] = {
            'storage_account': os.getenv('AZURE_STORAGE_ACCOUNT'),
            'storage_account_key': os.getenv('AZURE_STORAGE_ACCOUNT_KEY'),
            'connection_string': os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
            'container_name': os.getenv('AZURE_CONTAINER_NAME'),
            'tenant_id': os.getenv('AZURE_TENANT_ID'),
            'client_id': os.getenv('AZURE_CLIENT_ID'),
            'client_secret': os.getenv('AZURE_CLIENT_SECRET')
        }
    
    def _load_box_config(self):
        """Load Box configuration."""
        self.config['box'] = {
            'client_id': os.getenv('Box_Client_Id'),
            'client_secret': os.getenv('Box_Client_Secret'),
            'access_token': os.getenv('BOX_ACCESS_TOKEN'),
            'folder_id': os.getenv('BOX_FOLDER_ID')
        }
    
    def _load_confluence_config(self):
        """Load Confluence/Atlassian configuration."""
        self.config['confluence'] = {
            'base_url': os.getenv('CONFLUENCE_BASE_URL') or os.getenv('ATLASSIAN_BASE_URL'),
            'username': os.getenv('CONFLUENCE_USERNAME') or os.getenv('ATLASSIAN_USERNAME'),
            'token': os.getenv('CONFLUENCE_TOKEN') or os.getenv('ATLASSIAN_TOKEN'),
            'access_token': os.getenv('ATLASSIAN_ACCESS_TOKEN'),
            'cloud_id': os.getenv('ATLASSIAN_CLOUD_ID'),
            'refresh_token': os.getenv('ATLASSIAN_REFRESH_TOKEN'),
            'client_id': os.getenv('ATLASSIAN_CLIENT_ID'),
            'client_secret': os.getenv('ATLASSIAN_CLIENT_SECRET')
        }
    
    def _load_openai_config(self):
        """Load OpenAI/Azure OpenAI configuration."""
        self.config['openai'] = {
            'api_key': os.getenv('OPENAI_API_KEY'),
            'azure_endpoint': os.getenv('AZURE_OPENAI_ENDPOINT'),
            'azure_api_key': os.getenv('AZURE_OPENAI_API_KEY'),
            'azure_api_version': os.getenv('AZURE_OPENAI_API_VERSION', '2024-02-01'),
            'deployment_name': os.getenv('AZURE_OPENAI_DEPLOYMENT_NAME'),
            'embedding_deployment': os.getenv('AZURE_OPENAI_EMBEDDING_DEPLOYMENT'),
            'model': os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
        }
    
    def _load_qdrant_config(self):
        """Load Qdrant vector database configuration."""
        self.config['qdrant'] = {
            'url': os.getenv('QDRANT_URL'),
            'api_key': os.getenv('QDRANT_API_KEY'),
            'collection_name': os.getenv('QDRANT_COLLECTION_NAME'),
            'vector_size': int(os.getenv('QDRANT_VECTOR_SIZE', '1536'))
        }
    
    def _load_neo4j_config(self):
        """Load Neo4j graph database configuration."""
        self.config['neo4j'] = {
            'uri': os.getenv('NEO4J_URI'),
            'username': os.getenv('NEO4J_USERNAME'),
            'password': os.getenv('NEO4J_PASSWORD'),
            'database': os.getenv('NEO4J_DATABASE', 'neo4j')
        }
    
    def _load_langsmith_config(self):
        """Load LangSmith observability configuration."""
        self.config['langsmith'] = {
            'api_key': os.getenv('LANGSMITH_API_KEY'),
            'project': os.getenv('LANGSMITH_PROJECT'),
            'endpoint': os.getenv('LANGSMITH_ENDPOINT'),
            'tracing': os.getenv('LANGCHAIN_TRACING_V2', 'true').lower() == 'true'
        }
    
    def _load_api_config(self):
        """Load API configuration."""
        self.config['api'] = {
            'host': os.getenv('API_HOST', '0.0.0.0'),
            'port': int(os.getenv('API_PORT', '8000')),
            'debug': os.getenv('API_DEBUG', 'false').lower() == 'true',
            'reload': os.getenv('API_RELOAD', 'false').lower() == 'true'
        }
    
    def get_config(self, section: str, key: Optional[str] = None) -> Any:
        """
        Get configuration value.
        
        Args:
            section (str): Configuration section (e.g., 'azure', 'box', 'confluence')
            key (str, optional): Specific key within section
            
        Returns:
            Any: Configuration value or section
        """
        if section not in self.config:
            self.logger.warning(f"Configuration section '{section}' not found")
            return None
        
        if key is None:
            return self.config[section]
        
        return self.config[section].get(key)
    
    def get_azure_config(self) -> Dict[str, Any]:
        """Get Azure configuration."""
        return self.get_config('azure')
    
    def get_box_config(self) -> Dict[str, Any]:
        """Get Box configuration."""
        return self.get_config('box')
    
    def get_confluence_config(self) -> Dict[str, Any]:
        """Get Confluence configuration."""
        return self.get_config('confluence')
    
    def get_openai_config(self) -> Dict[str, Any]:
        """Get OpenAI configuration."""
        return self.get_config('openai')
    
    def get_qdrant_config(self) -> Dict[str, Any]:
        """Get Qdrant configuration."""
        return self.get_config('qdrant')
    
    def get_neo4j_config(self) -> Dict[str, Any]:
        """Get Neo4j configuration."""
        return self.get_config('neo4j')
    
    def get_langsmith_config(self) -> Dict[str, Any]:
        """Get LangSmith configuration."""
        return self.get_config('langsmith')
    
    def get_api_config(self) -> Dict[str, Any]:
        """Get API configuration."""
        return self.get_config('api')
    
    def validate_config(self) -> Dict[str, bool]:
        """
        Validate that required configuration is present.
        
        Returns:
            Dict[str, bool]: Validation results for each service
        """
        validation_results = {}
        
        # Validate Azure
        azure_config = self.get_azure_config()
        validation_results['azure'] = bool(
            azure_config.get('storage_account') and 
            (azure_config.get('storage_account_key') or azure_config.get('connection_string'))
        )
        
        # Validate Box
        box_config = self.get_box_config()
        validation_results['box'] = bool(
            box_config.get('client_id') and box_config.get('client_secret')
        )
        
        # Validate Confluence
        confluence_config = self.get_confluence_config()
        validation_results['confluence'] = bool(
            confluence_config.get('base_url') and 
            (confluence_config.get('access_token') or 
             (confluence_config.get('username') and confluence_config.get('token')))
        )
        
        # Validate OpenAI
        openai_config = self.get_openai_config()
        validation_results['openai'] = bool(
            openai_config.get('api_key') or 
            (openai_config.get('azure_endpoint') and openai_config.get('azure_api_key'))
        )
        
        # Validate Qdrant
        qdrant_config = self.get_qdrant_config()
        validation_results['qdrant'] = bool(
            qdrant_config.get('url') and qdrant_config.get('api_key')
        )
        
        # Validate Neo4j
        neo4j_config = self.get_neo4j_config()
        validation_results['neo4j'] = bool(
            neo4j_config.get('uri') and neo4j_config.get('username') and neo4j_config.get('password')
        )
        
        return validation_results
    
    def _load_logging_config(self):
        """Load logging configuration."""
        self.config['logging'] = {
            'environment': os.getenv('ENVIRONMENT', 'dev'),
            'log_level': os.getenv('LOG_LEVEL', 'INFO'),
            'structured_logging': LOGGER_CONFIG_AVAILABLE,
        }
    
    def print_status(self):
        """Print configuration status."""
        validation_results = self.validate_config()
        
        print("Configuration Status:")
        print("=" * 50)
        
        for service, is_valid in validation_results.items():
            status = "✓ Configured" if is_valid else "✗ Missing/Incomplete"
            print(f"{service.capitalize():15} {status}")
        
        print("=" * 50)
        
        # Print available configurations (without sensitive data)
        for service, config in self.config.items():
            if validation_results.get(service, False):
                print(f"\n{service.capitalize()} Configuration:")
                for key, value in config.items():
                    if value and 'key' not in key.lower() and 'secret' not in key.lower() and 'token' not in key.lower() and 'password' not in key.lower():
                        print(f"  {key}: {value}")
                    elif value:
                        print(f"  {key}: {'*' * 8}")


# Global configuration instance
config_manager = ConfigManager()


def get_config() -> ConfigManager:
    """Get the global configuration manager instance."""
    return config_manager
