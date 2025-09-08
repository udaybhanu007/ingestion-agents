"""
Configuration for Ingestion System

This module handles configuration loading for the ingestion system from .env.dev file.
"""

import os
from typing import Optional
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables from .env.dev
project_root = os.path.dirname(os.path.dirname(__file__))
env_dev_path = os.path.join(project_root, '.env.dev')

if os.path.exists(env_dev_path):
    load_dotenv(env_dev_path)


@dataclass
class IngestionConfig:
    """Configuration class for ingestion system"""
    
    # Azure OpenAI Configuration
    azure_openai_endpoint: str
    azure_openai_api_key: str
    azure_openai_deployment: str
    azure_openai_embedding_deployment: str
    azure_openai_api_version: str
    
    # Qdrant Configuration
    qdrant_api_url: str
    qdrant_api_key: str
    qdrant_collection: str
    
    # Neo4j Configuration
    neo4j_uri: str
    neo4j_username: str
    neo4j_password: str
    
    # Ingestion Settings
    batch_size: int = 10
    max_content_length: int = 1000000
    chunk_size: int = 1000
    chunk_overlap: int = 200
    
    # Content Directory
    content_directory: str = "downloaded_content"
    
    @classmethod
    def from_env(cls) -> "IngestionConfig":
        """Create configuration from environment variables"""
        
        # Required environment variables
        required_vars = {
            'azure_openai_endpoint': 'AZURE_OPENAI_ENDPOINT',
            'azure_openai_api_key': 'AZURE_OPENAI_API_KEY',
            'azure_openai_deployment': 'AZURE_OPENAI_DEPLOYMENT',
            'azure_openai_embedding_deployment': 'AZURE_OPENAI_EMBEDDING_DEPLOYMENT',
            'azure_openai_api_version': 'AZURE_OPENAI_API_VERSION',
            'qdrant_api_url': 'QDRANT_API_URL',
            'qdrant_api_key': 'QDRANT_API_KEY',
            'qdrant_collection': 'QDRANT_COLLECTION',
            'neo4j_uri': 'NEO4J_URI',
            'neo4j_username': 'NEO4J_USERNAME',
            'neo4j_password': 'NEO4J_PASSWORD'
        }
        
        config_values = {}
        missing_vars = []
        
        for config_key, env_var in required_vars.items():
            value = os.getenv(env_var)
            if value is None:
                missing_vars.append(env_var)
            else:
                config_values[config_key] = value
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Optional variables with defaults
        config_values['batch_size'] = int(os.getenv('INGESTION_BATCH_SIZE', '10'))
        config_values['max_content_length'] = int(os.getenv('MAX_CONTENT_LENGTH', '1000000'))
        config_values['chunk_size'] = int(os.getenv('CHUNK_SIZE', '1000'))
        config_values['chunk_overlap'] = int(os.getenv('CHUNK_OVERLAP', '200'))
        config_values['content_directory'] = os.getenv('CONTENT_DIRECTORY', 'downloaded_content')
        
        return cls(**config_values)


def get_config() -> IngestionConfig:
    """Get the ingestion configuration instance"""
    return IngestionConfig.from_env()
