"""
Configuration for document sources in the Planner Agent.
This module provides configuration management for Azure, Box, and Confluence document sources.
"""

import os
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv


class DocumentSourceConfig:
    """Configuration class for document sources."""
    
    def __init__(self, env_file: str = ".env.dev"):
        """Initialize configuration from environment file."""
        self._load_environment(env_file)
    
    def _load_environment(self, env_file: str):
        """Load environment variables from file."""
        env_files_to_try = [env_file, ".env.dev", ".env"]
        
        for env_path in env_files_to_try:
            if os.path.exists(env_path):
                load_dotenv(env_path, override=True)
                break
    
    def get_azure_config(self) -> Dict[str, Any]:
        """Get Azure Blob Storage configuration."""
        return {
            'enabled': os.getenv('AZURE_ENABLED', 'true').lower() == 'true',
            'container_name': os.getenv('AZURE_CONTAINER_NAME', 'documents'),
            'prefix': os.getenv('AZURE_BLOB_PREFIX'),  # Optional prefix filter
            'max_docs': int(os.getenv('AZURE_MAX_DOCS', '10')),
            'file_extensions': self._parse_extensions(os.getenv('AZURE_FILE_EXTENSIONS', '.txt,.pdf,.docx,.md')),
            'exclude_patterns': self._parse_list(os.getenv('AZURE_EXCLUDE_PATTERNS', '')),
            'batch_size': int(os.getenv('AZURE_BATCH_SIZE', '5'))
        }
    
    def get_box_config(self) -> Dict[str, Any]:
        """Get Box configuration."""
        return {
            'enabled': os.getenv('BOX_ENABLED', 'true').lower() == 'true',
            'folder_id': os.getenv('BOX_FOLDER_ID', '0'),  # 0 = root folder
            'max_docs': int(os.getenv('BOX_MAX_DOCS', '10')),
            'file_extensions': self._parse_extensions(os.getenv('BOX_FILE_EXTENSIONS', '.txt,.pdf,.docx,.md')),
            'exclude_patterns': self._parse_list(os.getenv('BOX_EXCLUDE_PATTERNS', '')),
            'recursive': os.getenv('BOX_RECURSIVE', 'false').lower() == 'true',
            'batch_size': int(os.getenv('BOX_BATCH_SIZE', '5'))
        }
    
    def get_confluence_config(self) -> Dict[str, Any]:
        """Get Confluence configuration."""
        page_titles_env = os.getenv('CONFLUENCE_PAGE_TITLES', '')
        page_titles = self._parse_list(page_titles_env) if page_titles_env else []
        
        return {
            'enabled': os.getenv('CONFLUENCE_ENABLED', 'true').lower() == 'true',
            'page_titles': page_titles,
            'space_key': os.getenv('CONFLUENCE_SPACE_KEY'),  # Optional space filter
            'max_docs': int(os.getenv('CONFLUENCE_MAX_DOCS', '10')),
            'include_attachments': os.getenv('CONFLUENCE_INCLUDE_ATTACHMENTS', 'false').lower() == 'true',
            'batch_size': int(os.getenv('CONFLUENCE_BATCH_SIZE', '3'))
        }
    
    def get_all_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get all source configurations."""
        return {
            'azure': self.get_azure_config(),
            'box': self.get_box_config(),
            'confluence': self.get_confluence_config()
        }
    
    def get_enabled_sources(self) -> List[str]:
        """Get list of enabled sources."""
        configs = self.get_all_configs()
        return [source for source, config in configs.items() if config.get('enabled', False)]
    
    def get_global_config(self) -> Dict[str, Any]:
        """Get global processing configuration."""
        return {
            'max_docs_per_source': int(os.getenv('MAX_DOCS_PER_SOURCE', '10')),
            'enable_parallel_processing': os.getenv('ENABLE_PARALLEL_PROCESSING', 'false').lower() == 'true',
            'processing_timeout': int(os.getenv('PROCESSING_TIMEOUT_SECONDS', '300')),
            'save_intermediate_results': os.getenv('SAVE_INTERMEDIATE_RESULTS', 'true').lower() == 'true',
            'output_directory': os.getenv('OUTPUT_DIRECTORY', './results'),
            'log_level': os.getenv('LOG_LEVEL', 'INFO')
        }
    
    def _parse_extensions(self, extensions_str: str) -> List[str]:
        """Parse file extensions from comma-separated string."""
        if not extensions_str:
            return []
        return [ext.strip() for ext in extensions_str.split(',') if ext.strip()]
    
    def _parse_list(self, list_str: str) -> List[str]:
        """Parse comma-separated list from string."""
        if not list_str:
            return []
        return [item.strip() for item in list_str.split(',') if item.strip()]


class DynamicSourceManager:
    """Manages dynamic document source configuration and processing."""
    
    def __init__(self, config: DocumentSourceConfig):
        """Initialize with configuration."""
        self.config = config
        self.global_config = config.get_global_config()
    
    def get_processing_plan(self) -> Dict[str, Any]:
        """Generate a processing plan based on enabled sources."""
        enabled_sources = self.config.get_enabled_sources()
        all_configs = self.config.get_all_configs()
        
        plan = {
            'enabled_sources': enabled_sources,
            'total_max_docs': sum(all_configs[source]['max_docs'] for source in enabled_sources),
            'processing_order': self._determine_processing_order(enabled_sources),
            'batch_configuration': {
                source: all_configs[source]['batch_size'] 
                for source in enabled_sources
            },
            'global_settings': self.global_config
        }
        
        return plan
    
    def _determine_processing_order(self, sources: List[str]) -> List[str]:
        """Determine optimal processing order for sources."""
        # Process faster sources first (typically local/cached sources)
        priority_order = ['confluence', 'box', 'azure']
        
        ordered_sources = []
        for priority_source in priority_order:
            if priority_source in sources:
                ordered_sources.append(priority_source)
        
        # Add any remaining sources
        for source in sources:
            if source not in ordered_sources:
                ordered_sources.append(source)
        
        return ordered_sources
    
    def validate_configuration(self) -> Dict[str, Any]:
        """Validate source configurations and return validation results."""
        validation_results = {
            'valid': True,
            'warnings': [],
            'errors': [],
            'source_validations': {}
        }
        
        all_configs = self.config.get_all_configs()
        
        for source, config in all_configs.items():
            if not config.get('enabled', False):
                continue
            
            source_validation = self._validate_source_config(source, config)
            validation_results['source_validations'][source] = source_validation
            
            if source_validation['errors']:
                validation_results['valid'] = False
                validation_results['errors'].extend(source_validation['errors'])
            
            if source_validation['warnings']:
                validation_results['warnings'].extend(source_validation['warnings'])
        
        return validation_results
    
    def _validate_source_config(self, source: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate configuration for a specific source."""
        validation = {
            'valid': True,
            'warnings': [],
            'errors': []
        }
        
        # Common validations
        if config.get('max_docs', 0) <= 0:
            validation['errors'].append(f"{source}: max_docs must be positive")
            validation['valid'] = False
        
        if config.get('max_docs', 0) > 100:
            validation['warnings'].append(f"{source}: max_docs > 100 may cause performance issues")
        
        # Source-specific validations
        if source == 'azure':
            if not config.get('container_name'):
                validation['errors'].append("Azure: container_name is required")
                validation['valid'] = False
        
        elif source == 'box':
            folder_id = config.get('folder_id')
            if folder_id is None or (isinstance(folder_id, str) and not folder_id.strip()):
                validation['errors'].append("Box: folder_id is required")
                validation['valid'] = False
        
        elif source == 'confluence':
            page_titles = config.get('page_titles', [])
            if not page_titles:
                validation['warnings'].append("Confluence: no page_titles specified")
        
        return validation


# Example usage and configuration templates
def create_sample_env_file(file_path: str = ".env.sample"):
    """Create a sample environment file with all configuration options."""
    sample_content = """
# Document Source Configuration

# Global Settings
MAX_DOCS_PER_SOURCE=10
ENABLE_PARALLEL_PROCESSING=false
PROCESSING_TIMEOUT_SECONDS=300
SAVE_INTERMEDIATE_RESULTS=true
OUTPUT_DIRECTORY=./results
LOG_LEVEL=INFO

# Azure Blob Storage Configuration
AZURE_ENABLED=true
AZURE_CONTAINER_NAME=documents
AZURE_BLOB_PREFIX=
AZURE_MAX_DOCS=20
AZURE_FILE_EXTENSIONS=.txt,.pdf,.docx,.md,.json
AZURE_EXCLUDE_PATTERNS=temp,backup,archive
AZURE_BATCH_SIZE=5

# Box Configuration
BOX_ENABLED=true
BOX_FOLDER_ID=0
BOX_MAX_DOCS=15
BOX_FILE_EXTENSIONS=.txt,.pdf,.docx,.md
BOX_EXCLUDE_PATTERNS=.tmp,.bak
BOX_RECURSIVE=false
BOX_BATCH_SIZE=5

# Confluence Configuration
CONFLUENCE_ENABLED=true
CONFLUENCE_PAGE_TITLES=API Documentation,User Guide,Technical Specifications,Project Overview
CONFLUENCE_SPACE_KEY=DEV
CONFLUENCE_MAX_DOCS=10
CONFLUENCE_INCLUDE_ATTACHMENTS=false
CONFLUENCE_BATCH_SIZE=3

# Azure OpenAI Configuration (for LLM classification)
AZURE_OPENAI_API_KEY=your_api_key_here
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_VERSION=2024-02-01
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4

# Connector-specific credentials
AZURE_STORAGE_CONNECTION_STRING=your_connection_string_here
BOX_CLIENT_ID=your_box_client_id
BOX_CLIENT_SECRET=your_box_client_secret
CONFLUENCE_BASE_URL=https://your-domain.atlassian.net
CONFLUENCE_USERNAME=your_username
CONFLUENCE_API_TOKEN=your_api_token
"""
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(sample_content.strip())
    
    print(f"Sample environment file created: {file_path}")


if __name__ == "__main__":
    # Example usage
    config = DocumentSourceConfig()
    manager = DynamicSourceManager(config)
    
    # Display configuration
    print("=== Document Source Configuration ===")
    for source, source_config in config.get_all_configs().items():
        print(f"\n{source.upper()}:")
        for key, value in source_config.items():
            print(f"  {key}: {value}")
    
    # Display processing plan
    print("\n=== Processing Plan ===")
    plan = manager.get_processing_plan()
    for key, value in plan.items():
        print(f"{key}: {value}")
    
    # Validate configuration
    print("\n=== Configuration Validation ===")
    validation = manager.validate_configuration()
    print(f"Valid: {validation['valid']}")
    if validation['warnings']:
        print("Warnings:")
        for warning in validation['warnings']:
            print(f"  - {warning}")
    if validation['errors']:
        print("Errors:")
        for error in validation['errors']:
            print(f"  - {error}")
    
    # Create sample environment file
    create_sample_env_file()
