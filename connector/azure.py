"""
Azure Connector for Ingestion Agent Application

This module provides functionality to connect to and retrieve data from Azure services.
"""

import os
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
from dotenv import load_dotenv

try:
    from azure.storage.blob import BlobServiceClient, ContainerClient
    from azure.core.exceptions import AzureError
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    BlobServiceClient = None
    ContainerClient = None
    AzureError = Exception


class AzureConnector:
    """Azure Storage connector for document ingestion."""
    
    def __init__(self, storage_account: Optional[str] = None, 
                 account_key: Optional[str] = None, 
                 connection_string: Optional[str] = None,
                 env_file: str = ".env.dev"):
        """
        Initialize Azure Storage connector.
        
        Args:
            storage_account (str, optional): Azure storage account name
            account_key (str, optional): Azure storage account key
            connection_string (str, optional): Azure storage connection string
            env_file (str): Path to environment file for credentials (defaults to .env.dev)
        """
        self.logger = logging.getLogger(__name__)
        
        if not AZURE_AVAILABLE:
            self.logger.error("azure-storage-blob is not available. Please install with: pip install azure-storage-blob python-dotenv")
            self.blob_service_client = None
            return
        
        # Try different environment files in order of preference
        env_files_to_try = [env_file, ".env.dev", ".env"]
        env_file_loaded = None
        
        for env_path in env_files_to_try:
            if os.path.exists(env_path):
                load_dotenv(env_path)
                env_file_loaded = env_path
                self.logger.info(f"Loaded environment from {env_path}")
                break
        
        if not env_file_loaded:
            self.logger.warning("No environment file found. Trying environment variables.")
        
        # Use provided credentials or load from environment
        self.storage_account = storage_account or os.getenv('AZURE_STORAGE_ACCOUNT')
        self.account_key = account_key or os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
        self.connection_string = connection_string or os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        
        # Create BlobServiceClient
        try:
            if self.connection_string:
                self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
                self.logger.info("Initialized Azure connector with connection string")
            elif self.storage_account and self.account_key:
                account_url = f"https://{self.storage_account}.blob.core.windows.net"
                self.blob_service_client = BlobServiceClient(account_url=account_url, credential=self.account_key)
                self.logger.info(f"Initialized Azure connector for account: {self.storage_account}")
            else:
                self.logger.error("Azure Storage credentials not found in environment or parameters")
                self.blob_service_client = None
                
        except Exception as e:
            self.logger.error(f"Failed to initialize Azure Storage client: {e}")
            self.blob_service_client = None
    
    def is_available(self) -> bool:
        """Check if Azure connector is available and configured."""
        return AZURE_AVAILABLE and self.blob_service_client is not None
    
    def list_containers(self) -> List[Dict[str, Any]]:
        """List all containers in the storage account."""
        if not self.is_available():
            self.logger.error("Azure connector not available")
            return []
        
        try:
            containers = []
            for container in self.blob_service_client.list_containers():
                containers.append({
                    'name': container.name,
                    'last_modified': container.last_modified.isoformat() if container.last_modified else None,
                    'metadata': container.metadata or {}
                })
            
            self.logger.info(f"Found {len(containers)} containers")
            return containers
            
        except AzureError as e:
            self.logger.error(f"Error listing containers: {e}")
            return []
    
    def list_blobs(self, container_name: str, prefix: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all blobs in a container."""
        if not self.is_available():
            self.logger.error("Azure connector not available")
            return []
        
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blobs = []
            
            for blob in container_client.list_blobs(name_starts_with=prefix):
                blobs.append({
                    'name': blob.name,
                    'size': blob.size,
                    'last_modified': blob.last_modified.isoformat() if blob.last_modified else None,
                    'content_type': blob.content_settings.content_type if blob.content_settings else None,
                    'etag': blob.etag,
                    'metadata': blob.metadata or {}
                })
            
            self.logger.info(f"Found {len(blobs)} blobs in container '{container_name}'")
            return blobs
            
        except AzureError as e:
            self.logger.error(f"Error listing blobs in container '{container_name}': {e}")
            return []
    
    def download_blob(self, container_name: str, blob_name: str, 
                     local_file_path: Optional[str] = None) -> Optional[bytes]:
        """Download a blob from Azure Storage."""
        if not self.is_available():
            self.logger.error("Azure connector not available")
            return None
        
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, 
                blob=blob_name
            )
            
            blob_data = blob_client.download_blob().readall()
            
            if local_file_path:
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                with open(local_file_path, 'wb') as f:
                    f.write(blob_data)
                self.logger.info(f"Downloaded blob '{blob_name}' to '{local_file_path}'")
            else:
                self.logger.info(f"Downloaded blob '{blob_name}' to memory")
            
            return blob_data
            
        except AzureError as e:
            self.logger.error(f"Error downloading blob '{blob_name}': {e}")
            return None
    
    def get_blob_metadata(self, container_name: str, blob_name: str) -> Dict[str, Any]:
        """Get metadata for a specific blob."""
        if not self.is_available():
            self.logger.error("Azure connector not available")
            return {}
        
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, 
                blob=blob_name
            )
            
            properties = blob_client.get_blob_properties()
            
            return {
                'name': blob_name,
                'size': properties.size,
                'last_modified': properties.last_modified.isoformat() if properties.last_modified else None,
                'content_type': properties.content_settings.content_type if properties.content_settings else None,
                'etag': properties.etag,
                'metadata': properties.metadata or {},
                'creation_time': properties.creation_time.isoformat() if properties.creation_time else None
            }
            
        except AzureError as e:
            self.logger.error(f"Error getting metadata for blob '{blob_name}': {e}")
            return {}
    
    def fetch_documents(self, container_name: str, 
                       file_patterns: Optional[List[str]] = None,
                       download_path: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch documents from Azure Storage container.
        
        Args:
            container_name (str): Name of the Azure Storage container
            file_patterns (List[str], optional): List of file patterns to match
            download_path (str, optional): Local path to download files
            
        Returns:
            List[Dict[str, Any]]: List of document metadata and content
        """
        if not self.is_available():
            self.logger.error("Azure connector not available")
            return []
        
        try:
            documents = []
            blobs = self.list_blobs(container_name)
            
            for blob in blobs:
                blob_name = blob['name']
                
                # Apply file pattern filters if provided
                if file_patterns:
                    match_found = False
                    for pattern in file_patterns:
                        if pattern.lower() in blob_name.lower():
                            match_found = True
                            break
                    if not match_found:
                        continue
                
                # Download blob content
                content = self.download_blob(container_name, blob_name, 
                                           os.path.join(download_path, blob_name) if download_path else None)
                
                if content:
                    document = {
                        'id': f"azure://{container_name}/{blob_name}",
                        'name': blob_name,
                        'source': 'azure_storage',
                        'container': container_name,
                        'size': blob['size'],
                        'last_modified': blob['last_modified'],
                        'content_type': blob['content_type'],
                        'content': content,
                        'metadata': blob['metadata']
                    }
                    documents.append(document)
                    self.logger.info(f"Fetched document: {blob_name}")
            
            self.logger.info(f"Successfully fetched {len(documents)} documents from container '{container_name}'")
            return documents
            
        except Exception as e:
            self.logger.error(f"Error fetching documents from container '{container_name}': {e}")
            return []


    # Legacy compatibility methods for backward compatibility
    def list_blob_containers(self, storage_account: str) -> List[Dict[str, Any]]:
        """Legacy method for backward compatibility."""
        return self.list_containers()
    
    def query_cognitive_search(self, search_service: str, index: str, query: str) -> List[Dict[str, Any]]:
        """
        Query Azure Cognitive Search.
        
        Args:
            search_service (str): Azure Search service name
            index (str): Search index name
            query (str): Search query
            
        Returns:
            List[Dict[str, Any]]: Search results
        """
        # TODO: Implement cognitive search query
        self.logger.info(f"Querying index {index} with query: {query}")
        return []
