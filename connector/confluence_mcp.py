"""
Confluence MCP Connector for Ingestion Agent Application

This module provides advanced Confluence integration using the Model Context Protocol (MCP) framework
with LLM-powered content analysis and extraction capabilities.
"""

import asyncio
import os
import re
import logging
from typing import Optional, List, Dict, Any, Union
from dotenv import load_dotenv

try:
    from mcp_use import MCPAgent, MCPClient
    from langchain_openai import AzureChatOpenAI
    from pydantic import SecretStr
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False
    MCPAgent = None
    MCPClient = None
    AzureChatOpenAI = None
    SecretStr = None

logger = logging.getLogger(__name__)


class ConfluenceMCPConnector:
    """
    Advanced Confluence connector using MCP (Model Context Protocol) framework
    with LLM-powered content analysis and intelligent extraction.
    """
    
    def __init__(self, 
                 mcp_server_url: str = "http://localhost:9000/mcp",
                 max_steps: int = 30,
                 azure_openai_deployment: str = "gpt-4o-mini",
                 azure_openai_api_version: str = "2024-12-01-preview",
                 env_file: str = ".env.dev"):
        """
        Initialize the Confluence MCP Connector.
        
        Args:
            mcp_server_url: URL of the MCP Atlassian server
            max_steps: Maximum steps for the MCP agent
            azure_openai_deployment: Azure OpenAI deployment name
            azure_openai_api_version: Azure OpenAI API version
            env_file: Path to environment file for credentials
        """
        self.logger = logging.getLogger(__name__)
        
        if not MCP_AVAILABLE:
            self.logger.error("MCP dependencies not available. Please install: pip install mcp-use langchain-openai")
            self.mcp_client = None
            self.agent = None
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
            self.logger.warning("No environment file found. Using system environment variables.")
        
        # Store configuration
        self.mcp_server_url = mcp_server_url
        self.max_steps = max_steps
        self.azure_openai_deployment = azure_openai_deployment
        self.azure_openai_api_version = azure_openai_api_version
        
        # Read environment variables
        self.confluence_url = os.getenv("CONFLUENCE_URL") or os.getenv("CONFLUENCE_BASE_URL") or os.getenv("ATLASSIAN_BASE_URL")
        self.confluence_username = os.getenv("CONFLUENCE_USERNAME") or os.getenv("ATLASSIAN_USERNAME")
        self.confluence_api_token = os.getenv("CONFLUENCE_API_TOKEN") or os.getenv("CONFLUENCE_TOKEN") or os.getenv("ATLASSIAN_TOKEN")
        self.access_token = os.getenv("ATLASSIAN_ACCESS_TOKEN")
        
        # JIRA configuration (if needed)
        self.jira_url = os.getenv("JIRA_URL") or os.getenv("ATLASSIAN_BASE_URL")
        self.jira_username = os.getenv("JIRA_USERNAME") or os.getenv("ATLASSIAN_USERNAME")
        self.jira_api_token = os.getenv("JIRA_API_TOKEN") or os.getenv("ATLASSIAN_TOKEN")
        
        # Azure OpenAI configuration
        self.azure_openai_api_key = os.getenv("AZURE_OPENAI_API_KEY")
        self.azure_openai_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        
        # Load Confluence pages from environment
        self.confluence_pages = self._load_confluence_pages_from_env()
        
        # Validate and initialize
        try:
            self._validate_environment_variables()
            self.mcp_client = self._create_mcp_client()
            self.llm = self._create_azure_openai_llm()
            
            if MCP_AVAILABLE and MCPAgent is not None and self.llm is not None and self.mcp_client is not None:
                self.agent = MCPAgent(llm=self.llm, client=self.mcp_client, max_steps=self.max_steps)
                self.logger.info("Confluence MCP connector initialized successfully")
            else:
                self.agent = None
                self.logger.warning("MCP dependencies not available, connector will not be functional")
        except Exception as e:
            self.logger.error(f"Failed to initialize Confluence MCP connector: {e}")
            self.mcp_client = None
            self.agent = None
    
    def is_available(self) -> bool:
        """Check if Confluence MCP connector is available and configured."""
        return MCP_AVAILABLE and self.agent is not None
    
    def _load_confluence_pages_from_env(self) -> List[str]:
        """Load Confluence page titles from environment variables."""
        pages = []
        
        # Method 1: Load from comma-separated CONFLUENCE_PAGES
        confluence_pages_str = os.getenv("CONFLUENCE_PAGES")
        if confluence_pages_str:
            pages.extend([page.strip() for page in confluence_pages_str.split(',') if page.strip()])
        
        # Method 2: Load from individual CONFLUENCE_PAGE_X variables
        page_num = 1
        while True:
            page_env_var = f"CONFLUENCE_PAGE_{page_num}"
            page_title = os.getenv(page_env_var)
            if page_title:
                pages.append(page_title.strip())
                page_num += 1
            else:
                break
        
        # Remove duplicates while preserving order
        unique_pages = []
        for page in pages:
            if page not in unique_pages:
                unique_pages.append(page)
        
        self.logger.info(f"Loaded {len(unique_pages)} Confluence pages from environment: {unique_pages}")
        return unique_pages
    
    def get_configured_pages(self) -> List[str]:
        """Get the list of Confluence pages configured in environment variables."""
        return self.confluence_pages.copy()
    
    def _validate_environment_variables(self):
        """Validate that all required environment variables are set."""
        required_vars = {
            "AZURE_OPENAI_API_KEY": self.azure_openai_api_key,
            "AZURE_OPENAI_ENDPOINT": self.azure_openai_endpoint,
        }
        
        # At least one Confluence authentication method is required
        confluence_auth_valid = (
            self.access_token or 
            (self.confluence_username and self.confluence_api_token)
        )
        
        if not confluence_auth_valid:
            required_vars.update({
                "CONFLUENCE_URL": self.confluence_url,
                "CONFLUENCE_USERNAME": self.confluence_username,
                "CONFLUENCE_API_TOKEN": self.confluence_api_token
            })
        
        missing_vars = [var for var, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"The following environment variables must be set: {', '.join(missing_vars)}")
    
    def _create_mcp_client(self):
        """Create and configure the MCP client."""
        if not MCP_AVAILABLE or MCPClient is None:
            return None
            
        config = {
            "mcpServers": {
                "mcp-atlassian": {
                    "url": self.mcp_server_url
                }
            }
        }
        return MCPClient.from_dict(config)
    
    def _create_azure_openai_llm(self):
        """Create and configure the Azure OpenAI LLM."""
        if not MCP_AVAILABLE or AzureChatOpenAI is None or SecretStr is None:
            return None
            
        if not self.azure_openai_api_key:
            raise ValueError("AZURE_OPENAI_API_KEY is required but not set")
        
        try:
            return AzureChatOpenAI(
                azure_deployment=self.azure_openai_deployment,
                api_version=self.azure_openai_api_version,
                azure_endpoint=self.azure_openai_endpoint,
                api_key=SecretStr(self.azure_openai_api_key)
            )
        except Exception as e:
            self.logger.error(f"Failed to create Azure OpenAI LLM: {e}")
            return None
    
    async def query(self, query_text: str) -> str:
        """
        Execute a query using the MCP agent.
        
        Args:
            query_text: The query to execute
            
        Returns:
            The result of the query
        """
        if not self.is_available():
            raise RuntimeError("Confluence MCP connector not available")
        
        if not self.agent:
            raise RuntimeError("MCP agent not initialized")
        
        try:
            result = await self.agent.run(query_text)
            return result
        except Exception as e:
            self.logger.error(f"Error executing MCP query: {e}")
            raise

    async def download_page_content(self, page_title: str, download_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Download content from a Confluence page based on its title.
        
        Args:
            page_title: The title of the Confluence page to download
            download_dir: Optional directory path for downloads
            
        Returns:
            Dictionary with download results
        """
        if not self.is_available():
            return {
                "page_title": page_title,
                "success": False,
                "error": "Confluence MCP connector not available"
            }
        
        try:
            # Create download directory if not provided
            if download_dir is None:
                download_dir = os.path.join(os.getcwd(), "downloaded_content")
            
            os.makedirs(download_dir, exist_ok=True)
            
            self.logger.info(f"Downloading content for page: '{page_title}' to {download_dir}")
            
            # Fetch the page content using MCP agent
            query_text = f"Download and return the full body content of the Confluence page named '{page_title}'"
            content = await self.query(query_text)
            
            if not content or content.strip() == "":
                return {
                    "page_title": page_title,
                    "download_dir": download_dir,
                    "file_path": None,
                    "success": False,
                    "error": "No content received from Confluence page"
                }
            
            # Create a safe filename from the page title
            safe_filename = re.sub(r'[<>:"/\\|?*]', '_', page_title)
            safe_filename = safe_filename.strip().replace(' ', '_')
            file_path = os.path.join(download_dir, f"{safe_filename}.md")
            
            # Save content to file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(f"# {page_title}\n\n")
                f.write(f"*Downloaded from Confluence via MCP*\n\n")
                f.write(content)
            
            self.logger.info(f"Successfully downloaded page content to: {file_path}")
            
            return {
                "page_title": page_title,
                "download_dir": download_dir,
                "file_path": file_path,
                "file_size": os.path.getsize(file_path),
                "content": content,
                "success": True,
                "error": None
            }
            
        except Exception as e:
            error_msg = f"Error downloading page '{page_title}': {str(e)}"
            self.logger.error(error_msg)
            return {
                "page_title": page_title,
                "download_dir": download_dir if 'download_dir' in locals() else "",
                "file_path": None,
                "success": False,
                "error": error_msg
            }

    async def download_multiple_pages(self, page_titles: List[str], download_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Download content from multiple Confluence pages.
        
        Args:
            page_titles: List of page titles to download
            download_dir: Optional directory path for downloads
            
        Returns:
            Dictionary with overall download results
        """
        if not self.is_available():
            return {
                "total_pages": len(page_titles),
                "successful_downloads": 0,
                "failed_downloads": len(page_titles),
                "error": "Confluence MCP connector not available"
            }
        
        self.logger.info(f"Starting download of {len(page_titles)} pages...")
        
        results = []
        successful_downloads = 0
        failed_downloads = 0
        
        for page_title in page_titles:
            result = await self.download_page_content(page_title, download_dir)
            results.append(result)
            
            if result["success"]:
                successful_downloads += 1
            else:
                failed_downloads += 1
                self.logger.warning(f"Failed to download: {page_title} - {result['error']}")
        
        overall_result = {
            "total_pages": len(page_titles),
            "successful_downloads": successful_downloads,
            "failed_downloads": failed_downloads,
            "download_dir": results[0]["download_dir"] if results else "",
            "individual_results": results,
            "downloaded_files": [r["file_path"] for r in results if r["success"]]
        }
        
        self.logger.info(f"Download completed: {successful_downloads} successful, {failed_downloads} failed")
        return overall_result

    async def search_content(self, query: str, space_key: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Search for content in Confluence using MCP agent.
        
        Args:
            query: Search query
            space_key: Optional space key to limit search
            limit: Maximum number of results
            
        Returns:
            List of search results
        """
        if not self.is_available():
            self.logger.error("Confluence MCP connector not available")
            return []
        
        try:
            search_query = f"Search Confluence for content matching '{query}'"
            if space_key:
                search_query += f" in space '{space_key}'"
            search_query += f" and return up to {limit} results with titles, URLs, and excerpts"
            
            result = await self.query(search_query)
            
            # Parse the LLM response to extract structured search results
            # This would need to be implemented based on the actual MCP server response format
            search_results = self._parse_search_results(result)
            
            self.logger.info(f"Found {len(search_results)} search results for query '{query}'")
            return search_results
            
        except Exception as e:
            self.logger.error(f"Error searching for '{query}': {e}")
            return []

    async def fetch_documents(self, space_keys: Optional[List[str]] = None,
                             page_titles: Optional[List[str]] = None,
                             search_query: Optional[str] = None,
                             download_dir: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch documents from Confluence using various methods.
        
        Args:
            space_keys: List of space keys to fetch from
            page_titles: Specific page titles to fetch
            search_query: Search query to find pages
            download_dir: Directory for downloaded content
            
        Returns:
            List of document metadata and content
        """
        if not self.is_available():
            self.logger.error("Confluence MCP connector not available")
            return []
        
        try:
            documents = []
            
            # Method 1: Fetch specific pages by title
            if page_titles:
                self.logger.info(f"Fetching {len(page_titles)} specific pages")
                download_result = await self.download_multiple_pages(page_titles, download_dir)
                
                for result in download_result.get("individual_results", []):
                    if result["success"]:
                        document = {
                            'id': f"confluence-mcp://{result['page_title']}",
                            'name': result['page_title'],
                            'source': 'confluence_mcp',
                            'content': result.get('content', ''),
                            'file_path': result['file_path'],
                            'file_size': result.get('file_size', 0),
                            'metadata': {
                                'page_title': result['page_title'],
                                'download_method': 'mcp_agent',
                                'file_path': result['file_path']
                            }
                        }
                        documents.append(document)
            
            # Method 2: Search and fetch based on query
            elif search_query:
                self.logger.info(f"Searching for pages matching: {search_query}")
                search_results = await self.search_content(search_query, limit=50)
                
                # Extract page titles from search results and download them
                found_titles = [result.get('title') for result in search_results if result.get('title')]
                # Filter out None values to ensure we have a List[str]
                valid_titles = [title for title in found_titles if title is not None and isinstance(title, str)]
                if valid_titles:
                    download_result = await self.download_multiple_pages(valid_titles, download_dir)
                    
                    for result in download_result.get("individual_results", []):
                        if result["success"]:
                            document = {
                                'id': f"confluence-mcp://{result['page_title']}",
                                'name': result['page_title'],
                                'source': 'confluence_mcp',
                                'content': result.get('content', ''),
                                'file_path': result['file_path'],
                                'file_size': result.get('file_size', 0),
                                'metadata': {
                                    'page_title': result['page_title'],
                                    'download_method': 'mcp_search',
                                    'search_query': search_query,
                                    'file_path': result['file_path']
                                }
                            }
                            documents.append(document)
            
            # Method 3: Fetch from specific spaces
            elif space_keys:
                self.logger.info(f"Fetching pages from {len(space_keys)} spaces")
                for space_key in space_keys:
                    space_query = f"List all pages in Confluence space '{space_key}' and return their titles"
                    space_result = await self.query(space_query)
                    
                    # Parse titles from the response and download them
                    page_titles_in_space = self._extract_page_titles_from_response(space_result)
                    if page_titles_in_space:
                        download_result = await self.download_multiple_pages(page_titles_in_space, download_dir)
                        
                        for result in download_result.get("individual_results", []):
                            if result["success"]:
                                document = {
                                    'id': f"confluence-mcp://{space_key}/{result['page_title']}",
                                    'name': result['page_title'],
                                    'source': 'confluence_mcp',
                                    'space_key': space_key,
                                    'content': result.get('content', ''),
                                    'file_path': result['file_path'],
                                    'file_size': result.get('file_size', 0),
                                    'metadata': {
                                        'space_key': space_key,
                                        'page_title': result['page_title'],
                                        'download_method': 'mcp_space',
                                        'file_path': result['file_path']
                                    }
                                }
                                documents.append(document)
            
            self.logger.info(f"Successfully fetched {len(documents)} documents via MCP")
            return documents
            
        except Exception as e:
            self.logger.error(f"Error fetching documents: {e}")
            return []

    def _parse_search_results(self, search_response: str) -> List[Dict[str, Any]]:
        """Parse search results from LLM response."""
        # This is a simplified parser - would need to be enhanced based on actual MCP response format
        results = []
        try:
            # Basic parsing logic - would need to match actual response format
            lines = search_response.split('\n')
            current_result = {}
            
            for line in lines:
                line = line.strip()
                if line.startswith('Title:'):
                    if current_result:
                        results.append(current_result)
                    current_result = {'title': line[6:].strip()}
                elif line.startswith('URL:'):
                    current_result['url'] = line[4:].strip()
                elif line.startswith('Excerpt:'):
                    current_result['excerpt'] = line[8:].strip()
            
            if current_result:
                results.append(current_result)
                
        except Exception as e:
            self.logger.warning(f"Error parsing search results: {e}")
        
        return results

    def _extract_page_titles_from_response(self, response: str) -> List[str]:
        """Extract page titles from LLM response."""
        titles = []
        try:
            # Basic extraction logic - would need to match actual response format
            lines = response.split('\n')
            for line in lines:
                line = line.strip()
                if line and not line.startswith('#') and not line.startswith('*'):
                    # Simple heuristic to extract titles
                    if '- ' in line:
                        title = line.split('- ', 1)[1].strip()
                        titles.append(title)
                    elif line and len(line) > 3:
                        titles.append(line)
        except Exception as e:
            self.logger.warning(f"Error extracting page titles: {e}")
        
        return titles[:50]  # Limit to reasonable number

    # Legacy compatibility methods
    async def get_spaces(self) -> List[Dict[str, Any]]:
        """Get all accessible spaces using MCP agent."""
        if not self.is_available():
            return []
        
        try:
            query = "List all Confluence spaces with their keys, names, and descriptions"
            result = await self.query(query)
            return self._parse_spaces_response(result)
        except Exception as e:
            self.logger.error(f"Error getting spaces: {e}")
            return []

    def _parse_spaces_response(self, response: str) -> List[Dict[str, Any]]:
        """Parse spaces from LLM response."""
        spaces = []
        try:
            # Basic parsing - would need enhancement for actual format
            lines = response.split('\n')
            for line in lines:
                if 'Key:' in line and 'Name:' in line:
                    parts = line.split()
                    space = {
                        'key': next((p.split(':', 1)[1] for p in parts if p.startswith('Key:')), ''),
                        'name': next((p.split(':', 1)[1] for p in parts if p.startswith('Name:')), ''),
                        'description': ''
                    }
                    if space['key']:
                        spaces.append(space)
        except Exception as e:
            self.logger.warning(f"Error parsing spaces response: {e}")
        
        return spaces


# Factory function for integration
def create_confluence_mcp_connector(env_file: str = ".env.dev") -> ConfluenceMCPConnector:
    """Create a Confluence MCP connector instance."""
    return ConfluenceMCPConnector(env_file=env_file)


# Test function
async def test_confluence_mcp():
    """Test the Confluence MCP connector."""
    connector = create_confluence_mcp_connector()
    
    if not connector.is_available():
        print("❌ Confluence MCP connector not available")
        return
    
    print("✓ Confluence MCP connector initialized")
    
    # Get pages from environment configuration
    test_pages = connector.get_configured_pages()
    if not test_pages:
        print("⚠️ No Confluence pages configured in environment variables")
        print("Add CONFLUENCE_PAGES or CONFLUENCE_PAGE_X variables to .env.dev")
        return
    
    print(f"📋 Found {len(test_pages)} configured pages: {test_pages}")
    
    # Download the configured pages
    result = await connector.download_multiple_pages(test_pages)
    
    print(f"📥 Downloaded {result['successful_downloads']}/{result['total_pages']} pages")
    for file_path in result['downloaded_files']:
        print(f"  ✓ {file_path}")
    
    if result['failed_downloads'] > 0:
        print(f"❌ Failed downloads: {result['failed_downloads']}")
        for individual_result in result['individual_results']:
            if not individual_result['success']:
                print(f"  ❌ {individual_result['page_title']}: {individual_result['error']}")


async def download_configured_confluence_pages(connector: ConfluenceMCPConnector, download_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Download all pages configured in environment variables.
    
    Args:
        connector: The Confluence MCP connector instance
        download_dir: Optional directory for downloads
        
    Returns:
        Dictionary with download results
    """
    if not connector.is_available():
        return {
            "success": False,
            "error": "Confluence MCP connector not available"
        }
    
    configured_pages = connector.get_configured_pages()
    if not configured_pages:
        return {
            "success": False,
            "error": "No Confluence pages configured in environment variables"
        }
    
    print(f"📋 Downloading {len(configured_pages)} configured Confluence pages...")
    result = await connector.download_multiple_pages(configured_pages, download_dir)
    
    return {
        "success": True,
        "configured_pages": configured_pages,
        "download_result": result
    }


if __name__ == "__main__":
    asyncio.run(test_confluence_mcp())
