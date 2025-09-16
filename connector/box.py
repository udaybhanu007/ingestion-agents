"""
Simplified Box Integration for File Downloading
Focuses on core functionality: authentication and folder file downloading

Features:
- OAuth2 authentication with automatic token management
- File downloading from specific folders
- Clean error handling and logging
- Files saved to downloaded_content folder
"""

import os
import json
import logging
import webbrowser
import time
import threading
import ssl
import certifi
from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse
from typing import Optional, Dict, List, Union, Any
from pathlib import Path
import re

try:
    from dotenv import load_dotenv
    from boxsdk import Client
    from boxsdk.auth.oauth2 import OAuth2
    from boxsdk.exception import BoxAPIException
    import requests
    BOXSDK_AVAILABLE = True
except ImportError:
    BOXSDK_AVAILABLE = False
    logging.warning("boxsdk not available. Install with: pip install boxsdk python-dotenv")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
REDIRECT_URI = "http://localhost:8080"
TOKEN_FILE = "box_oauth_tokens.json"


class BoxConnector:
    """Enhanced Box connector with OAuth2 authentication and file operations."""
    
    def __init__(self, client_id: Optional[str] = None, client_secret: Optional[str] = None, 
                 access_token: Optional[str] = None, env_file: str = ".env.dev"):
        """
        Initialize Box connector.
        
        Args:
            client_id (str, optional): Box application client ID
            client_secret (str, optional): Box application client secret
            access_token (str, optional): Box access token (deprecated - use OAuth2 flow)
            env_file (str): Path to environment file for credentials (defaults to .env.dev)
        """
        # Set up logger first
        self.logger = logging.getLogger(__name__)
        
        # Quick SSL fix for corporate networks
        self._apply_ssl_fix()
        
        if not BOXSDK_AVAILABLE:
            self.logger.error("boxsdk is not available. Please install with: pip install boxsdk python-dotenv")
            self.client = None
            self.oauth = None
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
        self.client_id = client_id or os.getenv('Box_Client_Id')
        self.client_secret = client_secret or os.getenv('Box_Client_Secret')
        
        if not self.client_id or not self.client_secret:
            raise ValueError("Box_Client_Id and Box_Client_Secret must be provided or set in environment file")
        
        self.logger.info(f"Initialized Box connector with Client ID: {self.client_id[:8]}...")
        
        self.client: Optional[Client] = None
        self.oauth: Optional[OAuth2] = None
        
        # Setup SSL context for certificate issues
        self._setup_ssl_context()
        
        # Initialize authentication
        self._authenticate()
    
    def _apply_ssl_fix(self):
        """Apply immediate SSL fix for corporate networks."""
        try:
            # Set environment variables for SSL verification
            os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
            os.environ['SSL_CERT_FILE'] = certifi.where()
            os.environ['CURL_CA_BUNDLE'] = certifi.where()
            
            # For corporate networks with strict SSL policies - disable verification
            os.environ['PYTHONHTTPSVERIFY'] = '0'
            
            # Disable SSL warnings
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            
            # Monkey patch the requests library to disable SSL verification globally
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.ssl_ import create_urllib3_context
            
            # Create an SSL context that doesn't verify certificates
            class SSLAdapter(HTTPAdapter):
                def init_poolmanager(self, *args, **kwargs):
                    context = create_urllib3_context()
                    context.check_hostname = False
                    context.verify_mode = ssl.CERT_NONE
                    kwargs['ssl_context'] = context
                    return super().init_poolmanager(*args, **kwargs)
            
            # Patch the default session
            session = requests.Session()
            session.verify = False
            adapter = SSLAdapter()
            session.mount('https://', adapter)
            session.mount('http://', adapter)
            
            # Replace the global requests session
            requests.sessions.Session = lambda: session
            
            # Additional fix for the Box SDK specifically
            try:
                from boxsdk.network.default_network import DefaultNetwork
                DefaultNetwork._session = session
                self.logger.info("Box SDK network session patched for SSL")
            except Exception as e:
                self.logger.warning(f"Box SDK network patch failed: {e}")
            
            self.logger.info("Aggressive SSL fix applied for Box authentication")
            
        except Exception as e:
            self.logger.warning(f"SSL fix failed: {e}")
    
    def _setup_ssl_context(self):
        """Setup SSL context to handle certificate verification issues."""
        try:
            # Set environment variables for SSL verification
            os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
            os.environ['SSL_CERT_FILE'] = certifi.where()
            os.environ['CURL_CA_BUNDLE'] = certifi.where()
            
            self.logger.info(f"SSL certificates configured: {certifi.where()}")
            
        except Exception as e:
            self.logger.warning(f"SSL context setup failed: {e}")
            
        # Additional fallback for SSL issues
        try:
            # Patch the Box SDK's requests session
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            
            # Create a custom session with SSL settings
            session = requests.Session()
            session.verify = certifi.where()
            
            # For corporate networks with SSL issues, uncomment the next line:
            # session.verify = False
            
            # Monkey patch the Box SDK to use our session
            from boxsdk.network.default_network import DefaultNetwork
            DefaultNetwork._session = session
            
            self.logger.info("Box SDK session patched for SSL")
            
        except Exception as e:
            self.logger.warning(f"Box SDK session patch failed: {e}")
            # Last resort - disable SSL verification
            os.environ['PYTHONHTTPSVERIFY'] = '0'
            import urllib3
            urllib3.disable_warnings()
            self.logger.warning("SSL verification disabled as last resort")
    
    def is_available(self) -> bool:
        """Check if Box connector is available and configured."""
        return BOXSDK_AVAILABLE and bool(self.client_id) and bool(self.client_secret)
    
    def _authenticate(self):
        """Handle authentication flow with better error handling"""
        try:
            if self._load_tokens() and self._validate_authentication():
                self.logger.info("Using existing authentication")
                return
            
            self.logger.info("Existing tokens invalid or missing")
            
        except Exception as e:
            self.logger.warning(f"Error during token validation: {e}")
        
        # Clear any invalid tokens
        self._clear_tokens()
        
        self.logger.info("Starting OAuth2 authentication...")
        if not self._oauth_flow():
            raise Exception("Authentication failed")
    
    def authenticate(self) -> bool:
        """
        Authenticate with Box API (legacy method for compatibility).
        
        Returns:
            bool: True if authentication successful, False otherwise
        """
        try:
            self._authenticate()
            return self.is_authenticated()
        except Exception as e:
            self.logger.error(f"Authentication failed: {e}")
            return False
    
    def clear_tokens_and_reauthenticate(self):
        """Manually clear tokens and force re-authentication"""
        self.logger.info("Manually clearing tokens and re-authenticating...")
        self._clear_tokens()
        self.client = None
        self.oauth = None
        self._authenticate()
    
    def is_authenticated(self) -> bool:
        """Check if currently authenticated"""
        return self._validate_authentication()
    
    def get_token_status(self) -> Dict[str, Union[str, bool, float]]:
        """Get current token status information"""
        try:
            if not os.path.exists(TOKEN_FILE):
                return {
                    "status": "no_tokens",
                    "authenticated": False,
                    "message": "No token file found"
                }
            
            with open(TOKEN_FILE, 'r') as f:
                token_data = json.load(f)
            
            timestamp = token_data.get('timestamp', 0)
            token_age_hours = (time.time() - timestamp) / 3600
            
            is_auth = self._validate_authentication()
            
            return {
                "status": "valid" if is_auth else "invalid",
                "authenticated": is_auth,
                "token_age_hours": round(token_age_hours, 2),
                "has_access_token": bool(token_data.get('access_token')),
                "has_refresh_token": bool(token_data.get('refresh_token')),
                "message": "Tokens are working" if is_auth else "Tokens need refresh or re-authentication"
            }
            
        except Exception as e:
            return {
                "status": "error",
                "authenticated": False,
                "message": f"Error checking token status: {e}"
            }
    
    def _clear_tokens(self):
        """Clear stored tokens"""
        try:
            if os.path.exists(TOKEN_FILE):
                os.remove(TOKEN_FILE)
                self.logger.info("Cleared expired tokens")
        except Exception as e:
            self.logger.error(f"Error clearing tokens: {e}")
    
    def _load_tokens(self) -> bool:
        """Load existing tokens from file"""
        try:
            if not os.path.exists(TOKEN_FILE):
                return False
            
            with open(TOKEN_FILE, 'r') as f:
                token_data = json.load(f)
            
            self.oauth = OAuth2(
                client_id=self.client_id,
                client_secret=self.client_secret,
                access_token=token_data.get('access_token'),
                refresh_token=token_data.get('refresh_token')
            )
            
            self.client = Client(self.oauth)
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading tokens: {e}")
            return False
    
    def _save_tokens(self, access_token: str, refresh_token: str):
        """Save tokens to file"""
        try:
            token_data = {
                'access_token': access_token,
                'refresh_token': refresh_token,
                'timestamp': time.time()
            }
            
            with open(TOKEN_FILE, 'w') as f:
                json.dump(token_data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Error saving tokens: {e}")
    
    def _validate_authentication(self) -> bool:
        """Validate current authentication and refresh token if needed"""
        try:
            if not self.client or not self.oauth:
                return False
            
            # Test API call
            try:
                self.client.user().get()
                self.logger.info("Authentication is valid")
                return True
                
            except BoxAPIException as e:
                if e.status == 401:  # Unauthorized - token expired
                    self.logger.info("Access token expired, attempting to refresh...")
                    return self._refresh_token()
                else:
                    self.logger.error(f"API error during validation: {e}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Error validating authentication: {e}")
            return False
    
    def _refresh_token(self) -> bool:
        """Refresh the access token using refresh token"""
        try:
            if not self.oauth:
                self.logger.error("No OAuth object available for token refresh")
                return False
            
            # Get current refresh token from saved data
            if not os.path.exists(TOKEN_FILE):
                self.logger.error("No token file found for refresh")
                return False
                
            with open(TOKEN_FILE, 'r') as f:
                token_data = json.load(f)
            
            current_refresh_token = token_data.get('refresh_token')
            if not current_refresh_token:
                self.logger.error("No refresh token available")
                return False
            
            # Attempt to refresh the token
            access_token, refresh_token = self.oauth.refresh(current_refresh_token)
            
            # Save the new tokens (handle None refresh_token)
            new_refresh_token = refresh_token if refresh_token else current_refresh_token
            self._save_tokens(access_token, new_refresh_token)
            
            # Update the client with new tokens
            self.oauth = OAuth2(
                client_id=self.client_id,
                client_secret=self.client_secret,
                access_token=access_token,
                refresh_token=new_refresh_token
            )
            self.client = Client(self.oauth)
            
            self.logger.info("Token refreshed successfully")
            return True
            
        except BoxAPIException as e:
            if e.status == 400 and 'invalid_grant' in str(e):
                self.logger.error("Refresh token expired or invalid - need to re-authenticate")
                self._clear_tokens()
                return False
            else:
                self.logger.error(f"Error refreshing token: {e}")
                return False
                
        except Exception as e:
            self.logger.error(f"Unexpected error during token refresh: {e}")
            return False
    
    def _oauth_flow(self) -> bool:
        """Execute OAuth2 flow with local server"""
        auth_code = None
        server_running = True
        
        class AuthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                nonlocal auth_code, server_running
                
                # Handle both root path and callback path
                if self.path.startswith('/') and ('code=' in self.path or 'state=' in self.path):
                    # Parse authorization code from any path
                    parsed = urllib.parse.urlparse(self.path)
                    query_params = urllib.parse.parse_qs(parsed.query)
                    
                    if 'code' in query_params:
                        auth_code = query_params['code'][0]
                        self.send_response(200)
                        self.send_header('Content-type', 'text/html')
                        self.end_headers()
                        success_page = """
                        <html>
                        <head><title>Box Authentication</title></head>
                        <body>
                            <h1>✅ Authentication Successful!</h1>
                            <p>You can close this window and return to your application.</p>
                            <script>window.close();</script>
                        </body>
                        </html>
                        """
                        self.wfile.write(success_page.encode())
                    else:
                        self.send_response(400)
                        self.send_header('Content-type', 'text/html')
                        self.end_headers()
                        error_page = """
                        <html>
                        <head><title>Box Authentication Error</title></head>
                        <body>
                            <h1>❌ Authentication Failed!</h1>
                            <p>No authorization code received.</p>
                        </body>
                        </html>
                        """
                        self.wfile.write(error_page.encode())
                    
                    server_running = False
                else:
                    # Handle other requests
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()
                    waiting_page = """
                    <html>
                    <head><title>Box Authentication</title></head>
                    <body>
                        <h1>⏳ Waiting for Box Authentication...</h1>
                        <p>Please complete the authentication in the Box tab.</p>
                    </body>
                    </html>
                    """
                    self.wfile.write(waiting_page.encode())
            
            def log_message(self, format, *args):
                pass  # Suppress server logs
        
        # Start local server
        server = HTTPServer(('localhost', 8080), AuthHandler)
        server_thread = threading.Thread(target=lambda: server.serve_forever())
        server_thread.daemon = True
        server_thread.start()
        
        try:
            # Create OAuth and get authorization URL
            self.oauth = OAuth2(
                client_id=self.client_id,
                client_secret=self.client_secret
            )
            
            auth_url, csrf_token = self.oauth.get_authorization_url(REDIRECT_URI)
            
            print(f"Opening browser for authentication: {auth_url}")
            webbrowser.open(auth_url)
            
            # Wait for authorization code
            timeout = time.time() + 300  # 5 minutes
            while server_running and time.time() < timeout:
                time.sleep(1)
            
            server.shutdown()
            
            if not auth_code:
                self.logger.error("No authorization code received")
                return False
            
            # Exchange code for tokens
            access_token, refresh_token = self.oauth.authenticate(auth_code)
            self._save_tokens(access_token, refresh_token)
            
            self.client = Client(self.oauth)
            self.logger.info("Authentication successful")
            return True
            
        except Exception as e:
            self.logger.error(f"OAuth flow failed: {e}")
            return False
    
    def find_folder(self, folder_name: str) -> Optional[Dict]:
        """Find folder by name in root directory"""
        try:
            if not self.client:
                return None
            
            root_folder = self.client.folder('0')
            items = root_folder.get_items()
            
            for item in items:
                if item.type == 'folder' and item.name == folder_name:
                    return {
                        'id': item.id,
                        'name': item.name,
                        'type': item.type
                    }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding folder '{folder_name}': {e}")
            return None
    
    def list_files(self, folder_id: str = "0") -> List[Dict[str, Any]]:
        """
        List files in a Box folder.
        
        Args:
            folder_id (str): Box folder ID (default is root folder)
            
        Returns:
            List[Dict[str, Any]]: List of file metadata
        """
        try:
            if not self.client:
                self.logger.error("Not authenticated")
                return []
            
            folder = self.client.folder(folder_id)
            items = folder.get_items()
            
            files = []
            for item in items:
                if item.type == 'file':
                    file_info = {
                        'id': item.id,
                        'name': item.name,
                        'size': getattr(item, 'size', 0),
                        'type': item.type,
                        'modified_at': getattr(item, 'modified_at', None),
                        'created_at': getattr(item, 'created_at', None),
                        'etag': getattr(item, 'etag', None)
                    }
                    files.append(file_info)
            
            self.logger.info(f"Listed {len(files)} files in folder {folder_id}")
            return files
            
        except Exception as e:
            self.logger.error(f"Error listing files in folder {folder_id}: {e}")
            return []
    
    def list_files_recursive(self, folder_id: str = "0", max_files: int = 100) -> List[Dict[str, Any]]:
        """
        List files recursively in Box folder and all subfolders.
        
        Args:
            folder_id (str): Box folder ID to start from
            max_files (int): Maximum number of files to return
            
        Returns:
            List[Dict[str, Any]]: List of file metadata from all subfolders
        """
        try:
            if not self.client:
                self.logger.error("Not authenticated")
                return []
            
            all_files = []
            folders_to_process = [folder_id]
            
            while folders_to_process and len(all_files) < max_files:
                current_folder_id = folders_to_process.pop(0)
                
                try:
                    folder = self.client.folder(current_folder_id)
                    items = folder.get_items()
                    
                    for item in items:
                        if len(all_files) >= max_files:
                            break
                            
                        if item.type == 'file':
                            file_info = {
                                'id': item.id,
                                'name': item.name,
                                'size': getattr(item, 'size', 0),
                                'type': item.type,
                                'modified_at': getattr(item, 'modified_at', None),
                                'created_at': getattr(item, 'created_at', None),
                                'etag': getattr(item, 'etag', None),
                                'folder_id': current_folder_id
                            }
                            all_files.append(file_info)
                        elif item.type == 'folder':
                            # Add subfolder to processing queue
                            folders_to_process.append(item.id)
                            self.logger.debug(f"Found subfolder: {item.name} (ID: {item.id})")
                            
                except Exception as e:
                    self.logger.warning(f"Error processing folder {current_folder_id}: {e}")
                    continue
            
            self.logger.info(f"Listed {len(all_files)} files recursively from folder {folder_id}")
            return all_files
            
        except Exception as e:
            self.logger.error(f"Error listing files recursively from folder {folder_id}: {e}")
            return []
    
    def find_folder_by_name(self, folder_name: str, parent_folder_id: str = "0") -> Optional[str]:
        """
        Find a folder by name within a parent folder.
        
        Args:
            folder_name (str): Name of the folder to find
            parent_folder_id (str): Parent folder ID to search in
            
        Returns:
            Optional[str]: Folder ID if found, None otherwise
        """
        try:
            if not self.client:
                self.logger.error("Not authenticated")
                return None
            
            folder = self.client.folder(parent_folder_id)
            items = folder.get_items()
            
            for item in items:
                if item.type == 'folder' and item.name == folder_name:
                    self.logger.info(f"Found folder '{folder_name}' with ID: {item.id}")
                    return item.id
            
            self.logger.warning(f"Folder '{folder_name}' not found in parent folder {parent_folder_id}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding folder '{folder_name}': {e}")
            return None
    
    def get_folder_files(self, folder_id: str) -> List[Dict]:
        """Get all files from a folder (alias for list_files)"""
        return self.list_files(folder_id)
    
    def download_file(self, file_id: str, destination: Optional[str] = None, file_name: Optional[str] = None) -> Union[bool, str]:
        """
        Download a file from Box.
        
        Args:
            file_id (str): Box file ID
            destination (str, optional): Local destination path or directory
            file_name (str, optional): Original file name (for automatic path generation)
            
        Returns:
            Union[bool, str]: True/False for legacy compatibility, or file path string
        """
        try:
            if not self.client:
                self.logger.error("Not authenticated")
                return False
            
            # Get file info if file_name not provided
            if not file_name:
                file_obj = self.client.file(file_id)
                file_info = file_obj.get()
                file_name = file_info.name
            
            # Determine destination path
            if destination is None:
                # Create downloaded_content folder if it doesn't exist
                project_root = Path(__file__).parent.parent
                download_folder = project_root / "downloaded_content"
                download_folder.mkdir(exist_ok=True)
                
                # Clean filename for safe saving
                safe_filename = re.sub(r'[<>:"/\\|?*]', '_', file_name)
                file_path = download_folder / safe_filename
            else:
                destination_path = Path(destination)
                if destination_path.is_dir():
                    # Destination is a directory
                    safe_filename = re.sub(r'[<>:"/\\|?*]', '_', file_name)
                    file_path = destination_path / safe_filename
                else:
                    # Destination is a file path
                    file_path = destination_path
                    file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Download the file
            file_obj = self.client.file(file_id)
            file_content = file_obj.content()
            
            # Write the file
            with open(file_path, 'wb') as f:
                f.write(file_content)
            
            self.logger.info(f"File downloaded to: {file_path}")
            return str(file_path)
            
        except Exception as e:
            self.logger.error(f"Error downloading file {file_id}: {e}")
            return False
    
    def get_file_metadata(self, file_id: str) -> Dict[str, Any]:
        """
        Get metadata for a specific file.
        
        Args:
            file_id (str): Box file ID
            
        Returns:
            Dict[str, Any]: File metadata
        """
        try:
            if not self.client:
                self.logger.error("Not authenticated")
                return {}
            
            file_obj = self.client.file(file_id)
            file_info = file_obj.get()
            
            metadata = {
                'id': file_info.id,
                'name': file_info.name,
                'size': file_info.size,
                'type': file_info.type,
                'created_at': file_info.created_at,
                'modified_at': file_info.modified_at,
                'description': getattr(file_info, 'description', ''),
                'path_collection': getattr(file_info, 'path_collection', {}),
                'owned_by': getattr(file_info, 'owned_by', {}),
                'shared_link': getattr(file_info, 'shared_link', {}),
                'permissions': getattr(file_info, 'permissions', {})
            }
            
            self.logger.info(f"Retrieved metadata for file {file_id}")
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error getting metadata for file {file_id}: {e}")
            return {}

    def get_file_content(self, file_id: str) -> Optional[str]:
        """
        Get file content as string directly from Box without saving to disk.
        Uses pypdf for PDF files.
        
        Args:
            file_id (str): Box file ID
        Returns:
            Optional[str]: File content as string, None if error
        """
        try:
            if not self.client:
                self.logger.error("Not authenticated")
                return None
            file_obj = self.client.file(file_id)
            file_info = file_obj.get()
            file_name = file_info.name if hasattr(file_info, 'name') else ''
            file_content = file_obj.content()
            # Check for PDF extension
            if file_name.lower().endswith('.pdf'):
                try:
                    from io import BytesIO
                    from pypdf import PdfReader
                    pdf_stream = BytesIO(file_content)
                    reader = PdfReader(pdf_stream)
                    text = "\n".join(page.extract_text() or "" for page in reader.pages)
                    self.logger.info(f"Extracted text from PDF file {file_id} ({len(text)} characters)")
                    return text
                except Exception as e:
                    self.logger.error(f"PDF extraction failed for file {file_id}: {e}")
                    return None
            # Non-PDF: decode as text
            if isinstance(file_content, bytes):
                content_str = file_content.decode('utf-8', errors='ignore')
            else:
                content_str = str(file_content)
            self.logger.info(f"Retrieved content for file {file_id} ({len(content_str)} characters)")
            return content_str
        except Exception as e:
            self.logger.error(f"Error getting content for file {file_id}: {e}")
            return None

    async def get_document_content(self, doc_uri: str) -> Optional[Dict[str, Any]]:
        """
        Get document content from Box URI (async interface for planner agent).
        
        Args:
            doc_uri (str): Document URI in format 'box://file/file_id'
            
        Returns:
            Optional[Dict[str, Any]]: Dictionary with content and metadata, None if error
        """
        try:
            # Parse Box URI: box://file/file_id
            if not doc_uri.startswith("box://"):
                self.logger.error(f"Invalid Box URI format: {doc_uri}")
                return None
            
            parts = doc_uri.replace("box://", "").split("/")
            if len(parts) < 2 or parts[0] != "file":
                self.logger.error(f"Invalid Box URI format: {doc_uri}. Expected 'box://file/file_id'")
                return None
            
            file_id = parts[1]
            self.logger.info(f"Extracting content for Box file ID: {file_id}")
            
            # Get file content and metadata
            content = self.get_file_content(file_id)
            if content is None:
                return None
            
            metadata = self.get_file_metadata(file_id)
            
            result = {
                "content": content,
                "metadata": {
                    "doc_uri": doc_uri,
                    "file_id": file_id,
                    "connector_type": "box",
                    "content_type": "text/plain",
                    "size": len(content),
                    **metadata
                }
            }
            
            self.logger.info(f"Successfully retrieved document content for {doc_uri}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error getting document content for {doc_uri}: {e}")
            return None
    
    def fetch_folder_documents(self, folder_name: str) -> Dict[str, Union[str, List, int, Dict]]:
        """
        Main method: Download all files from a folder
        
        Args:
            folder_name: Name of the folder to download files from
            
        Returns:
            Dictionary with folder info and download results
        """
        try:
            if not self.client:
                return {
                    "error": "Not authenticated",
                    "folder_name": folder_name,
                    "document_count": 0,
                    "files": []
                }
            
            # Find the folder
            folder = self.find_folder(folder_name)
            if not folder:
                return {
                    "error": f"Folder '{folder_name}' not found",
                    "folder_name": folder_name,
                    "document_count": 0,
                    "files": []
                }
            
            # Get files from folder
            files = self.get_folder_files(folder['id'])
            
            # Download each file
            processed_files = []
            for file_info in files:
                try:
                    # Download the actual file
                    downloaded_path = self.download_file(file_info['id'], file_name=file_info['name'])
                    
                    file_data = {
                        "id": file_info['id'],
                        "name": file_info['name'],
                        "size_bytes": file_info['size'],
                        "size_mb": round(file_info['size'] / (1024 * 1024), 2),
                        "status": "success"
                    }
                    
                    # Add downloaded file path
                    if downloaded_path and downloaded_path != False:
                        file_data["downloaded_file"] = downloaded_path
                    
                    processed_files.append(file_data)
                    
                except Exception as e:
                    processed_files.append({
                        "id": file_info['id'],
                        "name": file_info['name'],
                        "size_bytes": file_info['size'],
                        "size_mb": round(file_info['size'] / (1024 * 1024), 2),
                        "status": "error",
                        "error": str(e)
                    })
            
            return {
                "folder_name": folder_name,
                "folder_id": folder['id'],
                "document_count": len(processed_files),
                "files": processed_files,
                "status": "success",
                "summary": {
                    "total_files": len(processed_files),
                    "successful_downloads": len([f for f in processed_files if f.get('downloaded_file')]),
                    "errors": len([f for f in processed_files if f.get('status') == 'error'])
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error fetching documents from folder '{folder_name}': {e}")
            return {
                "error": str(e),
                "folder_name": folder_name,
                "document_count": 0,
                "files": []
            }


# Alias for backward compatibility
BoxOAuth2Integration = BoxConnector
BoxClient = BoxConnector


def main():
    """Test the Box integration with token management"""
    try:
        print("🔗 Testing Box Integration with Token Management...")
        
        # Initialize client
        box_client = BoxConnector()
        
        # Show token status
        token_status = box_client.get_token_status()
        print(f"🔑 Token Status: {token_status['status']}")
        print(f"   Authenticated: {token_status['authenticated']}")
        print(f"   Message: {token_status['message']}")
        if 'token_age_hours' in token_status:
            print(f"   Token Age: {token_status['token_age_hours']} hours")
        print()
        
        # Test folder document downloading
        folder_name = "documents-ingest"
        print(f"📂 Downloading files from folder: {folder_name}")
        
        result = box_client.fetch_folder_documents(folder_name)
        
        # Ensure result is a dictionary
        if not isinstance(result, dict):
            print(f"❌ Unexpected result type: {type(result)}")
            return
        
        print(f"✅ Found {result.get('document_count', 0)} files")
        
        # Show summary statistics if available
        if result.get('summary') and isinstance(result.get('summary'), dict):
            summary = result['summary']
            summary_dict = summary if isinstance(summary, dict) else {}
            print(f"📊 Summary:")
            print(f"   📁 Total files: {summary_dict.get('total_files', 0)}")
            print(f"   💾 Successfully downloaded: {summary_dict.get('successful_downloads', 0)}")
            if summary_dict.get('errors', 0) > 0:
                print(f"   ❌ Errors: {summary_dict.get('errors', 0)}")
            print()
        
        files = result.get('files', [])
        if isinstance(files, list):
            for file_info in files:
                if isinstance(file_info, dict):
                    print(f"   📄 {file_info.get('name', 'Unknown')} ({file_info.get('size_mb', 0)} MB)")
                    
                    # Show downloaded file
                    if file_info.get('downloaded_file'):
                        print(f"      💾 Downloaded to: {Path(file_info['downloaded_file']).name}")
                        print(f"      📁 Location: downloaded_content/")
                    else:
                        print(f"      ❌ Download failed")
                        
                    print()  # Add spacing between files
        else:
            print(f"❌ Files is not a list: {type(files)}")
        
        # Final token status check
        final_token_status = box_client.get_token_status()
        print(f"🔑 Final Token Status: {final_token_status['status']}")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        print("\n💡 Token Management Commands:")
        print("   - box_client.clear_tokens_and_reauthenticate()  # Force re-auth")
        print("   - box_client.get_token_status()                # Check token status")
        print("   - box_client.is_authenticated()                # Quick auth check")


if __name__ == "__main__":
    main()
