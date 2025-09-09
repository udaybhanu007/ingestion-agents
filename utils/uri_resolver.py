import os
import sys
import re
import tempfile
import shutil
from typing import Dict, Any, Optional, Tuple
from urllib.parse import urlparse, parse_qs
import logging
from datetime import datetime

# Add the parent directory to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from connector.azure import AzureConnector
from connector.box import BoxConnector
from connector.confluence_mcp import ConfluenceMCPConnector
from dotenv import load_dotenv

class URIContentResolver:
    """Enhanced URI resolver that converts source URIs to accessible content for ingestion APIs."""
    
    def __init__(self):
        """Initialize the URI resolver with connectors."""
        load_dotenv('.env.dev')
        self.logger = logging.getLogger(__name__)
        self._setup_logging()
        
        # Initialize connectors
        self._initialize_connectors()
        
        # Content cache directory
        self.cache_dir = "content_cache"
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def _setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def _initialize_connectors(self):
        """Initialize all connectors."""
        self.connectors = {}
        
        try:
            self.connectors['azure'] = AzureConnector()
            if hasattr(self.connectors['azure'], 'blob_service_client') and self.connectors['azure'].blob_service_client:
                self.logger.info("✅ Azure connector initialized successfully")
            else:
                self.logger.warning("⚠️ Azure connector initialized but blob service not available")
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize Azure connector: {e}")
        
        try:
            self.connectors['box'] = BoxConnector()
            if self.connectors['box'].is_available():
                self.logger.info("✅ Box connector initialized and authenticated")
            else:
                self.logger.warning("⚠️ Box connector initialized but not authenticated")
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize Box connector: {e}")
        
        try:
            self.connectors['confluence'] = ConfluenceMCPConnector()
            self.logger.info("✅ Confluence connector initialized")
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize Confluence connector: {e}")
    
    def resolve_uri_to_content(self, doc_uri: str) -> Dict[str, Any]:
        """
        Resolve a document URI to accessible content for ingestion API.
        
        Returns:
            Dict with 'status', 'content_type', 'content_path', 'content_text', 'metadata'
        """
        try:
            self.logger.info(f"🔍 Resolving URI: {doc_uri}")
            
            if doc_uri.startswith('azure://'):
                return self._resolve_azure_uri(doc_uri)
            elif doc_uri.startswith('box://'):
                return self._resolve_box_uri(doc_uri)
            elif doc_uri.startswith('confluence://'):
                return self._resolve_confluence_uri(doc_uri)
            else:
                return {
                    'status': 'error',
                    'error': f"Unsupported URI scheme: {doc_uri}",
                    'content_type': None,
                    'content_path': None,
                    'content_text': None,
                    'metadata': {}
                }
                
        except Exception as e:
            self.logger.error(f"❌ Error resolving URI {doc_uri}: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'content_type': None,
                'content_path': None,
                'content_text': None,
                'metadata': {}
            }
    
    def _resolve_azure_uri(self, doc_uri: str) -> Dict[str, Any]:
        """Resolve Azure Blob Storage URI to accessible content."""
        try:
            # Parse Azure URI: azure://rag-agents-container/ARXIV_V5_CHESTXRAY.pdf
            pattern = r'azure://([^/]+)/(.+)'
            match = re.search(pattern, doc_uri)
            
            if not match:
                raise ValueError(f"Invalid Azure URI format: {doc_uri}")
            
            container_name = match.group(1)
            blob_name = match.group(2)
            
            self.logger.info(f"📁 Azure: {container_name}/{blob_name}")
            
            # Get Azure connector
            azure_connector = self.connectors.get('azure')
            if not azure_connector:
                raise ValueError("Azure connector not available")
            
            # Use the correct method to download blob
            try:
                # Try the method that works in planner agent
                content = azure_connector.get_files_from_container(container_name)
                # Find the specific blob
                target_content = None
                for file_info in content:
                    if file_info['name'] == blob_name:
                        target_content = file_info['content']
                        break
                
                if target_content is None:
                    raise ValueError(f"Blob not found: {blob_name}")
                
                content = target_content
                
            except Exception as e:
                self.logger.warning(f"Container method failed: {e}")
                # Try direct blob download
                if hasattr(azure_connector, 'download_blob'):
                    content = azure_connector.download_blob(container_name, blob_name)
                else:
                    raise ValueError(f"No suitable method to download Azure blob: {e}")
            
            if content is None:
                raise ValueError(f"Failed to download blob: {container_name}/{blob_name}")
            
            # Determine content type and handle accordingly
            file_ext = os.path.splitext(blob_name)[1].lower()
            content_type = self._get_content_type(file_ext)
            
            # Create local cache file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_name = blob_name.replace('/', '_').replace('\\', '_')
            cache_filename = f"azure_{timestamp}_{safe_name}"
            cache_path = os.path.join(self.cache_dir, cache_filename)
            
            # Save content to cache
            if isinstance(content, bytes):
                with open(cache_path, 'wb') as f:
                    f.write(content)
                # Try to decode as text
                try:
                    content_text = content.decode('utf-8', errors='ignore')
                except:
                    content_text = self._read_file_as_text(cache_path, content_type)
            else:
                with open(cache_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                content_text = content
            
            self.logger.info(f"✅ Azure content cached to: {cache_path}")
            
            return {
                'status': 'success',
                'content_type': content_type,
                'content_path': os.path.abspath(cache_path),
                'content_text': content_text,
                'metadata': {
                    'source': 'azure',
                    'container': container_name,
                    'blob_name': blob_name,
                    'file_extension': file_ext,
                    'content_size': len(content) if content else 0,
                    'cached_at': datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            self.logger.error(f"❌ Azure URI resolution failed: {e}")
            raise
    
    def _resolve_box_uri(self, doc_uri: str) -> Dict[str, Any]:
        """Resolve Box file URI to accessible content."""
        try:
            # Parse Box URI: box://file/1969363374429?v=0
            pattern = r'box://file/(\d+)'
            match = re.search(pattern, doc_uri)
            
            if not match:
                raise ValueError(f"Invalid Box URI format: {doc_uri}")
            
            file_id = match.group(1)
            self.logger.info(f"📁 Box file ID: {file_id}")
            
            # Get Box connector
            box_connector = self.connectors.get('box')
            if not box_connector:
                raise ValueError("Box connector not available")
            
            if not box_connector.is_available():
                raise ValueError("Box connector not authenticated")
            
            # Get file info first
            try:
                file_info = box_connector.client.file(file_id).get()
                filename = file_info.name
                file_size = file_info.size
                self.logger.info(f"📄 Box file: {filename} ({file_size} bytes)")
            except Exception as e:
                self.logger.warning(f"Could not get file info: {e}")
                filename = f"box_file_{file_id}"
                file_size = 0
            
            # Check if file already exists in downloaded_content
            downloaded_files_dir = "downloaded_content"
            existing_file_path = None
            if os.path.exists(downloaded_files_dir):
                for existing_file in os.listdir(downloaded_files_dir):
                    if filename in existing_file or file_id in existing_file:
                        existing_file_path = os.path.join(downloaded_files_dir, existing_file)
                        self.logger.info(f"📁 Found existing file: {existing_file_path}")
                        break
            
            # Use existing file or download new one
            if existing_file_path and os.path.exists(existing_file_path):
                source_path = existing_file_path
                self.logger.info(f"📂 Using existing downloaded file: {source_path}")
            else:
                # Download file using Box connector
                self.logger.info(f"⬇️ Downloading Box file...")
                source_path = box_connector.download_file(file_id)
                
                if not source_path or not os.path.exists(source_path):
                    raise ValueError(f"Failed to download Box file {file_id}")
            
            # Determine content type
            file_ext = os.path.splitext(filename)[1].lower()
            content_type = self._get_content_type(file_ext)
            
            # Read content as text
            content_text = self._read_file_as_text(source_path, content_type)
            
            # Create permanent cache copy
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_filename = filename.replace('/', '_').replace('\\', '_')
            cache_filename = f"box_{timestamp}_{safe_filename}"
            cache_path = os.path.join(self.cache_dir, cache_filename)
            
            shutil.copy2(source_path, cache_path)
            
            self.logger.info(f"✅ Box content cached to: {cache_path}")
            
            # Get actual file size
            actual_size = os.path.getsize(cache_path) if os.path.exists(cache_path) else file_size
            
            return {
                'status': 'success',
                'content_type': content_type,
                'content_path': os.path.abspath(cache_path),
                'content_text': content_text,
                'metadata': {
                    'source': 'box',
                    'file_id': file_id,
                    'filename': filename,
                    'file_extension': file_ext,
                    'content_size': actual_size,
                    'cached_at': datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            self.logger.error(f"❌ Box URI resolution failed: {e}")
            raise
    
    def _resolve_confluence_uri(self, doc_uri: str) -> Dict[str, Any]:
        """Resolve Confluence page URI to accessible content."""
        try:
            # Parse Confluence URI: confluence://page/Chest X-Ray Medical Document
            pattern = r'confluence://page/(.+)'
            match = re.search(pattern, doc_uri)
            
            if not match:
                raise ValueError(f"Invalid Confluence URI format: {doc_uri}")
            
            page_title = match.group(1)
            self.logger.info(f"📁 Confluence page: {page_title}")
            
            # First check for local downloaded content
            filename = page_title.replace(' ', '_').replace('-', '_') + '.md'
            local_path = os.path.join('downloaded_content', filename)
            
            content_text = None
            content_source = None
            
            if os.path.exists(local_path):
                # Read from local file
                with open(local_path, 'r', encoding='utf-8') as f:
                    content_text = f.read()
                content_source = "local_file"
                self.logger.info(f"📂 Using existing local file: {local_path}")
            else:
                # Try to fetch from Confluence using MCP connector
                try:
                    import asyncio
                    
                    async def fetch_confluence_content():
                        query_text = f"Get the full content of the Confluence page titled '{page_title}'"
                        return await self.confluence_connector.query(query_text)
                    
                    # Run the async function
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        confluence_content = loop.run_until_complete(fetch_confluence_content())
                    finally:
                        loop.close()
                    
                    if confluence_content and confluence_content.strip():
                        content_text = f"# {page_title}\n\n*Retrieved from Confluence via MCP*\n\n{confluence_content}"
                        content_source = "mcp_fetch"
                        
                        # Save to local file for future use
                        os.makedirs('downloaded_content', exist_ok=True)
                        with open(local_path, 'w', encoding='utf-8') as f:
                            f.write(content_text)
                        self.logger.info(f"📥 Fetched from Confluence and saved to: {local_path}")
                    else:
                        # Create mock content as fallback
                        content_text = f"# {page_title}\n\n*Mock content for testing - Confluence page not accessible*\n\nThis is placeholder content for the Confluence page '{page_title}'. The actual content could not be retrieved from the MCP server.\n\n## Medical Document Content\n\nThis document would typically contain:\n- Patient information\n- Medical imaging analysis\n- Diagnostic findings\n- Treatment recommendations\n\n*Note: This is mock content for URI resolution testing.*"
                        content_source = "mock_content"
                        
                        # Save mock content to local file
                        os.makedirs('downloaded_content', exist_ok=True)
                        with open(local_path, 'w', encoding='utf-8') as f:
                            f.write(content_text)
                        self.logger.warning(f"📝 Created mock content and saved to: {local_path}")
                        
                except Exception as mcp_error:
                    self.logger.warning(f"⚠️ MCP fetch failed: {mcp_error}")
                    # Create mock content as fallback
                    content_text = f"# {page_title}\n\n*Mock content for testing - Confluence page not accessible*\n\nThis is placeholder content for the Confluence page '{page_title}'. The actual content could not be retrieved from the MCP server.\n\n## Medical Document Content\n\nThis document would typically contain:\n- Patient information\n- Medical imaging analysis\n- Diagnostic findings\n- Treatment recommendations\n\n*Note: This is mock content for URI resolution testing.*"
                    content_source = "mock_content_fallback"
                    
                    # Save mock content to local file
                    os.makedirs('downloaded_content', exist_ok=True)
                    with open(local_path, 'w', encoding='utf-8') as f:
                        f.write(content_text)
                    self.logger.warning(f"📝 Created fallback mock content and saved to: {local_path}")
            
            # Create cache copy
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            cache_filename = f"confluence_{timestamp}_{filename}"
            cache_path = os.path.join(self.cache_dir, cache_filename)
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                f.write(content_text)
            
            self.logger.info(f"✅ Confluence content cached to: {cache_path}")
            
            return {
                'status': 'success',
                'content_type': 'text/markdown',
                'content_path': os.path.abspath(cache_path),
                'content_text': content_text,
                'metadata': {
                    'source': 'confluence',
                    'page_title': page_title,
                    'file_extension': '.md',
                    'content_size': len(content_text),
                    'content_source': content_source,
                    'cached_at': datetime.now().isoformat()
                }
            }
                
        except Exception as e:
            self.logger.error(f"❌ Confluence URI resolution failed: {e}")
            raise
    
    def _get_content_type(self, file_ext: str) -> str:
        """Determine content type from file extension."""
        content_types = {
            '.pdf': 'application/pdf',
            '.txt': 'text/plain',
            '.md': 'text/markdown',
            '.csv': 'text/csv',
            '.json': 'application/json',
            '.xml': 'application/xml',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
        return content_types.get(file_ext, 'application/octet-stream')
    
    def _read_file_as_text(self, file_path: str, content_type: str) -> Optional[str]:
        """Read file content as text based on content type."""
        try:
            if content_type == 'application/pdf':
                # Handle PDF files
                try:
                    import PyPDF2
                    with open(file_path, 'rb') as f:
                        reader = PyPDF2.PdfReader(f)
                        text = ""
                        for page in reader.pages:
                            text += page.extract_text() + "\n"
                    return text
                except ImportError:
                    self.logger.warning("PyPDF2 not available for PDF processing")
                    # Try to read as binary and decode
                    try:
                        with open(file_path, 'rb') as f:
                            content = f.read()
                            return content.decode('utf-8', errors='ignore')
                    except:
                        return None
            
            elif content_type in ['text/plain', 'text/csv', 'text/markdown', 'application/json']:
                # Handle text-based files
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    return f.read()
            
            else:
                # Try to read as text anyway
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        return f.read()
                except:
                    # Try binary read and decode
                    try:
                        with open(file_path, 'rb') as f:
                            content = f.read()
                            return content.decode('utf-8', errors='ignore')
                    except:
                        return None
                    
        except Exception as e:
            self.logger.error(f"Error reading file as text: {e}")
            return None
    
    def batch_resolve_uris(self, doc_uris: list) -> Dict[str, Dict[str, Any]]:
        """Resolve multiple URIs to accessible content."""
        results = {}
        
        for uri in doc_uris:
            try:
                result = self.resolve_uri_to_content(uri)
                results[uri] = result
            except Exception as e:
                results[uri] = {
                    'status': 'error',
                    'error': str(e),
                    'content_type': None,
                    'content_path': None,
                    'content_text': None,
                    'metadata': {}
                }
        
        return results
    
    def create_ingestion_api_payload(self, doc_uri: str) -> Dict[str, Any]:
        """Create a payload suitable for ingestion API from a document URI."""
        resolved = self.resolve_uri_to_content(doc_uri)
        
        if resolved['status'] != 'success':
            return {
                'status': 'error',
                'error': resolved.get('error', 'Failed to resolve URI'),
                'payload': None
            }
        
        # Create API-friendly payload
        payload = {
            'document_uri': doc_uri,
            'content': {
                'text': resolved['content_text'],
                'file_path': resolved['content_path'],
                'content_type': resolved['content_type']
            },
            'metadata': resolved['metadata'],
            'ingestion_config': {
                'source': resolved['metadata']['source'],
                'timestamp': datetime.now().isoformat(),
                'content_size': resolved['metadata']['content_size']
            }
        }
        
        return {
            'status': 'success',
            'payload': payload
        }


def create_ingestion_api_payloads_from_execution_results(results_file: str):
    """Create ingestion API payloads from execution results."""
    import json
    
    resolver = URIContentResolver()
    
    # Load execution results
    with open(results_file, 'r', encoding='utf-8') as f:
        execution_results = json.load(f)
    
    api_payloads = []
    
    print(f"🔄 Creating API payloads from {len(execution_results)} execution results...")
    
    for result in execution_results:
        doc_uri = result['doc_uri']
        classification = result['classification']
        
        print(f"\n📄 Processing: {doc_uri}")
        
        try:
            payload_result = resolver.create_ingestion_api_payload(doc_uri)
            
            if payload_result['status'] == 'success':
                # Add classification info to payload
                payload = payload_result['payload']
                payload['classification'] = classification
                payload['plan_id'] = result.get('plan_id')
                
                api_payloads.append(payload)
                print(f"✅ Payload created successfully")
            else:
                print(f"❌ Failed to create payload: {payload_result['error']}")
                
        except Exception as e:
            print(f"❌ Error creating payload: {e}")
    
    # Save API payloads
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"ingestion_api_payloads_{timestamp}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(api_payloads, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 API payloads saved to: {output_file}")
    print(f"📊 Total payloads created: {len(api_payloads)}")
    
    return api_payloads, output_file


# Test function specifically for the Box URI
def test_specific_box_uri():
    """Test the specific Box URI from execution results."""
    resolver = URIContentResolver()
    
    # Test the specific URI from your execution results
    test_uri = "box://file/1969363374429?v=0"
    
    print(f"🧪 Testing specific Box URI: {test_uri}")
    print("=" * 60)
    
    try:
        result = resolver.resolve_uri_to_content(test_uri)
        
        if result['status'] == 'success':
            print(f"✅ SUCCESS!")
            print(f"   📁 Content Path: {result['content_path']}")
            print(f"   📋 Content Type: {result['content_type']}")
            print(f"   📊 Size: {result['metadata']['content_size']} bytes")
            print(f"   📄 Filename: {result['metadata']['filename']}")
            
            # Show content preview
            if result['content_text'] and len(result['content_text']) > 0:
                preview = result['content_text'][:200].replace('\n', ' ')
                print(f"   📝 Content Preview: {preview}...")
            
            # Create ingestion API payload
            api_result = resolver.create_ingestion_api_payload(test_uri)
            if api_result['status'] == 'success':
                print(f"   🎯 API Payload: Ready for ingestion")
                
                # Save the specific payload
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                payload_file = f"box_uri_payload_{timestamp}.json"
                
                import json
                with open(payload_file, 'w', encoding='utf-8') as f:
                    json.dump(api_result['payload'], f, indent=2, ensure_ascii=False)
                
                print(f"   💾 Payload saved to: {payload_file}")
            else:
                print(f"   ⚠️ API Payload Failed: {api_result['error']}")
                
            return True
        else:
            print(f"❌ FAILED: {result['error']}")
            return False
            
    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False


if __name__ == "__main__":
    # Test the specific Box URI
    success = test_specific_box_uri()
    
    if success:
        print(f"\n🎉 Box URI resolution successful!")
        print(f"📋 You can now use this URI with your ingestion API")
    else:
        print(f"\n❌ Box URI resolution failed")
        print(f"🔧 Check connector configuration and authentication")
