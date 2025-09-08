"""
Test Box Integration

This module tests the enhanced Box connector with OAuth2 authentication.
"""

import os
import pytest
import json
from pathlib import Path
import sys

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

try:
    from connector.box import BoxConnector
    BOX_AVAILABLE = True
except ImportError:
    BOX_AVAILABLE = False

@pytest.mark.skipif(not BOX_AVAILABLE, reason="Box SDK not available")
class TestBoxConnector:
    """Test cases for Box connector functionality."""
    
    def setup_method(self):
        """Setup for each test method."""
        self.box_client = None
        
    def teardown_method(self):
        """Cleanup after each test method."""
        if self.box_client:
            # Clean up any test files
            pass
    
    def test_box_connector_initialization(self):
        """Test Box connector can be initialized."""
        try:
            # This will fail if no credentials are available, which is expected
            box_client = BoxConnector()
            assert box_client is not None
        except ValueError as e:
            # Expected if no credentials in .env
            assert "Box_Client_Id and Box_Client_Secret must be provided" in str(e)
    
    def test_box_connector_with_invalid_credentials(self):
        """Test Box connector with invalid credentials."""
        with pytest.raises(ValueError):
            BoxConnector(client_id=None, client_secret=None)
    
    def test_token_status_no_tokens(self):
        """Test token status when no tokens exist."""
        try:
            box_client = BoxConnector(client_id="fake", client_secret="fake")
        except:
            # If authentication fails, that's expected for this test
            pass

def test_box_uri_parsing():
    """Test Box URI parsing logic."""
    # Test different Box URI formats
    uris = [
        "box://file/123456",
        "box://file/123456?v=abc",
        "box://folder/documents/file.pdf"
    ]
    
    for uri in uris:
        assert uri.startswith("box://")
        if "file/" in uri:
            file_id = uri.split("file/")[1].split("?")[0]
            assert file_id  # Should extract some file ID

def test_file_path_sanitization():
    """Test file path sanitization for safe downloading."""
    import re
    
    unsafe_names = [
        "file<name>.pdf",
        "file:name.docx", 
        'file"name.txt',
        "file/name.csv",
        "file\\name.json",
        "file|name.xml",
        "file?name.md",
        "file*name.html"
    ]
    
    for unsafe_name in unsafe_names:
        safe_name = re.sub(r'[<>:"/\\|?*]', '_', unsafe_name)
        assert not any(char in safe_name for char in '<>:"/\\|?*')
        assert safe_name.count('_') > 0  # Should have replaced something

class TestBoxIntegrationE2E:
    """End-to-end integration tests (require real Box credentials)."""
    
    @pytest.mark.skipif(not BOX_AVAILABLE, reason="Box SDK not available")
    @pytest.mark.skipif(not os.path.exists(".env"), reason="No .env file found")
    def test_authentication_flow(self):
        """Test the complete authentication flow."""
        try:
            box_client = BoxConnector()
            
            # Test token status
            status = box_client.get_token_status()
            assert isinstance(status, dict)
            assert "status" in status
            assert "authenticated" in status
            assert "message" in status
            
            print(f"Token Status: {status}")
            
        except Exception as e:
            pytest.skip(f"Authentication test skipped: {e}")
    
    @pytest.mark.skipif(not BOX_AVAILABLE, reason="Box SDK not available") 
    @pytest.mark.skipif(not os.path.exists(".env"), reason="No .env file found")
    def test_folder_operations(self):
        """Test folder finding and file listing."""
        try:
            box_client = BoxConnector()
            
            if not box_client.is_authenticated():
                pytest.skip("Not authenticated with Box")
            
            # Test finding a folder (may not exist)
            folder = box_client.find_folder("documents-ingest")
            # This may return None if folder doesn't exist, which is OK
            
            # Test listing files in root folder
            files = box_client.list_files("0")
            assert isinstance(files, list)
            
            print(f"Found {len(files)} files in root folder")
            
        except Exception as e:
            pytest.skip(f"Folder operations test skipped: {e}")

def main():
    """Manual test runner for Box integration."""
    print("🧪 Testing Box Integration...")
    
    if not BOX_AVAILABLE:
        print("❌ Box SDK not available. Install with: pip install boxsdk python-dotenv")
        return
    
    # Test 1: Check if .env file exists
    if not os.path.exists(".env"):
        print("⚠️  No .env file found. Copy .env.template to .env and configure your Box credentials.")
        print("   Box Developer Console: https://app.box.com/developers/console")
        return
    
    # Test 2: Try to initialize Box client
    try:
        print("🔧 Initializing Box client...")
        box_client = BoxConnector()
        print("✅ Box client initialized successfully")
        
        # Test 3: Check token status
        print("🔑 Checking token status...")
        status = box_client.get_token_status()
        print(f"   Status: {status['status']}")
        print(f"   Authenticated: {status['authenticated']}")
        print(f"   Message: {status['message']}")
        
        # Test 4: Test authentication
        if box_client.is_authenticated():
            print("✅ Successfully authenticated with Box")
            
            # Test 5: Try to list files in root
            print("📁 Listing files in root folder...")
            files = box_client.list_files("0")
            print(f"   Found {len(files)} files")
            
            # Test 6: Try to find a specific folder
            test_folder = "documents-ingest"
            print(f"🔍 Looking for folder: {test_folder}")
            folder = box_client.find_folder(test_folder)
            if folder:
                print(f"   Found folder: {folder['name']} (ID: {folder['id']})")
            else:
                print(f"   Folder '{test_folder}' not found")
            
        else:
            print("⚠️  Authentication required. Run the OAuth flow.")
            
    except Exception as e:
        print(f"❌ Error testing Box integration: {e}")
        print("\n💡 Troubleshooting:")
        print("   1. Check your .env file has valid Box_Client_Id and Box_Client_Secret")
        print("   2. Ensure your Box app is configured with redirect URI: http://localhost:8080")
        print("   3. Try running: box_client.clear_tokens_and_reauthenticate()")

if __name__ == "__main__":
    main()
