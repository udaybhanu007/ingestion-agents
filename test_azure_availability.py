#!/usr/bin/env python3
"""
Test script to verify Azure connector availability after installing missing packages
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

def test_azure_imports():
    """Test that Azure packages can be imported"""
    print("Testing Azure package imports...")
    
    try:
        from azure.storage.blob import BlobServiceClient, ContainerClient
        from azure.core.exceptions import AzureError
        print("✅ azure-storage-blob packages imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Failed to import Azure packages: {e}")
        return False

def test_azure_connector_availability():
    """Test that AzureConnector reports as available"""
    print("\nTesting AzureConnector availability...")
    
    try:
        from connector.azure import AzureConnector, AZURE_AVAILABLE
        
        print(f"AZURE_AVAILABLE flag: {AZURE_AVAILABLE}")
        
        if AZURE_AVAILABLE:
            print("✅ Azure connector is available")
            
            # Try to create an AzureConnector instance
            try:
                connector = AzureConnector()
                if connector.blob_service_client is None:
                    print("⚠️  AzureConnector created but blob_service_client is None (expected without credentials)")
                else:
                    print("✅ AzureConnector created successfully")
                return True
            except Exception as e:
                print(f"❌ Failed to create AzureConnector: {e}")
                return False
        else:
            print("❌ Azure connector reports as not available")
            return False
            
    except ImportError as e:
        print(f"❌ Failed to import AzureConnector: {e}")
        return False

def test_azure_connector_creation():
    """Test AzureConnector creation with mock credentials"""
    print("\nTesting AzureConnector with mock credentials...")
    
    try:
        from connector.azure import AzureConnector
        
        # Create connector with mock connection string (won't actually connect)
        connector = AzureConnector(
            connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=fake_key;EndpointSuffix=core.windows.net"
        )
        
        print("✅ AzureConnector created with mock credentials")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create AzureConnector with mock credentials: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all Azure connector tests"""
    print("🧪 Testing Azure Connector Availability")
    print("=" * 50)
    
    # Test 1: Azure package imports
    imports_ok = test_azure_imports()
    
    # Test 2: Azure connector availability
    availability_ok = test_azure_connector_availability()
    
    # Test 3: Azure connector creation
    creation_ok = test_azure_connector_creation()
    
    print("\n" + "=" * 50)
    
    if imports_ok and availability_ok and creation_ok:
        print("✅ All Azure connector tests passed!")
        print("The 'Azure connector not available' error should now be resolved.")
    else:
        print("❌ Some Azure connector tests failed.")
        if not imports_ok:
            print("  - Azure package import failed")
        if not availability_ok:
            print("  - Azure connector availability check failed")
        if not creation_ok:
            print("  - Azure connector creation failed")

if __name__ == "__main__":
    main()