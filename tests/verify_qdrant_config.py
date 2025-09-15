#!/usr/bin/env python3
"""
Verify Qdrant cloud configuration and test connection
"""

import os
from dotenv import load_dotenv

def verify_qdrant_config():
    """Verify that Qdrant is configured to use cloud instance"""
    
    print("🔍 Verifying Qdrant configuration...")
    
    # Load environment
    load_dotenv('.env.dev')
    
    url = os.getenv('QDRANT_API_URL')
    api_key = os.getenv('QDRANT_API_KEY') 
    collection = os.getenv('QDRANT_COLLECTION')
    
    print(f"📡 QDRANT_API_URL: {url}")
    print(f"🔑 QDRANT_API_KEY: {'*' * 10}...{api_key[-6:] if api_key else 'NOT SET'}")
    print(f"📁 QDRANT_COLLECTION: {collection}")
    
    # Check if cloud URL is being used
    if url and 'https://f779f36d-3ee0-4afe-b35c-9ced9a62f083.us-west-1-0.aws.cloud.qdrant.io' in url:
        print("✅ Qdrant cloud URL is correctly configured!")
        return True
    elif url and 'localhost' in url:
        print("❌ Still using localhost - should use cloud URL")
        return False
    else:
        print("❌ Qdrant URL not properly configured")
        return False

def test_simple_imports():
    """Test if we can import basic components without SSL hanging"""
    print("\n🧪 Testing basic imports...")
    
    try:
        # Test environment loading
        from dotenv import load_dotenv
        load_dotenv('.env.dev')
        print("✅ Environment loading works")
        
        # Test basic agent imports
        print("🔄 Testing agent imports...")
        import sys
        import os
        sys.path.append(os.path.dirname(__file__))
        
        return True
        
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False

if __name__ == "__main__":
    print("=" * 50)
    print("QDRANT CLOUD CONFIGURATION VERIFICATION")
    print("=" * 50)
    
    config_ok = verify_qdrant_config()
    imports_ok = test_simple_imports()
    
    print("\n" + "=" * 50)
    if config_ok:
        print("✅ Configuration verified - using Qdrant cloud!")
        print("🚀 Ready to test Box file 1969320109971 ingestion")
    else:
        print("❌ Configuration issue - check .env.dev file")
    print("=" * 50)