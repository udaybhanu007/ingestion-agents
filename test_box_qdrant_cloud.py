#!/usr/bin/env python3
"""
Test Box file 1969320109971 ingestion with Qdrant cloud and comprehensive SSL fixes
"""

import os
import sys
import ssl
import asyncio
import warnings
import json

# Comprehensive SSL fixes for Qdrant cloud
os.environ['PYTHONHTTPSVERIFY'] = '0'
os.environ['CURL_CA_BUNDLE'] = ''
os.environ['REQUESTS_CA_BUNDLE'] = ''

# Disable SSL warnings
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# Apply SSL context fixes early
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except:
    pass

# Monkey patch httpx for Qdrant cloud connection
def patch_httpx_ssl():
    try:
        import httpx
        from httpx._config import create_ssl_context
        
        def patched_create_ssl_context(*args, **kwargs):
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            return context
        
        httpx._config.create_ssl_context = patched_create_ssl_context
        print("✅ HTTPX SSL context patched for Qdrant cloud")
    except Exception as e:
        print(f"⚠️ HTTPX patch failed: {e}")

# Apply patches early
patch_httpx_ssl()

async def test_box_file_ingestion():
    """Test ingestion of Box file 1969320109971 with Qdrant cloud"""
    
    # Load environment to get collection name
    from dotenv import load_dotenv
    load_dotenv('.env.dev')
    
    collection_name = os.getenv('QDRANT_COLLECTION', 'agent_research_doc')
    
    print("🚀 Testing Box file ingestion with Qdrant cloud...")
    print(f"📦 Box File ID: 1969320109971")
    print(f"☁️ Qdrant Cloud: https://f779f36d-3ee0-4afe-b35c-9ced9a62f083.us-west-1-0.aws.cloud.qdrant.io")
    print(f"📁 Collection: {collection_name}")
    print("-" * 60)

    try:
        # Import with SSL fixes applied
        from agents.planner_agent import PlannerAgent
        from agents.execution_agent import ExecutionAgent

        doc_uri = "azure://rag-agents-container/Data_Entry_2017-small.csv"
        metadata = {
            "document_source": "box",
            "document_type": "txt",
            "content_type": "text/plain",
            "processing_options": {},
            "test_type": "graph_only"  # Focus on graph ingestion for nodes and relationships
        }


        

        print("🔧 Initializing planner and executor agents...")
        planner = PlannerAgent()
        executor = ExecutionAgent()

        print("📋 Creating ingestion plan...")
        plan = await planner.create_ingestion_plan_async(
            doc_uri=doc_uri,
            metadata=metadata
            #content=""
        )

        # Print the plan in JSON format (use repr fallback for non-serializable objects)
        try:
            print("📦 Ingestion plan (JSON):")
            print(json.dumps(plan, indent=2, default=lambda o: repr(o)))
        except Exception as e:
            print(f"⚠️ Failed to serialize plan to JSON: {e}")
            print("Raw plan:", repr(plan))

        print("⚡ Executing ingestion plan...")
        result = await executor.execute_plan_async(plan)

        print("🎉 Ingestion completed!")
        print(f"📈 Result: {result}")

        return result

    except Exception as e:
        print(f"❌ Error during ingestion: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main function to run the test"""
    print("=" * 60)
    print("BOX FILE INGESTION TEST - QDRANT CLOUD")
    print("=" * 60)
    
    result = asyncio.run(test_box_file_ingestion())
    
    if result:
        print("\n✅ SUCCESS: Box file ingestion completed!")
        print("🔍 Check your Qdrant cloud dashboard for the ingested vectors")
        print("📊 Collection: agent_research_doc")
        print("🌐 Dashboard: https://cloud.qdrant.io")
    else:
        print("\n❌ FAILED: Box file ingestion failed")
        print("🔧 Check SSL configuration and Qdrant cloud connectivity")

if __name__ == "__main__":
    main()