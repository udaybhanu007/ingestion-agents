"""
Integration Test for POST /ingest API Endpoint

This test demonstrates direct integration testing of the /ingest POST API
using actual agent implementations without mocks. It tests both planner and execution agents.
"""

import pytest
import sys
import os
import asyncio
from fastapi.testclient import TestClient
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables before importing agents
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
env_path = os.path.join(parent_dir, ".env.dev")

# Load .env.dev file (no fallback)
if os.path.exists(env_path):
    load_dotenv(env_path)
    print(f"✅ Environment file loaded from: {env_path}")
else:
    raise FileNotFoundError(f"Required .env.dev file not found at: {env_path}")

# Verify critical environment variables are loaded
required_vars = [
    'AZURE_OPENAI_API_KEY', 
    'AZURE_OPENAI_ENDPOINT', 
    'AZURE_STORAGE_ACCOUNT_KEY', 
    'AZURE_STORAGE_CONNECTION_STRING'
]

missing_vars = []
for var in required_vars:
    if not os.getenv(var):
        missing_vars.append(var)

if missing_vars:
    print(f"⚠️ Missing environment variables: {', '.join(missing_vars)}")
else:
    print("✅ All required environment variables are loaded")

class TestIngestAPIWorking:
    """Integration test class for POST /ingest endpoint"""
    
    async def test_agents_directly(self):
        """Test planner and execution agents directly without mocks"""
        print("🚀 Testing agents directly without mocks...")
        
        # Import agents directly
        from agents.planner_agent import PlannerAgent
        from agents.execution_agent import ExecutionAgent
        
        # Initialize agents
        planner = PlannerAgent()
        executor = ExecutionAgent()
        
        # Test document URI
        doc_uri = "azure://rag-agents-container/ARXIV_V5_CHESTXRAY.pdf"
        
        # Test metadata
        metadata = {
            "document_source": "azure",
            "document_type": "pdf", 
            "content_type": "application/pdf",
            "processing_options": {
                "extract_metadata": True,
                "chunk_size": 1024
            }
        }
        
        print(f"📄 Testing with document: {doc_uri}")
        
        try:
            # Step 1: Test planner agent directly
            print("🔍 Step 1: Testing planner agent...")
            plan = await planner.create_ingestion_plan_async(
                doc_uri=doc_uri,
                metadata=metadata
            )
            
            print("✅ Planner agent test completed!")
            print(f"🆔 Plan ID: {plan.get('plan_id', 'N/A')}")
            print(f"📝 Plan steps: {len(plan.get('steps', []))}")
            print(f"📊 Document classification: {plan.get('document_classification', 'N/A')}")
            
            # Verify plan structure
            assert isinstance(plan, dict), "Plan should be a dictionary"
            assert "plan_id" in plan, "Plan should have plan_id"
            assert "steps" in plan, "Plan should have steps"
            assert isinstance(plan["steps"], list), "Steps should be a list"
            assert len(plan["steps"]) > 0, "Plan should have at least one step"
            
            # Step 2: Test execution agent directly
            print("\n⚡ Step 2: Testing execution agent...")
            execution_results = await executor.execute_plan_async(plan)
            
            print("✅ Execution agent test completed!")
            print(f"📈 Execution results count: {len(execution_results)}")
            
            # Verify execution results
            assert isinstance(execution_results, list), "Execution results should be a list"
            assert len(execution_results) > 0, "Should have at least one execution result"
            
            # Display results
            for i, result in enumerate(execution_results):
                status = result.get('status', 'unknown')
                step_id = result.get('step_id', f'step-{i}')
                print(f"📋 Step {i+1} ({step_id}): {status}")
                
                if result.get('result'):
                    step_result = result['result']
                    if isinstance(step_result, dict):
                        print(f"   📊 Success: {step_result.get('success', 'unknown')}")
                        if 'chunks_created' in step_result:
                            print(f"   📄 Chunks created: {step_result['chunks_created']}")
                        if 'vectors_stored' in step_result:
                            print(f"   🔢 Vectors stored: {step_result['vectors_stored']}")
            
            print("\n🎉 Direct agents test completed successfully!")
            return True, plan, execution_results
            
        except Exception as e:
            print(f"\n❌ Direct agents test failed: {e}")
            import traceback
            traceback.print_exc()
            return False, None, None
    
    def test_ingest_endpoint_with_azure_document(self):
        """Test POST /ingest API with real agents (no mocks)"""
        print("🚀 Testing POST /ingest API endpoint...")
        
        # Import the FastAPI app
        from api.ingestion_api import app
        client = TestClient(app)
        
        # Test request payload
        test_request = {
            "doc_uri": "azure://rag-agents-container/ARXIV_V5_CHESTXRAY.pdf",
            "document_source": "azure", 
            "document_type": "pdf",
            "content_type": "application/pdf",
            "processing_options": {
                "extract_metadata": True,
                "chunk_size": 1024
            }
        }
        
        print(f"📄 Testing with document: {test_request['doc_uri']}")
        
        try:
            # Make the API call (no mocks - real integration test)
            response = client.post("/ingest", json=test_request)
            
            # Verify response
            assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
            
            response_data = response.json()
            
            # Verify response structure
            assert "run_id" in response_data, "Response should have run_id"
            assert "plan_id" in response_data, "Response should have plan_id"
            assert "message" in response_data, "Response should have message"
            assert "plan" in response_data, "Response should have plan"
            assert "execution_results" in response_data, "Response should have execution_results"
            
            # Verify plan structure
            plan = response_data["plan"]
            assert isinstance(plan, dict), "Plan should be a dictionary"
            assert "plan_id" in plan, "Plan should have plan_id"
            assert "steps" in plan, "Plan should have steps"
            assert len(plan["steps"]) > 0, "Plan should have at least one step"
            
            # Verify execution results
            execution_results = response_data["execution_results"]
            assert isinstance(execution_results, list), "Execution results should be a list"
            assert len(execution_results) > 0, "Should have at least one execution result"
            
            print("✅ POST /ingest API test completed successfully!")
            print(f"📄 Document: {test_request['doc_uri']}")
            print(f"🆔 Run ID: {response_data['run_id']}")
            print(f"🆔 Plan ID: {response_data['plan_id']}")
            print(f"� Steps executed: {len(execution_results)}")
            
            # Display execution results
            for i, result in enumerate(execution_results):
                status = result.get('status', 'unknown')
                step_id = result.get('step_id', f'step-{i}')
                print(f"📋 Step {i+1} ({step_id}): {status}")
                
                if result.get('result') and isinstance(result['result'], dict):
                    step_result = result['result']
                    if 'chunks_created' in step_result:
                        print(f"   📄 Chunks created: {step_result['chunks_created']}")
                    if 'vectors_stored' in step_result:
                        print(f"   🔢 Vectors stored: {step_result['vectors_stored']}")
            
            return True
            
        except Exception as e:
            print(f"\n❌ API test failed: {e}")
            import traceback
            traceback.print_exc()
            return False

def run_manual_test():
    """Run the test manually for demonstration"""
    test_instance = TestIngestAPIWorking()
    
    print("🔍 Running direct agents test first...")
    try:
        # Test agents directly first
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        success, plan, execution_results = loop.run_until_complete(
            test_instance.test_agents_directly()
        )
        loop.close()
        
        if not success:
            print("\n❌ Direct agents test failed!")
            return False
            
        print("\n🌐 Now testing API endpoint...")
        # Test API endpoint
        api_success = test_instance.test_ingest_endpoint_with_azure_document()
        
        if api_success:
            print("\n🎉 All tests passed! Both agents and API are working correctly.")
            return True
        else:
            print("\n❌ API test failed!")
            return False
            
    except Exception as e:
        print(f"\n❌ Test execution failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Testing ingestion agents and API without mocks")
    print("📄 Document: azure://rag-agents-container/ARXIV_V5_CHESTXRAY.pdf")
    print("=" * 60)
    run_manual_test()
