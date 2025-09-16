"""
Debug test for POST /ingest API endpoint
"""
import sys
import os
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_api_call():
    """Test basic POST request to /ingest endpoint"""
    try:
        from api.ingestion_api import app
        client = TestClient(app)
        
        # Mock all the dependencies
        with patch('api.ingestion_api.get_planner') as mock_get_planner, \
             patch('api.ingestion_api.get_execution_agent') as mock_get_execution, \
             patch('api.ingestion_api.api_logger') as mock_logger:
            
            # Setup simple mocks
            mock_planner = AsyncMock()
            mock_plan = {
                "plan_id": "test-123",
                "doc_uri": "azure://rag-agents-container/ARXIV_V5_CHESTXRAY.pdf",
                "steps": []
            }
            mock_planner.create_ingestion_plan_async.return_value = mock_plan
            mock_get_planner.return_value = mock_planner
            
            mock_executor = AsyncMock()
            mock_executor.execute_plan_async.return_value = []
            mock_get_execution.return_value = mock_executor
            
            # Test request
            test_request = {
                "doc_uri": "azure://rag-agents-container/ARXIV_V5_CHESTXRAY.pdf",
                "document_source": "azure",
                "document_type": "pdf"
            }
            
            print("Making POST request...")
            response = client.post("/ingest", json=test_request)
            print(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                print("✅ Test passed!")
                data = response.json()
                print(f"Plan ID: {data.get('plan_id')}")
                return True
            else:
                print(f"❌ Test failed with status {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🧪 Testing POST /ingest API...")
    test_api_call()
