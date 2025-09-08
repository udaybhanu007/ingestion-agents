"""
Test scenarios for the Ingestion API

This module contains test cases demonstrating the API functionality.
"""

import asyncio
import json
import pytest
from fastapi.testclient import TestClient
from api.ingestion_api import app

client = TestClient(app)

def test_health_check():
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_root_endpoint():
    """Test the root endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    assert "name" in response.json()
    assert "endpoints" in response.json()

def test_get_plan_vector_only():
    """Test getting a plan for a document that requires only vector ingestion."""
    doc_uri = "box://file/123.pdf"
    response = client.get(f"/plan/{doc_uri}")
    
    assert response.status_code == 200
    data = response.json()
    assert "plan_id" in data
    assert "steps" in data
    assert len(data["steps"]) == 1
    assert data["steps"][0]["tool"] == "vector_ingestion"
    assert data["steps"][0]["args"]["doc_uri"] == doc_uri

def test_get_plan_both_tools():
    """Test getting a plan for a document that requires both vector and graph ingestion."""
    doc_uri = "confluence://space/page/structured-data.json"
    response = client.get(f"/plan/{doc_uri}")
    
    assert response.status_code == 200
    data = response.json()
    assert "plan_id" in data
    assert "steps" in data
    assert len(data["steps"]) == 2
    
    # Check that vector ingestion comes first
    assert data["steps"][0]["tool"] == "vector_ingestion"
    assert data["steps"][0]["depends_on"] == []
    
    # Check that graph ingestion depends on vector
    assert data["steps"][1]["tool"] == "graph_ingestion"
    assert data["steps"][1]["depends_on"] == ["1"]

def test_ingest_document():
    """Test submitting a document for ingestion."""
    doc_uri = "azure://container/document.pdf"
    metadata = {"priority": 8, "department": "research"}
    
    request_data = {
        "doc_uri": doc_uri,
        "metadata": metadata,
        "priority": 8
    }
    
    response = client.post("/ingest", json=request_data)
    
    assert response.status_code == 200
    data = response.json()
    assert "run_id" in data
    assert "plan_id" in data
    assert data["status"] == "queued"
    assert doc_uri in data["message"]
    
    # Test status check
    run_id = data["run_id"]
    status_response = client.get(f"/status/{run_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["run_id"] == run_id
    assert "progress" in status_data

def test_status_not_found():
    """Test status check for non-existent run."""
    fake_run_id = "00000000-0000-0000-0000-000000000000"
    response = client.get(f"/status/{fake_run_id}")
    assert response.status_code == 404

def test_plan_with_metadata():
    """Test getting a plan with metadata that affects classification."""
    doc_uri = "box://file/medical-records.csv"
    metadata = json.dumps({"content_type": "medical", "patient_data": True})
    
    response = client.get(f"/plan/{doc_uri}?metadata={metadata}")
    
    assert response.status_code == 200
    data = response.json()
    assert "plan_id" in data
    assert "steps" in data
    
    # Should have both tools due to medical metadata
    assert len(data["steps"]) == 2

# Integration test examples
class TestIngestionFlow:
    """Integration tests for complete ingestion workflows."""
    
    def test_complete_ingestion_flow(self):
        """Test a complete ingestion flow from submission to completion."""
        # Step 1: Submit document for ingestion
        doc_uri = "box://file/sample-document.txt"
        request_data = {"doc_uri": doc_uri}
        
        response = client.post("/ingest", json=request_data)
        assert response.status_code == 200
        
        run_id = response.json()["run_id"]
        
        # Step 2: Check initial status
        status_response = client.get(f"/status/{run_id}")
        assert status_response.status_code == 200
        
        initial_status = status_response.json()
        assert initial_status["status"] in ["queued", "running"]
        
        # Note: In a real test, you'd wait for completion or mock the execution
        # For now, we just verify the API structure
        assert "progress" in initial_status
        assert "total_steps" in initial_status["progress"]
        assert "completed_steps" in initial_status["progress"]

# Sample usage examples
def example_api_usage():
    """
    Example showing how to use the API programmatically.
    """
    import requests
    
    base_url = "http://localhost:8000"
    
    # Example 1: Get a plan for a document
    doc_uri = "confluence://wiki/engineering/architecture.md"
    plan_response = requests.get(f"{base_url}/plan/{doc_uri}")
    plan = plan_response.json()
    print(f"Plan for {doc_uri}:")
    print(json.dumps(plan, indent=2))
    
    # Example 2: Submit document for ingestion
    ingest_data = {
        "doc_uri": "box://file/quarterly-report.pdf",
        "metadata": {
            "priority": 7,
            "department": "finance",
            "quarter": "Q3-2024"
        }
    }
    
    ingest_response = requests.post(f"{base_url}/ingest", json=ingest_data)
    ingest_result = ingest_response.json()
    print(f"Ingestion submitted: {ingest_result}")
    
    # Example 3: Check status
    run_id = ingest_result["run_id"]
    status_response = requests.get(f"{base_url}/status/{run_id}")
    status = status_response.json()
    print(f"Status: {status}")

if __name__ == "__main__":
    # Run example usage (requires API server to be running)
    # example_api_usage()
    
    # Run tests
    pytest.main([__file__, "-v"])
