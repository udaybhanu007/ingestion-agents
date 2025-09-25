"""
Ingestion API Endpoints

FastAPI endpoints for the ingestion system.
"""

import os
import sys
import copy
from datetime import datetime
from typing import Optional, Dict, Any, List
import uuid
import asyncio

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Add the current directory to the path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Load environment
from dotenv import load_dotenv
load_dotenv('.env.dev')

# Import agents
from agents.planner_agent import PlannerAgent
from agents.execution_agent import ExecutionAgent

# Import logging
from config.simple_logger import get_api_logger

app = FastAPI(
    title="Ingestion Agent API", 
    version="1.0.0"
)

# Initialize API logger
api_logger = get_api_logger("ingestion")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Agent instances (lazy-initialized)
planner = None
execution_agent = None

def get_planner() -> PlannerAgent:
    """Get or create planner agent instance"""
    global planner
    if planner is None:
        planner = PlannerAgent()
    return planner

def get_execution_agent() -> ExecutionAgent:
    """Get or create execution agent instance"""
    global execution_agent
    if execution_agent is None:
        execution_agent = ExecutionAgent()
    return execution_agent

def remove_content_from_dict(obj):
    """Recursively remove all 'content' keys from nested dictionaries"""
    if isinstance(obj, dict):
        obj.pop('content', None)
        for key, value in obj.items():
            if isinstance(value, dict):
                remove_content_from_dict(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        remove_content_from_dict(item)

# Request/Response models
class IngestRequest(BaseModel):
    doc_uri: str
    document_source: Optional[str] = None
    document_type: Optional[str] = None
    content_type: Optional[str] = None
    processing_options: Optional[Dict[str, Any]] = None

class IngestResponse(BaseModel):
    run_id: str
    plan_id: str
    message: str
    plan: Dict[str, Any]
    execution_results: Optional[List[Dict[str, Any]]] = None

class StatusResponse(BaseModel):
    run_id: str
    status: str
    plan: Dict[str, Any]
    errors: List[str]
    execution_results: Optional[List[Dict[str, Any]]] = None

# Global run tracking
active_runs: Dict[str, Dict[str, Any]] = {}

@app.post("/ingest", response_model=IngestResponse)
async def ingest_document(request: IngestRequest):
    """
    Create an ingestion plan for a document URI and execute it.
    """
    run_id = str(uuid.uuid4())
    
    api_logger.info(f"Ingestion request received - run_id: {run_id}, doc_uri: {request.doc_uri}")
    
    try:
        # Initialize run tracking
        active_runs[run_id] = {
            "status": "planning",
            "plan": {},
            "errors": [],
            "start_time": datetime.now().isoformat(),
            "request": request.model_dump()
        }
        
        # Build metadata
        metadata = {
            "document_source": request.document_source or "unknown",
            "document_type": request.document_type or "txt",
            "content_type": request.content_type or "text/plain",
            "processing_options": request.processing_options or {},
            "test_type": "graph_only"
        }
        
        # Create ingestion plan
        agent = get_planner()
        api_logger.info(f"Starting plan creation - run_id: {run_id}")
        
        plan_result = await agent.create_ingestion_plan_async(
            doc_uri=request.doc_uri,
            metadata=metadata
        )
        
        # Create clean copy for response
        clean_plan_for_response = copy.deepcopy(plan_result)
        remove_content_from_dict(clean_plan_for_response)
        
        # Update run status
        active_runs[run_id]["status"] = "executing"
        active_runs[run_id]["plan"] = clean_plan_for_response

        api_logger.info(f"Plan created, starting execution - run_id: {run_id}, plan_id: {plan_result.get('plan_id')}")

        # Execute the plan
        executor = get_execution_agent()
        execution_results = await executor.execute_plan_async(plan_result)
        
        # Calculate metrics
        execution_success = all(r.get('status') == 'success' for r in execution_results)
        total_entities = sum(r.get('result', {}).get('result', {}).get('entities_created', 0)
                            for r in execution_results if r.get('step_type') == 'graph_ingestion' and r.get('result', {}).get('success'))
        total_relationships = sum(r.get('result', {}).get('result', {}).get('relationships_created', 0)
                                for r in execution_results if r.get('step_type') == 'graph_ingestion' and r.get('result', {}).get('success'))

        final_status = "completed" if execution_success else "failed"
        
        # Clean execution results
        cleaned_execution_results = []
        for result in execution_results:
            cleaned_result = result.copy()
            remove_content_from_dict(cleaned_result)
            cleaned_execution_results.append(cleaned_result)
        
        # Store run data
        active_runs[run_id].update({
            "status": final_status,
            "completed_at": datetime.now().isoformat(),
            "plan": clean_plan_for_response,
            "execution_results": cleaned_execution_results,
            "errors": [r.get('error') for r in execution_results if r.get('error')],
            "metrics": {
                "entities_created": total_entities,
                "relationships_created": total_relationships
            }
        })

        api_logger.info(f"Ingestion completed - run_id: {run_id}, status: {final_status}, "
                       f"entities: {total_entities}, relationships: {total_relationships}")

        return IngestResponse(
            run_id=run_id,
            plan_id=plan_result["plan_id"],
            message=f"Ingestion completed for {request.doc_uri}. "
                   f"Created {total_entities} entities and {total_relationships} relationships.",
            plan=clean_plan_for_response,
            execution_results=cleaned_execution_results
        )
    except Exception as e:
        # Update run data with error
        if 'run_id' in locals():
            active_runs[run_id].update({
                "status": "failed",
                "completed_at": datetime.now().isoformat(),
                "errors": [str(e)]
            })
        
        api_logger.error(f"Ingestion failed - run_id: {run_id if 'run_id' in locals() else 'unknown'}, "
                        f"error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")

@app.get("/status/{run_id}", response_model=StatusResponse)
async def get_run_status(run_id: str):
    """Get the status of a specific ingestion run."""
    api_logger.info(f"Status request - run_id: {run_id}")
    
    if run_id not in active_runs:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    
    run_info = active_runs[run_id].copy()
    remove_content_from_dict(run_info)
    
    return StatusResponse(
        run_id=run_id,
        status=run_info["status"],
        plan=run_info.get("plan", {}),
        errors=run_info.get("errors", []),
        execution_results=run_info.get("execution_results", [])
    )

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy", 
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app, 
        host="127.0.0.1", 
        port=8020, 
        reload=False,
        access_log=True,
        log_level="info"
    )
