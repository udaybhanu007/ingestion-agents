"""
Ingestion API Endpoints

FastAPI endpoints for the ingestion system.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import uuid
from datetime import datetime
import sys
import os

# Add the current directory to the path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Import agents and connectors
from agents.planner_agent import PlannerAgent
from agents.execution_agent import ExecutionAgent

# Import simple logging
from config.simple_logger import get_api_logger, get_logger

app = FastAPI(title="Ingestion Agent API", version="1.0.0")

# Initialize API logger
api_logger = get_api_logger("ingestion")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Lazy-initialized planner agent
planner = None
execution_agent = None

def get_planner() -> PlannerAgent:
    """Get or create planner agent instance (singleton pattern)"""
    global planner
    if planner is None:
        planner = PlannerAgent()
    return planner

def get_execution_agent() -> ExecutionAgent:
    """Get or create execution agent instance (singleton pattern)"""
    global execution_agent
    if execution_agent is None:
        execution_agent = ExecutionAgent()
    return execution_agent

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

# Global run tracking
active_runs: Dict[str, Dict[str, Any]] = {}

# Status tracking for runs
class StatusResponse(BaseModel):
    run_id: str
    status: str
    plan: Dict[str, Any]
    errors: List[str]
    execution_results: Optional[List[Dict[str, Any]]] = None

@app.post("/ingest", response_model=IngestResponse)
async def ingest_document(request: IngestRequest):
    """
    Create an ingestion plan for a document URI and execute it.
    
    This endpoint:
    1. Takes a document URI
    2. Initializes appropriate connector based on URI scheme
    3. Calls the planner agent to create an ingestion plan asynchronously
    4. Executes the plan using execution agent
    5. Returns the plan and execution results
    """
    # Generate unique run ID
    run_id = str(uuid.uuid4())
    
    # Log API request
    api_logger.info(f"Ingestion request received - run_id: {run_id}, doc_uri: {request.doc_uri}, "
                   f"document_source: {request.document_source}, document_type: {request.document_type}, "
                   f"endpoint: /ingest")
    
    try:
        # Initialize run tracking
        active_runs[run_id] = {
            "status": "planning",
            "plan": {},
            "errors": [],
            "start_time": datetime.now().isoformat(),
            "request": request.dict()
        }
        
        # Get planner agent (lazy initialization)
        agent = get_planner()
        
        # Create ingestion plan using planner agent (async)
        api_logger.info(f"Starting plan creation - run_id: {run_id}, doc_uri: {request.doc_uri}")
        
        plan = await agent.create_ingestion_plan_async(
            doc_uri=request.doc_uri,
            metadata={
                "document_source": request.document_source,
                "document_type": request.document_type, 
                "content_type": request.content_type,
                "processing_options": request.processing_options or {}
            }
        )
        
        # Update run status
        active_runs[run_id]["status"] = "executing"
        active_runs[run_id]["plan"] = plan
        
        api_logger.info(f"Plan creation completed, starting execution - run_id: {run_id}, "
                       f"plan_id: {plan.get('plan_id')}, steps_count: {len(plan.get('steps', []))}")
        
        # Get execution agent and execute the plan
        executor = get_execution_agent()
        
        # Execute the plan using content from plan steps (Approach 2)
        execution_results = await executor.execute_plan_async(plan)
        
        # Determine overall execution status
        execution_success = all(r.get('status') == 'completed' for r in execution_results)
        final_status = "completed" if execution_success else "failed"
        
        # Store run information
        run_data = {
            "run_id": run_id,
            "plan_id": plan["plan_id"],
            "doc_uri": request.doc_uri,
            "status": final_status,
            "created_at": active_runs[run_id]["start_time"],
            "completed_at": datetime.now().isoformat(),
            "plan": plan,
            "execution_results": execution_results,
            "errors": [r.get('error') for r in execution_results if r.get('error')]
        }
        
        active_runs[run_id] = run_data
        
        # Log completion
        execution_time_ms = (datetime.now() - datetime.fromisoformat(active_runs[run_id]["start_time"])).total_seconds() * 1000
        api_logger.info(f"Ingestion request completed - run_id: {run_id}, plan_id: {plan['plan_id']}, "
                       f"status: {final_status}, execution_time_ms: {execution_time_ms}")
        
        return IngestResponse(
            run_id=run_id,
            plan_id=plan["plan_id"],
            message=f"Ingestion plan created and executed for {request.doc_uri}",
            plan=plan,
            execution_results=execution_results
        )
        
    except Exception as e:
        # Store error information if execution fails
        if 'run_id' in locals():
            error_data = {
                "run_id": run_id,
                "plan_id": plan.get("plan_id", "") if 'plan' in locals() else "",
                "doc_uri": request.doc_uri,
                "status": "failed",
                "created_at": active_runs.get(run_id, {}).get("start_time", datetime.now().isoformat()),
                "completed_at": datetime.now().isoformat(),
                "plan": plan if 'plan' in locals() else {},
                "execution_results": [],
                "errors": [str(e)]
            }
            active_runs[run_id] = error_data
        
        # Log error
        run_id_value = run_id if 'run_id' in locals() else "unknown"
        api_logger.error(f"Ingestion request failed - run_id: {run_id_value}, doc_uri: {request.doc_uri}, "
                        f"error: {str(e)}, error_type: {type(e).__name__}, endpoint: /ingest")
        
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")

@app.get("/status/{run_id}", response_model=StatusResponse)
async def get_run_status(run_id: str):
    """Get the status of a specific ingestion run."""
    api_logger.info(f"Status request received - run_id: {run_id}, endpoint: /status")
    
    if run_id not in active_runs:
        api_logger.warning(f"Status request for unknown run_id: {run_id}")
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    
    run_info = active_runs[run_id]
    
    api_logger.info(f"Status request completed - run_id: {run_id}, status: {run_info['status']}, endpoint: /status")
    
    return StatusResponse(
        run_id=run_id,
        status=run_info["status"],
        plan=run_info["plan"],
        errors=run_info.get("errors", []),
        execution_results=run_info.get("execution_results")
    )

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    api_logger.info("Health check requested - endpoint: /health")
    return {
        "status": "healthy", 
        "timestamp": datetime.now().isoformat(),
        "simplified_logging": True
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
