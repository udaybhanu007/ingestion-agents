"""
Ingestion API Endpoints

FastAPI endpoints for the ingestion system.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import uuid
from datetime import datetime
import sys
import os

# Add the current directory to the path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Import agents
from agents.planner_agent import PlannerAgent
from agents.execution_agent import ExecutionAgent

app = FastAPI(title="Ingestion Agent API", version="1.0.0")

# Initialize agents
planner = PlannerAgent()
executor = ExecutionAgent()

# Request/Response models
class IngestRequest(BaseModel):
    doc_uri: str
    metadata: Optional[Dict[str, Any]] = None
    priority: Optional[int] = 5

class IngestResponse(BaseModel):
    run_id: str
    plan_id: str
    status: str
    message: str

class PlanResponse(BaseModel):
    plan_id: str
    steps: List[Dict[str, Any]]

class StatusResponse(BaseModel):
    run_id: str
    status: str
    progress: Dict[str, Any]
    errors: List[str]

# Global run tracking
active_runs: Dict[str, Dict[str, Any]] = {}

@app.post("/ingest", response_model=IngestResponse)
async def ingest_document(request: IngestRequest, background_tasks: BackgroundTasks):
    """
    Ingest a document asynchronously.
    
    This endpoint:
    1. Creates an ingestion plan via Planner Agent
    2. Enqueues execution tasks
    3. Returns immediately with run_id
    """
    try:
        # Generate run ID
        run_id = str(uuid.uuid4())
        
        # Create ingestion plan
        plan = planner.create_ingestion_plan(
            doc_uri=request.doc_uri,
            metadata=request.metadata
        )
        
        # Store run information
        active_runs[run_id] = {
            "plan_id": plan["plan_id"],
            "doc_uri": request.doc_uri,
            "status": "queued",
            "created_at": datetime.now(),
            "steps": plan["steps"],
            "completed_steps": [],
            "errors": []
        }
        
        # Queue execution in background
        background_tasks.add_task(execute_plan_async, run_id, plan)
        
        return IngestResponse(
            run_id=run_id,
            plan_id=plan["plan_id"],
            status="queued",
            message=f"Ingestion queued for {request.doc_uri}"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/plan/{doc_uri:path}", response_model=PlanResponse)
async def get_ingestion_plan(doc_uri: str, metadata: Optional[str] = None):
    """
    Get an ingestion plan for a document without executing it.
    """
    try:
        # Parse metadata if provided
        import json
        parsed_metadata = json.loads(metadata) if metadata else None
        
        plan = planner.create_ingestion_plan(
            doc_uri=doc_uri,
            metadata=parsed_metadata
        )
        
        return PlanResponse(**plan)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/status/{run_id}", response_model=StatusResponse)
async def get_ingestion_status(run_id: str):
    """
    Get the status of an ingestion run.
    """
    if run_id not in active_runs:
        raise HTTPException(status_code=404, detail="Run not found")
    
    run_info = active_runs[run_id]
    
    # Calculate progress
    total_steps = len(run_info["steps"])
    completed_steps = len(run_info["completed_steps"])
    progress_percent = (completed_steps / total_steps * 100) if total_steps > 0 else 0
    
    return StatusResponse(
        run_id=run_id,
        status=run_info["status"],
        progress={
            "total_steps": total_steps,
            "completed_steps": completed_steps,
            "progress_percent": progress_percent,
            "current_step": run_info.get("current_step"),
            "estimated_completion": run_info.get("estimated_completion")
        },
        errors=run_info["errors"]
    )

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": "Ingestion Agent API",
        "version": "1.0.0",
        "endpoints": {
            "POST /ingest": "Submit a document for ingestion",
            "GET /plan/{doc_uri}": "Get ingestion plan for a document",
            "GET /status/{run_id}": "Get status of an ingestion run",
            "GET /health": "Health check",
        }
    }

async def execute_plan_async(run_id: str, plan: Dict[str, Any]):
    """
    Execute an ingestion plan asynchronously.
    """
    try:
        run_info = active_runs[run_id]
        run_info["status"] = "running"
        
        steps = plan["steps"]
        completed_task_ids = set()
        
        # Execute steps respecting dependencies
        for step in steps:
            # Check if dependencies are satisfied
            if all(dep_id in completed_task_ids for dep_id in step["depends_on"]):
                run_info["current_step"] = step["task_id"]
                
                # Execute the step
                result = await executor.execute_step(step)
                
                # Update run status
                run_info["completed_steps"].append({
                    "task_id": step["task_id"],
                    "status": result.status.value,
                    "records_processed": result.records_processed,
                    "errors": result.errors
                })
                
                if result.status.value in ["completed", "skipped"]:
                    completed_task_ids.add(step["task_id"])
                else:
                    run_info["errors"].extend(result.errors)
        
        # Mark run as completed
        if len(completed_task_ids) == len(steps):
            run_info["status"] = "completed"
        else:
            run_info["status"] = "failed"
            
        run_info["completed_at"] = datetime.now()
        
    except Exception as e:
        run_info["status"] = "failed"
        run_info["errors"].append(str(e))
        run_info["completed_at"] = datetime.now()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
