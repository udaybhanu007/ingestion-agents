"""
Ingestion API Endpoints

FastAPI endpoints for the ingestion system.
"""

import os
import sys
import ssl
import warnings

# Comprehensive SSL fixes for Qdrant cloud (like working test)
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

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import uuid
from datetime import datetime
import asyncio

# Add the current directory to the path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Load environment early like the working test
from dotenv import load_dotenv
load_dotenv('.env.dev')

# Import agents and connectors
from agents.planner_agent import PlannerAgent
from agents.execution_agent import ExecutionAgent

# Import simple logging
from config.simple_logger import get_api_logger, get_logger

app = FastAPI(
    title="Ingestion Agent API", 
    version="1.0.0",
    # Add timeout configuration for long-running operations
    timeout=900,  # 15 minutes total timeout
)

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

# Add timeout middleware for long-running operations
@app.middleware("http")
async def timeout_middleware(request: Request, call_next):
    """Middleware to handle timeouts for long-running operations"""
    if request.url.path == "/ingest":
        # Very long timeout for large file ingestion operations (30 minutes)
        # This accommodates processing of very large files like 100K+ line CSVs
        try:
            response = await asyncio.wait_for(call_next(request), timeout=1800.0)
            return response
        except asyncio.TimeoutError:
            return HTTPException(status_code=504, detail="Request timeout: Operation took longer than 30 minutes")
    else:
        # Normal timeout for other operations (30 seconds)
        try:
            response = await asyncio.wait_for(call_next(request), timeout=30.0)
            return response
        except asyncio.TimeoutError:
            return HTTPException(status_code=504, detail="Request timeout")

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
    
    Note: This operation can take 2-5 minutes for large datasets due to LLM processing.
    """
    # Generate unique run ID
    run_id = str(uuid.uuid4())
    
    # Log API request
    api_logger.info(f"Ingestion request received - run_id: {run_id}, doc_uri: {request.doc_uri}, "
                   f"document_source: {request.document_source}, document_type: {request.document_type}, "
                   f"endpoint: /ingest, note: processing may take 2-5 minutes")
    
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
        
        # Build metadata exactly like the working test to ensure same behavior
        metadata = {
            "document_source": request.document_source or "box",  # Use "box" like working test
            "document_type": request.document_type or "txt",      # Use "txt" like working test
            "content_type": request.content_type or "text/plain",
            "processing_options": request.processing_options or {},
            "test_type": "graph_only"  # Focus on graph ingestion like working test
        }
        
        plan = await agent.create_ingestion_plan_async(
            doc_uri=request.doc_uri,
            metadata=metadata
        )
        
        # Update run status
        active_runs[run_id]["status"] = "executing"
        active_runs[run_id]["plan"] = plan
        
        api_logger.info(f"Plan creation completed, starting execution - run_id: {run_id}, "
                       f"plan_id: {plan.get('plan_id')}, steps_count: {len(plan.get('steps', []))}")
        
        # Get execution agent and execute the plan
        executor = get_execution_agent()
        
        # Execute the plan using the same approach as the working test
        execution_results = await executor.execute_plan_async(plan)
        
        # Determine overall execution status - more detailed checking like the test
        execution_success = True
        total_entities = 0
        total_relationships = 0
        
        for result in execution_results:
            if result.get('status') != 'success':
                execution_success = False
            
            # Extract metrics from graph ingestion results
            if result.get('step_type') == 'graph_ingestion' and result.get('result', {}).get('success'):
                inner_result = result.get('result', {}).get('result', {})
                if isinstance(inner_result, dict):
                    total_entities += inner_result.get('entities_created', 0)
                    total_relationships += inner_result.get('relationships_created', 0)
        
        final_status = "completed" if execution_success else "failed"
        
        # Store run information (preserve start_time from active_runs)
        start_time = active_runs[run_id]["start_time"]
        run_data = {
            "run_id": run_id,
            "plan_id": plan["plan_id"],
            "doc_uri": request.doc_uri,
            "status": final_status,
            "created_at": start_time,
            "completed_at": datetime.now().isoformat(),
            "plan": plan,
            "execution_results": execution_results,
            "errors": [r.get('error') for r in execution_results if r.get('error')],
            "metrics": {
                "entities_created": total_entities,
                "relationships_created": total_relationships
            }
        }
        
        active_runs[run_id] = run_data
        
        # Log completion with metrics
        start_time_dt = datetime.fromisoformat(start_time)
        execution_time_ms = (datetime.now() - start_time_dt).total_seconds() * 1000
        api_logger.info(f"Ingestion request completed - run_id: {run_id}, plan_id: {plan['plan_id']}, "
                       f"status: {final_status}, execution_time_ms: {execution_time_ms}, "
                       f"entities_created: {total_entities}, relationships_created: {total_relationships}")
        
        return IngestResponse(
            run_id=run_id,
            plan_id=plan["plan_id"],
            message=f"Ingestion plan created and executed for {request.doc_uri}. "
                   f"Created {total_entities} entities and {total_relationships} relationships.",
            plan=plan,
            execution_results=execution_results
        )
        
    except Exception as e:
        # Store error information if execution fails
        current_time = datetime.now().isoformat()
        if 'run_id' in locals():
            error_data = {
                "run_id": run_id,
                "plan_id": plan.get("plan_id", "") if 'plan' in locals() else "",
                "doc_uri": request.doc_uri,
                "status": "failed",
                "created_at": active_runs.get(run_id, {}).get("start_time", current_time),
                "completed_at": current_time,
                "plan": plan if 'plan' in locals() else {},
                "execution_results": [],
                "errors": [str(e)],
                "metrics": {
                    "entities_created": 0,
                    "relationships_created": 0
                }
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
    # Increased timeouts to handle long LLM processing (2-5 minutes)
    uvicorn.run(
        app, 
        host="127.0.0.1", 
        port=8020, 
        reload=False,
        timeout_keep_alive=600,  # 10 minutes keep alive
        timeout_graceful_shutdown=30,  # 30 seconds graceful shutdown
        access_log=True,
        log_level="info"
    )
