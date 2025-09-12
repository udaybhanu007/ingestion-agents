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
from agents.execution_agent_v2 import ExecutionAgentV2

app = FastAPI(title="Ingestion Agent API", version="1.0.0")

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

def get_execution_agent() -> ExecutionAgentV2:
    """Get or create execution agent instance (singleton pattern)"""
    global execution_agent
    if execution_agent is None:
        execution_agent = ExecutionAgentV2()
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
    try:
        # Generate unique run ID
        run_id = str(uuid.uuid4())
        
        # Get planner agent (lazy initialization)
        agent = get_planner()
        
        # Create ingestion plan using planner agent (async)
        plan = await agent.create_ingestion_plan_async(
            doc_uri=request.doc_uri,
            metadata={
                "document_source": request.document_source,
                "document_type": request.document_type, 
                "content_type": request.content_type,
                "processing_options": request.processing_options or {}
            }
        )
        
        # Get execution agent and execute the plan
        executor = get_execution_agent()
        
        # Execute the plan using content from plan steps (Approach 2)
        execution_results = await executor.execute_plan_async(plan)
        
        # Store run information
        run_data = {
            "run_id": run_id,
            "plan_id": plan["plan_id"],
            "doc_uri": request.doc_uri,
            "status": "completed",
            "created_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat(),
            "plan": plan,
            "execution_results": execution_results
        }
        
        active_runs[run_id] = run_data
        
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
                "created_at": datetime.now().isoformat(),
                "errors": [str(e)]
            }
            if 'plan' in locals():
                error_data["plan"] = plan
            active_runs[run_id] = error_data
        
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/status/{run_id}", response_model=StatusResponse)
async def get_run_status(run_id: str):
    """Get the status of a specific ingestion run."""
    if run_id not in active_runs:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    
    run_info = active_runs[run_id]
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
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    import uvicorn
    
    # For debugging via Swagger UI, uncomment the line below:
    #uvicorn.run(app, host="127.0.0.1", port=8081, reload=True, log_level="debug")
    
    # For direct function debugging, comment out uvicorn.run above and use below:
    # print("🔧 API module loaded for debugging")
    # print("📋 Available endpoints:")
    # print("  - POST /ingest: Create and execute ingestion plan")
    # print("  - GET /status/{run_id}: Get ingestion run status")
    # print("  - GET /health: Health check")
    # print("\n💡 To run the server, uncomment the uvicorn.run() line above")
    # print("💡 To debug API functions directly, add your test calls below this line")
    
    # Example: Test the API functions directly
    import asyncio
    
    async def test_direct_call():
        """Test the ingestion API function directly for debugging."""
        print("\n🧪 Testing direct API call...")
        try:
            request = IngestRequest(
                doc_uri="box://file/1969320109971",
                document_source="box",
                document_type="text",
                content_type="text/plain"
            )
            
            print(f"📄 Testing with: {request.doc_uri}")
            result = await ingest_document(request)
            
            print(f"✅ Success! Run ID: {result.run_id}")
            print(f"📋 Plan ID: {result.plan_id}")
            print(f"🔄 Execution Results: {len(result.execution_results or [])} steps executed")
            
            # Print execution results summary
            if result.execution_results:
                for i, exec_result in enumerate(result.execution_results, 1):
                    status = exec_result.get('status', 'unknown')
                    tool = exec_result.get('tool', 'unknown')
                    print(f"   Step {i}: {tool} - {status}")
            
            return result
            
        except Exception as e:
            print(f"❌ Error during direct call: {e}")
            return None
    
    # Uncomment the line below to run the direct test
    asyncio.run(test_direct_call())
