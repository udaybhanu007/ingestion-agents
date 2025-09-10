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
from utils.uri_resolver import URIContentResolver

app = FastAPI(title="Ingestion Agent API", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Initialize planner agent and URI resolver
planner = PlannerAgent()
uri_resolver = URIContentResolver()

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
    status: str
    message: str
    plan: Dict[str, Any]

# Global run tracking
active_runs: Dict[str, Dict[str, Any]] = {}

# Status tracking for runs
class StatusResponse(BaseModel):
    run_id: str
    status: str
    plan: Dict[str, Any]
    errors: List[str]

@app.post("/ingest", response_model=IngestResponse)
async def ingest_document(request: IngestRequest):
    """
    Create an ingestion plan for a document URI.
    
    This endpoint:
    1. Takes a document URI
    2. Calls the planner agent to create an ingestion plan
    3. Saves the plan as JSON (using existing save functionality)
    4. Returns the plan
    """
    try:
        # Generate unique IDs
        run_id = str(uuid.uuid4())
        
        # Create ingestion plan using planner agent
        plan = planner.create_ingestion_plan(
            doc_uri=request.doc_uri,
            metadata={
                "document_source": request.document_source,
                "document_type": request.document_type, 
                "content_type": request.content_type,
                "processing_options": request.processing_options or {}
            }
        )
        
        # Store run information (keeping it simple)
        run_data = {
            "run_id": run_id,
            "plan_id": plan["plan_id"],
            "doc_uri": request.doc_uri,
            "status": "planned",
            "created_at": datetime.now().isoformat(),
            "plan": plan
        }
        
        active_runs[run_id] = run_data
        
        return IngestResponse(
            run_id=run_id,
            plan_id=plan["plan_id"],
            status="planned",
            message=f"Ingestion plan created for {request.doc_uri}",
            plan=plan
        )
        
    except Exception as e:
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
        errors=run_info.get("errors", [])
    )

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/content/display")
def display_document_content(uri: str):
    """
    Fetch and display the actual content of a document from URI using existing connectors.
    Returns the content along with metadata for viewing purposes.
    """
    try:
        # Use the existing URI resolver to fetch content
        content_info = uri_resolver.resolve_uri_to_content(uri)
        
        # Extract content for display
        content = content_info.get("content", "")
        content_type = content_info.get("content_type", "text/plain")
        
        # Handle different content types for display
        display_content = ""
        if isinstance(content, bytes):
            try:
                # Try UTF-8 first
                display_content = content.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    # Try other common encodings
                    display_content = content.decode('latin-1')
                except UnicodeDecodeError:
                    # If all fails, show as base64
                    import base64
                    display_content = f"[Binary content - Base64]: {base64.b64encode(content).decode('ascii')[:500]}..."
        else:
            display_content = str(content)
        
        # Truncate very long content for display
        if len(display_content) > 10000:
            display_content = display_content[:10000] + "\n\n[Content truncated - showing first 10,000 characters]"
        
        return {
            "doc_uri": uri,
            "content_type": content_type,
            "content_size": len(str(content).encode('utf-8')) if content else 0,
            "content": display_content,
            "metadata": content_info.get("metadata", {}),
            "status": "success"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch and display content: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8081)
