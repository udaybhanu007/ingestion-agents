# Ingestion Agents API

A comprehensive document ingestion system with intelligent planning, execution, and deduplication capabilities.

## 🏗️ Architecture Overview

This system implements an API-driven ingestion architecture with the following components:

### Core Components

- **Planner Agent** (`agents/planner-agent.py`): Analyzes documents and creates intelligent ingestion plans
- **Execution Agent** (`agents/execution_agent.py`): Enhanced execution with registry-based tool management
- **Vector Tool** (`agents/tools/vector_tool.py`): Advanced chunking and content deduplication via SHA256 hashing
- **Connectors** (`connector/`): Integration with Box, Confluence, and Azure Blob Storage
- **API Layer** (`api/ingestion_api.py`): RESTful endpoints for async ingestion workflows

### Key Features

- ✅ **Async & Idempotent**: `/ingest` API processes documents asynchronously 
- ✅ **Intelligent Planning**: Document classification determines optimal ingestion strategy
- ✅ **Content Deduplication**: SHA256 hashing in vector tool prevents re-processing unchanged content
- ✅ **Tool Integration**: Supports both vector and graph ingestion workflows
- ✅ **Dependency Management**: Respects step dependencies in execution plans

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start the API Server

```bash
python start_api.py
```

The API will be available at:
- **Base URL**: http://localhost:8000
- **Interactive Docs**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### 3. Test the API

```bash
# Health check
curl http://localhost:8000/health

# Get ingestion plan
curl "http://localhost:8000/plan/box://file/123.pdf"

# Submit document for ingestion
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"doc_uri": "box://file/123.pdf", "priority": 7}'
```

## 📋 API Endpoints

### POST `/ingest`
Submit a document for asynchronous ingestion.

**Request:**
```json
{
  "doc_uri": "box://file/123?v=abc",
  "metadata": {"priority": 7, "department": "research"},
  "priority": 7
}
```

**Response:**
```json
{
  "run_id": "uuid",
  "plan_id": "uuid", 
  "status": "queued",
  "message": "Ingestion queued for box://file/123?v=abc"
}
```

### GET `/plan/{doc_uri}`
Get an ingestion plan without executing it.

**Response:**
```json
{
  "plan_id": "uuid",
  "steps": [
    {
      "task_id": "1",
      "tool": "vector_ingestion",
      "args": {"doc_uri": "box://file/123?v=abc"},
      "depends_on": []
    },
    {
      "task_id": "2", 
      "tool": "graph_ingestion",
      "args": {"doc_uri": "box://file/123?v=abc"},
      "depends_on": ["1"]
    }
  ]
}
```

### GET `/status/{run_id}`
Check the status of an ingestion run.

**Response:**
```json
{
  "run_id": "uuid",
  "status": "completed",
  "progress": {
    "total_steps": 2,
    "completed_steps": 2,
    "progress_percent": 100.0
  },
  "errors": []
}
```

## 🧠 Document Classification

The Planner Agent automatically classifies documents to determine the optimal ingestion strategy:

| File Type | Vector Ingestion | Graph Ingestion | Use Case |
|-----------|------------------|-----------------|----------|
| `.pdf`, `.docx`, `.txt` | ✅ | ❌ | Document search & retrieval |
| `.csv`, `.json`, `.xml` | ✅ | ✅ | Structured data relationships |
| `.jpg`, `.png`, `.mp4` | ✅ | ❌ | Media content indexing |
| Medical/Clinical content | ✅ | ✅ | Domain-specific relationships |

## 🔄 Deduplication Workflow

The system prevents unnecessary re-processing through content hashing:

1. **Fetch Content**: Download document content from source
2. **Compute Hash**: Generate SHA256 hash of content
3. **Check Catalog**: Compare with previously ingested hash
4. **Decision**:
   - **Same hash** → Skip ingestion (mark as "no change")
   - **Different hash** → Re-ingest and update catalog
   - **No previous hash** → First-time ingestion

## 🗂️ Project Structure

```
ingestion-agents/
├── agents/
│   ├── planner-agent.py       # Planning & classification logic
│   ├── execution_agent.py      # Enhanced execution with registry-based tools
│   └── tools/
│       ├── vector_tool.py             # Enhanced vector ingestion with deduplication
│       └── tool_registry.py          # Centralized tool management
├── connector/
│   ├── box.py               # Box cloud storage connector
│   ├── azure.py             # Azure services connector
│   └── confluence_mcp.py    # Advanced Confluence MCP connector with AI capabilities
├── utils/
│   └── common_function.py   # Shared utilities
├── api/
│   └── ingestion_api.py     # FastAPI endpoints
├── config/
│   └── config.json          # Configuration settings
├── tests/
│   └── test_api.py          # API test cases
├── requirements.txt         # Python dependencies
├── start_api.py            # Server startup script
└── README.md               # This file
```

## 🔧 Configuration

### Box Integration Setup

1. **Create Box Application**:
   - Go to [Box Developer Console](https://app.box.com/developers/console)
   - Create a new Custom App with OAuth2 authentication
   - Set redirect URI to: `http://localhost:8080`
   - Note your Client ID and Client Secret

2. **Environment Configuration**:
   ```bash
   # Copy the template
   cp .env.template .env
   
   # Edit .env with your Box credentials
   Box_Client_Id=your_box_client_id_here
   Box_Client_Secret=your_box_client_secret_here
   ```

3. **Install Dependencies**:
   ```bash
   pip install boxsdk python-dotenv
   ```

### General Configuration

Edit `config/config.json` to configure:

- **API Settings**: Host, port, logging
- **Connector Credentials**: Box, Confluence, Azure
- **Database Settings**: Vector DB, Graph DB connections
- **Ingestion Limits**: Timeouts, retry attempts, concurrency

Environment variables can be used with `${VARIABLE_NAME}` syntax.

## 🧪 Testing

Run the test suite:

```bash
# Install test dependencies
pip install pytest pytest-asyncio

# Run tests
python -m pytest tests/ -v

# Run specific test
python -m pytest tests/test_api.py::test_ingest_document -v
```

## 🔌 Integration Examples

### Box Integration

```python
from connector.box import BoxConnector

# Initialize with OAuth2 authentication
box_client = BoxConnector()

# Check authentication status
status = box_client.get_token_status()
print(f"Authenticated: {status['authenticated']}")

# Download files from a folder
result = box_client.fetch_folder_documents("documents-ingest")
print(f"Downloaded {result['summary']['successful_downloads']} files")

# Manual authentication if needed
if not box_client.is_authenticated():
    box_client.clear_tokens_and_reauthenticate()
```

### API Client

```python
import requests

# Submit document for ingestion
response = requests.post("http://localhost:8000/ingest", json={
    "doc_uri": "box://file/123456",
    "metadata": {"priority": 8, "team": "platform"}
})

run_id = response.json()["run_id"]

# Check status
status = requests.get(f"http://localhost:8000/status/{run_id}")
print(status.json())
```

### Batch Processing

```python
documents = [
    "box://file/report1_id",
    "box://file/report2_id", 
    "confluence://space/page1"
]

for doc_uri in documents:
    requests.post("http://localhost:8000/ingest", json={
        "doc_uri": doc_uri,
        "priority": 5
    })
```

## 🚧 Future Enhancements

- [ ] Real connector implementations (Box, Confluence, Azure APIs)
- [ ] Persistent catalog storage (database backend)
- [ ] Webhook notifications for completion events
- [ ] Batch ingestion endpoint for multiple documents
- [ ] Advanced scheduling and retry logic
- [ ] Metrics and monitoring integration
- [ ] Authentication and authorization
- [ ] Rate limiting and quotas

## 📝 Development Notes

This implementation provides:

1. **API Contract Compliance**: Matches the specified request/response formats
2. **Async Processing**: Non-blocking ingestion with background execution  
3. **Intelligent Planning**: Document-type aware tool selection
4. **Content Deduplication**: Hash-based change detection
5. **Extensible Architecture**: Easy to add new connectors and tools

The system is designed to integrate with existing vector and graph ingestion tools while providing a clean API interface for external systems.