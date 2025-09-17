#!/usr/bin/env python3
"""
Test GraphIngestionTool configuration directly
"""

import sys
import os
from dotenv import load_dotenv

# Load environment variables first
load_dotenv('.env.dev')

sys.path.insert(0, '.')
from agents.tools.graph_tool import GraphIngestionTool

print("Testing GraphIngestionTool configuration...")

tool = GraphIngestionTool()
config = tool.config
openai_config = config.get_config('openai')

print(f"Config type: {type(config).__name__}")
print(f"OpenAI config: {openai_config}")

# Check specific values
print(f"Deployment: {openai_config.get('deployment_name')}")
print(f"API Version: {openai_config.get('azure_api_version')}")
print(f"Endpoint: {openai_config.get('azure_endpoint')}")
print(f"API Key: {openai_config.get('azure_api_key', '')[:10]}...")

print("\nTesting LLM call directly...")
try:
    response = tool.llm.invoke("Hello, respond with just 'Config test successful'")
    print(f"✅ LLM Response: {response.content}")
except Exception as e:
    print(f"❌ LLM Error: {e}")