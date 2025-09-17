#!/usr/bin/env python3
"""
Test Azure OpenAI configuration directly
"""

import os
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI
from pydantic import SecretStr

# Load environment variables
load_dotenv('.env.dev')

print("Testing Azure OpenAI configuration...")
print(f"Endpoint: {os.getenv('AZURE_OPENAI_ENDPOINT')}")
print(f"Deployment: {os.getenv('AZURE_OPENAI_DEPLOYMENT')}")
print(f"API Version: {os.getenv('AZURE_OPENAI_API_VERSION')}")
print(f"API Key: {os.getenv('AZURE_OPENAI_API_KEY', '')[:10]}...")

# Initialize Azure OpenAI client
try:
    llm = AzureChatOpenAI(
        azure_deployment=os.getenv('AZURE_OPENAI_DEPLOYMENT'),
        api_version=os.getenv('AZURE_OPENAI_API_VERSION'),
        azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
        api_key=SecretStr(os.getenv('AZURE_OPENAI_API_KEY', ''))
    )
    
    print("\nClient initialized successfully!")
    
    # Test simple call
    print("Testing simple LLM call...")
    response = llm.invoke("Hello, please respond with just 'Test successful'")
    print(f"Response: {response.content}")
    print("✅ Azure OpenAI connection working!")
    
except Exception as e:
    print(f"❌ Error: {e}")