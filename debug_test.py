#!/usr/bin/env python3
"""
Debug test to check imports and basic functionality
"""

import os
import sys

print("🚀 Starting debug test...")
print(f"📍 Current directory: {os.getcwd()}")
print(f"🐍 Python version: {sys.version}")
print(f"📁 Python path: {sys.path[:3]}...")

# Test environment loading
try:
    from dotenv import load_dotenv
    load_dotenv('.env.dev')
    print("✅ Environment loaded successfully")
    
    collection_name = os.getenv('QDRANT_COLLECTION', 'agent_research_doc')
    print(f"📁 Collection name: {collection_name}")
except Exception as e:
    print(f"❌ Environment loading failed: {e}")

# Test agent imports
try:
    from agents.planner_agent import PlannerAgent
    print("✅ PlannerAgent imported successfully")
except Exception as e:
    print(f"❌ PlannerAgent import failed: {e}")
    import traceback
    traceback.print_exc()

try:
    from agents.execution_agent import ExecutionAgent
    print("✅ ExecutionAgent imported successfully")
except Exception as e:
    print(f"❌ ExecutionAgent import failed: {e}")
    import traceback
    traceback.print_exc()

print("🏁 Debug test completed!")
