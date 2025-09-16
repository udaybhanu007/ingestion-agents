#!/usr/bin/env python3
"""
Debug script to check import issues
"""

print("🔍 Testing imports...")

try:
    from agents.tools.tool_registry import get_tool_registry
    print("✅ tool_registry import successful")
    
    registry = get_tool_registry()
    print("✅ get_tool_registry() successful")
    
    tools = registry.get_all_tools()
    print(f"✅ get_all_tools() successful - {len(tools)} tools available")
    print(f"📋 Available tools: {list(tools.keys())}")
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    import traceback
    traceback.print_exc()
except Exception as e:
    print(f"❌ Runtime error: {e}")
    import traceback
    traceback.print_exc()

print("\n🔍 Testing ExecutionAgent import...")
try:
    from agents.execution_agent import ExecutionAgent
    print("✅ ExecutionAgent import successful")
    
    print("🔧 Creating ExecutionAgent instance...")
    executor = ExecutionAgent()
    print("✅ ExecutionAgent initialization successful")
    
except Exception as e:
    print(f"❌ ExecutionAgent error: {e}")
    import traceback
    traceback.print_exc()
