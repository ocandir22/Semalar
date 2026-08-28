"""
Kafka Airspace Telemetry AI Agent — Project #2
Specialized AI Agent for Apache Kafka real-time flight telemetry streams,
supersonic speed filtering, 81 Turkish province polygon containment, and cockpit analytics.
Strictly decoupled from Live FlightRadar24 radar.
"""

import sys
from typing import Dict, Any, List

# Core LLM Engine
try:
    from core.llm_client import run_llm_cycle, get_agent_info
except ImportError:
    from ..core.llm_client import run_llm_cycle, get_agent_info

# Kafka Store
try:
    from .flight_kafka_store import kafka_store
except ImportError:
    from flight_kafka_store import kafka_store

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
# ============================================================
# Project #2 FastMCP Tools Integration (Single Source of Truth)
# ============================================================

def get_kafka_mcp_definitions() -> List[Dict[str, Any]]:
    """Dynamically extracts OpenAI/Gemini compatible function schemas directly from the FastMCP Server.
    Eliminates duplicated tool definitions so kafka_server.py is the Single Source of Truth.
    """
    try:
        try:
            from server import mcp_server
        except ImportError:
            from backend.server import mcp_server

        if mcp_server is not None and hasattr(mcp_server, "_tool_manager"):
            tools = []
            for tool in mcp_server._tool_manager.list_tools():
                schema = tool.parameters.copy() if hasattr(tool, "parameters") else {}
                props = {k: v for k, v in schema.get("properties", {}).items() if k != "kwargs"}
                required = [r for r in schema.get("required", []) if r != "kwargs"]
                tools.append({
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": {
                        "type": "object",
                        "properties": props,
                        "required": required
                    }
                })
            if tools:
                return tools
    except Exception:
        pass
    return []


# Dynamic Single Source of Truth tools definition
KAFKA_MCP_DEFINITIONS: List[Dict[str, Any]] = get_kafka_mcp_definitions()

KAFKA_SYSTEM_INSTRUCTION = (
    "You are an expert AI Aviation Assistant specialized in the Apache Kafka live aircraft telemetry stream (Project #2).\n"
    "To answer questions, you MUST strictly use ONLY the unified KAFKA MCP TOOL: 'query_kafka_stream'.\n\n"
    "💡 HOW TO USE 'query_kafka_stream':\n"
    "• Single flight info / search: pass query='TK10' or registration.\n"
    "• High-speed / supersonic filtering: pass min_speed_kmh=800 or 900.\n"
    "• 81 Turkish Provinces & Regions: You handle natural language entity resolution! If the user says 'Palandöken', 'Erzurum kenti/şehri', 'Dadaşlar diyarı', 'Boğaz', 'Kordon', 'Başkent' or any landmark/district, resolve it to the canonical province name (e.g. region='Erzurum', region='Ankara', region='İstanbul') or region (region='TR', region='MARMARA'). The Python backend calculates the exact polygon boundary with sub-millisecond ray-casting!\n"
    "• Airline colloquialisms: Resolve 'Türk Hava Yolları' -> airline='THY', 'Pegasus' -> airline='PGT', 'AJet' -> airline='TKJ', etc.\n"
    "• Metric to imperial conversion: Convert user metric requests (e.g. '10 bin metre üstü' -> min_altitude_feet=32800).\n"
    "• COMPOUND QUERIES: Combine any parameters! (e.g. Erzurum + min_speed_kmh=800 -> pass region='Erzurum', min_speed_kmh=800).\n"
    "• Kafka stream statistics: pass get_stats=true.\n\n"
    "📌 RULES:\n"
    "• Never fabricate or hallucinate flight telemetry or models. Only use exact data returned from query_kafka_stream.\n"
    "• State altitude in both feet and meters, speed in both knots and km/h.\n"
    "• Highlight aircraft model (e.g. Boeing 777-300ER, Airbus A321neo) and route (e.g. IST ➔ JFK).\n"
    "• Provide concise, clean, bulleted summaries."
)


def _execute_kafka_tool(tool_name: str, tool_args: dict) -> dict:
    """Executes Project #2 tools directly against local Kafka in-memory store with sub-millisecond latency."""
    try:
        if tool_name in ["query_kafka_stream", "kafka_query_stream"]:
            return kafka_store.query_flights(**tool_args)
        else:
            return {"status": "error", "error": f"Tool '{tool_name}' not recognized in Project #2 Kafka Cockpit."}
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def ask_kafka_agent(user_query: str) -> Dict[str, Any]:
    """Processes a natural language query for Project #2 (Kafka Cockpit) using dynamically discovered FastMCP tools."""
    tools = get_kafka_mcp_definitions() or KAFKA_MCP_DEFINITIONS
    return await run_llm_cycle(
        user_query=user_query,
        system_instruction=KAFKA_SYSTEM_INSTRUCTION,
        tool_definitions=tools,
        tool_executor=_execute_kafka_tool,
        project_label="⚡ Apache Kafka Telemetri Akışı (Proje #2)"
    )
