"""
Kafka Airspace Telemetry AI Agent — Project #2
Specialized AI Agent for Apache Kafka real-time flight telemetry streams,
supersonic speed filtering, 81 Turkish province polygon containment, emergency squawks,
airport terminal traffic, vertical climb/descent rates, international transit corridors, and fleet analytics.
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
    Eliminates duplicated tool definitions so server.py is the Single Source of Truth.
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
    "You are an expert AI Aviation Cockpit Assistant specialized in real-time Apache Kafka flight telemetry streams.\n"
    "You have access to 7 official FastMCP tools to analyze live aircraft in Turkish airspace.\n\n"
    "🛠️ YOUR ACTIVE FASTMCP TOOLS & WHEN TO USE THEM:\n"
    "1. `get_emergency_flights`: Use for emergency squawk codes (7700 general emergency, 7600 lost comms, 7500 hijack) or rapid descent alerts.\n"
    "2. `find_nearby_aircraft`: Use for radius/proximity queries around any city, landmark, airport, or coordinates (e.g. 'Ankara merkezine 50 km mesafedeki uçaklar', 'Kadıköy açıklarındaki uçuşlar').\n"
    "3. `get_airport_traffic`: Use for airport arrivals, departures, or approach terminal traffic for any Turkish airport (IST, SAW, ESB, AYT, ADB, DLM, BJV, TZX, etc.).\n"
    "4. `get_vertical_rate_flights`: Use for climb/descent rates (climbing > +500 fpm, descending < -500 fpm, cruising level flight).\n"
    "5. `get_transit_flights`: Use for international transit overflights crossing Turkish airspace without landing in Turkey (e.g. Europe ➔ Gulf/Asia).\n"
    "6. `get_fleet_aircraft_analytics`: Use for aircraft model distributions (Boeing 737/777/787, Airbus A320/A350), wide vs narrow-body shares, and airline fleet stats.\n"
    "7. `query_kafka_stream`: Use for general multi-filter queries (combining speed, 81-province boundary polygon, airline, flight ID, altitude, and stream stats).\n\n"
    "💡 ENTITY RESOLUTION & CONVERSIONS:\n"
    "• 81 Turkish Provinces & Regions: Resolve landmarks/districts to canonical province names (e.g. 'Palandöken' -> Erzurum, 'Boğaz' -> İstanbul, 'Başkent' -> Ankara, 'Kordon' -> İzmir).\n"
    "• Airlines: Resolve colloquial names (THY/Türk Hava Yolları -> 'THY', Pegasus -> 'PGT', AJet -> 'TKJ', Lufthansa -> 'DLH', Emirates -> 'UAE', Qatar -> 'QTR').\n"
    "• Metric conversions: Convert metric altitude/speed requests (e.g. '10 bin metre üstü' -> min_altitude_feet=32800).\n\n"
    "📌 RESPONSE GUIDELINES:\n"
    "• Never fabricate or hallucinate flight telemetry or models. Only use exact data returned by MCP tools.\n"
    "• State altitude in both feet and meters, speed in both knots and km/h.\n"
    "• Highlight aircraft model (e.g. Boeing 777-300ER, Airbus A321neo) and route (e.g. IST ➔ JFK).\n"
    "• Provide clear, structured, and informative markdown responses in Turkish."
)


def _execute_kafka_tool(tool_name: str, tool_args: dict) -> dict:
    """Executes FastMCP tools directly against local Kafka in-memory store with sub-millisecond latency."""
    try:
        if tool_name in ["query_kafka_stream", "kafka_query_stream"]:
            return kafka_store.query_flights(**tool_args)
        elif tool_name == "get_emergency_flights":
            return kafka_store.find_emergency_flights(**tool_args)
        elif tool_name == "find_nearby_aircraft":
            return kafka_store.find_nearby_aircraft(**tool_args)
        elif tool_name == "get_airport_traffic":
            return kafka_store.get_airport_traffic(**tool_args)
        elif tool_name == "get_vertical_rate_flights":
            return kafka_store.get_vertical_rate_flights(**tool_args)
        elif tool_name == "get_transit_flights":
            return kafka_store.get_transit_flights(**tool_args)
        elif tool_name == "get_fleet_aircraft_analytics":
            return kafka_store.get_fleet_aircraft_analytics(**tool_args)
        else:
            return {"status": "error", "error": f"Tool '{tool_name}' not recognized in FastMCP Server."}
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
