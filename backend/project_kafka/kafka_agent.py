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
# Project #2 MCP Tools Definitions (Apache Kafka Stream)
# ============================================================

KAFKA_MCP_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "query_kafka_stream",
        "description": "Unified multi-filter query tool for the Apache Kafka live flight telemetry stream. Supports compound queries combining ground speed, province/region boundaries, airline, flight number, and stream statistics simultaneously.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Specific flight number (e.g. 'TK10'), callsign ('THY10'), or registration ('TC-LJA')"},
                "region": {"type": "string", "description": "Target province or region name. As an intelligent assistant, resolve any colloquial user phrasing (e.g. 'Palandöken', 'Erzurum kenti/şehri', 'Dadaşlar diyarı', 'Boğaz', 'Kordon', 'Başkent') to the official Turkish province name (e.g. 'Erzurum', 'İstanbul', 'İzmir', 'Ankara') or macro-region ('MARMARA', 'EGE', 'TR'). The backend automatically evaluates exact 81-province boundary polygons."},
                "airline": {"type": "string", "description": "Airline code. Map colloquial airline names (e.g. 'Türk Hava Yolları' -> 'THY', 'Pegasus' -> 'PGT', 'AJet' -> 'TKJ' or 'VF', 'Lufthansa' -> 'DLH', 'SunExpress' -> 'SXS')."},
                "min_speed_kmh": {"type": "number", "description": "Minimum ground speed filter in km/h. Map expressions like 'hızlı uçaklar', 'ses hızına yakın', 'süpersonik' to appropriate values (e.g. 800 or 900)."},
                "min_altitude_feet": {"type": "number", "description": "Minimum altitude filter in feet. Convert user metric requests like '10 bin metre üzeri' (~32,800 ft) to feet."},
                "get_stats": {"type": "string", "description": "Pass 'true' to retrieve overall Kafka stream statistics (max/avg speed, altitude, airline count)"},
                "limit": {"type": "integer", "description": "Maximum number of flights to return (default: 15)"}
            }
        }
    }
]

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
        if tool_name == "query_kafka_stream":
            return kafka_store.query_flights(**tool_args)
        else:
            return {"status": "error", "error": f"Tool '{tool_name}' not recognized in Project #2 Kafka Cockpit."}
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def ask_kafka_agent(user_query: str) -> Dict[str, Any]:
    """Processes a natural language query for Project #2 (Kafka Cockpit) using isolated Kafka MCP tools."""
    return await run_llm_cycle(
        user_query=user_query,
        system_instruction=KAFKA_SYSTEM_INSTRUCTION,
        tool_definitions=KAFKA_MCP_DEFINITIONS,
        tool_executor=_execute_kafka_tool,
        project_label="⚡ Apache Kafka Telemetri Akışı (Proje #2)"
    )
