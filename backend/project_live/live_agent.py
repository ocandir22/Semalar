"""
Live Radar AI Agent — Project #1
Specialized AI Agent for Real-time FlightRadar24 live flight tracking,
airspace radar, airline operations, and airport information.
Strictly decoupled from Kafka telemetry.
"""

import sys
from typing import Dict, Any, List

# Core LLM Engine
try:
    from core.llm_client import run_llm_cycle, get_agent_info
except ImportError:
    from ..core.llm_client import run_llm_cycle, get_agent_info

# Live Services
try:
    from .flight_service import (
        get_flight_info,
        search_airline_flights,
        get_flights_over_region,
        get_most_tracked_flights,
        get_airport_info
    )
except ImportError:
    from flight_service import (
        get_flight_info,
        search_airline_flights,
        get_flights_over_region,
        get_most_tracked_flights,
        get_airport_info
    )

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


# ============================================================
# Project #1 MCP Tools Definitions (Live FlightRadar24)
# ============================================================

LIVE_MCP_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "get_flight_info",
        "description": "Retrieves real-time live ADS-B coordinates, altitude, ground speed, route, and aircraft model for a flight number (e.g. 'TK10', 'PC2020', 'DLH400') or tail registration (e.g. 'TC-JYA') from FlightRadar24.",
        "parameters": {
            "type": "object",
            "properties": {
                "flight_code": {"type": "string", "description": "Flight number or aircraft tail registration"}
            },
            "required": ["flight_code"]
        }
    },
    {
        "name": "search_airline_flights",
        "description": "Lists live airborne flights currently operated by a specific airline (e.g. 'THY', 'PGT', 'DLH', 'BAW') on FlightRadar24.",
        "parameters": {
            "type": "object",
            "properties": {
                "airline_code": {"type": "string", "description": "Airline ICAO or IATA code (e.g. 'THY', 'TK', 'PGT', 'PC')"},
                "limit": {"type": "integer", "description": "Maximum number of flights to return (default: 10)"}
            },
            "required": ["airline_code"]
        }
    },
    {
        "name": "get_flights_over_region",
        "description": "Finds live flights flying within Turkish national airspace, 81 Turkish province polygons, or macro-regions on FlightRadar24. Supports speed filtering.",
        "parameters": {
            "type": "object",
            "properties": {
                "region": {"type": "string", "description": "Target province or region name. Resolve colloquial user phrasing (e.g. 'Palandöken', 'Erzurum kenti', 'Boğaz', 'Başkent') to the official province name (e.g. 'Erzurum', 'İstanbul', 'Ankara') or macro-region ('MARMARA', 'EGE', 'TR'). The backend automatically evaluates exact 81-province boundary polygons."},
                "min_speed_kmh": {"type": "number", "description": "Minimum ground speed filter in km/h. Map expressions like 'hızlı uçaklar', 'ses hızına yakın' to appropriate values (e.g. 800 or 900)."},
                "limit": {"type": "integer", "description": "Maximum number of flights to return (default: 15)"}
            }
        }
    },
    {
        "name": "get_most_tracked_flights",
        "description": "Fetches the top live most-tracked flights in the world right now on FlightRadar24.",
        "parameters": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Number of top tracked flights to return (default: 10)"}
            }
        }
    },
    {
        "name": "get_airport_info",
        "description": "Retrieves airport coordinates, city, country, elevation, and details for a given 3-letter IATA or 4-letter ICAO airport code (e.g. 'IST', 'SAW', 'LHR', 'JFK').",
        "parameters": {
            "type": "object",
            "properties": {
                "airport_code": {"type": "string", "description": "Airport IATA or ICAO code (e.g. 'IST', 'SAW', 'LHR')"}
            },
            "required": ["airport_code"]
        }
    }
]

LIVE_SYSTEM_INSTRUCTION = (
    "You are an expert AI Aviation Assistant specialized in live FlightRadar24 ADS-B aircraft radar tracking (Project #1).\n"
    "To answer questions, you MUST strictly use ONLY the provided LIVE FLIGHTRADAR24 MCP TOOLS:\n\n"
    "🎯 TOOL USAGE:\n"
    "1. Flight Info / Location / Altitude / Speed: Call 'get_flight_info' (e.g. 'TK10', 'TC-JYA', 'LH400').\n"
    "2. Airline Active Airborne Flights: Call 'search_airline_flights' (resolve 'Türk Hava Yolları' -> 'THY', 'Pegasus' -> 'PGT', 'Lufthansa' -> 'DLH', etc.).\n"
    "3. Regional Airspace Radar: Call 'get_flights_over_region'.\n"
    "   • For 81 Turkish Provinces & Regions: Resolve user terms ('Palandöken', 'Erzurum kenti', 'Başkent', 'Boğaz', etc.) to the canonical province name (e.g. region='Erzurum', region='Ankara', region='TR'). The backend automatically evaluates exact GeoJSON polygon borders!\n"
    "4. Top Most-Tracked Flights: Call 'get_most_tracked_flights'.\n"
    "5. Airport Details: Call 'get_airport_info'.\n\n"
    "📌 RULES:\n"
    "• Never fabricate or hallucinate flight telemetry or models. Only use exact values returned from the live FlightRadar tools.\n"
    "• State altitude in both feet and meters, speed in both knots and km/h.\n"
    "• Highlight aircraft model (e.g. Boeing 777-300ER, Airbus A321neo) and route.\n"
    "• Provide concise, clean, bulleted summaries."
)


def _execute_live_tool(tool_name: str, tool_args: dict) -> dict:
    """Executes Project #1 tools directly against local FlightRadar24 services with zero network latency."""
    try:
        if tool_name == "get_flight_info":
            q = tool_args.get("flight_code") or tool_args.get("query") or ""
            return get_flight_info(query=q)
        elif tool_name == "search_airline_flights":
            code = tool_args.get("airline_code") or tool_args.get("airline") or ""
            limit = int(tool_args.get("limit", 10))
            return search_airline_flights(airline_code=code, limit=limit)
        elif tool_name == "get_flights_over_region":
            reg = tool_args.get("region", "Turkey")
            spd = tool_args.get("min_speed_kmh")
            limit = int(tool_args.get("limit", 15))
            return get_flights_over_region(region=reg, min_speed_kmh=spd, limit=limit)
        elif tool_name == "get_most_tracked_flights":
            limit = int(tool_args.get("limit", 10))
            return get_most_tracked_flights(limit=limit)
        elif tool_name == "get_airport_info":
            code = tool_args.get("airport_code") or tool_args.get("code") or ""
            return get_airport_info(airport_code=code)
        else:
            return {"status": "error", "error": f"Tool '{tool_name}' not recognized in Project #1 Live Radar."}
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def ask_live_agent(user_query: str) -> Dict[str, Any]:
    """Processes a natural language query for Project #1 (Live Radar) using isolated Live MCP tools."""
    return await run_llm_cycle(
        user_query=user_query,
        system_instruction=LIVE_SYSTEM_INSTRUCTION,
        tool_definitions=LIVE_MCP_DEFINITIONS,
        tool_executor=_execute_live_tool,
        project_label="🔴 Canlı FlightRadar24 Radarı (Proje #1)"
    )
