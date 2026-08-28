"""
Project #1 Live Radar — Standalone ASGI & FastMCP Server (Port 8000)
Serves Live FlightRadar24 ADS-B aircraft tracking, airline fleets, airport lookups,
and the Project #1 UI (index.html).
"""

import os
import sys
import json
import time
from typing import Optional, Dict, Any
from mcp.server import MCPServer
from starlette.applications import Starlette
from starlette.responses import HTMLResponse, JSONResponse
from starlette.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
import uvicorn

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(BASE_DIR)
ROOT_DIR = os.path.dirname(BACKEND_DIR)
for p in [ROOT_DIR, BACKEND_DIR, BASE_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

from project_live.flight_service import (
    get_flight_info,
    search_airline_flights,
    get_flights_over_region,
    get_most_tracked_flights,
    get_airport_info
)
from project_live.live_agent import ask_live_agent
from core.llm_client import get_agent_info
from core.audit_logger import log_mcp_request

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# FastMCP Server for Project #1
mcp_server = MCPServer("Semalar-Live-Radar")


@mcp_server.tool()
def live_flight_info(flight_code: str) -> Dict[str, Any]:
    """Retrieves live ADS-B coordinates, altitude, speed, and aircraft model for a flight number or tail registration."""
    return get_flight_info(query=flight_code)


@mcp_server.tool()
def live_airline_flights(airline_code: str, limit: int = 15) -> Dict[str, Any]:
    """Searches live airborne flights currently operated by a specific airline (e.g. 'THY', 'PGT', 'DLH')."""
    return search_airline_flights(airline_code=airline_code, limit=limit)


@mcp_server.tool()
def live_flights_over_region(region: str = "Turkey", min_speed_kmh: Optional[float] = None, limit: int = 15) -> Dict[str, Any]:
    """Finds live flights within Turkish 81-province boundary polygons or national/regional macro-zones."""
    return get_flights_over_region(region=region, min_speed_kmh=min_speed_kmh, limit=limit)


@mcp_server.tool()
def live_most_tracked_flights(limit: int = 10) -> Dict[str, Any]:
    """Fetches the top live most-tracked flights worldwide on FlightRadar24."""
    return get_most_tracked_flights(limit=limit)


@mcp_server.tool()
def live_airport_info(airport_code: str) -> Dict[str, Any]:
    """Retrieves airport details (name, city, country, coordinates, elevation) for an IATA/ICAO code."""
    return get_airport_info(airport_code=airport_code)


LIVE_MCP_REGISTRY = {
    "get_flight_info": get_flight_info,
    "search_airline_flights": search_airline_flights,
    "get_flights_over_region": get_flights_over_region,
    "get_most_tracked_flights": get_most_tracked_flights,
    "get_airport_info": get_airport_info,
}

# Expose Streamable HTTP ASGI app (Starlette) on /mcp endpoint
app = mcp_server.streamable_http_app()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def api_tools_execute(request):
    """Executes a requested Live MCP tool on the server via HTTP RPC for remote AI Agent clients."""
    try:
        data = await request.json()
        tool_name = data.get("tool_name")
        args = data.get("args", {})

        if tool_name not in LIVE_MCP_REGISTRY:
            return JSONResponse({"status": "error", "error": f"Tool '{tool_name}' not found on this MCP server."}, status_code=404)

        tool_func = LIVE_MCP_REGISTRY[tool_name]

        if tool_name == "get_flight_info":
            target_val = args.get("query") or args.get("flight_code") or args.get("flight_number") or args.get("callsign") or ""
            args = {"query": target_val}
        elif tool_name == "get_airport_info":
            code = args.get("airport_code") or args.get("query") or args.get("code") or ""
            args = {"airport_code": code}
        elif tool_name == "search_airline_flights":
            code = args.get("airline_code") or args.get("airline") or args.get("code") or ""
            limit = int(args.get("limit", 15))
            args = {"airline_code": code, "limit": limit}
        elif tool_name == "get_flights_over_region":
            if "min_speed" in args and "min_speed_kmh" not in args:
                args["min_speed_kmh"] = args.pop("min_speed")

        t_start = time.perf_counter()
        res = tool_func(**args)
        elapsed_ms = (time.perf_counter() - t_start) * 1000
        log_mcp_request(tool_name, args, res, elapsed_ms)
        return JSONResponse(res)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


async def api_chat(request):
    """Processes Project #1 natural language queries via isolated live_agent."""
    try:
        data = await request.json()
        message = data.get("message", "").strip()
        if not message:
            return JSONResponse({"status": "error", "error": "Message cannot be empty."}, status_code=400)
        result = await ask_live_agent(message)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


async def api_tracked(request):
    """Returns top live tracked flights from FlightRadar24."""
    try:
        limit = int(request.query_params.get("limit", 10))
        return JSONResponse(get_most_tracked_flights(limit=limit))
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


async def api_status(request):
    """Returns Project #1 status info."""
    agent_info = get_agent_info()
    return JSONResponse({
        "status": "online",
        "project": "Project #1: FlightRadar24 Live Radar",
        "provider": agent_info["provider"],
        "model": agent_info["model"],
        "mcp_url": "http://localhost:8000/mcp",
        "tools": list(LIVE_MCP_REGISTRY.keys())
    })


FRONTEND_DIR = os.path.join(ROOT_DIR, "frontend")


async def serve_index(request):
    """Serves Project #1 UI (index.html)."""
    index_file = os.path.join(FRONTEND_DIR, "index.html")
    if os.path.exists(index_file):
        with open(index_file, "r", encoding="utf-8") as f:
            content = f.read()
        return HTMLResponse(content)
    return HTMLResponse("<h1>Semalar Project #1 (index.html) not found.</h1>")


app.add_route("/api/tools/execute", api_tools_execute, methods=["POST"])
app.add_route("/api/chat", api_chat, methods=["POST"])
app.add_route("/api/tracked", api_tracked, methods=["GET"])
app.add_route("/api/status", api_status, methods=["GET"])
app.add_route("/", serve_index, methods=["GET"])

if os.path.exists(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="frontend_static")


if __name__ == "__main__":
    port = int(os.getenv("PORT_LIVE", 8000))
    print("=" * 65)
    print("🔴 Semalar — Project #1: Live FlightRadar24 Radar Server")
    print(f"📡 MCP Endpoint : http://localhost:{port}/mcp")
    print(f"🌐 Radar UI     : http://localhost:{port}")
    print("=" * 65)
    uvicorn.run(app, host="0.0.0.0", port=port)
