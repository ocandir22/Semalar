import os
import sys
import json
from typing import Optional, List, Dict, Any
from mcp.server import MCPServer
from starlette.responses import HTMLResponse, JSONResponse, FileResponse
from starlette.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
import uvicorn

# Ensure backend directory is in sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(BASE_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from flight_service import (
    get_flight_info as fetch_flight_info,
    search_airline_flights as fetch_airline_flights,
    get_flights_over_region as fetch_flights_over_region,
    get_most_tracked_flights as fetch_most_tracked_flights,
    get_airport_info as fetch_airport_info
)
from flight_agent import ask_flight_agent, get_agent_info

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Create the MCP Server instance
mcp_server = MCPServer(
    name="semalar-flight-mcp",
    description="Streamable HTTP MCP Server providing live FlightRadar24 aircraft telemetry, flight tracking, and airport tools."
)


@mcp_server.tool()
def get_flight_info(query: str) -> Dict[str, Any]:
    """Finds a live flight by flight number (e.g. 'TK10', 'PC2020', 'BA123'), callsign (e.g. 'THY10', 'PGT45K'), or aircraft registration tail (e.g. 'TC-LJA').
    Returns live coordinates, altitude (ft/m), ground speed (kts/kmh), heading, aircraft model (e.g. Boeing 777-3F2(ER)), origin and destination airports, and status.
    
    Args:
        query: Flight number, callsign, or aircraft registration (e.g. 'TK10', 'THY10', 'PC2020')
    """
    return fetch_flight_info(query)


@mcp_server.tool()
def search_airline_flights(airline_code: str, limit: int = 15) -> Dict[str, Any]:
    """Searches live airborne flights currently operated by a specific airline (e.g. 'THY' or 'TK' for Turkish Airlines, 'PGT' or 'PC' for Pegasus, 'DLH' for Lufthansa, 'BAW' for British Airways, 'UAE' for Emirates).
    
    Args:
        airline_code: 3-letter ICAO (e.g. 'THY', 'PGT', 'DLH', 'BAW') or 2-letter IATA (e.g. 'TK', 'PC', 'LH', 'BA') airline code.
        limit: Maximum number of flights to return (default: 15).
    """
    return fetch_airline_flights(airline_code, limit=limit)


@mcp_server.tool()
def get_flights_over_region(latitude: float, longitude: float, radius_km: float = 100.0, limit: int = 15) -> Dict[str, Any]:
    """Finds live flights flying within a given radius (km) around a specific geographic coordinate (latitude, longitude).
    For example: Istanbul (41.0082, 28.9784), Ankara (39.9334, 32.8597), London (51.5074, -0.1278), New York (40.7128, -74.0060).
    
    Args:
        latitude: Latitude in decimal degrees (e.g. 41.0082 for Istanbul)
        longitude: Longitude in decimal degrees (e.g. 28.9784 for Istanbul)
        radius_km: Search radius in kilometers (default: 100 km)
        limit: Maximum number of flights to return (default: 15)
    """
    return fetch_flights_over_region(latitude=latitude, longitude=longitude, radius_km=radius_km, limit=limit)


@mcp_server.tool()
def get_most_tracked_flights(limit: int = 10) -> Dict[str, Any]:
    """Fetches the top live most-tracked flights in the world right now on FlightRadar24, including callsigns, routes, aircraft models, and live tracker counts.
    
    Args:
        limit: Number of top tracked flights to return (default: 10).
    """
    return fetch_most_tracked_flights(limit=limit)


@mcp_server.tool()
def get_airport_info(airport_code: str) -> Dict[str, Any]:
    """Retrieves airport details (name, city, country, coordinates, elevation) for a given 3-letter IATA code (e.g. 'IST', 'SAW', 'ESB', 'LHR', 'JFK') or 4-letter ICAO code (e.g. 'LTFM', 'EGLL', 'KJFK').
    
    Args:
        airport_code: 3-letter IATA or 4-letter ICAO airport code.
    """
    return fetch_airport_info(airport_code)


# Expose Streamable HTTP ASGI app (Starlette) on /mcp endpoint
app = mcp_server.streamable_http_app()

# Enable CORS for Frontend communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# REST API Endpoints for Web Chat UI & Telemetry
# ============================================================

async def api_chat(request):
    """Processes incoming chat messages from the Web UI using AI + live MCP tools."""
    try:
        data = await request.json()
        message = data.get("message", "").strip()
        if not message:
            return JSONResponse({"status": "error", "error": "Boş mesaj gönderilemez."}, status_code=400)
        
        result = await ask_flight_agent(message, mcp_url="http://localhost:8000/mcp")
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({
            "status": "error",
            "error": str(e),
            "answer": f"İstek işlenirken sunucu hatası oluştu: {e}"
        }, status_code=500)


async def api_tracked(request):
    """Returns top live tracked flights from FlightRadar24."""
    try:
        limit = int(request.query_params.get("limit", 10))
        result = fetch_most_tracked_flights(limit=limit)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


async def api_status(request):
    """Returns server and agent status info."""
    agent_info = get_agent_info()
    return JSONResponse({
        "status": "online",
        "service": "Semalar Flight MCP & AI Assistant",
        "provider": agent_info["provider"],
        "model": agent_info["model"],
        "mcp_url": "http://localhost:8000/mcp",
        "tools": [
            {"name": "get_flight_info", "description": "Canlı uçuş ve telemetri arama (TK10, TC-JYA vb.)"},
            {"name": "search_airline_flights", "description": "Havayolu aktif uçuşları (THY, PGT vb.)"},
            {"name": "get_flights_over_region", "description": "Bölgesel radar tarama (enlem, boylam, yarıçap)"},
            {"name": "get_most_tracked_flights", "description": "Dünyada en çok izlenen canlı uçuşlar"},
            {"name": "get_airport_info", "description": "Havalimanı bilgileri ve koordinatları (IST, SAW vb.)"}
        ]
    })


# Frontend Directory Resolution
FRONTEND_DIR = os.path.join(PARENT_DIR, "frontend")


async def serve_index(request):
    """Serves the React frontend index.html."""
    index_file = os.path.join(FRONTEND_DIR, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return HTMLResponse("<h1>Semalar Frontend klasörü bulunamadı.</h1>")


app.add_route("/api/chat", api_chat, methods=["POST"])
app.add_route("/api/tracked", api_tracked, methods=["GET"])
app.add_route("/api/status", api_status, methods=["GET"])
app.add_route("/", serve_index, methods=["GET"])

# Mount frontend directory for static assets
if os.path.exists(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="frontend_static")


if __name__ == "__main__":
    print("=" * 65)
    print("✈️ Semalar — Canlı Uçuş Radarı & AI Havacılık Asistanı")
    print("📡 MCP Endpoint  : http://localhost:8000/mcp")
    print("💬 Web Chat UI    : http://localhost:8000")
    print("⚡ REST API       : http://localhost:8000/api/chat")
    print("=" * 65)
    uvicorn.run(app, host="0.0.0.0", port=8000)
