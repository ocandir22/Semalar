import os
import sys
import json
from typing import Optional, List, Dict, Any
from mcp.server import MCPServer
from starlette.responses import HTMLResponse, JSONResponse, FileResponse
from starlette.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
import uvicorn
import functools
import inspect
import time
import collections
import logging
from datetime import datetime, timezone
import threading
from kafka import KafkaProducer

# Ensure backend directory is in sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(BASE_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Silence noisy kafka connection logs
logging.getLogger("kafka").setLevel(logging.WARNING)

# Kafka Telemetry Services, Store & AI Agent
from project_kafka.flight_kafka_store import kafka_store, query_kafka_stream as fetch_kafka_stream
from project_kafka.flight_producer import FlightKafkaProducer
from project_kafka.kafka_agent import ask_kafka_agent

# Core LLM Engine
from core.llm_client import get_agent_info

# Centralized Kafka Audit Logger (Topic 'mcp-requests')
from core.audit_logger import log_mcp_request, get_recent_audit_logs

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


def audit_tool(name: str):
    """Decorator that intercepts all MCP tool calls and records execution metrics to Kafka."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            result = func(*args, **kwargs)
            elapsed_ms = (time.perf_counter() - start) * 1000

            try:
                sig = inspect.signature(func)
                bound = sig.bind(*args, **kwargs)
                bound.apply_defaults()
                call_args = dict(bound.arguments)
            except Exception:
                call_args = kwargs or ({"arg_0": args[0]} if args else {})

            log_mcp_request(name, call_args, result, elapsed_ms)
            return result
        return wrapper
    return decorator


# Create the FastMCP Server instance
mcp_server = MCPServer(
    name="semalar-kafka-flight-mcp",
    description="Streamable HTTP FastMCP Server providing live Apache Kafka aircraft telemetry stream cache, 81-province polygon filtering, emergency squawks, airport traffic, and AI cockpit tools."
)


# ============================================================
# 1. UNIFIED KAFKA STREAM MCP TOOL
# ============================================================

@mcp_server.tool()
@audit_tool("query_kafka_stream")
def query_kafka_stream(
    query: str = "",
    region: str = "",
    airline: str = "",
    min_speed_kmh: Optional[float] = None,
    min_altitude_feet: Optional[float] = None,
    get_stats: bool = False,
    limit: int = 15,
    **kwargs
) -> Dict[str, Any]:
    """Primary unified multi-filter query tool for live Apache Kafka aircraft telemetry.

    PRIMARY USE CASE FOR PROVINCE AIRSPACES: Use this tool whenever a user asks for aircraft over any of
    the 81 Turkish provinces (e.g., 'Erzurum üzerindeki uçaklar', 'İstanbul semaları', 'Ankara hava sahası').
    It performs EXACT sub-millisecond 81-province boundary polygon ray-casting (Point-in-Polygon) rather than an approximate circular radius.
    Also supports compound queries with flight numbers, callsigns, registration tails, airlines, ground speed, and altitude.

    Args:
        query: Specific flight number (e.g. 'TK10', 'MH21'), callsign ('THY10', 'PGT45K'), or aircraft registration tail ('TC-LJA').
        region: Target Turkish province name (e.g. 'Erzurum', 'İstanbul', 'Ankara') or macro-region ('MARMARA', 'EGE', 'TR'). Evaluates exact official 81-province boundary polygons via ray-casting.
        airline: 3-letter ICAO (e.g. 'THY', 'PGT', 'DLH') or 2-letter IATA ('TK', 'PC', 'LH') airline code.
        min_speed_kmh: Minimum ground speed filter in km/h (e.g. 800, 900).
        min_altitude_feet: Minimum altitude filter in feet (e.g. 30000).
        get_stats: Set to True to retrieve overall Kafka stream statistical summary.
        limit: Maximum number of flight records to return (default: 15).

    Returns:
        Dict[str, Any]: Structured JSON containing matched aircraft with telemetry, exact polygon match metadata, speed, altitude, and route.
    """
    return kafka_store.query_flights(
        query=query,
        region=region or kwargs.get("country", ""),
        airline=airline,
        min_speed_kmh=min_speed_kmh,
        min_altitude_feet=min_altitude_feet,
        get_stats=get_stats,
        limit=limit
    )


# ============================================================
# 🚨 2. EMERGENCY & SQUAWK ALERTS MCP TOOL
# ============================================================

@mcp_server.tool()
@audit_tool("get_emergency_flights")
def get_emergency_flights(
    emergency_type: str = "ALL",
    include_rapid_descent: bool = True,
    limit: int = 15
) -> Dict[str, Any]:
    """Detects aircraft broadcasting emergency squawk codes (7700, 7600, 7500) or experiencing severe emergency descent in Turkish airspace.

    Args:
        emergency_type: Emergency squawk code filter ('7700' for General Emergency, '7600' for Lost Radio Comms, '7500' for Hijacking, or 'ALL').
        include_rapid_descent: Whether to flag aircraft descending faster than -3000 feet/min.
        limit: Maximum number of emergency flight records to return.

    Returns:
        Dict[str, Any]: Emergency status, detected alert flights, squawk descriptions, and alert level.
    """
    return kafka_store.find_emergency_flights(
        emergency_type=emergency_type,
        include_rapid_descent=include_rapid_descent,
        limit=limit
    )


# ============================================================
# 📍 3. NEARBY AIRCRAFT RADIUS MCP TOOL
# ============================================================

@mcp_server.tool()
@audit_tool("find_nearby_aircraft")
def find_nearby_aircraft(
    location: str = "",
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
    radius_km: float = 50.0,
    min_altitude_feet: Optional[float] = None,
    limit: int = 15
) -> Dict[str, Any]:
    """Finds aircraft within a radial distance circle (radius in km) around a specific airport or coordinate point.

    NOTE: For official province/city airspace queries (e.g. 'Erzurum üzerindeki uçaklar'), do NOT use this radial tool;
    use 'query_kafka_stream(region=...)' instead to perform exact 81-province boundary polygon ray-casting.
    Use this tool ONLY when the user explicitly requests a proximity radius (e.g. '50 km çevresi', 'Esenboğa etrafındaki 30 km').

    Args:
        location: Airport code (e.g. 'IST', 'ESB', 'AYT', 'LTAC') or landmark name with radius.
        latitude: Center latitude coordinate.
        longitude: Center longitude coordinate.
        radius_km: Search circle radius in kilometers (default: 50.0 km).
        min_altitude_feet: Optional minimum altitude filter.
        limit: Maximum number of nearby flight records to return.

    Returns:
        Dict[str, Any]: List of aircraft sorted by ascending radial distance in kilometers from the center point.
    """
    return kafka_store.find_nearby_aircraft(
        location=location,
        latitude=latitude,
        longitude=longitude,
        radius_km=radius_km,
        min_altitude_feet=min_altitude_feet,
        limit=limit
    )


# ============================================================
# 🛫 4. AIRPORT TRAFFIC & APPROACH MCP TOOL
# ============================================================

@mcp_server.tool()
@audit_tool("get_airport_traffic")
def get_airport_traffic(
    airport_code: str,
    traffic_type: str = "ALL",
    airline: str = "",
    limit: int = 15
) -> Dict[str, Any]:
    """Retrieves live arriving (inbound/approach), departing (outbound), or nearby terminal traffic for any Turkish airport (IST, SAW, ESB, AYT, ADB, etc.).

    Args:
        airport_code: 3-letter IATA code (e.g. 'IST', 'SAW', 'ESB', 'AYT', 'ADB', 'DLM', 'BJV', 'TZX') or 4-letter ICAO code.
        traffic_type: Filter traffic type ('ARRIVALS', 'DEPARTURES', or 'ALL').
        airline: Optional airline filter (e.g. 'THY', 'PGT', 'TKJ').
        limit: Maximum number of flights to return.

    Returns:
        Dict[str, Any]: Airport metadata, arrival/departure counts, and detailed flight schedules in terminal area.
    """
    return kafka_store.get_airport_traffic(
        airport_code=airport_code,
        traffic_type=traffic_type,
        airline=airline,
        limit=limit
    )


# ============================================================
# 📈 5. VERTICAL TELEMETRY & CLIMB/DESCENT MCP TOOL
# ============================================================

@mcp_server.tool()
@audit_tool("get_vertical_rate_flights")
def get_vertical_rate_flights(
    flight_phase: str = "ALL",
    min_vertical_speed_fpm: Optional[float] = None,
    region: str = "",
    airline: str = "",
    limit: int = 15
) -> Dict[str, Any]:
    """Filters aircraft by vertical climb or descent speed rates and flight phases (climbing, descending, cruising).

    Args:
        flight_phase: Target flight phase ('CLIMBING' for > +500 fpm, 'DESCENDING' for < -500 fpm, 'CRUISING' for level flight, or 'ALL').
        min_vertical_speed_fpm: Minimum vertical speed threshold in feet per minute (e.g. 1500 for steep climbs/descents).
        region: Optional Turkish province or region filter.
        airline: Optional airline filter.
        limit: Maximum number of flights to return.

    Returns:
        Dict[str, Any]: List of aircraft with vertical speed (fpm and m/s), flight phase, and altitude profiles.
    """
    return kafka_store.get_vertical_rate_flights(
        flight_phase=flight_phase,
        min_vertical_speed_fpm=min_vertical_speed_fpm,
        region=region,
        airline=airline,
        limit=limit
    )


# ============================================================
# 🌍 6. TRANSIT OVERFLIGHT CORRIDOR MCP TOOL
# ============================================================

@mcp_server.tool()
@audit_tool("get_transit_flights")
def get_transit_flights(
    min_altitude_feet: Optional[float] = 28000.0,
    airline: str = "",
    limit: int = 15
) -> Dict[str, Any]:
    """Identifies international transit overflights cruising through Turkish airspace without landing or departing in Turkey.

    Args:
        min_altitude_feet: Minimum cruising altitude in feet (default: 28,000 ft).
        airline: Optional airline filter (e.g. 'UAE', 'QTR', 'BAW', 'DLH').
        limit: Maximum number of transit flights to return.

    Returns:
        Dict[str, Any]: International overflight records with origin, destination, corridor info, and cruising telemetry.
    """
    return kafka_store.get_transit_flights(
        min_altitude_feet=min_altitude_feet,
        airline=airline,
        limit=limit
    )


# ============================================================
# 📊 7. FLEET & AIRCRAFT MODEL ANALYTICS MCP TOOL
# ============================================================

@mcp_server.tool()
@audit_tool("get_fleet_aircraft_analytics")
def get_fleet_aircraft_analytics(
    aircraft_family: str = "",
    airline: str = "",
    include_breakdown: bool = True
) -> Dict[str, Any]:
    """Calculates active fleet statistics, aircraft model distributions (Boeing 737/777/787, Airbus A320/A350), and wide-body vs narrow-body shares.

    Args:
        aircraft_family: Model family code filter (e.g. 'B77W', 'A359', 'B738', 'A21N').
        airline: Specific airline filter (e.g. 'THY', 'PGT').
        include_breakdown: Whether to return full percentage breakdown.

    Returns:
        Dict[str, Any]: Top aircraft models, wide-body vs narrow-body counts, and active airline fleet breakdown in Turkish airspace.
    """
    return kafka_store.get_fleet_aircraft_analytics(
        aircraft_family=aircraft_family,
        airline=airline,
        include_breakdown=include_breakdown
    )


# Registry of all FastMCP Tools available on this Server
MCP_TOOLS_REGISTRY = {
    "query_kafka_stream": query_kafka_stream,
    "kafka_query_stream": query_kafka_stream,
    "get_emergency_flights": get_emergency_flights,
    "find_nearby_aircraft": find_nearby_aircraft,
    "get_airport_traffic": get_airport_traffic,
    "get_vertical_rate_flights": get_vertical_rate_flights,
    "get_transit_flights": get_transit_flights,
    "get_fleet_aircraft_analytics": get_fleet_aircraft_analytics
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
    """Executes a requested MCP tool on the server via HTTP RPC for remote AI Agent clients."""
    try:
        data = await request.json()
        tool_name = data.get("tool_name")
        args = data.get("args", {})

        if tool_name not in MCP_TOOLS_REGISTRY:
            return JSONResponse({"status": "error", "error": f"Tool '{tool_name}' not found on this MCP server."}, status_code=404)

        tool_func = MCP_TOOLS_REGISTRY[tool_name]

        # Handle parameter aliases safely
        if "flight_code" in args and "query" not in args:
            args["query"] = args.pop("flight_code")
        if "airline_code" in args and "airline" not in args:
            args["airline"] = args.pop("airline_code")
        if "min_speed" in args and "min_speed_kmh" not in args:
            args["min_speed_kmh"] = args.pop("min_speed")
        if "speed" in args and "min_speed_kmh" not in args:
            args["min_speed_kmh"] = args.pop("speed")
        if "airport" in args and "airport_code" not in args:
            args["airport_code"] = args.pop("airport")
        if "city" in args and "location" not in args:
            args["location"] = args.pop("city")

        res = tool_func(**args)
        return JSONResponse(res)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


# ============================================================
# REST API Endpoints for Web Chat UI & Telemetry
# ============================================================

async def api_chat(request):
    """Processes user natural language queries via dedicated AI flight agent."""
    try:
        data = await request.json()
        message = data.get("message", "").strip()
        if not message:
            return JSONResponse({"status": "error", "error": "Message cannot be empty."}, status_code=400)

        result = await ask_kafka_agent(message)
        return JSONResponse(result)

    except Exception as e:
        return JSONResponse({
            "status": "error",
            "error": str(e),
            "answer": f"Server error occurred while processing request: {e}"
        }, status_code=500)


async def api_status(request):
    """Returns server and agent status info with full list of active FastMCP tools."""
    agent_info = get_agent_info()
    return JSONResponse({
        "status": "online",
        "service": "Semalar Kafka Flight FastMCP & AI Cockpit",
        "provider": agent_info["provider"],
        "model": agent_info["model"],
        "mcp_url": "http://localhost:8000/mcp",
        "total_tools": len(MCP_TOOLS_REGISTRY),
        "tools": [
            {
                "name": "query_kafka_stream",
                "description": "Unified multi-filter query for Kafka live telemetry (81 provinces, speed, altitude, airline, flight ID, stats)"
            },
            {
                "name": "get_emergency_flights",
                "description": "Detects squawk 7700/7600/7500 emergencies and rapid descent anomalies"
            },
            {
                "name": "find_nearby_aircraft",
                "description": "Radius-based nearest aircraft search around any city, airport, or coordinates"
            },
            {
                "name": "get_airport_traffic",
                "description": "Live inbound/approach, outbound departure, and terminal traffic for Turkish airports"
            },
            {
                "name": "get_vertical_rate_flights",
                "description": "Vertical speed telemetry analysis (climbing, descending, cruising levels)"
            },
            {
                "name": "get_transit_flights",
                "description": "International transit overflights cruising through Turkish airspace"
            },
            {
                "name": "get_fleet_aircraft_analytics",
                "description": "Active aircraft model breakdown, wide vs narrow-body shares, and airline fleet distribution"
            }
        ]
    })


async def api_kafka_stats(request):
    """Returns real-time Kafka flight stream stats."""
    try:
        stats = kafka_store.get_telemetry_stats()
        return JSONResponse(stats)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


async def api_kafka_flights(request):
    """Returns filtered flights from Kafka memory store."""
    try:
        min_speed = request.query_params.get("min_speed")
        min_speed_kmh = float(min_speed) if min_speed else None
        airline = request.query_params.get("airline", "").strip().upper()
        search_query = request.query_params.get("query", "").strip().upper()
        region = request.query_params.get("region", "").strip()
        limit = int(request.query_params.get("limit", 60))

        result = kafka_store.query_flights(
            query=search_query,
            region=region,
            airline=airline,
            min_speed_kmh=min_speed_kmh,
            limit=limit
        )
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


async def api_kafka_fastest(request):
    """Returns top fastest flights above specified speed."""
    try:
        min_speed = float(request.query_params.get("min_speed", 800))
        limit = int(request.query_params.get("limit", 20))
        result = kafka_store.query_flights(min_speed_kmh=min_speed, limit=limit)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


async def api_kafka_logs(request):
    """Returns recent MCP tool audit logs in real-time (backed by Kafka 'mcp-requests')."""
    logs_list = get_recent_audit_logs()
    return JSONResponse({
        "status": "success",
        "total_logs": len(logs_list),
        "returned_count": len(logs_list),
        "logs": logs_list
    })


async def api_kafka_sync(request):
    """Syncs in-memory store with Kafka topic."""
    try:
        count = kafka_store.sync_from_kafka()
        return JSONResponse({
            "status": "success",
            "message": f"Kafka stream synchronized. Total {count} flights in memory.",
            "total_cached_flights": count
        })
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


_streamer_stop_event = threading.Event()

def _turkey_streamer_worker():
    """Background daemon worker continuously ingesting ALL Turkey flights from FlightRadar24 into Kafka topic 'live-flights'."""
    print("🇹🇷 [KAFKA STREAMER] Real-time Turkey Airspace ➔ Kafka streaming daemon active (interval: 15s, no flight limits).")
    try:
        producer = FlightKafkaProducer()
        producer.stream_turkey_flights(
            interval_seconds=15,
            topic="live-flights",
            callback=kafka_store.refresh_turkey_telemetry,
            stop_event=_streamer_stop_event
        )
    except Exception as e:
        print(f"⚠️ [KAFKA STREAMER] Background streamer thread ended: {e}")

_streamer_thread = None

def start_streamer_if_needed():
    """Starts the real-time Turkey flight ingestion streamer thread if not already running."""
    global _streamer_thread
    if _streamer_thread is None or not _streamer_thread.is_alive():
        _streamer_thread = threading.Thread(target=_turkey_streamer_worker, daemon=True, name="TurkeyKafkaStreamer")
        _streamer_thread.start()


async def api_kafka_produce_fresh(request):
    """Fetches ALL fresh flights across Turkey from FlightRadar24 and publishes to Kafka."""
    try:
        producer = FlightKafkaProducer()
        report = producer.publish_turkey_flights(topic="live-flights")
        producer.close()
        kafka_store.sync_from_kafka()
        return JSONResponse(report)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


# Frontend Directory Resolution
FRONTEND_DIR = os.path.join(PARENT_DIR, "frontend")


async def serve_kafka(request):
    """Serves Semalar: Apache Kafka Telemetry & Speed Dashboard (kafka.html)."""
    kafka_file = os.path.join(FRONTEND_DIR, "kafka.html")
    if os.path.exists(kafka_file):
        with open(kafka_file, "r", encoding="utf-8") as f:
            content = f.read()
        return HTMLResponse(
            content,
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
    return HTMLResponse("<h1>Semalar (kafka.html) not found.</h1>")


app.add_route("/api/tools/execute", api_tools_execute, methods=["POST"])
app.add_route("/api/chat", api_chat, methods=["POST"])
app.add_route("/api/status", api_status, methods=["GET"])
app.add_route("/api/kafka/stats", api_kafka_stats, methods=["GET"])
app.add_route("/api/kafka/flights", api_kafka_flights, methods=["GET"])
app.add_route("/api/kafka/fastest", api_kafka_fastest, methods=["GET"])
app.add_route("/api/kafka/logs", api_kafka_logs, methods=["GET"])
app.add_route("/api/kafka/sync", api_kafka_sync, methods=["POST"])
app.add_route("/api/kafka/produce", api_kafka_produce_fresh, methods=["POST"])
app.add_route("/", serve_kafka, methods=["GET"])
app.add_route("/kafka", serve_kafka, methods=["GET"])

# Mount frontend directory for static assets
if os.path.exists(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="frontend_static")


if __name__ == "__main__":
    print("=" * 65)
    print("✈️ Semalar — Apache Kafka Live Flight Telemetry & AI Cockpit")
    print("📡 FastMCP Streamable HTTP   : http://localhost:8000/mcp")
    print("⚡ Kafka Telemetry Cockpit UI: http://localhost:8000")
    print(f"🛠️ Active FastMCP Tools ({len(MCP_TOOLS_REGISTRY)}):")
    for t_name in MCP_TOOLS_REGISTRY:
        if t_name != "kafka_query_stream":
            print(f"   • {t_name}")
    print("🇹🇷 Real-time Ingestion       : ALL Live Turkey Airspace Flights ➔ Kafka (15s Loop)")
    print("📊 Apache Kafka UI Panel     : http://localhost:8080")
    print("=" * 65)
    print("👉 Open in Browser : http://localhost:8000")
    print("=" * 65)
    start_streamer_if_needed()
    uvicorn.run(app, host="0.0.0.0", port=8000)
