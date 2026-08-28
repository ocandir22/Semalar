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

import functools
import inspect
import time
import collections
import logging
from datetime import datetime, timezone
import threading
from kafka import KafkaProducer

# Silence noisy kafka connection logs
logging.getLogger("kafka").setLevel(logging.WARNING)

# Project #1: Live Radar Services & Agent
from project_live.flight_service import (
    get_flight_info as fetch_flight_info,
    search_airline_flights as fetch_airline_flights,
    get_flights_over_region as fetch_flights_over_region,
    get_most_tracked_flights as fetch_most_tracked_flights,
    get_airport_info as fetch_airport_info
)
from project_live.live_agent import ask_live_agent

# Project #2: Kafka Telemetry Services & Agent
from project_kafka.flight_kafka_store import kafka_store, query_kafka_stream as fetch_kafka_stream
from project_kafka.flight_producer import FlightKafkaProducer
from project_kafka.kafka_agent import ask_kafka_agent

# Core LLM Engine
from core.llm_client import get_agent_info

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Centralized Kafka Audit Logger (Topic 'mcp-requests')
from core.audit_logger import log_mcp_request, get_recent_audit_logs


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


# Create the MCP Server instance
mcp_server = MCPServer(
    name="semalar-flight-mcp",
    description="Streamable HTTP MCP Server providing live FlightRadar24 aircraft telemetry, Apache Kafka stream cache, flight tracking, and airport tools."
)


@mcp_server.tool()
@audit_tool("get_flight_info")
def get_flight_info(query: str = "", flight_code: str = "") -> Dict[str, Any]:
    """Finds a live flight by flight number (e.g. 'TK10', 'PC2020', 'BA123'), callsign (e.g. 'THY10', 'PGT45K'), or aircraft registration tail (e.g. 'TC-LJA').
    Returns live coordinates, altitude (ft/m), ground speed (kts/kmh), heading, aircraft model (e.g. Boeing 777-3F2(ER)), origin and destination airports, and status.
    
    Args:
        query: Flight number, callsign, or aircraft registration (e.g. 'TK10', 'THY10', 'PC2020')
        flight_code: Alternative parameter alias for query
    """
    target = query or flight_code or ""
    return fetch_flight_info(target)


@mcp_server.tool()
@audit_tool("search_airline_flights")
def search_airline_flights(airline_code: str, limit: int = 15) -> Dict[str, Any]:
    """Searches live airborne flights currently operated by a specific airline (e.g. 'THY' or 'TK' for Turkish Airlines, 'PGT' or 'PC' for Pegasus, 'DLH' for Lufthansa, 'BAW' for British Airways, 'UAE' for Emirates).
    
    Args:
        airline_code: 3-letter ICAO (e.g. 'THY', 'PGT', 'DLH', 'BAW') or 2-letter IATA (e.g. 'TK', 'PC', 'LH', 'BA') airline code.
        limit: Maximum number of flights to return (default: 15).
    """
    return fetch_airline_flights(airline_code, limit=limit)


@mcp_server.tool()
@audit_tool("get_flights_over_region")
def get_flights_over_region(
    region: str = "Turkey",
    min_speed_kmh: Optional[float] = None,
    limit: int = 15,
    **kwargs
) -> Dict[str, Any]:
    """Finds live flights within official 81-province boundaries or national/regional macro-zones (e.g. 'TR' / 'Turkey', 'Erzurum', 'Istanbul'). Supports speed filtering."""
    return fetch_flights_over_region(
        region=region or kwargs.get("country", "Turkey"),
        min_speed_kmh=min_speed_kmh,
        limit=limit
    )


@mcp_server.tool()
@audit_tool("get_most_tracked_flights")
def get_most_tracked_flights(limit: int = 10) -> Dict[str, Any]:
    """Fetches the top live most-tracked flights in the world right now on FlightRadar24, including callsigns, routes, aircraft models, and live tracker counts.
    
    Args:
        limit: Number of top tracked flights to return (default: 10).
    """
    return fetch_most_tracked_flights(limit=limit)


@mcp_server.tool()
@audit_tool("get_airport_info")
def get_airport_info(airport_code: str) -> Dict[str, Any]:
    """Retrieves airport details (name, city, country, coordinates, elevation) for a given 3-letter IATA code (e.g. 'IST', 'SAW', 'ESB', 'LHR', 'JFK') or 4-letter ICAO code (e.g. 'LTFM', 'EGLL', 'KJFK').
    
    Args:
        airport_code: 3-letter IATA or 4-letter ICAO airport code.
    """
    return fetch_airport_info(airport_code)


# ============================================================
# UNIFIED KAFKA STREAM MCP TOOL
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
    """Unified multi-filter query tool for the Apache Kafka live flight telemetry stream.
    Supports compound queries combining ground speed, province/region boundaries, airline, flight number, and stream statistics simultaneously.
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


# Registry of all MCP Tools available on this Server
MCP_TOOLS_REGISTRY = {
    # 1. Project Live Tools (FlightRadar24)
    "get_flight_info": get_flight_info,
    "search_airline_flights": search_airline_flights,
    "get_flights_over_region": get_flights_over_region,
    "get_most_tracked_flights": get_most_tracked_flights,
    "get_airport_info": get_airport_info,
    # 2. Project Unified Kafka Tool
    "query_kafka_stream": query_kafka_stream,
}


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
        if tool_name == "query_kafka_stream":
            if "flight_code" in args and "query" not in args:
                args["query"] = args.pop("flight_code")
            if "airline_code" in args and "airline" not in args:
                args["airline"] = args.pop("airline_code")
            if "min_speed" in args and "min_speed_kmh" not in args:
                args["min_speed_kmh"] = args.pop("min_speed")
            if "speed" in args and "min_speed_kmh" not in args:
                args["min_speed_kmh"] = args.pop("speed")
        elif tool_name == "get_flights_over_region":
            if "min_speed" in args and "min_speed_kmh" not in args:
                args["min_speed_kmh"] = args.pop("min_speed")
            if "speed" in args and "min_speed_kmh" not in args:
                args["min_speed_kmh"] = args.pop("speed")
        elif tool_name == "get_flight_info":
            target_val = args.get("query") or args.get("flight_code") or args.get("flight_number") or args.get("callsign") or ""
            args = {"query": target_val}
        elif tool_name == "get_airport_info":
            code = args.get("airport_code") or args.get("query") or args.get("code") or ""
            args = {"airport_code": code}
        elif tool_name == "search_airline_flights":
            code = args.get("airline_code") or args.get("airline") or args.get("code") or ""
            limit = int(args.get("limit", 15))
            args = {"airline_code": code, "limit": limit}

        res = tool_func(**args)
        return JSONResponse(res)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


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
    """Processes user queries via dedicated, isolated AI flight agents for each project."""
    try:
        data = await request.json()
        message = data.get("message", "").strip()
        project_mode = str(data.get("project", "live")).lower().strip()
        if not message:
            return JSONResponse({"status": "error", "error": "Message cannot be empty."}, status_code=400)

        # Isolated routing: Project #2 (Kafka) vs Project #1 (Live Radar)
        if project_mode == "kafka":
            result = await ask_kafka_agent(message)
        else:
            result = await ask_live_agent(message)

        return JSONResponse(result)

    except Exception as e:
        return JSONResponse({
            "status": "error",
            "error": str(e),
            "answer": f"Server error occurred while processing request: {e}"
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
            {"name": "get_flight_info", "description": "Live flight and telemetry search (e.g. TK10, TC-JYA, DLH400)"},
            {"name": "search_airline_flights", "description": "Airline active airborne flights (e.g. THY, PGT, DLH, BAW)"},
            {"name": "get_flights_over_region", "description": "Regional radar search (81 Turkish province polygons or macro-regions, speed filter)"},
            {"name": "get_most_tracked_flights", "description": "Top live most-tracked flights worldwide"},
            {"name": "get_airport_info", "description": "Airport details and coordinates (e.g. IST, SAW, ESB)"},
            {"name": "query_kafka_stream", "description": "Unified multi-filter query for Kafka live telemetry (81 provinces, speed, altitude, airline, flight ID, stats)"}
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
        limit = int(request.query_params.get("limit", 60))

        filtered = []
        for f in kafka_store.flights.values():
            telemetry = f.get("telemetry", {})
            spd = telemetry.get("ground_speed_kmh") or 0
            if min_speed_kmh is not None and spd < min_speed_kmh:
                continue

            f_num = str(f.get("flight_number") or "").upper()
            c_sign = str(f.get("callsign") or "").upper()
            reg = str(f.get("registration") or "").upper()
            model = str(f.get("aircraft_model") or "").upper()
            f_iata = str(f.get("airline_iata") or "").upper()
            f_icao = str(f.get("airline_icao") or "").upper()

            if airline and (airline not in [f_iata, f_icao] and not c_sign.startswith(airline) and not f_num.startswith(airline)):
                continue

            if search_query:
                if (search_query not in f_num and search_query not in c_sign and 
                    search_query not in reg and search_query not in model and
                    search_query not in f_iata and search_query not in f_icao):
                    continue

            filtered.append(f)

        # Sort by speed descending by default
        filtered.sort(key=lambda x: x.get("telemetry", {}).get("ground_speed_kmh", 0) or 0, reverse=True)

        return JSONResponse({
            "status": "success",
            "source": "kafka_live_store",
            "total_in_kafka": len(kafka_store.flights),
            "matched_count": len(filtered),
            "returned_count": min(len(filtered), limit),
            "flights": filtered[:limit]
        })
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


async def api_kafka_fastest(request):
    """Returns top fastest flights above specified speed."""
    try:
        min_speed = float(request.query_params.get("min_speed", 800))
        limit = int(request.query_params.get("limit", 20))
        result = kafka_store.find_flights_above_speed(min_speed_kmh=min_speed, limit=limit)
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


async def serve_index(request):
    """Serves 1. Project: FlightRadar24 Live Radar & AI Chat (index.html)."""
    index_file = os.path.join(FRONTEND_DIR, "index.html")
    if os.path.exists(index_file):
        with open(index_file, "r", encoding="utf-8") as f:
            content = f.read()
        return HTMLResponse(
            content,
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
    return HTMLResponse("<h1>Semalar 1. Project (index.html) not found.</h1>")


async def serve_kafka(request):
    """Serves 2. Project: Apache Kafka 1200 Telemetry & Speed Dashboard (kafka.html)."""
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
    return HTMLResponse("<h1>Semalar 2. Project (kafka.html) not found.</h1>")


app.add_route("/api/tools/execute", api_tools_execute, methods=["POST"])
app.add_route("/api/chat", api_chat, methods=["POST"])
app.add_route("/api/tracked", api_tracked, methods=["GET"])
app.add_route("/api/status", api_status, methods=["GET"])
app.add_route("/api/kafka/stats", api_kafka_stats, methods=["GET"])
app.add_route("/api/kafka/flights", api_kafka_flights, methods=["GET"])
app.add_route("/api/kafka/fastest", api_kafka_fastest, methods=["GET"])
app.add_route("/api/kafka/logs", api_kafka_logs, methods=["GET"])
app.add_route("/api/kafka/sync", api_kafka_sync, methods=["POST"])
app.add_route("/api/kafka/produce", api_kafka_produce_fresh, methods=["POST"])
app.add_route("/", serve_index, methods=["GET"])
app.add_route("/kafka", serve_kafka, methods=["GET"])

# Mount frontend directory for static assets
if os.path.exists(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="frontend_static")



if __name__ == "__main__":
    print("=" * 65)
    print("✈️ Semalar — Live Flight Radar & Apache Kafka Telemetry Platform")
    print("📡 MCP Endpoint              : http://localhost:8000/mcp")
    print("🔴 1. Project (Live Radar UI): http://localhost:8000")
    print("⚡ 2. Project (Kafka Cockpit): http://localhost:8000/kafka")
    print("🇹🇷 Real-time Ingestion       : ALL Live Turkey Airspace Flights ➔ Kafka (15s Loop)")
    print("📊 Apache Kafka UI Panel     : http://localhost:8080")
    print("=" * 65)
    print("👉 Open in Browser : http://localhost:8000  or  http://localhost:8000/kafka")
    print("⚠️  NOTE FOR WINDOWS: Do NOT navigate to 'http://0.0.0.0:8000' in browser;")
    print("   always use 'http://localhost:8000' on the local machine!")
    print("=" * 65)
    start_streamer_if_needed()
    uvicorn.run(app, host="0.0.0.0", port=8000)

