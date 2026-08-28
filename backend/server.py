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
    description="Streamable HTTP MCP Server providing live Apache Kafka aircraft telemetry stream cache, 81-province polygon filtering, and AI assistant tools."
)


# ============================================================
# UNIFIED KAFKA STREAM MCP TOOL (Single Source of Truth)
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

    Performs sub-millisecond in-memory filtering against 500+ live aircraft records synchronized
    from the Apache Kafka topic 'live-flights'. Supports compound multi-parameter queries
    combining ground speed, 81 Turkish province boundary polygons, airlines, altitude, and stats.

    Args:
        query: Specific flight number (e.g. 'TK10', 'MH21'), callsign ('THY10', 'PGT45K'), or aircraft registration tail ('TC-LJA').
        region: Target Turkish province name (e.g. 'Erzurum', 'İstanbul', 'Ankara') or macro-region ('MARMARA', 'EGE', 'TR'). Evaluates exact 81-province boundary polygons via sub-millisecond ray-casting.
        airline: 3-letter ICAO (e.g. 'THY', 'PGT', 'DLH', 'BAW') or 2-letter IATA ('TK', 'PC', 'LH', 'BA') airline code.
        min_speed_kmh: Minimum ground speed filter in km/h (e.g. 800, 900 for high-speed or near-supersonic aircraft).
        min_altitude_feet: Minimum altitude filter in feet (e.g. 32800 for 10,000 meters and above).
        get_stats: Set to True to retrieve overall Kafka stream statistical summary (maximum/average speed and altitude, active airlines count).
        limit: Maximum number of flight records to return (default: 15).

    Returns:
        Dict[str, Any]: Structured JSON response containing:
            - status (str): 'success' or 'error'
            - source (str): 'kafka_in_memory_stream'
            - total_matches (int): Total number of matching aircraft in the Kafka stream
            - returned_count (int): Number of flights returned in this batch
            - flights (list): Array of matching flight objects with telemetry, aircraft model, route, speed, altitude, and coordinates.
            - applied_region / province_details (dict, optional): Provincial polygon metadata when region filter is used.
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
    "query_kafka_stream": query_kafka_stream,
    "kafka_query_stream": query_kafka_stream
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
    """Returns server and agent status info."""
    agent_info = get_agent_info()
    return JSONResponse({
        "status": "online",
        "service": "Semalar Kafka Flight MCP & AI Cockpit",
        "provider": agent_info["provider"],
        "model": agent_info["model"],
        "mcp_url": "http://localhost:8000/mcp",
        "tools": [
            {
                "name": "query_kafka_stream",
                "description": "Unified multi-filter query for Kafka live telemetry (81 provinces, speed, altitude, airline, flight ID, stats)"
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
    print("📡 MCP Streamable HTTP       : http://localhost:8000/mcp")
    print("⚡ Kafka Telemetry Cockpit UI: http://localhost:8000")
    print("🇹🇷 Real-time Ingestion       : ALL Live Turkey Airspace Flights ➔ Kafka (15s Loop)")
    print("📊 Apache Kafka UI Panel     : http://localhost:8080")
    print("=" * 65)
    print("👉 Open in Browser : http://localhost:8000")
    print("⚠️  NOTE FOR WINDOWS: Do NOT navigate to 'http://0.0.0.0:8000' in browser;")
    print("   always use 'http://localhost:8000' on the local machine!")
    print("=" * 65)
    start_streamer_if_needed()
    uvicorn.run(app, host="0.0.0.0", port=8000)
