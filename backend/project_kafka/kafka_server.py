"""
Project #2 Kafka Cockpit — Standalone ASGI & FastMCP Server (Port 8001)
Serves real-time Apache Kafka flight telemetry streams, supersonic speed filtering,
81-province containment, and the Project #2 Cockpit UI (kafka.html).
"""

import os
import sys
import json
import time
import threading
import collections
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from mcp.server import MCPServer
from starlette.applications import Starlette
from starlette.responses import HTMLResponse, JSONResponse
from starlette.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
from kafka import KafkaProducer
import uvicorn

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(BASE_DIR)
ROOT_DIR = os.path.dirname(BACKEND_DIR)
for p in [ROOT_DIR, BACKEND_DIR, BASE_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

from project_kafka.flight_kafka_store import kafka_store
from project_kafka.flight_producer import FlightKafkaProducer
from project_kafka.kafka_agent import ask_kafka_agent
from core.llm_client import get_agent_info
from core.audit_logger import log_mcp_request, get_recent_audit_logs

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# FastMCP Server for Project #2
mcp_server = MCPServer("Semalar-Kafka-Cockpit")


@mcp_server.tool()
def kafka_query_stream(
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
    """Executes a requested Kafka MCP tool on the server via HTTP RPC for remote AI Agent clients."""
    try:
        data = await request.json()
        tool_name = data.get("tool_name")
        args = data.get("args", {})

        if tool_name not in ["query_kafka_stream", "kafka_query_stream"]:
            return JSONResponse({"status": "error", "error": f"Tool '{tool_name}' not found on Kafka MCP server."}, status_code=404)

        if "flight_code" in args and "query" not in args:
            args["query"] = args.pop("flight_code")
        if "airline_code" in args and "airline" not in args:
            args["airline"] = args.pop("airline_code")
        if "min_speed" in args and "min_speed_kmh" not in args:
            args["min_speed_kmh"] = args.pop("min_speed")
        if "speed" in args and "min_speed_kmh" not in args:
            args["min_speed_kmh"] = args.pop("speed")

        t_start = time.perf_counter()
        res = kafka_store.query_flights(**args)
        elapsed_ms = (time.perf_counter() - t_start) * 1000
        log_mcp_request(tool_name, args, res, elapsed_ms)
        return JSONResponse(res)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

# Background Turkey flight ingestion daemon
_streamer_stop_event = threading.Event()
_streamer_thread = None


def _turkey_streamer_worker():
    """Background daemon worker continuously ingesting ALL Turkey flights from FlightRadar24 into Kafka."""
    print("🇹🇷 [KAFKA STREAMER] Real-time Turkey Airspace ➔ Kafka streaming daemon active (interval: 15s).")
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


def start_streamer_if_needed():
    global _streamer_thread
    if _streamer_thread is None or not _streamer_thread.is_alive():
        _streamer_thread = threading.Thread(target=_turkey_streamer_worker, daemon=True, name="KafkaServerStreamer")
        _streamer_thread.start()


# In-Memory Audit Log Ring Buffer
_recent_audit_logs = collections.deque(maxlen=100)


async def api_chat(request):
    """Processes Project #2 natural language queries via isolated kafka_agent."""
    try:
        data = await request.json()
        message = data.get("message", "").strip()
        if not message:
            return JSONResponse({"status": "error", "error": "Message cannot be empty."}, status_code=400)
        result = await ask_kafka_agent(message)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


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

        result = kafka_store.query_flights(
            query=search_query,
            airline=airline,
            min_speed_kmh=min_speed_kmh,
            limit=limit
        )
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


async def api_kafka_logs(request):
    """Returns recent Kafka audit logs."""
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


async def api_kafka_produce(request):
    """Fetches fresh flights across Turkey from FlightRadar24 and publishes to Kafka."""
    try:
        producer = FlightKafkaProducer()
        report = producer.publish_turkey_flights(topic="live-flights")
        producer.close()
        kafka_store.sync_from_kafka()
        return JSONResponse(report)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


async def api_status(request):
    """Returns Project #2 status info."""
    agent_info = get_agent_info()
    return JSONResponse({
        "status": "online",
        "project": "Project #2: Apache Kafka Telemetry Cockpit",
        "provider": agent_info["provider"],
        "model": agent_info["model"],
        "mcp_url": "http://localhost:8001/mcp",
        "total_flights_in_kafka": len(kafka_store.flights),
        "tools": ["query_kafka_stream"]
    })


FRONTEND_DIR = os.path.join(ROOT_DIR, "frontend")


async def serve_kafka(request):
    """Serves Project #2 Cockpit UI (kafka.html)."""
    kafka_file = os.path.join(FRONTEND_DIR, "kafka.html")
    if os.path.exists(kafka_file):
        with open(kafka_file, "r", encoding="utf-8") as f:
            content = f.read()
        return HTMLResponse(content)
    return HTMLResponse("<h1>Semalar Project #2 (kafka.html) not found.</h1>")


app.add_route("/api/tools/execute", api_tools_execute, methods=["POST"])
app.add_route("/api/chat", api_chat, methods=["POST"])
app.add_route("/api/kafka/stats", api_kafka_stats, methods=["GET"])
app.add_route("/api/kafka/flights", api_kafka_flights, methods=["GET"])
app.add_route("/api/kafka/logs", api_kafka_logs, methods=["GET"])
app.add_route("/api/kafka/sync", api_kafka_sync, methods=["POST"])
app.add_route("/api/kafka/produce", api_kafka_produce, methods=["POST"])
app.add_route("/api/status", api_status, methods=["GET"])
app.add_route("/", serve_kafka, methods=["GET"])
app.add_route("/kafka", serve_kafka, methods=["GET"])

if os.path.exists(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="frontend_static")


if __name__ == "__main__":
    port = int(os.getenv("PORT_KAFKA", 8001))
    print("=" * 65)
    print("⚡ Semalar — Project #2: Apache Kafka Telemetry Cockpit Server")
    print(f"📡 MCP Endpoint  : http://localhost:{port}/mcp")
    print(f"🌐 Cockpit UI    : http://localhost:{port}")
    print("🇹🇷 Background    : Real-time Ingestion Daemon (15s Loop)")
    print("=" * 65)
    start_streamer_if_needed()
    uvicorn.run(app, host="0.0.0.0", port=port)
