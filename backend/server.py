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
from kafka import KafkaProducer

# Silence noisy kafka connection logs
logging.getLogger("kafka").setLevel(logging.WARNING)

from flight_service import (
    get_flight_info as fetch_flight_info,
    search_airline_flights as fetch_airline_flights,
    get_flights_over_region as fetch_flights_over_region,
    get_most_tracked_flights as fetch_most_tracked_flights,
    get_airport_info as fetch_airport_info
)
from flight_kafka_store import kafka_store
from flight_producer import FlightKafkaProducer
from flight_agent import ask_flight_agent, get_agent_info

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# In-Memory Audit Log Ring Buffer & Kafka Audit Producer
_recent_audit_logs = collections.deque(maxlen=100)
_audit_producer = None
try:
    _audit_producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k else None,
        acks=0,
        retries=1
    )
except Exception:
    pass

print("✅ MCP Kafka İstek Günlüğü (Audit Logger) aktif: Topic 'mcp-requests'")


def log_mcp_to_kafka(tool_name: str, args: dict, result: Any, elapsed_ms: float):
    """Gelen MCP isteklerini anlık olarak Kafka 'mcp-requests' topic'ine ve bellek kuyruğuna yazar."""
    try:
        status = "success"
        matched_count = None
        if isinstance(result, dict):
            status = result.get("status", "success")
            if "total_matches" in result:
                matched_count = result["total_matches"]
            elif "returned_count" in result:
                matched_count = result["returned_count"]
            elif "flights" in result and isinstance(result["flights"], list):
                matched_count = len(result["flights"])
            elif "total_tracked" in result:
                matched_count = result["total_tracked"]

        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": tool_name,
            "arguments": args,
            "status": status,
            "matched_records": matched_count,
            "execution_time_ms": round(elapsed_ms, 2)
        }
        _recent_audit_logs.appendleft(payload)
        if _audit_producer:
            _audit_producer.send("mcp-requests", key=tool_name, value=payload)
    except Exception:
        pass


def audit_tool(name: str):
    """Tüm MCP araç çağrılarını yakalayıp Kafka'ya kaydeden dekoratör."""
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

            log_mcp_to_kafka(name, call_args, result, elapsed_ms)
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
# KAFKA STREAM POWERED MCP TOOLS
# ============================================================

@mcp_server.tool()
@audit_tool("get_flights_above_speed")
def get_flights_above_speed(min_speed_kmh: float = 800.0, limit: int = 15) -> Dict[str, Any]:
    """Finds and lists live flights from the Kafka telemetry stream that are flying at or above a specified ground speed in km/h (e.g. 800 km/h, 900 km/h, 1000 km/h).
    Returns flights sorted from fastest to slowest with aircraft models, routes, coordinates, and exact telemetry speeds.
    
    Args:
        min_speed_kmh: Minimum ground speed filter in km/h (default: 800.0 km/h).
        limit: Maximum number of fast flights to return (default: 15).
    """
    return kafka_store.find_flights_above_speed(min_speed_kmh=min_speed_kmh, limit=limit)


@mcp_server.tool()
@audit_tool("get_flight_from_kafka")
def get_flight_from_kafka(query: str = "", flight_code: str = "") -> Dict[str, Any]:
    """Finds a live flight from the Kafka stream cache by flight number (e.g. 'TK10', 'PC2020'), callsign (e.g. 'THY10'), or registration tail (e.g. 'TC-LJA').
    Provides instant sub-millisecond telemetry response from the 1200+ buffered flights.
    
    Args:
        query: Flight number, callsign, or registration.
        flight_code: Alternative parameter alias for query.
    """
    target = query or flight_code or ""
    return kafka_store.find_flight(target)


@mcp_server.tool()
@audit_tool("get_flights_over_region_from_kafka")
def get_flights_over_region_from_kafka(latitude: float, longitude: float, radius_km: float = 150.0, limit: int = 15) -> Dict[str, Any]:
    """Finds live flights from the Kafka telemetry stream within a given radius (km) around specified coordinates (latitude, longitude).
    
    Args:
        latitude: Latitude in decimal degrees (e.g. 41.0082 for Istanbul).
        longitude: Longitude in decimal degrees (e.g. 28.9784 for Istanbul).
        radius_km: Search radius in kilometers (default: 150 km).
        limit: Maximum number of flights to return (default: 15).
    """
    return kafka_store.find_nearby(latitude=latitude, longitude=longitude, radius_km=radius_km, limit=limit)


@mcp_server.tool()
@audit_tool("search_airline_from_kafka")
def search_airline_from_kafka(airline_code: str, limit: int = 15) -> Dict[str, Any]:
    """Searches active flights for a given airline (e.g. 'TK'/'THY', 'PC'/'PGT', 'LH'/'DLH') from the Kafka live stream cache.
    
    Args:
        airline_code: 2-letter IATA or 3-letter ICAO airline code.
        limit: Maximum number of flights to return (default: 15).
    """
    return kafka_store.find_by_airline(airline_code=airline_code, limit=limit)


@mcp_server.tool()
@audit_tool("get_kafka_stream_stats")
def get_kafka_stream_stats() -> Dict[str, Any]:
    """Retrieves real-time statistical analytics across the Kafka live flight stream, including total indexed flights, unique airlines, maximum & average speeds, and maximum & average altitudes."""
    return kafka_store.get_telemetry_stats()


@mcp_server.tool()
@audit_tool("refresh_kafka_stream")
def refresh_kafka_stream() -> Dict[str, Any]:
    """Refreshes and synchronizes the in-memory cache with the latest messages from the Kafka 'live-flights' topic."""
    total = kafka_store.sync_from_kafka()
    return {
        "status": "success",
        "message": f"Kafka akışı senkronize edildi. Toplam {total} aktif uçuş hafızada güncel.",
        "total_cached_flights": total
    }


# Registry of all MCP Tools available on this Server
MCP_TOOLS_REGISTRY = {
    # 1. Proje Live Tools
    "get_flight_info": get_flight_info,
    "search_airline_flights": search_airline_flights,
    "get_flights_over_region": get_flights_over_region,
    "get_most_tracked_flights": get_most_tracked_flights,
    "get_airport_info": get_airport_info,
    # 2. Proje Kafka Tools
    "get_flights_above_speed": get_flights_above_speed,
    "get_flight_from_kafka": get_flight_from_kafka,
    "get_flights_over_region_from_kafka": get_flights_over_region_from_kafka,
    "search_airline_from_kafka": search_airline_from_kafka,
    "get_kafka_stream_stats": get_kafka_stream_stats,
    "refresh_kafka_stream": refresh_kafka_stream,
}


async def api_tools_execute(request):
    """Executes a requested MCP tool on the server via HTTP RPC for remote AI Agent clients."""
    try:
        data = await request.json()
        tool_name = data.get("tool_name")
        args = data.get("args", {})

        if tool_name not in MCP_TOOLS_REGISTRY:
            return JSONResponse({"status": "error", "error": f"Tool '{tool_name}' bu MCP sunucusunda bulunamadı."}, status_code=404)

        tool_func = MCP_TOOLS_REGISTRY[tool_name]

        # Handle parameter aliases safely
        if tool_name in ["get_flight_info", "get_flight_from_kafka"]:
            target_val = args.get("flight_code") or args.get("query") or args.get("flight_number") or args.get("callsign") or ""
            args = {"query": target_val, "flight_code": target_val}
        elif tool_name == "get_airport_info":
            code = args.get("airport_code") or args.get("query") or args.get("code") or ""
            args = {"airport_code": code}
        elif tool_name in ["search_airline_flights", "search_airline_from_kafka"]:
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
    """Processes user queries via the AI flight agent."""
    try:
        data = await request.json()
        message = data.get("message", "").strip()
        project_mode = data.get("project", "auto")
        if not message:
            return JSONResponse({"status": "error", "error": "Boş mesaj gönderilemez."}, status_code=400)
        
        result = await ask_flight_agent(message, project_mode=project_mode, mcp_url="http://localhost:8000/mcp")
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
            {"name": "get_airport_info", "description": "Havalimanı bilgileri ve koordinatları (IST, SAW vb.)"},
            {"name": "get_flights_above_speed", "description": "Kafka akışından belirli hızın üzerindeki süpersonik/hızlı uçaklar"},
            {"name": "get_flight_from_kafka", "description": "Kafka canlı telemetri deposundan anlık uçuş arama"},
            {"name": "get_flights_over_region_from_kafka", "description": "Kafka telemetri akışından bölgesel radar sorgusu"},
            {"name": "search_airline_from_kafka", "description": "Kafka telemetri akışından havayolu uçuşlarını arama"},
            {"name": "get_kafka_stream_stats", "description": "Kafka 1200+ uçuş akışının hız, irtifa ve havayolu istatistikleri"},
            {"name": "refresh_kafka_stream", "description": "Kafka 'live-flights' topic'inden hafızayı anlık tazeleme"}
        ]
    })


from flight_producer import FlightKafkaProducer

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
    logs_list = list(_recent_audit_logs)
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
            "message": f"Kafka akışı senkronize edildi. Toplam {count} uçuş bellekte.",
            "total_cached_flights": count
        })
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


async def api_kafka_produce_fresh(request):
    """Fetches 1200 fresh flights from FlightRadar24 and publishes to Kafka."""
    try:
        producer = FlightKafkaProducer()
        report = producer.collect_and_publish(target_count=1200, topic="live-flights")
        producer.close()
        kafka_store.sync_from_kafka()
        return JSONResponse(report)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


# Frontend Directory Resolution
FRONTEND_DIR = os.path.join(PARENT_DIR, "frontend")


async def serve_index(request):
    """Serves 1. Proje: FlightRadar24 Canlı Radar & AI Chat (index.html)."""
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
    return HTMLResponse("<h1>Semalar 1. Proje (index.html) bulunamadı.</h1>")


async def serve_kafka(request):
    """Serves 2. Proje: Apache Kafka 1200 Telemetri & Hız Dashboard'u (kafka.html)."""
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
    return HTMLResponse("<h1>Semalar 2. Proje (kafka.html) bulunamadı.</h1>")


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
    print("✈️ Semalar — Canlı Uçuş Radarı & Apache Kafka Telemetri Platformu")
    print("📡 MCP Endpoint              : http://localhost:8000/mcp")
    print("🔴 1. Proje (Canlı Radar UI) : http://localhost:8000")
    print("⚡ 2. Proje (Kafka Dashboard): http://localhost:8000/kafka")
    print("📊 Apache Kafka UI Paneli    : http://localhost:8080")
    print("=" * 65)
    uvicorn.run(app, host="0.0.0.0", port=8000)

