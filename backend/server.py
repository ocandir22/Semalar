import os
import sys
import json
from typing import Optional, List, Dict, Any
from mcp.server import MCPServer
from starlette.responses import HTMLResponse, JSONResponse, FileResponse, RedirectResponse
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
    city: str = "",
    region: str = "",
    query: str = "",
    airline: str = "",
    min_speed_kmh: Optional[float] = None,
    min_altitude_feet: Optional[float] = None,
    get_stats: bool = False,
    limit: int = 15,
    **kwargs
) -> Dict[str, Any]:
    """Canlı Apache Kafka ADS-B uçuş telemetri akışından uçakları sorgular (81 İl Poligonu, hız, irtifa, havayolu, uçuş kodu).

    ÖNEMLİ KURAL (İL / İLÇE / BÖLGE FİLTRESİ):
    Kullanıcı Türkiye'deki herhangi bir il, ilçe, semt veya bölge üzerindeki uçakları sorduğunda (Örn: 'Çankaya üzerindeki uçaklar', 'Kadıköy semaları', 'Bornova', 'Alanya', 'Bodrum', 'Erzurum', 'İstanbul', 'Ankara'):
    1. Kullanıcı bir İLÇE, SEMT veya YERLEŞİM YERİ belirtirse (Örn: 'Çankaya', 'Kadıköy', 'Bornova', 'Bodrum', 'Alanya'), LLM olarak bu konumu MUTLAKA bağlı olduğu ana İL adına ('Ankara', 'İstanbul', 'İzmir', 'Muğla', 'Antalya') dönüştürün ve `city` (veya `region`) parametresine gönderin!
    2. Backend, o ilin mülki sınır poligonunu Ray-Casting PIP (Point-in-Polygon) algoritmasıyla tarayarak tam o ilin/ilçenin hava sahasında bulunan uçakları döndürür.
    3. Asla il/ilçe sorgularında bu parametreyi boş bırakmayın (`city='Ankara'` vb. gönderin)!

    Args:
        city: Uçuşların filtreleneceği hedef İL adı (Örn: 'Ankara', 'İstanbul', 'İzmir', 'Erzurum'). Kullanıcı bir ilçe/semt belirtirse ('Çankaya', 'Kadıköy', 'Bornova', 'Bodrum') bağlı olduğu İL adını yazın.
        region: `city` ile eşdeğer İL veya makro coğrafi bölge filtresi (Örn: 'Ankara', 'MARMARA', 'EGE', 'TR').
        query: Uçuş numarası (örn: 'TK10', 'PC2024'), çağrı kodu (callsign: 'THY10', 'PGT45K') veya uçak kuyruk tescili ('TC-LJA').
        airline: 3 harfli ICAO (örn: 'THY', 'PGT', 'DLH') veya 2 harfli IATA ('TK', 'PC') havayolu kodu.
        min_speed_kmh: Filtrelenecek minimum yer hızı (km/s cinsinden, örn: 800).
        min_altitude_feet: Filtrelenecek minimum uçuş irtifası (feet cinsinden, örn: 30000).
        get_stats: Tüm Kafka telemetri akışı genel istatistik özetini almak için True yapın.
        limit: Dönecek maksimum uçuş sayısı (varsayılan: 15).

    Returns:
        Dict[str, Any]: Eşleşen uçakların canlı telemetrisi, irtifası, hızı, koordinatları ve rota bilgileri.
    """
    target_region = city or region or kwargs.get("province") or kwargs.get("country") or ""
    return kafka_store.query_flights(
        query=query,
        region=target_region,
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


# ============================================================
# 🛡️ 8. AI TCAS & AIRSPACE CONFLICT DETECTION MCP TOOL
# ============================================================

@mcp_server.tool()
@audit_tool("detect_airspace_conflicts")
def detect_airspace_conflicts(
    min_horizontal_km: float = 10.0,
    min_vertical_feet: float = 1000.0,
    limit: int = 10
) -> Dict[str, Any]:
    """Scans active aircraft pairs across Turkish airspace to detect loss of separation, TCAS Traffic Advisories (TA/RA), and close-proximity airborne conflicts.

    Args:
        min_horizontal_km: Minimum horizontal proximity threshold in km (default: 10.0 km / ~5.4 NM).
        min_vertical_feet: Minimum vertical separation threshold in feet (default: 1000.0 ft).
        limit: Maximum number of conflict pairs to return.

    Returns:
        Dict[str, Any]: List of detected close-proximity aircraft pairs, separation distances, altitudes, routes, and TCAS alert levels.
    """
    return kafka_store.detect_airspace_conflicts(
        min_horizontal_km=min_horizontal_km,
        min_vertical_feet=min_vertical_feet,
        limit=limit
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
    "get_fleet_aircraft_analytics": get_fleet_aircraft_analytics,
    "detect_airspace_conflicts": detect_airspace_conflicts
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
        if tool_name == "find_nearby_aircraft" and "city" in args and "location" not in args:
            args["location"] = args.pop("city")
        if "province" in args and "city" not in args and "region" not in args:
            args["city"] = args.pop("province")

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
            },
            {
                "name": "detect_airspace_conflicts",
                "description": "AI TCAS loss of separation and airborne close-proximity conflict detection"
            }
        ]
    })


async def api_geo_provinces(request):
    """Returns list of all 81 Turkish provinces."""
    return JSONResponse({"provinces": geo_engine.list_provinces()})


async def api_geo_province_polygon(request):
    """Returns GeoJSON boundary polygon coordinates for a requested province."""
    name = request.query_params.get("name", "").strip()
    if not name:
        return JSONResponse({"status": "error", "error": "Province name required."}, status_code=400)
    poly = geo_engine.get_province_polygon(name)
    if not poly:
        return JSONResponse({"status": "not_found", "error": f"Province '{name}' not found."}, status_code=404)
    return JSONResponse({"status": "success", "polygon": poly})


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


async def serve_semalar(request):
    """Serves Semalar: Live Flight Telemetry & AI Cockpit (semalar.html / kafka.html)."""
    semalar_file = os.path.join(FRONTEND_DIR, "semalar.html")
    if not os.path.exists(semalar_file):
        semalar_file = os.path.join(FRONTEND_DIR, "kafka.html")
    if os.path.exists(semalar_file):
        with open(semalar_file, "r", encoding="utf-8") as f:
            content = f.read()
        return HTMLResponse(
            content,
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
    return HTMLResponse("<h1>Semalar (semalar.html / kafka.html) not found.</h1>")


async def redirect_to_semalar(request):
    """Redirects legacy /kafka to /semalar."""
    return RedirectResponse(url="/semalar", status_code=307)


app.add_route("/api/tools/execute", api_tools_execute, methods=["POST"])
app.add_route("/api/chat", api_chat, methods=["POST"])
app.add_route("/api/status", api_status, methods=["GET"])
app.add_route("/api/geo/provinces", api_geo_provinces, methods=["GET"])
app.add_route("/api/geo/province-polygon", api_geo_province_polygon, methods=["GET"])
app.add_route("/api/kafka/stats", api_kafka_stats, methods=["GET"])
app.add_route("/api/kafka/flights", api_kafka_flights, methods=["GET"])
app.add_route("/api/kafka/fastest", api_kafka_fastest, methods=["GET"])
app.add_route("/api/kafka/logs", api_kafka_logs, methods=["GET"])
app.add_route("/api/kafka/sync", api_kafka_sync, methods=["POST"])
app.add_route("/api/kafka/produce", api_kafka_produce_fresh, methods=["POST"])
app.add_route("/", serve_semalar, methods=["GET"])
app.add_route("/semalar", serve_semalar, methods=["GET"])
app.add_route("/kafka", redirect_to_semalar, methods=["GET"])

# Mount frontend directory for static assets
if os.path.exists(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="frontend_static")


if __name__ == "__main__":
    print("=" * 65)
    print("✈️ Semalar — Apache Kafka Live Flight Telemetry & AI Cockpit")
    print("📡 FastMCP Streamable HTTP   : http://localhost:8000/mcp")
    print("⚡ Semalar Cockpit UI        : http://localhost:8000/semalar")
    print(f"🛠️ Active FastMCP Tools ({len(MCP_TOOLS_REGISTRY)}):")
    for t_name in MCP_TOOLS_REGISTRY:
        if t_name != "kafka_query_stream":
            print(f"   • {t_name}")
    print("🇹🇷 Real-time Ingestion       : ALL Live Turkey Airspace Flights ➔ Kafka (15s Loop)")
    print("📊 Apache Kafka UI Panel     : http://localhost:8080")
    print("=" * 65)
    print("👉 Open in Browser : http://localhost:8000/semalar")
    print("=" * 65)
    start_streamer_if_needed()
    uvicorn.run(app, host="0.0.0.0", port=8000)
