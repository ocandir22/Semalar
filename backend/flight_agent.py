import os
import sys
import json
import re
import asyncio
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Load environment variables from parent directory or current directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(BASE_DIR)
load_dotenv(os.path.join(PARENT_DIR, ".env"))
load_dotenv(os.path.join(BASE_DIR, ".env"))
load_dotenv()

# Active Provider: "gemini", "groq", "openrouter", "ollama", "openai", "deepseek"
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini").lower().strip()

# Gemini Config
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.7-flash")

# Groq Config
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "qwen/qwen3.6-27b")

# OpenRouter Config
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct")

# Ollama Config (Local)
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")

# OpenAI Config
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# DeepSeek Config
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

# MCP Server URL
PUBLIC_MCP_URL = os.getenv("PUBLIC_MCP_URL", "http://localhost:8000/mcp")

# Fallback models for Gemini
FALLBACK_MODELS = [
    GEMINI_MODEL,
    "gemini-3.5-flash-lite",
    "gemini-3.7-flash"
]
FALLBACK_MODELS = list(dict.fromkeys(FALLBACK_MODELS))


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

# Shared In-Memory Audit Log Ring Buffer
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


SYSTEM_INSTRUCTION = (
    "Sen havacılık, uçak telemetrisi, canlı uçuş radarı ve Apache Kafka canlı telemetri deposu konusunda uzman, net ve doğrudan yanıt veren bir AI asistanısın.\n"
    "Kullanıcıların sorularına yanıt vermek için SADECE ve SADECE sana sağlanan 11 MCP Tool aracını kullan.\n\n"
    "🎯 ARAÇ SEÇİM REHBERİ (Smart Tool Selection):\n"
    "1. Uçuş Bilgisi / Nerede / İrtifa / Hız Soruları: 'get_flight_info' (canlı radar) veya 'get_flight_from_kafka' (Kafka deposu) aracını çağır.\n"
    "2. Belirli Bir Hızın Üzerindeki Uçaklar (örn: 800 km/s, 900 km/s, 1000 km/s üstü en hızlı uçaklar): 'get_flights_above_speed' aracını çağır.\n"
    "3. Bölgesel / Koordinat Soruları (örn: İstanbul, Ankara, New York semalarında uçanlar): 'get_flights_over_region' veya 'get_flights_over_region_from_kafka' aracını çağır.\n"
    "4. Havayolu Filoları (örn: THY, Pegasus, Lufthansa uçakları): 'search_airline_flights' veya 'search_airline_from_kafka' aracını çağır.\n"
    "5. En Çok İzlenen Canlı Uçuşlar: 'get_most_tracked_flights' aracını çağır.\n"
    "6. Havalimanı Bilgisi (örn: IST, SAW, LHR, JFK): 'get_airport_info' aracını çağır.\n"
    "7. Kafka Akış İstatistikleri (ortalama hız, tepe irtifa, toplam uçak): 'get_kafka_stream_stats' aracını çağır.\n\n"
    "📌 YANIT FORMATI KURALLARI:\n"
    "• Uçuş verilerini, irtifayı, hızları veya modelleri asla uydurma. Yalnızca tool'dan dönen gerçek JSON değerlerini kullan.\n"
    "• İrtifayı hem feet hem metre cinsinden belirt (örnek: 37.000 ft / ~11.277 m).\n"
    "• Hızı hem knot hem km/s cinsinden belirt (örnek: 480 kts / ~889 km/s).\n"
    "• Uçak modelini (Boeing 777-300ER, Airbus A321neo vb.) ve rotasını (IST ➔ JFK) mutlaka vurgula.\n"
    "• Cevapları kısa, net, anlaşılır ve madde işaretli liste formatında sun."
)


def clean_model_output(text: str) -> str:
    """Removes internal reasoning tags or cleanup formatting."""
    if not text:
        return ""
    cleaned = re.sub(r"<think>[\s\S]*?</think>", "", text)
    return cleaned.strip()


def extract_text_from_response(response) -> str:
    """Extracts text from candidate parts even if thought/reasoning or function response is present."""
    if not response:
        return "(Yanıt alınamadı)"
    if hasattr(response, "text") and response.text:
        return clean_model_output(response.text)
    if hasattr(response, "candidates") and response.candidates:
        cand = response.candidates[0]
        if hasattr(cand, "content") and cand.content and cand.content.parts:
            text_parts = [p.text for p in cand.content.parts if hasattr(p, "text") and p.text]
            if text_parts:
                return clean_model_output("\n".join(text_parts))
    return "(Yanıt alınamadı)"




def get_agent_info() -> Dict[str, str]:
    """Returns information about the currently active AI provider and model."""
    provider = LLM_PROVIDER
    if provider == "gemini":
        model = GEMINI_MODEL
    elif provider == "groq":
        model = GROQ_MODEL
    elif provider == "openrouter":
        model = OPENROUTER_MODEL
    elif provider == "ollama":
        model = OLLAMA_MODEL
    elif provider == "deepseek":
        model = DEEPSEEK_MODEL
    elif provider == "openai":
        model = OPENAI_MODEL
    else:
        model = "unknown"
    return {
        "provider": provider,
        "model": model,
        "mcp_url": PUBLIC_MCP_URL
    }


# ============================================================
# 11 MCP Tools Direct Definitions & Local Executor
# ============================================================

LOCAL_MCP_DEFINITIONS = [
    {
        "name": "get_flight_info",
        "description": "FlightRadar24 canlı ADS-B ağından uçuş kodu (TK10, THY10) veya kuyruk tescili ile anlık konum, irtifa, yer hızı ve uçak modelini getirir.",
        "parameters": {
            "type": "object",
            "properties": {
                "flight_code": {"type": "string", "description": "Uçuş numarası (örn: TK10, THY10) veya tescil (örn: TC-JYA)"}
            },
            "required": ["flight_code"]
        }
    },
    {
        "name": "search_airline_flights",
        "description": "Belirli bir havayolunun (THY, PGT, DLH, BAW) FlightRadar24'teki canlı havadaki uçuşlarını listeler.",
        "parameters": {
            "type": "object",
            "properties": {
                "airline_code": {"type": "string", "description": "Havayolu ICAO/IATA kodu (örn: THY, TK, PGT, PC)"},
                "limit": {"type": "integer", "description": "Döndürülecek maksimum uçuş sayısı (varsayılan: 10)"}
            },
            "required": ["airline_code"]
        }
    },
    {
        "name": "get_flights_over_region",
        "description": "Belirli bir coğrafi koordinatın etrafında belirli bir yarıçap (km) içinde uçan canlı uçakları listeler.",
        "parameters": {
            "type": "object",
            "properties": {
                "latitude": {"type": "number", "description": "Merkez enlem (örn: 41.0082)"},
                "longitude": {"type": "number", "description": "Merkez boylam (örn: 28.9784)"},
                "radius_km": {"type": "number", "description": "Arama yarıçapı km (varsayılan: 100)"},
                "limit": {"type": "integer", "description": "Maksimum uçak sayısı (varsayılan: 15)"}
            },
            "required": ["latitude", "longitude"]
        }
    },
    {
        "name": "get_most_tracked_flights",
        "description": "FlightRadar24 canlı verisinde dünya genelinde anlık olarak en çok takip edilen popüler uçuşları listeler.",
        "parameters": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Döndürülecek uçuş sayısı (varsayılan: 10)"}
            }
        }
    },
    {
        "name": "get_airport_info",
        "description": "IATA veya ICAO kodu verilen havalimanının (IST, SAW, LHR, JFK) koordinatlarını, şehir ve ülke detaylarını getirir.",
        "parameters": {
            "type": "object",
            "properties": {
                "airport_code": {"type": "string", "description": "Havalimanı IATA veya ICAO kodu (örn: IST, SAW, LHR)"}
            },
            "required": ["airport_code"]
        }
    },
    {
        "name": "get_flights_above_speed",
        "description": "Apache Kafka telemetri akışından belirli bir yer hızının (km/s) üzerindeki süpersonik / en hızlı uçakları filtreler.",
        "parameters": {
            "type": "object",
            "properties": {
                "min_speed_kmh": {"type": "number", "description": "Filtrelenecek minimum yer hızı km/s (örn: 800 veya 900)"},
                "limit": {"type": "integer", "description": "Maksimum uçak sayısı (varsayılan: 15)"}
            }
        }
    },
    {
        "name": "get_flight_from_kafka",
        "description": "Apache Kafka 1200+ uçuşluk telemetri deposundan uçuş numarası veya tescil ile anlık uçuş ve telemetri arar.",
        "parameters": {
            "type": "object",
            "properties": {
                "flight_code": {"type": "string", "description": "Uçuş kodu (örn: TK10, SABIR741) veya tescil (örn: TC-LJA)"}
            },
            "required": ["flight_code"]
        }
    },
    {
        "name": "get_flights_over_region_from_kafka",
        "description": "Apache Kafka telemetri akışından belirli koordinat etrafındaki yarıçap içinde uçan uçakları filtreler.",
        "parameters": {
            "type": "object",
            "properties": {
                "latitude": {"type": "number", "description": "Merkez enlem (örn: 41.0082)"},
                "longitude": {"type": "number", "description": "Merkez boylam (örn: 28.9784)"},
                "radius_km": {"type": "number", "description": "Arama yarıçapı km (varsayılan: 100)"},
                "limit": {"type": "integer", "description": "Maksimum uçak sayısı (varsayılan: 15)"}
            },
            "required": ["latitude", "longitude"]
        }
    },
    {
        "name": "search_airline_from_kafka",
        "description": "Apache Kafka telemetri deposundaki belirli bir havayoluna ait uçuşları listeler.",
        "parameters": {
            "type": "object",
            "properties": {
                "airline_code": {"type": "string", "description": "Havayolu ICAO/IATA kodu (örn: THY, TK, PGT)"},
                "limit": {"type": "integer", "description": "Maksimum uçak sayısı (varsayılan: 10)"}
            },
            "required": ["airline_code"]
        }
    },
    {
        "name": "get_kafka_stream_stats",
        "description": "Apache Kafka'daki 1200 uçaklık telemetri havuzunun hız (maks/ortalama), irtifa ve havayolu istatistiklerini getirir.",
        "parameters": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "refresh_kafka_stream",
        "description": "Apache Kafka 'live-flights' topic'inden bellek havuzunu sıfırdan tazeler.",
        "parameters": {
            "type": "object",
            "properties": {
                "target_count": {"type": "integer", "description": "Tazelenecek hedef uçuş sayısı (varsayılan: 1200)"}
            }
        }
    }
]


def execute_local_mcp_tool(tool_name: str, tool_args: dict) -> dict:
    """Executes the requested tool directly in memory in 0 milliseconds and logs to Kafka audit topic."""
    start_time = time.perf_counter()
    try:
        if tool_name == "get_flight_info":
            # 1. Proje: SADECE Canlı FlightRadar24 ADS-B API
            res = fetch_flight_info(str(tool_args.get("flight_code", "")))
        elif tool_name == "get_flight_from_kafka":
            # 2. Proje: SADECE Apache Kafka 1200 Uçaklık Telemetri Havuzu
            res = kafka_store.find_flight(str(tool_args.get("flight_code", "")))
        elif tool_name == "search_airline_flights":
            # 1. Proje: Canlı FlightRadar Havayolu
            res = fetch_airline_flights(str(tool_args.get("airline_code", "")), int(tool_args.get("limit", 10)))
        elif tool_name == "get_flights_over_region":
            # 1. Proje: Canlı FlightRadar Bölgesel Radar
            res = fetch_flights_over_region(float(tool_args.get("latitude", 0)), float(tool_args.get("longitude", 0)), float(tool_args.get("radius_km", 100)), int(tool_args.get("limit", 15)))
        elif tool_name == "get_most_tracked_flights":
            # 1. Proje: Canlı FlightRadar En Çok İzlenenler
            res = fetch_most_tracked_flights(int(tool_args.get("limit", 10)))
        elif tool_name == "get_airport_info":
            # 1. Proje: Havalimanı Koordinat ve Detayları
            res = fetch_airport_info(str(tool_args.get("airport_code", "")))
        elif tool_name == "get_flights_above_speed":
            # 2. Proje: Kafka Hız Filtresi
            res = kafka_store.find_flights_above_speed(float(tool_args.get("min_speed_kmh", 800)), int(tool_args.get("limit", 15)))
        elif tool_name == "get_flights_over_region_from_kafka":
            # 2. Proje: Kafka Bölgesel Radar
            res = kafka_store.find_flights_over_region(float(tool_args.get("latitude", 0)), float(tool_args.get("longitude", 0)), float(tool_args.get("radius_km", 100)), int(tool_args.get("limit", 15)))
        elif tool_name == "search_airline_from_kafka":
            # 2. Proje: Kafka Havayolu Araması
            res = kafka_store.search_airline(str(tool_args.get("airline_code", "")), int(tool_args.get("limit", 10)))
        elif tool_name == "get_kafka_stream_stats":
            # 2. Proje: Kafka İstatistikleri
            res = kafka_store.get_telemetry_stats()
        elif tool_name == "refresh_kafka_stream":
            # 2. Proje: Kafka Akışını Yenile
            cnt = kafka_store.sync_from_kafka(int(tool_args.get("target_count", 1200)))
            res = {"status": "success", "message": f"Kafka'dan {cnt} uçuş tazelendi."}
        else:
            res = {"status": "error", "error": f"Bilinmeyen araç: {tool_name}"}
    except Exception as e:
        res = {"status": "error", "error": str(e)}

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    log_mcp_to_kafka(tool_name, tool_args, res, elapsed_ms)
    return res




KAFKA_MCP_DEFINITIONS = [
    d for d in LOCAL_MCP_DEFINITIONS if d["name"] in [
        "get_flight_from_kafka",
        "get_flights_above_speed",
        "get_flights_over_region_from_kafka",
        "search_airline_from_kafka",
        "get_kafka_stream_stats",
        "refresh_kafka_stream"
    ]
]

LIVE_MCP_DEFINITIONS = [
    d for d in LOCAL_MCP_DEFINITIONS if d["name"] in [
        "get_flight_info",
        "search_airline_flights",
        "get_flights_over_region",
        "get_most_tracked_flights",
        "get_airport_info"
    ]
]

KAFKA_SYSTEM_INSTRUCTION = (
    "Sen Apache Kafka 1200 Uçaklık Canlı Telemetri Deposu ve Havacılık Radarı Asistanısın.\n"
    "Kullanıcının sorularına yanıt vermek için SADECE ve SADECE sana sağlanan KAFKA MCP ARAÇLARINI KULLAN:\n"
    "1. Uçuş Bilgisi / Nerede / İrtifa / Hız Soruları: 'get_flight_from_kafka' aracını çağır.\n"
    "2. Belirli Bir Hızın Üzerindeki Uçaklar (örn: 800 km/s, 900 km/s üstü): 'get_flights_above_speed' aracını çağır.\n"
    "3. Bölgesel / Koordinat Soruları (örn: İstanbul, koordinat yarıçapı): 'get_flights_over_region_from_kafka' aracını çağır.\n"
    "4. Havayolu Filosu (örn: THY, Pegasus): 'search_airline_from_kafka' aracını çağır.\n"
    "5. Kafka İstatistikleri: 'get_kafka_stream_stats' aracını çağır.\n\n"
    "📌 KURALLAR:\n"
    "• Uçuş verilerini asla uydurma. Yalnızca Kafka tool'undan dönen gerçek değerleri kullan.\n"
    "• İrtifayı hem feet hem metre, hızı hem knot hem km/s cinsinden belirt.\n"
    "• Cevapları kısa, net, anlaşılır ve madde işaretli liste formatında sun."
)

LIVE_SYSTEM_INSTRUCTION = (
    "Sen FlightRadar24 Canlı ADS-B Uçuş Radarı ve AI Havacılık Asistanısın.\n"
    "Kullanıcının sorularına yanıt vermek için SADECE ve SADECE sana sağlanan CANLI FLIGHTRADAR24 ARAÇLARINI KULLAN:\n"
    "1. Uçuş Bilgisi / Nerede / İrtifa / Hız Soruları: 'get_flight_info' aracını çağır.\n"
    "2. Havayolu Aktif Uçuşları: 'search_airline_flights' aracını çağır.\n"
    "3. Bölgesel / Koordinat Radarı: 'get_flights_over_region' aracını çağır.\n"
    "4. En Çok İzlenen Canlı Uçuşlar: 'get_most_tracked_flights' aracını çağır.\n"
    "5. Havalimanı Detayları: 'get_airport_info' aracını çağır.\n\n"
    "📌 KURALLAR:\n"
    "• Uçuş verilerini asla uydurma. Yalnızca canlı FlightRadar tool'undan dönen değerleri kullan.\n"
    "• İrtifayı hem feet hem metre, hızı hem knot hem km/s cinsinden belirt.\n"
    "• Cevapları kısa, net ve madde işaretli liste formatında sun."
)


def build_gemini_tools(tool_definitions=None):
    """Converts local tool definitions into Gemini types.Tool format."""
    from google.genai import types
    definitions = tool_definitions if tool_definitions is not None else LOCAL_MCP_DEFINITIONS
    function_declarations = []
    for tool_def in definitions:
        raw_props = tool_def.get("parameters", {}).get("properties", {})
        clean_props = {}
        for prop_name, prop_info in raw_props.items():
            clean_props[prop_name] = {
                "type": prop_info.get("type", "STRING").upper(),
                "description": prop_info.get("description", "")
            }
        
        schema = {
            "type": "OBJECT",
            "properties": clean_props,
            "required": tool_def.get("parameters", {}).get("required", [])
        }
        function_declarations.append(
            types.FunctionDeclaration(
                name=tool_def["name"],
                description=tool_def["description"],
                parameters=schema
            )
        )
    return [types.Tool(function_declarations=function_declarations)]


def build_openai_tools(tool_definitions=None):
    """Converts local tool definitions to standard OpenAI / Groq tool definitions."""
    definitions = tool_definitions if tool_definitions is not None else LOCAL_MCP_DEFINITIONS
    tools = []
    for tool_def in definitions:
        tools.append({
            "type": "function",
            "function": {
                "name": tool_def["name"],
                "description": tool_def["description"],
                "parameters": tool_def.get("parameters", {"type": "object", "properties": {}})
            }
        })
    return tools


async def call_gemini_with_retry(genai_client, model: str, contents: list, config, max_retries: int = 3):
    """Calls Gemini with automatic retry and model fallback in case of temporary 503/429 spikes."""
    models_to_try = [model] + [m for m in FALLBACK_MODELS if m != model]
    
    last_error = None
    for current_model in models_to_try:
        for attempt in range(1, max_retries + 1):
            try:
                response = await genai_client.aio.models.generate_content(
                    model=current_model,
                    contents=contents,
                    config=config
                )
                return response, current_model
            except Exception as e:
                err_str = str(e)
                last_error = e
                is_transient = "503" in err_str or "429" in err_str or "UNAVAILABLE" in err_str or "high demand" in err_str
                
                if is_transient and attempt < max_retries:
                    wait_time = attempt * 1.5
                    await asyncio.sleep(wait_time)
                elif is_transient and attempt == max_retries:
                    break
                else:
                    raise e
                    
    raise last_error if last_error else Exception("Tüm model denemeleri başarısız oldu.")


# ============================================================
# Main AI Processing Function (Instant Direct Local Execution)
# ============================================================

async def ask_flight_agent(user_query: str, project_mode: str = "auto", mcp_url: Optional[str] = None) -> Dict[str, Any]:
    """Processes a natural language query using the configured LLM and direct MCP tools."""
    provider = LLM_PROVIDER
    tool_calls_executed = []

    # Determine tool set and instruction based on project mode
    mode = str(project_mode).lower().strip()
    if mode == "kafka":
        active_tools_defs = KAFKA_MCP_DEFINITIONS
        active_instruction = KAFKA_SYSTEM_INSTRUCTION
    elif mode == "live":
        active_tools_defs = LIVE_MCP_DEFINITIONS
        active_instruction = LIVE_SYSTEM_INSTRUCTION
    else:
        # Auto detection based on keywords
        q_lower = user_query.lower()
        if any(k in q_lower for k in ["kafka", "900", "800", "hız", "hızlı", "km/s", "km/h", "süpersonik", "depo", "havuz"]):
            active_tools_defs = KAFKA_MCP_DEFINITIONS
            active_instruction = KAFKA_SYSTEM_INSTRUCTION
        else:
            active_tools_defs = LOCAL_MCP_DEFINITIONS
            active_instruction = SYSTEM_INSTRUCTION


    # ============================================================
    # Provider: Gemini
    # ============================================================
    if provider == "gemini":
        from google import genai
        from google.genai import types

        if not GEMINI_API_KEY or GEMINI_API_KEY.strip() in ["", "your_gemini_api_key_here"]:
            return {
                "status": "error",
                "answer": "GEMINI_API_KEY .env dosyasında tanımlı değil.",
                "tool_calls": [],
                "model": GEMINI_MODEL,
                "provider": provider,
                "error": "Missing GEMINI_API_KEY"
            }

        genai_client = genai.Client(api_key=GEMINI_API_KEY)
        gemini_tools = build_gemini_tools(active_tools_defs)

        contents = [
            types.Content(
                role="user",
                parts=[types.Part.from_text(text=user_query)]
            )
        ]

        config = types.GenerateContentConfig(
            tools=gemini_tools,
            temperature=0.0,
            system_instruction=active_instruction
        )

        try:
            response, active_model = await call_gemini_with_retry(genai_client, GEMINI_MODEL, contents, config)
        except Exception as e:
            return {
                "status": "error",
                "answer": f"Gemini API Hatası: {e}",
                "tool_calls": [],
                "model": GEMINI_MODEL,
                "provider": provider,
                "error": str(e)
            }

        if response.function_calls:
            contents.append(response.candidates[0].content)
            tool_response_parts = []

            for function_call in response.function_calls:
                tool_name = function_call.name
                tool_args = function_call.args or {}

                # Execute tool directly in memory (0 ms)
                parsed_json = execute_local_mcp_tool(tool_name, tool_args)

                tool_calls_executed.append({
                    "name": tool_name,
                    "args": tool_args,
                    "result": parsed_json
                })

                tool_response_parts.append(
                    types.Part.from_function_response(
                        name=tool_name,
                        response={"output": parsed_json}
                    )
                )

            contents.append(types.Content(role="user", parts=tool_response_parts))

            try:
                final_response, _ = await call_gemini_with_retry(
                    genai_client,
                    active_model,
                    contents,
                    types.GenerateContentConfig(tools=gemini_tools, temperature=0.0, system_instruction=active_instruction)
                )
                answer = extract_text_from_response(final_response)
                return {
                    "status": "success",
                    "answer": clean_model_output(answer),
                    "tool_calls": tool_calls_executed,
                    "model": active_model,
                    "provider": provider
                }
            except Exception as e:
                return {
                    "status": "error",
                    "answer": f"Gemini Yanıt Oluşturma Hatası: {e}",
                    "tool_calls": tool_calls_executed,
                    "model": active_model,
                    "provider": provider,
                    "error": str(e)
                }
        else:
            answer = extract_text_from_response(response)
            return {
                "status": "success",
                "answer": clean_model_output(answer),
                "tool_calls": [],
                "model": active_model,
                "provider": provider
            }


    # ============================================================
    # Provider: OpenAI Compatible (Groq, OpenRouter, Ollama, OpenAI, DeepSeek)
    # ============================================================
    else:
        from openai import AsyncOpenAI

        if provider == "groq":
            api_key = GROQ_API_KEY
            base_url = "https://api.groq.com/openai/v1"
            model_name = GROQ_MODEL
        elif provider == "openrouter":
            api_key = OPENROUTER_API_KEY
            base_url = "https://openrouter.ai/api/v1"
            model_name = OPENROUTER_MODEL
        elif provider == "ollama":
            api_key = "ollama"
            base_url = OLLAMA_BASE_URL
            model_name = OLLAMA_MODEL
        elif provider == "deepseek":
            api_key = DEEPSEEK_API_KEY
            base_url = "https://api.deepseek.com/v1"
            model_name = DEEPSEEK_MODEL
        elif provider == "openai":
            api_key = OPENAI_API_KEY
            base_url = "https://api.openai.com/v1"
            model_name = OPENAI_MODEL
        else:
            api_key = os.getenv("API_KEY", "")
            base_url = None
            model_name = "default"

        if not api_key:
            return {
                "status": "error",
                "answer": f"{provider.upper()}_API_KEY .env dosyasında tanımlı değil.",
                "tool_calls": [],
                "model": model_name,
                "provider": provider,
                "error": f"Missing {provider.upper()}_API_KEY"
            }

        client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        openai_tools = build_openai_tools(active_tools_defs)

        messages = [
            {"role": "system", "content": active_instruction},
            {"role": "user", "content": user_query}
        ]


        try:
            chat_completion = await client.chat.completions.create(
                model=model_name,
                messages=messages,
                tools=openai_tools,
                tool_choice="auto",
                temperature=0.0
            )
            response_message = chat_completion.choices[0].message
        except Exception as e:
            return {
                "status": "error",
                "answer": f"{provider.upper()} API Hatası: {e}",
                "tool_calls": [],
                "model": model_name,
                "provider": provider,
                "error": str(e)
            }

        if response_message.tool_calls:
            messages.append(response_message)

            for tool_call in response_message.tool_calls:
                func_name = tool_call.function.name
                try:
                    func_args = json.loads(tool_call.function.arguments)
                except Exception:
                    func_args = {}

                parsed_json = execute_local_mcp_tool(func_name, func_args)
                result_text = json.dumps(parsed_json, ensure_ascii=False)

                tool_calls_executed.append({
                    "name": func_name,
                    "args": func_args,
                    "result": parsed_json
                })

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": result_text
                })

            try:
                final_completion = await client.chat.completions.create(
                    model=model_name,
                    messages=messages,
                    temperature=0.0
                )
                answer = final_completion.choices[0].message.content or "(Yanıt alınamadı)"
                return {
                    "status": "success",
                    "answer": clean_model_output(answer),
                    "tool_calls": tool_calls_executed,
                    "model": model_name,
                    "provider": provider
                }
            except Exception as e:
                return {
                    "status": "error",
                    "answer": f"{provider.upper()} Yanıt Oluşturma Hatası: {e}",
                    "tool_calls": tool_calls_executed,
                    "model": model_name,
                    "provider": provider,
                    "error": str(e)
                }
        else:
            answer = response_message.content or "(Yanıt alınamadı)"
            return {
                "status": "success",
                "answer": clean_model_output(answer),
                "tool_calls": [],
                "model": model_name,
                "provider": provider
            }
