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
FALLBACK_MODELS = list(dict.fromkeys([
    GEMINI_MODEL,
    "gemini-3.5-flash-lite",
    "gemini-3.7-flash"
]))


SYSTEM_INSTRUCTION = (
    "You are an expert aviation, aircraft telemetry, live radar, and Apache Kafka live flight telemetry stream AI assistant.\n"
    "To answer user questions, you MUST strictly and exclusively use the provided MCP tools.\n\n"
    "🎯 TOOL SELECTION GUIDE:\n"
    "1. Live FlightRadar24 queries: Use 'get_flight_info', 'search_airline_flights', 'get_flights_over_region', 'get_most_tracked_flights', 'get_airport_info'.\n"
    "2. Kafka stream telemetry queries: Use the unified 'query_kafka_stream' tool. It supports compound filtering combining country='TR', region='Ankara'/'Istanbul', query, airline, min_speed_kmh, coordinates, and get_stats simultaneously.\n\n"
    "📌 RESPONSE FORMAT GUIDELINES:\n"
    "• Never hallucinate flight telemetry or models. Only use exact data returned by the tools.\n"
    "• Report altitude in both feet and meters (e.g. 37,000 ft / ~11,277 m).\n"
    "• Report ground speed in both knots and km/h (e.g. 480 kts / ~889 km/h).\n"
    "• Highlight aircraft model (e.g. Boeing 777-300ER, Airbus A321neo) and route (e.g. IST ➔ JFK).\n"
    "• Provide clear, concise, bulleted responses."
)


def clean_model_output(text: str) -> str:
    """Removes internal reasoning tags or cleanup formatting."""
    if not text:
        return ""
    cleaned = re.sub(r"<think>[\s\S]*?</think>", "", text)
    return cleaned.strip()


def extract_text_from_response(response) -> str:
    """Extracts text from candidate parts safely without triggering non-text warnings."""
    if not response:
        return "(No response received)"
    if hasattr(response, "candidates") and response.candidates:
        cand = response.candidates[0]
        if hasattr(cand, "content") and cand.content and cand.content.parts:
            text_parts = [p.text for p in cand.content.parts if hasattr(p, "text") and p.text]
            if text_parts:
                return clean_model_output("\n".join(text_parts))
    try:
        if hasattr(response, "text") and response.text:
            return clean_model_output(response.text)
    except Exception:
        pass
    return "(No response received)"




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
# 11 MCP Tools Direct Definitions & Remote Executor
# ============================================================

LOCAL_MCP_DEFINITIONS = [
    {
        "name": "get_flight_info",
        "description": "Retrieves real-time live ADS-B coordinates, altitude, ground speed, route, and aircraft model for a flight number (e.g. 'TK10', 'PC2020') or tail registration from FlightRadar24.",
        "parameters": {
            "type": "object",
            "properties": {
                "flight_code": {"type": "string", "description": "Flight number (e.g. 'TK10', 'THY10') or registration (e.g. 'TC-JYA')"}
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
        "description": "Finds live flights flying within national or regional airspace (e.g. Turkey / 'TR', 'Ankara', 'Istanbul') on FlightRadar24. Supports speed filtering.",
        "parameters": {
            "type": "object",
            "properties": {
                "region": {"type": "string", "description": "Target province or region name. As an intelligent assistant, resolve any colloquial user phrasing (e.g. 'Palandöken', 'Erzurum kenti/şehri', 'Dadaşlar diyarı', 'Boğaz', 'Kordon') to the official province name (e.g. 'Erzurum', 'İstanbul', 'İzmir') or macro-region ('MARMARA', 'EGE', 'TR'). The backend automatically evaluates exact 81-province boundary polygons."},
                "min_speed_kmh": {"type": "number", "description": "Minimum ground speed filter in km/h. Map expressions like 'hızlı uçaklar', 'ses hızına yakın', 'süpersonik' to appropriate values (e.g. 800 or 900)."},
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
        "description": "Retrieves airport coordinates, city, country, and details for a given 3-letter IATA or 4-letter ICAO airport code (e.g. 'IST', 'SAW', 'LHR', 'JFK').",
        "parameters": {
            "type": "object",
            "properties": {
                "airport_code": {"type": "string", "description": "Airport IATA or ICAO code (e.g. 'IST', 'SAW', 'LHR')"}
            },
            "required": ["airport_code"]
        }
    },
    {
        "name": "query_kafka_stream",
        "description": "Unified multi-filter query tool for the Apache Kafka live flight telemetry stream. Supports compound queries combining ground speed, province/region boundaries, airline, flight number, and stream statistics simultaneously.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Specific flight number (e.g. 'TK10'), callsign ('THY10'), or registration ('TC-LJA')"},
                "region": {"type": "string", "description": "Target province or region name. As an intelligent assistant, resolve any colloquial user phrasing (e.g. 'Palandöken', 'Erzurum kenti/şehri', 'Dadaşlar diyarı', 'Boğaz', 'Kordon', 'Başkent') to the official Turkish province name (e.g. 'Erzurum', 'İstanbul', 'İzmir', 'Ankara') or macro-region ('MARMARA', 'EGE', 'TR'). The backend automatically evaluates exact 81-province boundary polygons."},
                "airline": {"type": "string", "description": "Airline code. Map colloquial airline names (e.g. 'Türk Hava Yolları' -> 'THY', 'Pegasus' -> 'PGT', 'AJet' -> 'TKJ' or 'VF', 'Lufthansa' -> 'DLH', 'SunExpress' -> 'SXS')."},
                "min_speed_kmh": {"type": "number", "description": "Minimum ground speed filter in km/h. Map expressions like 'hızlı uçaklar', 'ses hızına yakın', 'süpersonik' to appropriate values (e.g. 800 or 900)."},
                "min_altitude_feet": {"type": "number", "description": "Minimum altitude filter in feet. Convert user metric requests like '10 bin metre üzeri' (~32,800 ft) to feet."},
                "get_stats": {"type": "boolean", "description": "Set to true to retrieve overall Kafka stream statistics (max/avg speed, altitude, airline count)"},
                "limit": {"type": "integer", "description": "Maximum number of flights to return (default: 15)"}
            }
        }
    }
]


async def execute_mcp_tool(tool_name: str, tool_args: dict, mcp_url: Optional[str] = None) -> dict:
    """Executes the requested MCP tool directly via the local registry for maximum performance and zero network latency."""
    try:
        from server import MCP_TOOLS_REGISTRY
        if tool_name in MCP_TOOLS_REGISTRY:
            fn = MCP_TOOLS_REGISTRY[tool_name]
            if asyncio.iscoroutinefunction(fn):
                return await fn(**tool_args)
            return fn(**tool_args)
        return {"status": "error", "error": f"Tool '{tool_name}' not found."}
    except Exception as e:
        print(f"❌ [Tool Error] {tool_name}: {e}")
        return {"status": "error", "error": str(e)}


KAFKA_MCP_DEFINITIONS = [d for d in LOCAL_MCP_DEFINITIONS if d["name"] == "query_kafka_stream"]
LIVE_MCP_DEFINITIONS = [d for d in LOCAL_MCP_DEFINITIONS if d["name"] != "query_kafka_stream"]

KAFKA_SYSTEM_INSTRUCTION = (
    "You are an AI Aviation Assistant specialized in the Apache Kafka live aircraft telemetry stream.\n"
    "To answer questions, you MUST strictly use ONLY the unified KAFKA MCP TOOL: 'query_kafka_stream'.\n\n"
    "💡 HOW TO USE 'query_kafka_stream':\n"
    "• Single flight info / search: pass query='TK10' or registration.\n"
    "• High-speed / supersonic filtering: pass min_speed_kmh=800 or 900.\n"
    "• 81 Turkish Provinces & Regions: You handle natural language entity resolution! If the user says 'Palandöken', 'Erzurum kenti/şehri', 'Dadaşlar diyarı', 'Boğaz', 'Kordon', 'Başkent' or any landmark/district, resolve it to the canonical province name (e.g. region='Erzurum', region='Ankara', region='İstanbul') or region (region='TR', region='MARMARA'). The Python backend calculates the exact polygon boundary with sub-millisecond ray-casting!\n"
    "• Airline colloquialisms: Resolve 'Türk Hava Yolları' -> airline='THY', 'Pegasus' -> airline='PGT', 'AJet' -> airline='TKJ', etc.\n"
    "• Metric to imperial conversion: Convert user metric requests (e.g. '10 bin metre üstü' -> min_altitude_feet=32800).\n"
    "• COMPOUND QUERIES: Combine any parameters! (e.g. Erzurum + min_speed_kmh=800 -> pass region='Erzurum', min_speed_kmh=800).\n"
    "• Kafka stream statistics: pass get_stats=true.\n\n"
    "📌 RULES:\n"
    "• Never fabricate data. Only use exact values returned from query_kafka_stream.\n"
    "• State altitude in both feet and meters, speed in both knots and km/h.\n"
    "• Provide concise, clean, bulleted summaries."
)

LIVE_SYSTEM_INSTRUCTION = (
    "You are an AI Aviation Assistant specialized in live FlightRadar24 ADS-B aircraft radar tracking.\n"
    "To answer questions, you MUST strictly use ONLY the provided LIVE FLIGHTRADAR24 MCP TOOLS:\n"
    "1. Flight Info / Location / Altitude / Speed: Call 'get_flight_info'.\n"
    "2. Airline Active Airborne Flights: Call 'search_airline_flights' (resolve 'Türk Hava Yolları' -> 'THY', etc.).\n"
    "3. Regional Airspace Radar: Call 'get_flights_over_region'.\n"
    "   • For 81 Turkish Provinces & Regions: Resolve user terms ('Palandöken', 'Erzurum kenti', 'Başkent', 'Türkiye geneli', etc.) to the canonical province name (e.g. region='Erzurum', region='Ankara', region='TR'). The backend automatically evaluates exact GeoJSON polygon borders!\n"
    "4. Top Most-Tracked Flights: Call 'get_most_tracked_flights'.\n"
    "5. Airport Details: Call 'get_airport_info'.\n\n"
    "📌 RULES:\n"
    "• Never fabricate data. Only use exact values returned from the live FlightRadar tool.\n"
    "• State altitude in both feet and meters, speed in both knots and km/h.\n"
    "• Provide concise, clean, bulleted summaries."
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
                    
    raise last_error if last_error else Exception("All model attempts failed.")


# ============================================================
# Main AI Processing Function (Remote HTTP MCP Execution)
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
        if any(k in q_lower for k in ["kafka", "900", "800", "speed", "fast", "km/s", "km/h", "supersonic", "buffer", "stream", "hız", "hızlı"]):
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
                "answer": "GEMINI_API_KEY is not configured in .env file.",
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
                "answer": f"Gemini API Error: {e}",
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

                # Execute tool remotely via HTTP RPC against MCP Server node
                parsed_json = await execute_mcp_tool(tool_name, tool_args, mcp_url=mcp_url)

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
                    "answer": f"Gemini Response Error: {e}",
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
                "answer": f"{provider.upper()}_API_KEY is not configured in .env file.",
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
                "answer": f"{provider.upper()} API Error: {e}",
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

                parsed_json = await execute_mcp_tool(func_name, func_args, mcp_url=mcp_url)
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
                answer = final_completion.choices[0].message.content or "(No response received)"
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
                    "answer": f"{provider.upper()} Response Error: {e}",
                    "tool_calls": tool_calls_executed,
                    "model": model_name,
                    "provider": provider,
                    "error": str(e)
                }
        else:
            answer = response_message.content or "(No response received)"
            return {
                "status": "success",
                "answer": clean_model_output(answer),
                "tool_calls": [],
                "model": model_name,
                "provider": provider
            }
