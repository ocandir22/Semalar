"""
Core LLM Client — Multi-Provider AI Engine (Gemini, Groq, OpenRouter, Ollama, OpenAI, DeepSeek)
Provides unified function calling, resilient error handling, model fallbacks,
and real-time ANSI terminal observability for Semalar independent projects.
"""

import os
import sys
import json
import re
import asyncio
import time
from typing import Dict, Any, List, Optional, Callable
from dotenv import load_dotenv

try:
    from .audit_logger import log_mcp_request
except ImportError:
    try:
        from core.audit_logger import log_mcp_request
    except ImportError:
        def log_mcp_request(tool_name, args, result, elapsed_ms): pass

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Load environment variables
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(BASE_DIR)
ROOT_DIR = os.path.dirname(BACKEND_DIR)
load_dotenv(os.path.join(ROOT_DIR, ".env"))
load_dotenv(os.path.join(BACKEND_DIR, ".env"))
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

# Fallback models for Gemini
FALLBACK_MODELS = list(dict.fromkeys([
    GEMINI_MODEL,
    "gemini-3.5-flash-lite",
    "gemini-3.7-flash"
]))

# ANSI Terminal Color Constants
_ANSI_RESET = "\033[0m"
_ANSI_BOLD = "\033[1m"
_ANSI_CYAN = "\033[96m"
_ANSI_GREEN = "\033[92m"
_ANSI_YELLOW = "\033[93m"
_ANSI_MAGENTA = "\033[95m"
_ANSI_BLUE = "\033[94m"
_ANSI_GRAY = "\033[90m"


def get_agent_info() -> Dict[str, str]:
    """Returns active LLM provider and model information."""
    model_map = {
        "gemini": GEMINI_MODEL,
        "groq": GROQ_MODEL,
        "openrouter": OPENROUTER_MODEL,
        "ollama": OLLAMA_MODEL,
        "openai": OPENAI_MODEL,
        "deepseek": DEEPSEEK_MODEL,
    }
    return {
        "provider": LLM_PROVIDER.upper(),
        "model": model_map.get(LLM_PROVIDER, "unknown"),
        "mcp_url": "http://localhost:8000/mcp"
    }


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
    try:
        candidates = getattr(response, "candidates", None)
        if candidates and len(candidates) > 0:
            content = getattr(candidates[0], "content", None)
            if content and getattr(content, "parts", None):
                text_parts = []
                for part in content.parts:
                    text_val = getattr(part, "text", None)
                    if text_val:
                        text_parts.append(text_val)
                if text_parts:
                    return "".join(text_parts)
    except Exception:
        pass

    try:
        return response.text or ""
    except Exception:
        return str(response)


def _print_agent_banner(user_query: str, project_label: str, provider: str, model_name: str):
    """Prints a clear, formatted terminal banner for an incoming AI user query."""
    print(f"\n{_ANSI_CYAN}══════════════════════════════════════════════════════════════════════{_ANSI_RESET}")
    print(f"{_ANSI_BOLD}🤖 [LLM AGENT] Yeni Kullanıcı Talebi Alındı{_ANSI_RESET}")
    print(f"  {_ANSI_YELLOW}💬 Soru        :{_ANSI_RESET} \"{user_query}\"")
    print(f"  {_ANSI_MAGENTA}🎯 Proje Modu  :{_ANSI_RESET} {project_label}")
    print(f"  {_ANSI_BLUE}🧠 Model/Sağlayıcı:{_ANSI_RESET} {provider.upper()} ({model_name})")
    print(f"{_ANSI_CYAN}──────────────────────────────────────────────────────────────────────{_ANSI_RESET}")


def _print_tool_decision(tool_name: str, tool_args: dict):
    """Prints the LLM's tool calling decision to the terminal."""
    args_json = json.dumps(tool_args, ensure_ascii=False)
    print(f"  {_ANSI_YELLOW}⚙️  [LLM Tool Çağrı Kararı]:{_ANSI_RESET}")
    print(f"     {_ANSI_BOLD}Araç        :{_ANSI_RESET} {_ANSI_CYAN}{tool_name}{_ANSI_RESET}")
    print(f"     {_ANSI_BOLD}Parametreler:{_ANSI_RESET} {_ANSI_GRAY}{args_json}{_ANSI_RESET}")


def _print_tool_result(tool_name: str, parsed_json: dict, elapsed_ms: float):
    """Prints a concise summary of the executed MCP tool result to the terminal."""
    status = parsed_json.get("status", "unknown")
    status_icon = "✅" if status == "success" else ("⚠️" if status == "empty" else "❌")

    summary_lines = []
    if tool_name == "query_kafka_stream":
        tot = parsed_json.get("total_matched", 0)
        ret = parsed_json.get("returned_count", 0)
        prov = parsed_json.get("filter_province")
        prov_str = f" | İl: {prov}" if prov else ""
        summary_lines.append(f"{ret} uçak listelendi (Toplam: {tot}{prov_str})")
        flights = parsed_json.get("flights", [])[:3]
        for idx, fl in enumerate(flights, 1):
            route = fl.get("route")
            route_str = route.get("display") if isinstance(route, dict) else str(route or "N/A")
            summary_lines.append(
                f"  {idx}. {fl.get('flight_number') or fl.get('callsign')} ({fl.get('aircraft_model')}) "
                f"➔ Hız: {fl.get('telemetry', {}).get('ground_speed_kmh')} km/s | "
                f"İrtifa: {fl.get('telemetry', {}).get('altitude_feet')} ft | Rota: {route_str}"
            )
        if len(parsed_json.get("flights", [])) > 3:
            summary_lines.append(f"  ... ve {len(parsed_json.get('flights', [])) - 3} uçak daha.")
    elif tool_name == "get_flight_info":
        if status == "success":
            aircraft = parsed_json.get("aircraft", {})
            model = aircraft.get("model") if isinstance(aircraft, dict) else aircraft
            reg = aircraft.get("registration") if isinstance(aircraft, dict) else ""
            reg_str = f" ({reg})" if reg else ""
            summary_lines.append(
                f"Uçuş: {parsed_json.get('flight_number')} | Model: {model}{reg_str} | "
                f"Hız: {parsed_json.get('ground_speed_kmh')} km/s | İrtifa: {parsed_json.get('altitude_feet')} ft"
            )
        else:
            summary_lines.append(f"Mesaj: {parsed_json.get('message', 'Bulunamadı')}")
    elif tool_name == "search_airline_flights":
        tot = parsed_json.get("total_active_flights_found", 0)
        ret = parsed_json.get("returned_count", 0)
        summary_lines.append(f"Havayolu: {parsed_json.get('airline_code')} | {ret}/{tot} uçuş listelendi")
    elif tool_name == "get_flights_over_region":
        tot = parsed_json.get("total_flights_found", 0)
        reg_name = parsed_json.get("applied_region", "Bölge")
        summary_lines.append(f"{reg_name} üzerinde {tot} uçuş tespit edildi")
    elif tool_name == "get_airport_info":
        summary_lines.append(f"{parsed_json.get('name')} ({parsed_json.get('airport_code')}) | {parsed_json.get('city')}, {parsed_json.get('country')}")
    elif tool_name == "get_most_tracked_flights":
        tot = parsed_json.get("total_tracked", 0)
        summary_lines.append(f"En çok takip edilen {tot} uçuş getirildi")
    else:
        summary_lines.append(f"İşlem tamamlandı ({status})")

    print(f"  {_ANSI_GREEN}⚡ [MCP Tool Sonucu — {elapsed_ms:.1f}ms]:{_ANSI_RESET}")
    print(f"     {_ANSI_BOLD}Durum       :{_ANSI_RESET} {status_icon} {status}")
    if summary_lines:
        print(f"     {_ANSI_BOLD}Eşleşme     :{_ANSI_RESET} {summary_lines[0]}")
        for sub in summary_lines[1:]:
            print(f"       {_ANSI_GRAY}{sub}{_ANSI_RESET}")


def _print_agent_final_response(answer: str, elapsed_total_s: float):
    """Prints the final synthesized LLM response and total cycle time to the terminal."""
    print(f"{_ANSI_CYAN}──────────────────────────────────────────────────────────────────────{_ANSI_RESET}")
    print(f"  {_ANSI_GREEN}📝 [LLM Final Yanıtı — Toplam Süre: {elapsed_total_s:.2f}s]:{_ANSI_RESET}")
    lines = answer.strip().split("\n")
    for line in lines[:8]:
        print(f"     {line}")
    if len(lines) > 8:
        print(f"     {_ANSI_GRAY}... ({len(lines) - 8} satır daha){_ANSI_RESET}")
    print(f"{_ANSI_CYAN}══════════════════════════════════════════════════════════════════════\n{_ANSI_RESET}")


def build_gemini_tools(tool_definitions: List[Dict[str, Any]]):
    """Converts local tool definitions into Gemini types.Tool format."""
    from google.genai import types
    function_declarations = []
    for tool_def in tool_definitions:
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


def build_openai_tools(tool_definitions: List[Dict[str, Any]]):
    """Converts local tool definitions to standard OpenAI / Groq tool definitions."""
    tools = []
    for tool_def in tool_definitions:
        tools.append({
            "type": "function",
            "function": {
                "name": tool_def["name"],
                "description": tool_def["description"],
                "parameters": tool_def.get("parameters", {"type": "object", "properties": {}})
            }
        })
    return tools


def build_thought_process(
    user_query: str,
    tool_calls_executed: list,
    reasoning_text: Optional[str] = None,
    total_elapsed_seconds: float = 0.0
) -> Dict[str, Any]:
    """Builds a structured step-by-step thinking and MCP reasoning breakdown for the UI thinking block."""
    steps = []

    # 1. Intent & Parameter Mapping Stage
    steps.append({
        "stage": "intent",
        "icon": "🎯",
        "title": "Kullanıcı Niyeti & Parametre Çözümleme",
        "detail": f"Kullanıcı sorgusu incelendi: \"{user_query}\". Türkiye 81 il poligonu, irtifa, hız ve havayolu filtreleri haritalandı."
    })

    # 2. Tool Execution Stage
    if tool_calls_executed:
        for tc in tool_calls_executed:
            t_name = tc.get("name", "query_kafka_stream")
            t_args = tc.get("args", {})
            t_res = tc.get("result", {})

            matched_count = 0
            if isinstance(t_res, dict):
                matched_count = t_res.get("total_matches", len(t_res.get("flights", [])))
            elif isinstance(t_res, list):
                matched_count = len(t_res)

            steps.append({
                "stage": "tool_call",
                "icon": "⚙️",
                "title": f"FastMCP Tool Kararı: `{t_name}`",
                "detail": f"Parametreler: {json.dumps(t_args, ensure_ascii=False)}",
                "result_summary": f"Kafka topic 'live-flights' havuzundan {matched_count} eşleşen telemetri kaydı çekildi."
            })
    else:
        steps.append({
            "stage": "direct_reasoning",
            "icon": "💡",
            "title": "Doğrudan Yanıt Kararı",
            "detail": "Kullanıcı sorusu genel havacılık / radar kavramı içerdiğinden ek telemetri filtrelemesine ihtiyaç duyulmadı."
        })

    # 3. Telemetry Synthesis Stage
    steps.append({
        "stage": "synthesis",
        "icon": "📝",
        "title": "Telemetri Sentezi ve Yanıt Üretimi",
        "detail": "Elde edilen gerçek zamanlı telemetriler (irtifa, hız, rota, uçak tipi) analiz edilerek Türkçe dilbilgisine uygun olarak hazırlandı."
    })

    summary = (
        f"Kullanıcı talebi analiz edildi, {len(tool_calls_executed)} MCP aracı ile Kafka telemetrisi sorgulandı."
        if tool_calls_executed else "Kullanıcı talebi doğrudan yanıtlandı."
    )

    return {
        "summary": summary,
        "duration_seconds": round(total_elapsed_seconds, 2),
        "steps": steps,
        "raw_reasoning": reasoning_text if reasoning_text else None
    }


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
                    wait_sec = attempt * 1.5
                    print(f"  \033[93m⏳ [Gemini Retry]: Model '{current_model}' geçici olarak meşgul. {wait_sec:.1f}s bekleniyor... (Deneme {attempt}/{max_retries})\033[0m")
                    await asyncio.sleep(wait_sec)
                    continue
                break

    raise last_error or Exception("All Gemini models failed")


async def run_llm_cycle(
    user_query: str,
    system_instruction: str,
    tool_definitions: List[Dict[str, Any]],
    tool_executor: Callable[[str, dict], Any],
    project_label: str = "Aviation Project"
) -> Dict[str, Any]:
    """Executes a full AI Agent turn (reasoning ➔ tool calling ➔ result synthesis)
    for any independent project using the configured LLM provider.
    """
    t_start = time.perf_counter()
    provider = LLM_PROVIDER
    tool_calls_executed = []

    # Print request banner
    model_name = get_agent_info()["model"]
    _print_agent_banner(user_query, project_label, provider, model_name)

    # 1. Provider: Gemini
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
        gemini_tools = build_gemini_tools(tool_definitions)

        contents = [types.Content(role="user", parts=[types.Part.from_text(text=user_query)])]
        config = types.GenerateContentConfig(
            tools=gemini_tools,
            temperature=0.0,
            system_instruction=system_instruction
        )

        try:
            response, active_model = await call_gemini_with_retry(genai_client, GEMINI_MODEL, contents, config)
        except Exception as e:
            print(f"  \033[91m❌ [LLM Hatası]: Gemini API Error: {e}\033[0m")
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

                _print_tool_decision(tool_name, tool_args)

                t_tool_start = time.perf_counter()
                if asyncio.iscoroutinefunction(tool_executor):
                    parsed_json = await tool_executor(tool_name, tool_args)
                else:
                    parsed_json = tool_executor(tool_name, tool_args)
                t_tool_elapsed = (time.perf_counter() - t_tool_start) * 1000

                _print_tool_result(tool_name, parsed_json, t_tool_elapsed)
                log_mcp_request(tool_name, tool_args, parsed_json, t_tool_elapsed)

                tool_calls_executed.append({
                    "name": tool_name,
                    "args": tool_args,
                    "result": parsed_json
                })

                tool_response_parts.append(
                    types.Part.from_function_response(name=tool_name, response={"output": parsed_json})
                )

            contents.append(types.Content(role="user", parts=tool_response_parts))

            try:
                final_response, _ = await call_gemini_with_retry(
                    genai_client,
                    active_model,
                    contents,
                    types.GenerateContentConfig(tools=gemini_tools, temperature=0.0, system_instruction=system_instruction)
                )
                answer = extract_text_from_response(final_response)
                cleaned_answer = clean_model_output(answer)
                _print_agent_final_response(cleaned_answer, time.perf_counter() - t_start)

                thought_process = build_thought_process(
                    user_query=user_query,
                    tool_calls_executed=tool_calls_executed,
                    reasoning_text=None,
                    total_elapsed_seconds=time.perf_counter() - t_start
                )

                return {
                    "status": "success",
                    "answer": cleaned_answer,
                    "tool_calls": tool_calls_executed,
                    "thought_process": thought_process,
                    "model": active_model,
                    "provider": provider
                }
            except Exception as e:
                print(f"  \033[91m❌ [LLM Hatası]: Gemini Response Error: {e}\033[0m")
                return {
                    "status": "error",
                    "answer": f"Gemini Response Error: {e}",
                    "tool_calls": tool_calls_executed,
                    "model": active_model,
                    "provider": provider,
                    "error": str(e)
                }
        else:
            print(f"  {_ANSI_GRAY}ℹ️  [LLM Kararı]: Doğrudan Yanıt (Tool çağrısı gerekmedi){_ANSI_RESET}")
            answer = extract_text_from_response(response)
            cleaned_answer = clean_model_output(answer)
            _print_agent_final_response(cleaned_answer, time.perf_counter() - t_start)

            thought_process = build_thought_process(
                user_query=user_query,
                tool_calls_executed=[],
                reasoning_text=None,
                total_elapsed_seconds=time.perf_counter() - t_start
            )

            return {
                "status": "success",
                "answer": cleaned_answer,
                "tool_calls": [],
                "thought_process": thought_process,
                "model": active_model,
                "provider": provider
            }

    # 2. Provider: OpenAI Compatible (Groq, OpenRouter, Ollama, OpenAI, DeepSeek)
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
            api_key = GROQ_API_KEY
            base_url = "https://api.groq.com/openai/v1"
            model_name = GROQ_MODEL

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
        tools = build_openai_tools(tool_definitions)

        messages = [
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": user_query}
        ]

        try:
            call_kwargs = {"model": model_name, "messages": messages, "temperature": 0.0}
            if tools:
                call_kwargs["tools"] = tools
                call_kwargs["tool_choice"] = "auto"

            completion = await client.chat.completions.create(**call_kwargs)
            message = completion.choices[0].message
        except Exception as e:
            print(f"  \033[91m❌ [LLM Hatası]: {provider.upper()} API Error: {e}\033[0m")
            return {
                "status": "error",
                "answer": f"{provider.upper()} API Error: {e}",
                "tool_calls": [],
                "model": model_name,
                "provider": provider,
                "error": str(e)
            }

        reasoning_text = getattr(message, "reasoning_content", None) or getattr(message, "reasoning", None)

        if message.tool_calls:
            messages.append(message)
            for tool_call in message.tool_calls:
                t_id = tool_call.id
                t_name = tool_call.function.name
                t_args_str = tool_call.function.arguments or "{}"
                try:
                    t_args = json.loads(t_args_str)
                except Exception:
                    t_args = {}

                _print_tool_decision(t_name, t_args)

                t_tool_start = time.perf_counter()
                if asyncio.iscoroutinefunction(tool_executor):
                    tool_result = await tool_executor(t_name, t_args)
                else:
                    tool_result = tool_executor(t_name, t_args)
                t_tool_elapsed = (time.perf_counter() - t_tool_start) * 1000

                _print_tool_result(t_name, tool_result, t_tool_elapsed)
                log_mcp_request(t_name, t_args, tool_result, t_tool_elapsed)

                tool_calls_executed.append({
                    "name": t_name,
                    "args": t_args,
                    "result": tool_result
                })

                messages.append({
                    "role": "tool",
                    "tool_call_id": t_id,
                    "name": t_name,
                    "content": json.dumps(tool_result, ensure_ascii=False)
                })

            try:
                final_completion = await client.chat.completions.create(
                    model=model_name,
                    messages=messages,
                    temperature=0.0
                )
                raw_ans = final_completion.choices[0].message.content or ""
                cleaned_ans = clean_model_output(raw_ans)
                _print_agent_final_response(cleaned_ans, time.perf_counter() - t_start)

                final_msg = final_completion.choices[0].message
                if not reasoning_text:
                    reasoning_text = getattr(final_msg, "reasoning_content", None) or getattr(final_msg, "reasoning", None)

                thought_process = build_thought_process(
                    user_query=user_query,
                    tool_calls_executed=tool_calls_executed,
                    reasoning_text=reasoning_text,
                    total_elapsed_seconds=time.perf_counter() - t_start
                )

                return {
                    "status": "success",
                    "answer": cleaned_ans,
                    "tool_calls": tool_calls_executed,
                    "thought_process": thought_process,
                    "model": model_name,
                    "provider": provider
                }
            except Exception as e:
                print(f"  \033[91m❌ [LLM Hatası]: {provider.upper()} Response Error: {e}\033[0m")
                return {
                    "status": "error",
                    "answer": f"{provider.upper()} Response Error: {e}",
                    "tool_calls": tool_calls_executed,
                    "model": model_name,
                    "provider": provider,
                    "error": str(e)
                }
        else:
            print(f"  {_ANSI_GRAY}ℹ️  [LLM Kararı]: Doğrudan Yanıt (Tool çağrısı gerekmedi){_ANSI_RESET}")
            cleaned_ans = clean_model_output(message.content or "")
            _print_agent_final_response(cleaned_ans, time.perf_counter() - t_start)

            thought_process = build_thought_process(
                user_query=user_query,
                tool_calls_executed=[],
                reasoning_text=reasoning_text,
                total_elapsed_seconds=time.perf_counter() - t_start
            )

            return {
                "status": "success",
                "answer": cleaned_ans,
                "tool_calls": [],
                "thought_process": thought_process,
                "model": model_name,
                "provider": provider
            }
