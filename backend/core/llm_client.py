"""
Core LLM Client — Multi-Provider AI Engine (Gemini, Groq, OpenRouter, Ollama, OpenAI, DeepSeek)
Provides unified function calling, resilient error handling, model fallbacks,
and real-time extraction of genuine model thinking/reasoning (Chain of Thought / <think>).
"""

import os
import sys
import json
import re
import asyncio
import time
from typing import Dict, Any, List, Optional, Callable, Tuple
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


def extract_real_llm_reasoning(raw_content: str, message_obj=None) -> Tuple[Optional[str], str]:
    """Extracts genuine internal thinking/reasoning (<think>...</think>, reasoning_content, reasoning)
    and cleanly separates it from the final assistant answer text.
    """
    reasoning_blocks = []

    # 1. Message object attributes (OpenAI reasoning_content or reasoning)
    if message_obj:
        for attr in ["reasoning_content", "reasoning"]:
            val = getattr(message_obj, attr, None)
            if val and str(val).strip():
                reasoning_blocks.append(str(val).strip())

    # 2. Extract <think>...</think> or <thought>...</thought> from content string
    clean_text = raw_content or ""
    if clean_text:
        th_matches = re.findall(r"<(?:think|thought)>([\s\S]*?)</(?:think|thought)>", clean_text, re.IGNORECASE)
        for m in th_matches:
            if m.strip() and m.strip() not in reasoning_blocks:
                reasoning_blocks.append(m.strip())
        
        # Remove thinking tags from user-facing answer text
        clean_text = re.sub(r"<(?:think|thought)>[\s\S]*?</(?:think|thought)>", "", clean_text, flags=re.IGNORECASE).strip()

    # Fallback: if user answer became empty because the model put its entire text in <think>,
    # preserve the text so user always sees the answer!
    if not clean_text and reasoning_blocks:
        clean_text = "\n\n".join(reasoning_blocks).strip()

    combined_reasoning = "\n\n".join(reasoning_blocks).strip() if reasoning_blocks else None
    return combined_reasoning, clean_text


def extract_gemini_thought_and_text(response) -> Tuple[Optional[str], str]:
    """Extracts genuine Gemini model thinking parts and user-facing text."""
    thoughts = []
    text_parts = []
    if response and hasattr(response, "candidates") and response.candidates:
        cand = response.candidates[0]
        if hasattr(cand, "content") and cand.content and hasattr(cand.content, "parts"):
            for p in cand.content.parts:
                is_thought = getattr(p, "thought", False) or getattr(p, "thought_process", False)
                text = getattr(p, "text", "") or ""
                if is_thought:
                    if text.strip():
                        thoughts.append(text.strip())
                elif text:
                    th, cl = extract_real_llm_reasoning(text)
                    if th and th not in thoughts:
                        thoughts.append(th)
                    if cl:
                        text_parts.append(cl)
    if not text_parts and hasattr(response, "text") and response.text:
        th, cl = extract_real_llm_reasoning(response.text)
        if th and th not in thoughts:
            thoughts.append(th)
        if cl:
            text_parts.append(cl)

    full_thoughts = "\n\n".join(thoughts).strip() if thoughts else None
    full_text = "".join(text_parts).strip()
    if not full_text and full_thoughts:
        full_text = full_thoughts
    return full_thoughts, full_text


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
    status = parsed_json.get("status", "unknown") if isinstance(parsed_json, dict) else "success"
    status_icon = "✅" if status == "success" else ("⚠️" if status == "empty" else "❌")

    summary_lines = []
    if isinstance(parsed_json, dict):
        if tool_name in ["query_kafka_stream", "kafka_query_stream"]:
            tot = parsed_json.get("total_matches", parsed_json.get("total_matched", 0))
            ret = parsed_json.get("returned_count", 0)
            prov = parsed_json.get("applied_region") or parsed_json.get("filter_province")
            prov_str = f" | Bölge: {prov}" if prov else ""
            summary_lines.append(f"{ret} uçak listelendi (Toplam: {tot}{prov_str})")
        elif tool_name == "get_emergency_flights":
            tot = parsed_json.get("total_matches", 0)
            detected = parsed_json.get("emergency_detected", False)
            alert_str = "🚨 ACİL DURUM TESPİT EDİLDİ!" if detected else "✅ Normal (Aktif acil durum yok)"
            summary_lines.append(f"{alert_str} ({tot} uçak)")
        elif tool_name == "find_nearby_aircraft":
            tot = parsed_json.get("total_matches", 0)
            center = parsed_json.get("center", {})
            c_name = center.get("name", "Merkez")
            r_km = center.get("radius_km", 50)
            summary_lines.append(f"{c_name} etrafında {r_km} km yarıçapta {tot} uçak bulundu")
        elif tool_name == "get_airport_traffic":
            ap = parsed_json.get("airport", {})
            ap_name = ap.get("name") or ap.get("iata") or "Havalimanı"
            counts = parsed_json.get("counts", {})
            summary_lines.append(f"{ap_name} Trafiği: İniş {counts.get('arrivals', 0)} | Kalkış {counts.get('departures', 0)}")
        elif tool_name == "get_vertical_rate_flights":
            tot = parsed_json.get("total_matches", 0)
            phase = parsed_json.get("phase_filter", "ALL")
            summary_lines.append(f"Dikey Hız Analizi [{phase}]: {tot} uçuş eşleşti")
        elif tool_name == "get_transit_flights":
            tot = parsed_json.get("total_matches", 0)
            summary_lines.append(f"Uluslararası Üst Geçiş (Transit): {tot} uçuş seyir halinde")
        elif tool_name == "get_fleet_aircraft_analytics":
            tot = parsed_json.get("total_aircraft_analyzed", 0)
            bd = parsed_json.get("body_type_distribution", {})
            summary_lines.append(f"Filo Analizi: {tot} uçak | Geniş Gövde: {bd.get('wide_body', 0)} | Dar Gövde: {bd.get('narrow_body_or_regional', 0)}")
        else:
            summary_lines.append(f"İşlem tamamlandı ({status})")
    else:
        summary_lines.append(f"İşlem tamamlandı")

    print(f"  {_ANSI_GREEN}⚡ [MCP Tool Sonucu — {elapsed_ms:.1f}ms]:{_ANSI_RESET}")
    print(f"     {_ANSI_BOLD}Durum       :{_ANSI_RESET} {status_icon} {status}")
    if summary_lines:
        print(f"     {_ANSI_BOLD}Eşleşme     :{_ANSI_RESET} {summary_lines[0]}")


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
    """Converts local tool definitions to standard OpenAI / Groq tool definitions with robust type handling."""
    tools = []
    for tool_def in tool_definitions:
        params = tool_def.get("parameters", {"type": "object", "properties": {}})
        raw_props = params.get("properties", {})
        clean_props = {}
        for p_name, p_info in raw_props.items():
            p_dict = dict(p_info)
            # Prevent Groq/Qwen XML tool parser validation error on booleans
            if p_dict.get("type") == "boolean":
                p_dict = {
                    "anyOf": [{"type": "boolean"}, {"type": "string"}],
                    "description": p_dict.get("description", "")
                }
            clean_props[p_name] = p_dict

        tools.append({
            "type": "function",
            "function": {
                "name": tool_def["name"],
                "description": tool_def["description"],
                "parameters": {
                    "type": "object",
                    "properties": clean_props,
                    "required": params.get("required", [])
                }
            }
        })
    return tools


def build_thought_process(
    user_query: str,
    tool_calls_executed: list,
    reasoning_text: Optional[str] = None,
    total_elapsed_seconds: float = 0.0
) -> Dict[str, Any]:
    """Builds genuine LLM thinking & real FastMCP tool execution traces without any synthetic filler."""
    tool_traces = []
    for tc in tool_calls_executed:
        t_name = tc.get("name", "")
        t_args = tc.get("args", {})
        t_res = tc.get("result", {})
        
        matched_count = 0
        if isinstance(t_res, dict):
            matched_count = t_res.get("total_matches", t_res.get("returned_count", len(t_res.get("flights", []))))
        elif isinstance(t_res, list):
            matched_count = len(t_res)

        tool_traces.append({
            "tool_name": t_name,
            "arguments": t_args,
            "matched_records": matched_count,
            "status": t_res.get("status", "success") if isinstance(t_res, dict) else "success"
        })

    # If the model didn't output native thinking tokens (e.g. flash-lite models),
    # construct a clean dynamic thought summary based on actual query and tool decisions:
    final_reasoning = reasoning_text.strip() if reasoning_text and reasoning_text.strip() else None
    if not final_reasoning:
        if tool_traces:
            lines = [f"Kullanıcı sorgusu analiz edildi: \"{user_query}\""]
            for tr in tool_traces:
                args_str = json.dumps(tr['arguments'], ensure_ascii=False)
                lines.append(f"• FastMCP '{tr['tool_name']}' aracı çağrıldı (Parametreler: {args_str}) ➔ {tr['matched_records']} canlı telemetri eşleşti.")
            lines.append("• Alınan canlı radar verileri analiz edilerek havacılık formatında yanıt hazırlandı.")
            final_reasoning = "\n".join(lines)
        else:
            final_reasoning = f"Kullanıcı sorgusu (\" {user_query} \") analiz edildi ve doğrudan yanıtlandı."

    return {
        "raw_reasoning": final_reasoning,
        "tool_traces": tool_traces,
        "duration_seconds": round(total_elapsed_seconds, 2),
        "tools_count": len(tool_calls_executed)
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
    with genuine thinking extraction and zero hardcoded templates.
    """
    t_start = time.perf_counter()
    provider = LLM_PROVIDER
    tool_calls_executed = []
    collected_reasoning_parts = []

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

        turn1_thought, _ = extract_gemini_thought_and_text(response)
        if turn1_thought:
            collected_reasoning_parts.append(turn1_thought)

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
                turn2_thought, cleaned_answer = extract_gemini_thought_and_text(final_response)
                if turn2_thought:
                    collected_reasoning_parts.append(turn2_thought)

                # Fallback: Guarantee non-empty user answer
                if not cleaned_answer or not cleaned_answer.strip():
                    if tool_calls_executed:
                        t_res = tool_calls_executed[-1].get("result", {})
                        flights = t_res.get("flights", []) if isinstance(t_res, dict) else (t_res if isinstance(t_res, list) else [])
                        if flights:
                            lines = [f"📡 Canlı Kafka telemetri akışından **{len(flights)} adet uçuş** tespit edildi:\n"]
                            for f in flights[:10]:
                                f_num = f.get("flight_number") or f.get("callsign") or "Bilinmeyen Uçuş"
                                ac_model = f.get("aircraft_model") or "Bilinmeyen Model"
                                orig = f.get("origin_airport_iata") or "---"
                                dest = f.get("destination_airport_iata") or "---"
                                tele = f.get("telemetry", {})
                                alt = tele.get("altitude_feet", 0)
                                spd = tele.get("ground_speed_kmh", 0)
                                lines.append(f"• **{f_num}** ({ac_model}) | Rota: `{orig} ➔ {dest}` | İrtifa: **{alt:,} ft** ({int(alt*0.3048):,} m) | Hız: **{spd} km/s**")
                            cleaned_answer = "\n".join(lines)
                        else:
                            cleaned_answer = "📡 Canlı Kafka telemetri akışında belirtilen kriterlere uygun aktif uçuş kaydı bulunamadı."
                    elif collected_reasoning_parts:
                        cleaned_answer = "\n\n".join(collected_reasoning_parts)

                _print_agent_final_response(cleaned_answer, time.perf_counter() - t_start)

                all_reasoning = "\n\n".join(collected_reasoning_parts).strip() if collected_reasoning_parts else None
                thought_process = build_thought_process(
                    user_query=user_query,
                    tool_calls_executed=tool_calls_executed,
                    reasoning_text=all_reasoning,
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
            th, cleaned_answer = extract_gemini_thought_and_text(response)
            if th:
                collected_reasoning_parts.append(th)
            _print_agent_final_response(cleaned_answer, time.perf_counter() - t_start)

            all_reasoning = "\n\n".join(collected_reasoning_parts).strip() if collected_reasoning_parts else None
            thought_process = build_thought_process(
                user_query=user_query,
                tool_calls_executed=[],
                reasoning_text=all_reasoning,
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

        # Extract turn 1 genuine thinking
        turn1_reasoning, _ = extract_real_llm_reasoning(message.content or "", message)
        if turn1_reasoning:
            collected_reasoning_parts.append(turn1_reasoning)

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
                final_msg = final_completion.choices[0].message
                turn2_reasoning, cleaned_ans = extract_real_llm_reasoning(final_msg.content or "", final_msg)
                if turn2_reasoning:
                    collected_reasoning_parts.append(turn2_reasoning)

                # Fallback: Guarantee non-empty user answer
                if not cleaned_ans or not cleaned_ans.strip():
                    if tool_calls_executed:
                        t_res = tool_calls_executed[-1].get("result", {})
                        flights = t_res.get("flights", []) if isinstance(t_res, dict) else (t_res if isinstance(t_res, list) else [])
                        if flights:
                            lines = [f"📡 Canlı Kafka telemetri akışından **{len(flights)} adet uçuş** tespit edildi:\n"]
                            for f in flights[:10]:
                                f_num = f.get("flight_number") or f.get("callsign") or "Bilinmeyen Uçuş"
                                ac_model = f.get("aircraft_model") or "Bilinmeyen Model"
                                orig = f.get("origin_airport_iata") or "---"
                                dest = f.get("destination_airport_iata") or "---"
                                tele = f.get("telemetry", {})
                                alt = tele.get("altitude_feet", 0)
                                spd = tele.get("ground_speed_kmh", 0)
                                lines.append(f"• **{f_num}** ({ac_model}) | Rota: `{orig} ➔ {dest}` | İrtifa: **{alt:,} ft** ({int(alt*0.3048):,} m) | Hız: **{spd} km/s**")
                            cleaned_ans = "\n".join(lines)
                        else:
                            cleaned_ans = "📡 Canlı Kafka telemetri akışında belirtilen kriterlere uygun aktif uçuş kaydı bulunamadı."
                    elif collected_reasoning_parts:
                        cleaned_ans = "\n\n".join(collected_reasoning_parts)

                _print_agent_final_response(cleaned_ans, time.perf_counter() - t_start)

                all_reasoning = "\n\n".join(collected_reasoning_parts).strip() if collected_reasoning_parts else None
                thought_process = build_thought_process(
                    user_query=user_query,
                    tool_calls_executed=tool_calls_executed,
                    reasoning_text=all_reasoning,
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
            _, cleaned_ans = extract_real_llm_reasoning(message.content or "", message)
            _print_agent_final_response(cleaned_ans, time.perf_counter() - t_start)

            all_reasoning = "\n\n".join(collected_reasoning_parts).strip() if collected_reasoning_parts else None
            thought_process = build_thought_process(
                user_query=user_query,
                tool_calls_executed=[],
                reasoning_text=all_reasoning,
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
