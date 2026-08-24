import os
import sys
import json
import re
import asyncio
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Load environment variables
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
    "gemini-2.5-flash",
    "gemini-2.5-pro"
]
FALLBACK_MODELS = list(dict.fromkeys(FALLBACK_MODELS))

SYSTEM_INSTRUCTION = (
    "Sen havacılık, uçak telemetrisi ve canlı uçuş takip konusunda uzman, net ve doğrudan yanıt veren bir asistansın. "
    "Kullanıcıların uçuşlar, uçak modelleri, havayolları, canlı radar ve havalimanı sorularını yanıtlamak için "
    "SADECE ve SADECE sana sağlanan FlightRadar24 MCP Tool araçlarını kullan.\n\n"
    "Kurallar:\n"
    "1. Uçuş verilerini, irtifayı, hızları veya uçak modellerini asla uydurma. Yalnızca tool'dan gelen canlı JSON çıktısındaki değerleri kullan.\n"
    "2. İrtifayı belirtirken hem feet hem metre cinsinden ver (örnek: 37.000 ft / ~11.277 m).\n"
    "3. Hızı belirtirken hem knot hem km/s cinsinden ver (örnek: 480 kts / ~889 km/s).\n"
    "4. Uçak modeli bilgisini (Boeing 777-300ER, Airbus A321neo vb.) mutlaka vurgula.\n"
    "5. Eğer uçuş bulunamadıysa (not_found), uçuşun henüz kalkmamış veya inmiş olabileceğini nazikçe belirt.\n"
    "6. CEVAPLARI KISA, NET VE DOĞRUDAN TUT. Uzun iç ses monologları, gereksiz tekrarlar veya felsefi açıklamalar yapma. Kullanıcının sorusuna net madde işaretleriyle odaklan."
)


def clean_model_output(text: str) -> str:
    """Removes internal <think>...</think> reasoning monologue from CoT models."""
    if not text:
        return ""
    cleaned = re.sub(r"<think>[\s\S]*?</think>", "", text)
    return cleaned.strip()


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
# MCP Helper Functions
# ============================================================

async def get_remote_mcp_tools(mcp_url: str):
    """Connects to the MCP server and fetches available tools."""
    async with streamable_http_client(mcp_url) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            tools_result = await session.list_tools()
            return tools_result.tools


async def execute_remote_mcp_tool(mcp_url: str, tool_name: str, tool_args: dict) -> str:
    """Executes a tool on the Remote MCP server and returns the text result."""
    async with streamable_http_client(mcp_url) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            mcp_result = await session.call_tool(tool_name, tool_args)
            result_text = ""
            if mcp_result.content:
                for item in mcp_result.content:
                    if hasattr(item, "text"):
                        result_text += item.text
                    else:
                        result_text += str(item)
            else:
                result_text = json.dumps({"status": "no_content"})
            return result_text


# ============================================================
# Gemini Schema & Tool Utilities
# ============================================================

def sanitize_schema_for_gemini(raw_schema: dict) -> dict:
    """Cleans up Pydantic/MCP JSON schema so Gemini can understand tool parameter requirements."""
    if not isinstance(raw_schema, dict):
        return {"type": "OBJECT", "properties": {}}

    clean_schema = {
        "type": "OBJECT",
        "properties": {},
        "required": raw_schema.get("required", [])
    }

    for prop_name, prop_def in raw_schema.get("properties", {}).items():
        prop_copy = dict(prop_def)
        if "anyOf" in prop_copy:
            types_list = [x.get("type") for x in prop_copy["anyOf"] if isinstance(x, dict) and x.get("type") != "null"]
            prop_type = types_list[0] if types_list else "string"
        else:
            prop_type = prop_copy.get("type", "string")

        clean_schema["properties"][prop_name] = {
            "type": prop_type.upper(),
            "description": prop_copy.get("description", prop_copy.get("title", ""))
        }
    return clean_schema


def build_gemini_tools_from_mcp(mcp_tools):
    """Converts MCP tool definitions into Gemini FunctionDeclaration format."""
    from google.genai import types
    function_declarations = []
    for tool in mcp_tools:
        raw_schema = getattr(tool, "input_schema", getattr(tool, "inputSchema", {}))
        schema = sanitize_schema_for_gemini(raw_schema)
            
        function_declarations.append(
            types.FunctionDeclaration(
                name=tool.name,
                description=tool.description or "",
                parameters=schema
            )
        )
    return [types.Tool(function_declarations=function_declarations)]


def build_openai_tools_from_mcp(mcp_tools):
    """Converts MCP tools to standard OpenAI / Groq tool definitions."""
    tools = []
    for tool in mcp_tools:
        raw_schema = getattr(tool, "input_schema", getattr(tool, "inputSchema", {}))
        if hasattr(raw_schema, "model_dump"):
            raw_schema = raw_schema.model_dump()
        elif not isinstance(raw_schema, dict):
            raw_schema = {"type": "object", "properties": {}}

        tools.append({
            "type": "function",
            "function": {
                "name": tool.name,
                "description": tool.description or "",
                "parameters": raw_schema
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
# Main AI Processing Function (Unified for CLI & Web API)
# ============================================================

async def ask_flight_agent(user_query: str, mcp_url: Optional[str] = None) -> Dict[str, Any]:
    """Processes a natural language query using the configured LLM and live MCP tools.
    
    Returns a dictionary containing:
        - status: 'success' | 'error'
        - answer: str (the formatted response)
        - tool_calls: list of tool calls made with args and outputs
        - model: active model name
        - provider: active provider name
        - error: error message if status == 'error'
    """
    target_mcp_url = mcp_url or PUBLIC_MCP_URL
    provider = LLM_PROVIDER
    tool_calls_executed = []

    try:
        mcp_tools = await get_remote_mcp_tools(target_mcp_url)
    except Exception as e:
        return {
            "status": "error",
            "answer": f"MCP sunucusuna bağlanılamadı ({target_mcp_url}). Sunucunun çalıştığından emin olun.",
            "tool_calls": [],
            "model": "unknown",
            "provider": provider,
            "error": str(e)
        }

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
        gemini_tools = build_gemini_tools_from_mcp(mcp_tools)

        contents = [
            types.Content(
                role="user",
                parts=[types.Part.from_text(text=user_query)]
            )
        ]

        config = types.GenerateContentConfig(
            tools=gemini_tools,
            temperature=0.0,
            system_instruction=SYSTEM_INSTRUCTION
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

                try:
                    result_text = await execute_remote_mcp_tool(target_mcp_url, tool_name, tool_args)
                except Exception as e:
                    result_text = json.dumps({"error": str(e)})

                try:
                    parsed_json = json.loads(result_text)
                except Exception:
                    parsed_json = {"result": result_text}

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
                    types.GenerateContentConfig(tools=gemini_tools, temperature=0.0, system_instruction=SYSTEM_INSTRUCTION)
                )
                answer = final_response.text or "(Yanıt alınamadı)"
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
            answer = response.text or "(Yanıt alınamadı)"
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
            base_url = "https://api.deepseek.com"
            model_name = DEEPSEEK_MODEL
        elif provider == "openai":
            api_key = OPENAI_API_KEY
            base_url = None
            model_name = OPENAI_MODEL
        else:
            return {
                "status": "error",
                "answer": f"Bilinmeyen LLM_PROVIDER: {provider}",
                "tool_calls": [],
                "model": "unknown",
                "provider": provider,
                "error": "Invalid provider"
            }

        if provider != "ollama" and (not api_key or api_key.strip() in ["", "your_api_key_here"]):
            return {
                "status": "error",
                "answer": f"{provider.upper()}_API_KEY .env dosyasında tanımlı değil.",
                "tool_calls": [],
                "model": model_name,
                "provider": provider,
                "error": f"Missing {provider.upper()}_API_KEY"
            }

        openai_client = AsyncOpenAI(base_url=base_url, api_key=api_key)
        openai_tools = build_openai_tools_from_mcp(mcp_tools)

        messages = [
            {"role": "system", "content": SYSTEM_INSTRUCTION},
            {"role": "user", "content": user_query}
        ]

        try:
            response = await openai_client.chat.completions.create(
                model=model_name,
                messages=messages,
                tools=openai_tools,
                temperature=0.0
            )
        except Exception as e:
            return {
                "status": "error",
                "answer": f"{provider.upper()} API Hatası: {e}",
                "tool_calls": [],
                "model": model_name,
                "provider": provider,
                "error": str(e)
            }

        choice = response.choices[0]
        message = choice.message

        if message.tool_calls:
            messages.append(message)

            for tool_call in message.tool_calls:
                tool_name = tool_call.function.name
                try:
                    tool_args = json.loads(tool_call.function.arguments) if tool_call.function.arguments else {}
                except Exception:
                    tool_args = {}

                try:
                    result_text = await execute_remote_mcp_tool(target_mcp_url, tool_name, tool_args)
                except Exception as e:
                    result_text = json.dumps({"error": str(e)})

                try:
                    parsed_json = json.loads(result_text)
                except Exception:
                    parsed_json = {"result": result_text}

                tool_calls_executed.append({
                    "name": tool_name,
                    "args": tool_args,
                    "result": parsed_json
                })

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": result_text
                })

            try:
                final_response = await openai_client.chat.completions.create(
                    model=model_name,
                    messages=messages,
                    temperature=0.0
                )
                answer = final_response.choices[0].message.content or "(Yanıt alınamadı)"
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
            answer = message.content or "(Yanıt alınamadı)"
            return {
                "status": "success",
                "answer": clean_model_output(answer),
                "tool_calls": [],
                "model": model_name,
                "provider": provider
            }
