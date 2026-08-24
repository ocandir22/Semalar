import os
import sys
import json
import re
import asyncio
from dotenv import load_dotenv
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

def clean_model_output(text: str) -> str:
    """Removes internal <think>...</think> reasoning monologue from CoT models like Qwen/DeepSeek."""
    if not text:
        return ""
    cleaned = re.sub(r"<think>[\s\S]*?</think>", "", text)
    return cleaned.strip()

# Load environment variables
load_dotenv()

# Active Provider: "gemini", "groq", "openrouter", "ollama", "openai"
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini").lower().strip()

# Gemini Config
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.7-flash")

# Groq Config (Open Source Models: llama-3.3-70b-versatile, qwen-2.5-32b, etc.)
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
# Google Gemini Integration
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
                    print(f"⏳ [Gemini model meşgul ({current_model}), {wait_time}s sonra yeniden deneniyor... (Deneme {attempt}/{max_retries})]")
                    await asyncio.sleep(wait_time)
                elif is_transient and attempt == max_retries:
                    print(f"⚠️ Model '{current_model}' meşgul, yedek modele geçiliyor...")
                    break
                else:
                    raise e
                    
    raise last_error if last_error else Exception("Tüm model denemeleri başarısız oldu.")


async def process_with_gemini(user_query: str, genai_client, gemini_tools: list, mcp_url: str):
    """Processes query using Gemini + MCP Tool Calling."""
    from google.genai import types
    print(f"📡 Gemini ({GEMINI_MODEL}) modeline iletiliyor...")

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
        print(f"❌ Gemini API Hatası: {e}")
        return

    if response.function_calls:
        contents.append(response.candidates[0].content)

        tool_response_parts = []
        for function_call in response.function_calls:
            tool_name = function_call.name
            tool_args = function_call.args or {}

            print(f"⚙️  [MCP Araç Çağrısı] {tool_name}({tool_args}) ...")
            
            try:
                result_text = await execute_remote_mcp_tool(mcp_url, tool_name, tool_args)
                print(f"📥 [MCP Yanıtı Alındı]")
            except Exception as e:
                print(f"❌ MCP Çalıştırma Hatası: {e}")
                result_text = json.dumps({"error": str(e)})

            try:
                parsed_json = json.loads(result_text)
            except Exception:
                parsed_json = {"result": result_text}

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
            print(f"\n✈️ [Gemini Canlı Havacılık Yanıtı]:\n{answer}\n" + "=" * 60)
            return answer
        except Exception as e:
            print(f"❌ Gemini Yanıt Oluşturma Hatası: {e}")
            return
    else:
        answer = response.text or "(Yanıt alınamadı)"
        print(f"\n✈️ [Gemini Doğrudan Yanıt]:\n{answer}\n" + "=" * 60)
        return answer


# ============================================================
# OpenAI-Compatible Integration (Groq, OpenRouter, Ollama, OpenAI)
# ============================================================

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


async def process_with_openai_compatible(user_query: str, client, model_name: str, provider_name: str, openai_tools: list, mcp_url: str):
    """Processes query using OpenAI-compatible APIs (Groq, OpenRouter, Ollama, OpenAI) + MCP."""
    print(f"📡 {provider_name.upper()} ({model_name}) modeline iletiliyor...")

    messages = [
        {"role": "system", "content": SYSTEM_INSTRUCTION},
        {"role": "user", "content": user_query}
    ]

    try:
        response = await client.chat.completions.create(
            model=model_name,
            messages=messages,
            tools=openai_tools,
            temperature=0.0
        )
    except Exception as e:
        print(f"❌ {provider_name.upper()} API Hatası: {e}")
        return

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

            print(f"⚙️  [MCP Araç Çağrısı] {tool_name}({tool_args}) ...")

            try:
                result_text = await execute_remote_mcp_tool(mcp_url, tool_name, tool_args)
                print(f"📥 [MCP Yanıtı Alındı]")
            except Exception as e:
                print(f"❌ MCP Çalıştırma Hatası: {e}")
                result_text = json.dumps({"error": str(e)})

            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": result_text
            })

        try:
            final_response = await client.chat.completions.create(
                model=model_name,
                messages=messages,
                temperature=0.0
            )
            answer = final_response.choices[0].message.content or "(Yanıt alınamadı)"
            answer = clean_model_output(answer)
            print(f"\n✈️ [{provider_name.upper()} Canlı Havacılık Yanıtı]:\n{answer}\n" + "=" * 60)
            return answer
        except Exception as e:
            print(f"❌ {provider_name.upper()} Yanıt Oluşturma Hatası: {e}")
            return
    else:
        answer = message.content or "(Yanıt alınamadı)"
        answer = clean_model_output(answer)
        print(f"\n✈️ [{provider_name.upper()} Doğrudan Yanıt]:\n{answer}\n" + "=" * 60)
        return answer


# ============================================================
# Main Entrypoint
# ============================================================

async def main():
    print("=" * 60)
    print("✈️ Semalar — Canlı Uçuş ve Havacılık AI İstemcisi")
    print(f"🔧 Aktif Sağlayıcı (LLM_PROVIDER) : {LLM_PROVIDER.upper()}")
    print(f"📡 MCP Server URL                 : {PUBLIC_MCP_URL}")

    # Setup Provider Client
    genai_client = None
    openai_client = None
    active_model_name = ""

    if LLM_PROVIDER == "gemini":
        from google import genai
        if not GEMINI_API_KEY or GEMINI_API_KEY.strip() in ["", "your_gemini_api_key_here"]:
            print("❌ Hata: GEMINI_API_KEY .env dosyasında tanımlı değil.")
            sys.exit(1)
        genai_client = genai.Client(api_key=GEMINI_API_KEY)
        active_model_name = GEMINI_MODEL
        print(f"🧠 Model                          : {GEMINI_MODEL}")

    elif LLM_PROVIDER == "groq":
        from openai import AsyncOpenAI
        if not GROQ_API_KEY or GROQ_API_KEY.strip() in ["", "your_groq_api_key_here"]:
            print("❌ Hata: GROQ_API_KEY .env dosyasında tanımlı değil.")
            print("💡 https://console.groq.com/keys adresinden ücretsiz anahtar alabilirsiniz.")
            sys.exit(1)
        openai_client = AsyncOpenAI(base_url="https://api.groq.com/openai/v1", api_key=GROQ_API_KEY)
        active_model_name = GROQ_MODEL
        print(f"🧠 Groq Açık Kaynak Model         : {GROQ_MODEL}")

    elif LLM_PROVIDER == "openrouter":
        from openai import AsyncOpenAI
        if not OPENROUTER_API_KEY or OPENROUTER_API_KEY.strip() in ["", "your_openrouter_api_key_here"]:
            print("❌ Hata: OPENROUTER_API_KEY .env dosyasında tanımlı değil.")
            print("💡 https://openrouter.ai/keys adresinden anahtar alabilirsiniz.")
            sys.exit(1)
        openai_client = AsyncOpenAI(base_url="https://openrouter.ai/api/v1", api_key=OPENROUTER_API_KEY)
        active_model_name = OPENROUTER_MODEL
        print(f"🧠 OpenRouter Model               : {OPENROUTER_MODEL}")

    elif LLM_PROVIDER == "ollama":
        from openai import AsyncOpenAI
        openai_client = AsyncOpenAI(base_url=OLLAMA_BASE_URL, api_key="ollama")
        active_model_name = OLLAMA_MODEL
        print(f"🧠 Yerel Ollama Modeli            : {OLLAMA_MODEL} ({OLLAMA_BASE_URL})")

    elif LLM_PROVIDER == "deepseek":
        from openai import AsyncOpenAI
        if not DEEPSEEK_API_KEY or DEEPSEEK_API_KEY.strip() in ["", "your_deepseek_api_key_here"]:
            print("❌ Hata: DEEPSEEK_API_KEY .env dosyasında tanımlı değil.")
            print("💡 https://platform.deepseek.com adresinden anahtar alabilirsiniz.")
            sys.exit(1)
        openai_client = AsyncOpenAI(base_url="https://api.deepseek.com", api_key=DEEPSEEK_API_KEY)
        active_model_name = DEEPSEEK_MODEL
        print(f"🧠 DeepSeek Modeli                : {DEEPSEEK_MODEL} (https://api.deepseek.com)")

    elif LLM_PROVIDER == "openai":
        from openai import AsyncOpenAI
        if not OPENAI_API_KEY or OPENAI_API_KEY.strip() in ["", "your_openai_api_key_here"]:
            print("❌ Hata: OPENAI_API_KEY .env dosyasında tanımlı değil.")
            sys.exit(1)
        openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        active_model_name = OPENAI_MODEL
        print(f"🧠 OpenAI Model                   : {OPENAI_MODEL}")

    else:
        print(f"❌ Bilinmeyen LLM_PROVIDER: '{LLM_PROVIDER}'. Geçerli seçenekler: gemini, groq, openrouter, ollama, deepseek, openai")
        sys.exit(1)

    print("=" * 60)

    # Fetch Tools from MCP Server
    try:
        print(f"MCP Sunucusuna bağlanılıyor ({PUBLIC_MCP_URL})...")
        mcp_tools = await get_remote_mcp_tools(PUBLIC_MCP_URL)
        if LLM_PROVIDER == "gemini":
            prepared_tools = build_gemini_tools_from_mcp(mcp_tools)
        else:
            prepared_tools = build_openai_tools_from_mcp(mcp_tools)

        print(f"✅ Bağlantı başarılı! Yüklenen {len(mcp_tools)} MCP Uçuş Aracı:")
        for t in mcp_tools:
            first_line = t.description.splitlines()[0] if t.description else ""
            print(f"   - {t.name}: {first_line}")
    except Exception as e:
        print(f"❌ Bağlantı Hatası: {PUBLIC_MCP_URL} adresindeki MCP sunucusuna erişilemedi.")
        print(f"Detay: {e}")
        print("Lütfen başka bir terminalde 'python server.py' çalıştırdığınızdan emin olun.")
        return

    async def execute_query(query: str):
        print(f"\n💬 Soru: \"{query}\"")
        if LLM_PROVIDER == "gemini":
            return await process_with_gemini(query, genai_client, prepared_tools, PUBLIC_MCP_URL)
        else:
            return await process_with_openai_compatible(query, openai_client, active_model_name, LLM_PROVIDER, prepared_tools, PUBLIC_MCP_URL)

    # If query passed as CLI argument, run once and exit
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        await execute_query(query)
        return

    # Interactive chat mode
    print("\n💡 Canlı Uçuş ve Uçak Takip Modu: Sorunuzu yazın (çıkmak için 'exit'):")
    print("Örnek Sorular:")
    print("  • THY10 nolu uçak şu an nerede, irtifası kaç ve uçağın modeli ne?")
    print("  • Dünyada şu an en çok takip edilen ilk 3 uçuş hangisi?")
    print("  • İstanbul (41.0082, 28.9784) semalarında uçan uçakları göster")
    print("  • Pegasus'un (PGT) havadaki uçaklarını listele")
    print("  • IST ve SAW havalimanı bilgileri nelerdir?\n")

    while True:
        try:
            user_input = await asyncio.to_thread(input, f"FlightRadar [{LLM_PROVIDER}] > ")
            user_input = user_input.strip()
            if not user_input:
                continue
            if user_input.lower() in ["exit", "quit", "q"]:
                print("İyi uçuşlar!")
                break
            await execute_query(user_input)
        except (KeyboardInterrupt, EOFError):
            print("\nOturum sonlandırıldı.")
            break
        except Exception as e:
            print(f"❌ Beklenmeyen Hata: {e}")


if __name__ == "__main__":
    asyncio.run(main())
