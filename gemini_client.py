import os
import sys
import json
import asyncio
from dotenv import load_dotenv
from google import genai
from google.genai import types
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Load environment variables
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.7-flash")
PUBLIC_MCP_URL = os.getenv("PUBLIC_MCP_URL", "http://localhost:8000/mcp")

FALLBACK_MODELS = [
    GEMINI_MODEL,
    "gemini-3.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-2.5-pro"
]
FALLBACK_MODELS = list(dict.fromkeys(FALLBACK_MODELS))


async def call_gemini_with_retry(genai_client: genai.Client, model: str, contents: list, config: types.GenerateContentConfig, max_retries: int = 3):
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
                    print(f"⏳ [Gemini model busy ({current_model}), retrying in {wait_time}s... (Attempt {attempt}/{max_retries})]")
                    await asyncio.sleep(wait_time)
                elif is_transient and attempt == max_retries:
                    print(f"⚠️ Model '{current_model}' busy, switching to next fallback...")
                    break
                else:
                    raise e
                    
    raise last_error if last_error else Exception("All model attempts failed.")


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


def sanitize_schema_for_gemini(raw_schema: dict) -> dict:
    """Cleans up Pydantic/MCP JSON schema so Gemini can understand tool parameter requirements perfectly."""
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


SYSTEM_INSTRUCTION = (
    "Sen havacılık, uçak telemetrisi ve canlı uçuş takip konusunda uzman bir asistansın. "
    "Kullanıcıların uçuşlar, uçak modelleri, havayolları, canlı radar ve havalimanı sorularını yanıtlamak için "
    "SADECE ve SADECE sana sağlanan FlightRadar24 MCP Tool araçlarını kullan.\n\n"
    "Kurallar:\n"
    "1. Uçuş verilerini, irtifayı, hızları veya uçak modellerini asla uydurma. Yalnızca tool'dan gelen canlı JSON çıktısındaki değerleri kullan.\n"
    "2. İrtifayı belirtirken hem feet hem metre cinsinden ver (örnek: 37.000 ft / ~11.277 m).\n"
    "3. Hızı belirtirken hem knot hem km/s cinsinden ver (örnek: 480 kts / ~889 km/s).\n"
    "4. Uçak modeli bilgisini (Boeing 777-300ER, Airbus A321neo vb.) mutlaka vurgula.\n"
    "5. Eğer uçuş bulunamadıysa (not_found), uçuşun henüz kalkmamış veya inmiş olabileceğini nazikçe belirt.\n"
    "6. Yanıtlarını kullanıcının dilinde (Türkçe/İngilizce), net, okunabilir ve madde işaretleriyle zenginleştirilmiş formatta ver."
)


async def process_user_query(user_query: str, genai_client: genai.Client, gemini_tools: list, mcp_url: str):
    """Processes a natural language query using Gemini + Remote MCP Server."""
    print(f"\n💬 Soru: \"{user_query}\"")
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

    # Check if Gemini decided to call one or more tools
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


async def main():
    if not GEMINI_API_KEY or GEMINI_API_KEY.strip() in ["", "your_gemini_api_key_here"]:
        print("❌ Hata: GEMINI_API_KEY .env dosyasında tanımlı değil.")
        sys.exit(1)

    print("=" * 60)
    print("✈️ FlightRadar24 + Gemini Canlı Havacılık İstemcisi")
    print(f"📡 MCP Server URL : {PUBLIC_MCP_URL}")
    print(f"🧠 Gemini Model   : {GEMINI_MODEL}")
    print("=" * 60)

    genai_client = genai.Client(api_key=GEMINI_API_KEY)

    try:
        print(f"MCP Sunucusuna bağlanılıyor ({PUBLIC_MCP_URL})...")
        mcp_tools = await get_remote_mcp_tools(PUBLIC_MCP_URL)
        gemini_tools = build_gemini_tools_from_mcp(mcp_tools)
        print(f"✅ Bağlantı başarılı! Yüklenen {len(mcp_tools)} MCP Uçuş Aracı:")
        for t in mcp_tools:
            first_line = t.description.splitlines()[0] if t.description else ""
            print(f"   - {t.name}: {first_line}")
    except Exception as e:
        print(f"❌ Bağlantı Hatası: {PUBLIC_MCP_URL} adresindeki MCP sunucusuna erişilemedi.")
        print(f"Detay: {e}")
        print("Lütfen başka bir terminalde '.venv/bin/python server.py' çalıştırdığınızdan emin olun.")
        return

    # If query passed as CLI argument, run once and exit
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        await process_user_query(query, genai_client, gemini_tools, PUBLIC_MCP_URL)
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
            user_input = await asyncio.to_thread(input, "FlightRadar > ")
            user_input = user_input.strip()
            if not user_input:
                continue
            if user_input.lower() in ["exit", "quit", "q"]:
                print("İyi uçuşlar!")
                break
            await process_user_query(user_input, genai_client, gemini_tools, PUBLIC_MCP_URL)
        except (KeyboardInterrupt, EOFError):
            print("\nOturum sonlandırıldı.")
            break
        except Exception as e:
            print(f"❌ Beklenmeyen Hata: {e}")


if __name__ == "__main__":
    asyncio.run(main())
