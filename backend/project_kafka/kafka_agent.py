"""
Kafka Airspace Telemetry AI Agent — Project #2
Generic MCP Client Agent: Dynamically discovers tools from the FastMCP Server,
dispatches tool executions dynamically without hardcoded routing or bloated system prompts.
"""

import sys
from typing import Dict, Any, List

# Core LLM Engine
try:
    from core.llm_client import run_llm_cycle, get_agent_info
except ImportError:
    from ..core.llm_client import run_llm_cycle, get_agent_info

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# ============================================================
# Dynamic FastMCP Tool Discovery (True MCP Architecture)
# ============================================================

def get_dynamic_mcp_tools() -> List[Dict[str, Any]]:
    """Dynamically queries the FastMCP Server for all registered tools.
    Extracts name, description, and JSON Schema parameters at runtime.
    The agent never needs to know tool names in advance.
    """
    try:
        try:
            from server import mcp_server
        except ImportError:
            from backend.server import mcp_server

        if mcp_server is not None and hasattr(mcp_server, "_tool_manager"):
            tools = []
            for tool in mcp_server._tool_manager.list_tools():
                schema = tool.parameters.copy() if hasattr(tool, "parameters") else {}
                props = {k: v for k, v in schema.get("properties", {}).items() if k != "kwargs"}
                required = [r for r in schema.get("required", []) if r != "kwargs"]
                tools.append({
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": {
                        "type": "object",
                        "properties": props,
                        "required": required
                    }
                })
            if tools:
                return tools
    except Exception as e:
        print(f"⚠️ [MCP Agent] Could not dynamically load tools from FastMCP: {e}")
    return []


def dynamic_mcp_tool_executor(tool_name: str, tool_args: dict) -> Any:
    """Universal dynamic tool executor.
    Directly dispatches execution to the FastMCP Server registry without any hardcoded if/elif blocks.
    """
    try:
        try:
            from server import MCP_TOOLS_REGISTRY
        except ImportError:
            from backend.server import MCP_TOOLS_REGISTRY

        if tool_name in MCP_TOOLS_REGISTRY:
            tool_func = MCP_TOOLS_REGISTRY[tool_name]
            return tool_func(**tool_args)
        else:
            return {"status": "error", "error": f"Tool '{tool_name}' not found on FastMCP Server."}
    except Exception as e:
        return {"status": "error", "error": f"Error executing FastMCP tool '{tool_name}': {str(e)}"}


# ============================================================
# Clean, Generic MCP System Instruction (No Hardcoded Tool Names)
# ============================================================

GENERIC_MCP_SYSTEM_INSTRUCTION = (
    "Sen Türkiye hava sahası ve canlı uçak telemetrisi konusunda uzmanlaşmış profesyonel bir Havacılık Yapay Zeka Kokpit Asistanısın.\n\n"
    "🎯 ÇALIŞMA PRENSİBİ VE MCP ARAÇLARI:\n"
    "• Sana sağlanan dinamik MCP (Model Context Protocol) araçlarını incele ve kullanıcının sorusuna en uygun aracı/araçları seçerek çalıştır.\n"
    "• Kullanıcı doğal dilde şehir, bölge, havalimanı (IATA/ICAO), irtifa, hız, acil durum veya filo analitiği sorduğunda, araçların parametre tanımlarına göre uygun argümanları oluştur.\n\n"
    "📌 TEMEL KURALLAR:\n"
    "• Asla telemetri veya uçuş verisi uydurma (halüsinasyon görme). Yalnızca MCP araçlarından dönen kesin verileri kullan.\n"
    "• İrtifaları hem feet hem metre, hızları hem knot hem km/s cinsinden belirt.\n"
    "• Varsa uçak modelini (örn. Boeing 777-300ER, Airbus A321neo) ve rotayı (örn. IST ➔ JFK) vurgula.\n"
    "• Yanıtlarını Türkçe, yapılandırılmış, maddeli ve profesyonel bir havacılık diliyle sun."
)


async def ask_kafka_agent(user_query: str) -> Dict[str, Any]:
    """Processes natural language queries by dynamically querying available MCP tools and delegating execution."""
    dynamic_tools = get_dynamic_mcp_tools()
    return await run_llm_cycle(
        user_query=user_query,
        system_instruction=GENERIC_MCP_SYSTEM_INSTRUCTION,
        tool_definitions=dynamic_tools,
        tool_executor=dynamic_mcp_tool_executor,
        project_label="⚡ Apache Kafka Telemetri Akışı (MCP Architecture)"
    )
