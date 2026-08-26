"""
Project #1 Live Radar — Terminal CLI Client
Interactive terminal client for querying real-time FlightRadar24 ADS-B aircraft telemetry,
airports, airline fleets, and most-tracked flights.
"""

import os
import sys
import asyncio

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(BASE_DIR)
ROOT_DIR = os.path.dirname(BACKEND_DIR)
for p in [ROOT_DIR, BACKEND_DIR, BASE_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    from project_live.live_agent import ask_live_agent
    from core.llm_client import get_agent_info
except ImportError:
    from live_agent import ask_live_agent
    from core.llm_client import get_agent_info

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


async def execute_cli_query(query: str):
    """Executes a single natural language query for Project #1 (Live Radar)."""
    print(f"\n💬 Query: \"{query}\"")
    print("📡 Dispatching to Live Radar AI Agent (FlightRadar24)...")
    res = await ask_live_agent(query)
    if res.get("status") == "success":
        provider = res.get("provider", "AI").upper()
        model_name = res.get("model", "")
        answer = res.get("answer", "")
        print(f"\n✈️ [{provider} - {model_name} Live Aviation Response]:\n{answer}\n" + "=" * 65)
        return answer
    else:
        error_msg = res.get("answer") or res.get("error")
        print(f"❌ Error: {error_msg}\n" + "=" * 65)
        return None


async def main():
    info = get_agent_info()
    print("=" * 65)
    print("✈️ Semalar — Project #1 Live FlightRadar24 AI Assistant (CLI)")
    print(f"🔧 Provider : {info['provider']}")
    print(f"🧠 Model    : {info['model']}")
    print(f"📡 MCP URL  : {info['mcp_url']}")
    print("=" * 65)

    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        await execute_cli_query(query)
        return

    print("\n💡 Live Radar Mode: Type your question (type 'exit' to quit):")
    print("Sample Queries:")
    print("  • Where is flight THY10 right now, what is its altitude and aircraft model?")
    print("  • What are the top 3 most tracked flights in the world right now?")
    print("  • Show flights currently in the skies above Istanbul")
    print("  • List airborne Pegasus (PGT) flights")
    print("  • What are the details for IST and SAW airports?\n")

    while True:
        try:
            user_input = await asyncio.to_thread(input, f"LiveRadar [{info['provider']}] > ")
            user_input = user_input.strip()
            if not user_input:
                continue
            if user_input.lower() in ["exit", "quit", "q"]:
                print("Have a safe flight!")
                break
            await execute_cli_query(user_input)
        except (KeyboardInterrupt, EOFError):
            print("\nSession terminated.")
            break
        except Exception as e:
            print(f"❌ Unexpected Error: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
        print("\nHave a safe flight!")
