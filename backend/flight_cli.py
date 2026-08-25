import os
import sys
import asyncio

# Ensure backend directory is in sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from flight_agent import ask_flight_agent, get_agent_info

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


async def execute_cli_query(query: str):
    """Executes a single natural language aviation query and prints tool calls and response."""
    print(f"\n💬 Query: \"{query}\"")
    print(f"📡 Dispatching to AI model and checking live MCP telemetry tools...")
    
    res = await ask_flight_agent(query)
    
    if res.get("status") == "success":
        tool_calls = res.get("tool_calls", [])
        if tool_calls:
            for tc in tool_calls:
                print(f"⚙️  [MCP Tool Call] {tc.get('name')}({tc.get('args')})")
            print(f"📥 [MCP Response Received]")

        model_name = res.get("model", "")
        provider = res.get("provider", "AI").upper()
        answer = res.get("answer", "")
        
        print(f"\n✈️ [{provider} - {model_name} Live Aviation Response]:\n{answer}\n" + "=" * 60)
        return answer
    else:
        error_msg = res.get("answer") or res.get("error")
        print(f"❌ Error: {error_msg}\n" + "=" * 60)
        return None


async def main():
    info = get_agent_info()
    print("=" * 60)
    print("✈️ Semalar — Live Flight & Aviation AI Client (Terminal Mode)")
    print(f"🔧 Active Provider (LLM_PROVIDER) : {info['provider'].upper()}")
    print(f"🧠 Model                          : {info['model']}")
    print(f"📡 MCP Server URL                 : {info['mcp_url']}")
    print("=" * 60)

    # If query passed as CLI argument, run once and exit
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        await execute_cli_query(query)
        return

    # Interactive chat mode
    print("\n💡 Live Flight & Aircraft Tracking Mode: Type your question (type 'exit' to quit):")
    print("Sample Queries:")
    print("  • Where is flight THY10 right now, what is its altitude and aircraft model?")
    print("  • What are the top 3 most tracked flights in the world right now?")
    print("  • Show flights currently in the skies above Istanbul (41.0082, 28.9784)")
    print("  • List airborne Pegasus (PGT) flights")
    print("  • What are the details for IST and SAW airports?\n")

    while True:
        try:
            user_input = await asyncio.to_thread(input, f"FlightRadar [{info['provider']}] > ")
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
