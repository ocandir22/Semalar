"""
Project #2 Kafka Cockpit — Terminal CLI Client
Interactive terminal client for querying real-time Apache Kafka flight telemetry streams,
supersonic speed filtering, 81 Turkish province polygon containment, and cockpit metrics.
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
    from project_kafka.kafka_agent import ask_kafka_agent
    from core.llm_client import get_agent_info
except ImportError:
    from kafka_agent import ask_kafka_agent
    from core.llm_client import get_agent_info

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


async def execute_cli_query(query: str):
    """Executes a single natural language query for Project #2 (Kafka Telemetry)."""
    print(f"\n💬 Query: \"{query}\"")
    print("📡 Dispatching to Kafka Telemetry AI Agent (Apache Kafka Stream)...")
    res = await ask_kafka_agent(query)
    if res.get("status") == "success":
        provider = res.get("provider", "AI").upper()
        model_name = res.get("model", "")
        answer = res.get("answer", "")
        print(f"\n⚡ [{provider} - {model_name} Kafka Cockpit Response]:\n{answer}\n" + "=" * 65)
        return answer
    else:
        error_msg = res.get("answer") or res.get("error")
        print(f"❌ Error: {error_msg}\n" + "=" * 65)
        return None


async def main():
    info = get_agent_info()
    print("=" * 65)
    print("⚡ Semalar — Project #2 Apache Kafka Telemetry Cockpit (CLI)")
    print(f"🔧 Provider : {info['provider']}")
    print(f"🧠 Model    : {info['model']}")
    print(f"📡 MCP URL  : {info['mcp_url']}")
    print("=" * 65)

    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        await execute_cli_query(query)
        return

    print("\n💡 Kafka Cockpit Mode: Type your question (type 'exit' to quit):")
    print("Sample Queries:")
    print("  • Erzurum hava sahasındaki uçakları listele")
    print("  • Hızı 850 km üzeri süpersonik uçuşları bul")
    print("  • Türk Hava Yolları'nın hız rekoru kıran uçakları hangileri?")
    print("  • Şu an Kafka akışının genel istatistikleri neler?")
    print("  • Ankara üzerinde 10 bin metre üstündeki uçaklar\n")

    while True:
        try:
            user_input = await asyncio.to_thread(input, f"KafkaCockpit [{info['provider']}] > ")
            user_input = user_input.strip()
            if not user_input:
                continue
            if user_input.lower() in ["exit", "quit", "q"]:
                print("Telemetry monitoring closed.")
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
        print("\nTelemetry monitoring closed.")
