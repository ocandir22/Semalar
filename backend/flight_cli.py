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
    print(f"\n💬 Soru: \"{query}\"")
    print(f"📡 AI modeline iletiliyor ve canlı MCP telemetri araçları kontrol ediliyor...")
    
    res = await ask_flight_agent(query)
    
    if res.get("status") == "success":
        tool_calls = res.get("tool_calls", [])
        if tool_calls:
            for tc in tool_calls:
                print(f"⚙️  [MCP Araç Çağrısı] {tc.get('name')}({tc.get('args')})")
            print(f"📥 [MCP Yanıtı Alındı]")

        model_name = res.get("model", "")
        provider = res.get("provider", "AI").upper()
        answer = res.get("answer", "")
        
        print(f"\n✈️ [{provider} - {model_name} Canlı Havacılık Yanıtı]:\n{answer}\n" + "=" * 60)
        return answer
    else:
        error_msg = res.get("answer") or res.get("error")
        print(f"❌ Hata: {error_msg}\n" + "=" * 60)
        return None


async def main():
    info = get_agent_info()
    print("=" * 60)
    print("✈️ Semalar — Canlı Uçuş ve Havacılık AI İstemcisi (Terminal Modu)")
    print(f"🔧 Aktif Sağlayıcı (LLM_PROVIDER) : {info['provider'].upper()}")
    print(f"🧠 Model                          : {info['model']}")
    print(f"📡 MCP Server URL                 : {info['mcp_url']}")
    print("=" * 60)

    # If query passed as CLI argument, run once and exit
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        await execute_cli_query(query)
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
            user_input = await asyncio.to_thread(input, f"FlightRadar [{info['provider']}] > ")
            user_input = user_input.strip()
            if not user_input:
                continue
            if user_input.lower() in ["exit", "quit", "q"]:
                print("İyi uçuşlar!")
                break
            await execute_cli_query(user_input)
        except (KeyboardInterrupt, EOFError):
            print("\nOturum sonlandırıldı.")
            break
        except Exception as e:
            print(f"❌ Beklenmeyen Hata: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
        print("\nİyi uçuşlar!")
