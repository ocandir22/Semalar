import asyncio
import sys
import json
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

MCP_URL = "http://localhost:8000/mcp"


async def main():
    print(f"📡 Semalar Flight MCP Sunucusuna bağlanılıyor: {MCP_URL} ...")
    async with streamable_http_client(MCP_URL) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            print("✅ MCP Bağlantısı kuruldu ve el sıkışma tamamlandı!")

            # 1. List tools
            tools_response = await session.list_tools()
            print(f"\n📋 Sunucudaki Aktif MCP Araçları ({len(tools_response.tools)} adet):")
            for tool in tools_response.tools:
                first_line = tool.description.splitlines()[0] if tool.description else ""
                print(f" • {tool.name}: {first_line}")

            # 2. Test get_kafka_stream_stats
            print("\n" + "=" * 60)
            print("1️⃣ [MCP TEST] get_kafka_stream_stats() çağrılıyor...")
            res_stats = await session.call_tool("get_kafka_stream_stats", {})
            print(res_stats.content[0].text if res_stats.content else res_stats)

            # 3. Test get_flights_above_speed (Kullanıcı Özel İsteği)
            print("\n" + "=" * 60)
            print("2️⃣ [MCP TEST] get_flights_above_speed(min_speed_kmh=900, limit=2) çağrılıyor...")
            res_speed = await session.call_tool("get_flights_above_speed", {"min_speed_kmh": 900.0, "limit": 2})
            print(res_speed.content[0].text if res_speed.content else res_speed)

            # 4. Test search_airline_from_kafka
            print("\n" + "=" * 60)
            print("3️⃣ [MCP TEST] search_airline_from_kafka(airline_code='TK', limit=2) çağrılıyor...")
            res_airline = await session.call_tool("search_airline_from_kafka", {"airline_code": "TK", "limit": 2})
            print(res_airline.content[0].text if res_airline.content else res_airline)


if __name__ == "__main__":
    asyncio.run(main())
