import asyncio
import sys
import json
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

MCP_URL = "http://localhost:8000/mcp"


async def main():
    print(f"📡 Connecting to Semalar Flight MCP Server: {MCP_URL} ...")
    async with streamable_http_client(MCP_URL) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            print("✅ MCP Connection established and protocol handshake completed!")

            # 1. List tools
            tools_response = await session.list_tools()
            print(f"\n📋 Active MCP Tools on Server ({len(tools_response.tools)} available):")
            for tool in tools_response.tools:
                first_line = tool.description.splitlines()[0] if tool.description else ""
                print(f" • {tool.name}: {first_line}")

            # 2. Test Stream Stats
            print("\n" + "=" * 60)
            print("1️⃣ [MCP TEST] Calling query_kafka_stream(get_stats=True)...")
            res_stats = await session.call_tool("query_kafka_stream", {"get_stats": True})
            print(res_stats.content[0].text if res_stats.content else res_stats)

            # 3. Test High Speed Flights
            print("\n" + "=" * 60)
            print("2️⃣ [MCP TEST] Calling query_kafka_stream(min_speed_kmh=900, limit=2)...")
            res_speed = await session.call_tool("query_kafka_stream", {"min_speed_kmh": 900.0, "limit": 2})
            print(res_speed.content[0].text if res_speed.content else res_speed)

            # 4. Test Compound Query: Istanbul Airspace + Speed >= 750 km/h
            print("\n" + "=" * 60)
            print("3️⃣ [MCP TEST] COMPOUND: query_kafka_stream(latitude=41.0082, longitude=28.9784, radius_km=250, min_speed_kmh=750, limit=2)...")
            res_compound = await session.call_tool("query_kafka_stream", {
                "latitude": 41.0082,
                "longitude": 28.9784,
                "radius_km": 250.0,
                "min_speed_kmh": 750.0,
                "limit": 2
            })
            print(res_compound.content[0].text if res_compound.content else res_compound)

            # 5. Test Compound Query: Airline TK + Speed >= 800 km/h
            print("\n" + "=" * 60)
            print("4️⃣ [MCP TEST] COMPOUND: query_kafka_stream(airline='TK', min_speed_kmh=800, limit=2)...")
            res_airline = await session.call_tool("query_kafka_stream", {"airline": "TK", "min_speed_kmh": 800.0, "limit": 2})
            print(res_airline.content[0].text if res_airline.content else res_airline)


if __name__ == "__main__":
    asyncio.run(main())
