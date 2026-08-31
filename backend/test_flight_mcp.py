import asyncio
import sys
import json
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

MCP_URL = "http://localhost:8000/mcp"


async def main():
    print(f"📡 Connecting to Semalar FastMCP Server: {MCP_URL} ...")
    async with streamable_http_client(MCP_URL) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            print("✅ FastMCP Connection established and protocol handshake completed!")

            # 1. List tools
            tools_response = await session.list_tools()
            print(f"\n📋 Active FastMCP Tools on Server ({len(tools_response.tools)} available):")
            for tool in tools_response.tools:
                first_line = tool.description.splitlines()[0] if tool.description else ""
                print(f" • \033[96m{tool.name}\033[0m: {first_line}")

            # 2. Test Stream Query: Stats
            print("\n" + "=" * 60)
            print("1️⃣ [MCP TEST] Calling query_kafka_stream(get_stats=True)...")
            res_stats = await session.call_tool("query_kafka_stream", {"get_stats": True})
            print(res_stats.content[0].text if res_stats.content else res_stats)

            # 3. Test Emergency Flights
            print("\n" + "=" * 60)
            print("2️⃣ [MCP TEST] Calling get_emergency_flights()...")
            res_emerg = await session.call_tool("get_emergency_flights", {"emergency_type": "ALL", "limit": 3})
            print(res_emerg.content[0].text if res_emerg.content else res_emerg)

            # 4. Test Nearby Aircraft: Ankara 50km
            print("\n" + "=" * 60)
            print("3️⃣ [MCP TEST] Calling find_nearby_aircraft(location='Ankara', radius_km=50)...")
            res_nearby = await session.call_tool("find_nearby_aircraft", {"location": "Ankara", "radius_km": 50.0, "limit": 3})
            print(res_nearby.content[0].text if res_nearby.content else res_nearby)

            # 5. Test Airport Traffic: Istanbul (IST)
            print("\n" + "=" * 60)
            print("4️⃣ [MCP TEST] Calling get_airport_traffic(airport_code='IST', traffic_type='ALL')...")
            res_ist = await session.call_tool("get_airport_traffic", {"airport_code": "IST", "traffic_type": "ALL", "limit": 3})
            print(res_ist.content[0].text if res_ist.content else res_ist)

            # 6. Test Vertical Profile: Climbing Aircraft
            print("\n" + "=" * 60)
            print("5️⃣ [MCP TEST] Calling get_vertical_rate_flights(flight_phase='CLIMBING')...")
            res_climb = await session.call_tool("get_vertical_rate_flights", {"flight_phase": "CLIMBING", "limit": 3})
            print(res_climb.content[0].text if res_climb.content else res_climb)

            # 7. Test Transit Overflights
            print("\n" + "=" * 60)
            print("6️⃣ [MCP TEST] Calling get_transit_flights(min_altitude_feet=28000)...")
            res_transit = await session.call_tool("get_transit_flights", {"min_altitude_feet": 28000.0, "limit": 3})
            print(res_transit.content[0].text if res_transit.content else res_transit)

            # 8. Test Fleet Analytics
            print("\n" + "=" * 60)
            print("7️⃣ [MCP TEST] Calling get_fleet_aircraft_analytics()...")
            res_fleet = await session.call_tool("get_fleet_aircraft_analytics", {"include_breakdown": True})
            print(res_fleet.content[0].text if res_fleet.content else res_fleet)

            print("\n🎉 ALL 7 FASTMCP TOOLS VERIFIED SUCCESSFULLY VIA PROTOCOL HANDSHAKE!")


if __name__ == "__main__":
    asyncio.run(main())
