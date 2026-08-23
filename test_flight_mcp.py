import asyncio
import sys
import json
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

MCP_URL = "http://localhost:8000/mcp"


async def main():
    print(f"Connecting to Flight Radar MCP Server at: {MCP_URL} ...")
    async with streamable_http_client(MCP_URL) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            print("Connected and initialized!")

            # 1. List tools
            tools_response = await session.list_tools()
            print(f"\nAvailable Tools ({len(tools_response.tools)}):")
            for tool in tools_response.tools:
                first_line = tool.description.splitlines()[0] if tool.description else ""
                print(f" • {tool.name}: {first_line}")

            # 2. Call get_most_tracked_flights
            print("\n" + "=" * 50)
            print("Calling get_most_tracked_flights(limit=3)...")
            res_tracked = await session.call_tool("get_most_tracked_flights", {"limit": 3})
            print("Result:")
            print(res_tracked.content[0].text if res_tracked.content else res_tracked)

            # 3. Call get_airport_info(airport_code='IST')
            print("\n" + "=" * 50)
            print("Calling get_airport_info(airport_code='IST')...")
            res_airport = await session.call_tool("get_airport_info", {"airport_code": "IST"})
            print("Result:")
            print(res_airport.content[0].text if res_airport.content else res_airport)

            # 4. Call search_airline_flights(airline_code='THY', limit=2)
            print("\n" + "=" * 50)
            print("Calling search_airline_flights(airline_code='THY', limit=2)...")
            res_airline = await session.call_tool("search_airline_flights", {"airline_code": "THY", "limit": 2})
            print("Result:")
            print(res_airline.content[0].text if res_airline.content else res_airline)


if __name__ == "__main__":
    asyncio.run(main())
