import asyncio
import sys
import json
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

MCP_URL = "http://localhost:8000/mcp"


async def main():
    print(f"Connecting to MCP Server at: {MCP_URL} ...")
    async with streamable_http_client(MCP_URL) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            print("Connected and initialized!")

            # 1. List tools
            tools_response = await session.list_tools()
            print("\nAvailable Tools:")
            for tool in tools_response.tools:
                print(f" - {tool.name}: {tool.description}")

            # 2. Call get_person
            print("\nCalling get_person(name='Ali Yılmaz')...")
            result_ali = await session.call_tool("get_person", {"name": "Ali Yılmaz"})
            print(f"Result:\n{result_ali.content[0].text if result_ali.content else result_ali}")

            # 3. Call search_people (birth_place='Ankara')
            print("\nCalling search_people(birth_place='Ankara')...")
            result_ankara = await session.call_tool("search_people", {"birth_place": "Ankara"})
            print(f"Result:\n{result_ankara.content[0].text if result_ankara.content else result_ankara}")


if __name__ == "__main__":
    asyncio.run(main())
