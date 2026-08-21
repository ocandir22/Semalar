import os
import sys
import time
from dotenv import load_dotenv
from pyngrok import ngrok

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()

NGROK_AUTHTOKEN = os.getenv("NGROK_AUTHTOKEN")


def start_tunnel():
    print("=" * 60)
    print("🌐 Starting ngrok tunnel for local MCP Server (port 8000)...")
    print("=" * 60)

    if NGROK_AUTHTOKEN:
        ngrok.set_auth_token(NGROK_AUTHTOKEN)

    try:
        tunnel = ngrok.connect(8000, "http")
        public_url = tunnel.public_url.replace("http://", "https://")
        mcp_public_url = f"{public_url}/mcp"

        print(f"\n✅ ngrok Tunnel Active!")
        print(f"🔗 Public Base URL : {public_url}")
        print(f"🎯 Public MCP URL  : {mcp_public_url}")
        print("\n👉 Update your .env file with:")
        print(f"PUBLIC_MCP_URL={mcp_public_url}")
        print("\nPress Ctrl+C to stop tunnel.\n")

        # Keep alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down ngrok tunnel...")
        ngrok.disconnect(tunnel.public_url)
        ngrok.kill()
        print("Tunnel closed.")
    except Exception as e:
        print(f"❌ Error starting ngrok tunnel: {e}")
        print("Tip: If you have ngrok CLI installed, you can also run: ngrok http 8000")


if __name__ == "__main__":
    start_tunnel()
