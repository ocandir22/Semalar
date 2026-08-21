import sys
from typing import Optional, List, Dict, Any
from mcp.server import MCPServer
from starlette.responses import HTMLResponse
import uvicorn
from database import get_person_by_name, list_all_people, search_people_db, init_db

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Ensure database is initialized
init_db()

# Create the MCP Server instance
mcp_server = MCPServer(
    name="people-mcp",
    description="MCP Server providing tools to query people and calculated age from SQLite"
)


@mcp_server.tool()
def get_person(name: str) -> Dict[str, Any]:
    """Get detailed information about a specific person by their name.
    Returns the person's name, calculated age, birth date, profession, and birth place.
    
    Args:
        name: Full or partial name of the person (e.g. 'Ali', 'Ali Yılmaz', 'Ayşe')
    """
    person = get_person_by_name(name)
    if not person:
        search_results = search_people_db(profession=None, birth_place=None)
        matches = [p for p in search_results if name.lower() in p["name"].lower()]
        if len(matches) == 1:
            return {"status": "success", "data": matches[0]}
        elif len(matches) > 1:
            return {"status": "multiple_matches", "matches": matches}
        return {"status": "not_found", "message": f"Person '{name}' was not found in the database."}
    return {"status": "success", "data": person}


@mcp_server.tool()
def list_people() -> List[Dict[str, Any]]:
    """List all people stored in the database with their details and calculated age."""
    return list_all_people()


@mcp_server.tool()
def search_people(profession: Optional[str] = None, birth_place: Optional[str] = None) -> List[Dict[str, Any]]:
    """Search and filter people by profession and/or birth place. Returns matched people with their calculated age.
    
    Args:
        profession: Filter by profession (e.g. 'Yazılım Mühendisi', 'Doktor', 'Avukat', 'Mimar')
        birth_place: Filter by city of birth (e.g. 'Ankara', 'İstanbul', 'İzmir', 'Antalya')
    """
    return search_people_db(profession=profession, birth_place=birth_place)


# Expose Streamable HTTP ASGI app (Starlette) on /mcp endpoint
app = mcp_server.streamable_http_app()


async def home_dashboard(request):
    people = list_all_people()
    html_content = rf"""
    <!DOCTYPE html>
    <html lang="tr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>People MCP Server Status</title>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                background: #0f172a;
                color: #f8fafc;
                margin: 0;
                padding: 40px 20px;
                display: flex;
                justify-content: center;
            }}
            .card {{
                background: #1e293b;
                border: 1px solid #334155;
                border-radius: 12px;
                max-width: 750px;
                width: 100%;
                padding: 30px;
                box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.3);
            }}
            .badge {{
                display: inline-block;
                background: #10b981;
                color: #064e3b;
                font-weight: bold;
                font-size: 13px;
                padding: 4px 10px;
                border-radius: 20px;
                margin-bottom: 15px;
            }}
            h1 {{ margin: 0 0 10px 0; font-size: 24px; color: #38bdf8; }}
            p {{ color: #94a3b8; line-height: 1.6; margin: 6px 0; }}
            .endpoint {{
                background: #0f172a;
                padding: 12px 16px;
                border-radius: 8px;
                border: 1px solid #475569;
                font-family: monospace;
                color: #38bdf8;
                margin: 15px 0;
                word-break: break-all;
            }}
            .tools-list {{
                list-style: none;
                padding: 0;
                margin: 15px 0;
            }}
            .tools-list li {{
                background: #0f172a;
                margin-bottom: 8px;
                padding: 10px 14px;
                border-radius: 6px;
                border-left: 3px solid #38bdf8;
                font-size: 14px;
            }}
            .info-box {{
                background: rgba(56, 189, 248, 0.1);
                border: 1px solid rgba(56, 189, 248, 0.3);
                padding: 12px 16px;
                border-radius: 8px;
                margin-top: 20px;
                font-size: 14px;
                color: #bae6fd;
            }}
        </style>
    </head>
    <body>
        <div class="card">
            <span class="badge">● SERVER RUNNING</span>
            <h1>People MCP Server (Streamable HTTP)</h1>
            <p>MCP Server başarıyla çalışıyor! Toplam <strong>{len(people)} kişi</strong> SQLite veritabanına yüklü.</p>
            
            <p><strong>📡 MCP Endpoint (Gemini / Claude / MCP Client için):</strong></p>
            <div class="endpoint">http://localhost:8000/mcp</div>

            <p><strong>🛠️ Tanımlı MCP Tool'ları:</strong></p>
            <ul class="tools-list">
                <li><code>get_person(name)</code> — İsim ile kişi bilgilerini ve dinamik hesaplanan yaşını getirir.</li>
                <li><code>search_people(profession, birth_place)</code> — Meslek ve doğum yerine göre arama yapar.</li>
                <li><code>list_people()</code> — Veritabanındaki tüm kişileri listeler.</li>
            </ul>

            <div class="info-box">
                ℹ️ <strong>Nasıl Kullanılır?</strong><br>
                Bu sunucu bir web sitesi değil, bir <strong>Model Context Protocol (MCP)</strong> servisidir. 
                Soru sormak için yeni bir terminal açıp <code>.\.venv\Scripts\python gemini_client.py</code> komutunu çalıştırın.
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


# Add dashboard route for browser visits
app.add_route("/", home_dashboard, methods=["GET"])


if __name__ == "__main__":
    print("🚀 Starting People MCP Server on http://0.0.0.0:8000/mcp (Streamable HTTP)")
    print("🌐 Web Dashboard: http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
