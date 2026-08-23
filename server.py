import sys
from typing import Optional, List, Dict, Any
from mcp.server import MCPServer
from starlette.responses import HTMLResponse
import uvicorn
from flight_service import (
    get_flight_info as fetch_flight_info,
    search_airline_flights as fetch_airline_flights,
    get_flights_over_region as fetch_flights_over_region,
    get_most_tracked_flights as fetch_most_tracked_flights,
    get_airport_info as fetch_airport_info
)

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Create the MCP Server instance
mcp_server = MCPServer(
    name="flight-radar-mcp",
    description="Streamable HTTP MCP Server providing live FlightRadar24 aircraft telemetry, flight tracking, and airport tools."
)


@mcp_server.tool()
def get_flight_info(query: str) -> Dict[str, Any]:
    """Finds a live flight by flight number (e.g. 'TK10', 'PC2020', 'BA123'), callsign (e.g. 'THY10', 'PGT45K'), or aircraft registration tail (e.g. 'TC-LJA').
    Returns live coordinates, altitude (ft/m), ground speed (kts/kmh), heading, aircraft model (e.g. Boeing 777-3F2(ER)), origin and destination airports, and status.
    
    Args:
        query: Flight number, callsign, or aircraft registration (e.g. 'TK10', 'THY10', 'PC2020')
    """
    return fetch_flight_info(query)


@mcp_server.tool()
def search_airline_flights(airline_code: str, limit: int = 15) -> Dict[str, Any]:
    """Searches live airborne flights currently operated by a specific airline (e.g. 'THY' or 'TK' for Turkish Airlines, 'PGT' or 'PC' for Pegasus, 'DLH' for Lufthansa, 'BAW' for British Airways, 'UAE' for Emirates).
    
    Args:
        airline_code: 3-letter ICAO (e.g. 'THY', 'PGT', 'DLH', 'BAW') or 2-letter IATA (e.g. 'TK', 'PC', 'LH', 'BA') airline code.
        limit: Maximum number of flights to return (default: 15).
    """
    return fetch_airline_flights(airline_code, limit=limit)


@mcp_server.tool()
def get_flights_over_region(latitude: float, longitude: float, radius_km: float = 100.0, limit: int = 15) -> Dict[str, Any]:
    """Finds live flights flying within a given radius (km) around a specific geographic coordinate (latitude, longitude).
    For example: Istanbul (41.0082, 28.9784), Ankara (39.9334, 32.8597), London (51.5074, -0.1278), New York (40.7128, -74.0060).
    
    Args:
        latitude: Latitude in decimal degrees (e.g. 41.0082 for Istanbul)
        longitude: Longitude in decimal degrees (e.g. 28.9784 for Istanbul)
        radius_km: Search radius in kilometers (default: 100 km)
        limit: Maximum number of flights to return (default: 15)
    """
    return fetch_flights_over_region(latitude=latitude, longitude=longitude, radius_km=radius_km, limit=limit)


@mcp_server.tool()
def get_most_tracked_flights(limit: int = 10) -> Dict[str, Any]:
    """Fetches the top live most-tracked flights in the world right now on FlightRadar24, including callsigns, routes, aircraft models, and live tracker counts.
    
    Args:
        limit: Number of top tracked flights to return (default: 10).
    """
    return fetch_most_tracked_flights(limit=limit)


@mcp_server.tool()
def get_airport_info(airport_code: str) -> Dict[str, Any]:
    """Retrieves airport details (name, city, country, coordinates, elevation) for a given 3-letter IATA code (e.g. 'IST', 'SAW', 'ESB', 'LHR', 'JFK') or 4-letter ICAO code (e.g. 'LTFM', 'EGLL', 'KJFK').
    
    Args:
        airport_code: 3-letter IATA or 4-letter ICAO airport code.
    """
    return fetch_airport_info(airport_code)


# Expose Streamable HTTP ASGI app (Starlette) on /mcp endpoint
app = mcp_server.streamable_http_app()


async def home_dashboard(request):
    top_tracked_res = fetch_most_tracked_flights(limit=5)
    top_flights = top_tracked_res.get("most_tracked_flights", []) if isinstance(top_tracked_res, dict) else []

    tracked_rows_html = ""
    for f in top_flights:
        tracked_rows_html += f"""
        <tr>
            <td><strong style="color: #38bdf8;">{f.get('flight_number') or f.get('callsign') or 'N/A'}</strong></td>
            <td>{f.get('callsign') or '-'}</td>
            <td><span class="route-badge">{f.get('route')}</span></td>
            <td>{f.get('aircraft_type') or f.get('model') or '-'}</td>
            <td style="color: #f59e0b; font-weight: bold;">👥 {f.get('live_trackers', 0):,}</td>
        </tr>
        """

    if not tracked_rows_html:
        tracked_rows_html = "<tr><td colspan='5' style='text-align:center; color:#94a3b8;'>Canlı uçuş verisi alınıyor...</td></tr>"

    html_content = rf"""
    <!DOCTYPE html>
    <html lang="tr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Flight Radar MCP Server</title>
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
        <style>
            :root {{
                --bg-main: #090d16;
                --card-bg: #111827;
                --border-color: #1f293d;
                --accent-blue: #0ea5e9;
                --accent-glow: rgba(14, 165, 233, 0.15);
                --accent-green: #10b981;
                --accent-amber: #f59e0b;
                --text-primary: #f8fafc;
                --text-secondary: #94a3b8;
            }}
            * {{ box-sizing: border-box; }}
            body {{
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
                background-color: var(--bg-main);
                background-image: 
                    radial-gradient(circle at 10% 20%, rgba(14, 165, 233, 0.08) 0%, transparent 40%),
                    radial-gradient(circle at 90% 80%, rgba(16, 185, 129, 0.05) 0%, transparent 40%);
                color: var(--text-primary);
                margin: 0;
                padding: 40px 20px;
                display: flex;
                justify-content: center;
                min-height: 100vh;
            }}
            .container {{
                max-width: 900px;
                width: 100%;
            }}
            .card {{
                background: var(--card-bg);
                border: 1px solid var(--border-color);
                border-radius: 16px;
                padding: 32px;
                box-shadow: 0 20px 40px -15px rgba(0, 0, 0, 0.5);
                margin-bottom: 24px;
            }}
            .header-bar {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 20px;
                flex-wrap: wrap;
                gap: 12px;
            }}
            .badge-live {{
                display: inline-flex;
                align-items: center;
                gap: 6px;
                background: rgba(16, 185, 129, 0.12);
                border: 1px solid rgba(16, 185, 129, 0.3);
                color: #34d399;
                font-weight: 600;
                font-size: 13px;
                padding: 6px 14px;
                border-radius: 9999px;
            }}
            .pulse-dot {{
                width: 8px;
                height: 8px;
                background-color: #10b981;
                border-radius: 50%;
                box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
                animation: pulse 1.8s infinite;
            }}
            @keyframes pulse {{
                0% {{ transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }}
                70% {{ transform: scale(1); box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }}
                100% {{ transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }}
            }}
            h1 {{
                margin: 0;
                font-size: 26px;
                font-weight: 700;
                color: #ffffff;
                display: flex;
                align-items: center;
                gap: 10px;
            }}
            p {{ color: var(--text-secondary); line-height: 1.6; margin: 8px 0; }}
            .endpoint-box {{
                background: #0b111e;
                border: 1px solid #24324d;
                padding: 14px 18px;
                border-radius: 10px;
                font-family: 'JetBrains Mono', monospace;
                color: var(--accent-blue);
                font-size: 14px;
                margin: 18px 0;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }}
            .tools-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
                gap: 12px;
                margin: 20px 0;
            }}
            .tool-card {{
                background: #0d1424;
                border: 1px solid #1a263d;
                border-radius: 10px;
                padding: 14px;
                border-left: 3px solid var(--accent-blue);
                transition: transform 0.15s ease, border-color 0.15s ease;
            }}
            .tool-card:hover {{
                transform: translateY(-2px);
                border-left-color: #38bdf8;
                border-color: #2b3d61;
            }}
            .tool-name {{
                font-family: 'JetBrains Mono', monospace;
                font-size: 13px;
                font-weight: 600;
                color: #38bdf8;
                margin-bottom: 4px;
            }}
            .tool-desc {{
                font-size: 12px;
                color: var(--text-secondary);
                line-height: 1.4;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 14px;
                font-size: 13px;
            }}
            th {{
                text-align: left;
                padding: 10px 12px;
                background: #0b111e;
                color: var(--text-secondary);
                font-weight: 600;
                border-bottom: 1px solid var(--border-color);
            }}
            td {{
                padding: 10px 12px;
                border-bottom: 1px solid #1a263d;
            }}
            .route-badge {{
                background: rgba(14, 165, 233, 0.1);
                color: #38bdf8;
                padding: 3px 8px;
                border-radius: 4px;
                font-family: 'JetBrains Mono', monospace;
                font-size: 12px;
            }}
            .instruction-card {{
                background: rgba(14, 165, 233, 0.05);
                border: 1px solid rgba(14, 165, 233, 0.2);
                border-radius: 10px;
                padding: 16px 20px;
                font-size: 13px;
                color: #bae6fd;
            }}
            code {{
                background: #0f172a;
                padding: 2px 6px;
                border-radius: 4px;
                font-family: 'JetBrains Mono', monospace;
                color: #38bdf8;
                font-size: 12px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="card">
                <div class="header-bar">
                    <h1>✈️ FlightRadar24 MCP Server</h1>
                    <span class="badge-live"><div class="pulse-dot"></div> CANLI TELEMETRİ</span>
                </div>
                <p>Canlı FlightRadar24 uçuş takibi, uçak modelleri, coğrafi radar taraması ve havalimanı verilerini Gemini & MCP istemcilerine sunan Model Context Protocol servisi.</p>
                
                <div class="endpoint-box">
                    <span>📡 <strong>MCP Endpoint:</strong> http://localhost:8000/mcp</span>
                    <span style="color: #64748b; font-size: 12px;">Streamable HTTP</span>
                </div>

                <h3 style="margin-top: 24px; font-size: 16px; color: #e2e8f0;">🛠️ Tanımlı Havacılık MCP Araçları</h3>
                <div class="tools-grid">
                    <div class="tool-card">
                        <div class="tool-name">get_flight_info(query)</div>
                        <div class="tool-desc">Uçuş no (TK10), çağrı adı (THY10) veya kuyruk tescilinden (TC-LJA) anlık konum, irtifa, hız ve uçak modelini getirir.</div>
                    </div>
                    <div class="tool-card">
                        <div class="tool-name">search_airline_flights(airline_code)</div>
                        <div class="tool-desc">Havayolu ICAO/IATA koduyla (THY, PGT, BAW, DLH) havadaki tüm aktif uçakları listeler.</div>
                    </div>
                    <div class="tool-card">
                        <div class="tool-name">get_flights_over_region(lat, lon, radius)</div>
                        <div class="tool-desc">Belirtilen enlem/boylam ve yarıçap (km) çevresindeki hava sahasını tarar (örn: İstanbul semaları).</div>
                    </div>
                    <div class="tool-card">
                        <div class="tool-name">get_most_tracked_flights(limit)</div>
                        <div class="tool-desc">Dünya genelinde Flightradar24'te anlık olarak en çok takip edilen ilk 10 uçuşu listeler.</div>
                    </div>
                    <div class="tool-card">
                        <div class="tool-name">get_airport_info(airport_code)</div>
                        <div class="tool-desc">IATA/ICAO koduna göre havalimanı detayları (IST, SAW, ESB, LHR vb.).</div>
                    </div>
                </div>

                <h3 style="margin-top: 28px; font-size: 16px; color: #e2e8f0;">🔥 Dünyada Şu An En Çok Takip Edilen Uçuşlar (Canlı)</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Uçuş No</th>
                            <th>Çağrı Kodu</th>
                            <th>Rota</th>
                            <th>Uçak Tipi</th>
                            <th>Canlı Takipçi</th>
                        </tr>
                    </thead>
                    <tbody>
                        {tracked_rows_html}
                    </tbody>
                </table>

                <div style="margin-top: 24px;" class="instruction-card">
                    💡 <strong>Doğal Dil ile Uçuş Sorgulamak İçin:</strong><br>
                    Terminalde <code>.venv/bin/python gemini_client.py</code> komutunu çalıştırarak Türkçe veya İngilizce soru sorabilirsiniz.
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


app.add_route("/", home_dashboard, methods=["GET"])


if __name__ == "__main__":
    print("=" * 60)
    print("✈️ Starting Flight Radar MCP Server on http://0.0.0.0:8000/mcp")
    print("🌐 Web Dashboard: http://localhost:8000")
    print("=" * 60)
    uvicorn.run(app, host="0.0.0.0", port=8000)
