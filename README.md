# ✈️ Semalar — Live Flight & Aircraft Tracking MCP Server (FlightRadar24 + Gemini AI)

**Semalar** is a modern AI-powered aviation assistant that connects **FlightRadar24's live ADS-B telemetry network**, the **Model Context Protocol (MCP - Streamable HTTP)**, and **Google Gemini AI (`google-genai`)** to deliver real-time flight tracking, aircraft model lookup, airspace radar scanning, and airport telemetry through natural language queries.

---

## 🏗️ 1. Architecture & Overview

To prevent hallucination, the system relies entirely on **Tool Calling (Function Calling)**. The single source of truth is the live FlightRadar24 network.

```text
 ┌─────────────────┐       (1) Natural Language Query       ┌──────────────────┐
 │      User       │ ─────────────────────────────────────> │  Gemini API (AI) │
 └─────────────────┘                                        └─────────┬────────┘
          ▲                                                           │ (2) "get_flight_info(query='TK2167')"
          │ (5) Natural Language Report                               ▼     (MCP Tool Selection)
 ┌────────┴────────┐                                        ┌──────────────────┐
 │  gemini_client  │ <───────────────────────────────────── │   server.py      │
 │    (Client)     │       (4) Live Telemetry JSON          │   (MCP Server)   │
 └─────────────────┘                                        └─────────┬────────┘
                                                                      │ (3) Multi-Tiered Search
                                                                      ▼
                                                            ┌──────────────────┐
                                                            │ flight_service.py│
                                                            │ (FlightRadar24)  │
                                                            └──────────────────┘
```

---

## ⚙️ 2. Step-by-Step Layer Breakdown

The project is architected into 3 decoupled layers:

### Layer 1: Data & Telemetry Engine (`flight_service.py`)
Connects directly to the live FlightRadar24 network to query over 15,000+ active airborne aircraft globally:
* **Multi-Tiered Global Search:**
  1. `fr_api.search(query)` looks up FlightRadar's global search index to pinpoint the exact live `flight_id`.
  2. `fr_api.get_flight_details()` pulls real-time GPS coordinates, altitude/speed breadcrumbs (`trail`), and exact aircraft model information.
  3. Seamlessly resolves callsigns (`THY9UC`), flight numbers (`TK2167`), and tail registrations (`TC-JYA`).
* **Unit Conversions & Geometrics:** Converts knots to km/h, feet to meters, and computes distance from coordinates using the Haversine formula.

### Layer 2: MCP Server Layer (`server.py`)
Exposes Python functions via the Model Context Protocol (MCP):
* **`@mcp_server.tool()`:** Generates JSON Schema definitions from docstrings and parameter type hints.
* **Streamable HTTP (`/mcp`):** Serves tools via Streamable HTTP at `http://localhost:8000/mcp` for local and remote clients.
* **Live Web Dashboard (`http://localhost:8000`):** Interactive status monitor displaying registered MCP tools and globally top-tracked live flights.

### Layer 3: Gemini AI Client Layer (`gemini_client.py`)
Handles the conversational interface between the user, Gemini AI, and the MCP server:
* **Dynamic Tool Discovery:** Automatically converts MCP tool definitions into Gemini `FunctionDeclaration` objects upon connection.
* **Zero Hallucination (`temperature=0.0`):** Strict system instructions guarantee that Gemini grounds answers only in the returned JSON telemetry.
* **Fault Tolerance & Fallback:** Automatically handles transient 503/429 spikes by retrying and falling back to alternative models (`gemini-3.7-flash` ➔ `gemini-3.5-flash-lite` ➔ `gemini-2.5-flash`).

---

## 📋 Registered MCP Aviation Tools

| Tool Name | Parameters | Description |
| :--- | :--- | :--- |
| **`get_flight_info`** | `query: str` | Retrieves live telemetry, altitude (ft/m), ground speed (kts/kmh), heading, route, and aircraft model by flight number (`TK10`), callsign (`THY9UC`), or registration (`TC-JYA`). |
| **`search_airline_flights`** | `airline_code: str`, `limit: int?` | Lists all active airborne flights for an airline by ICAO/IATA code (`THY`, `PGT`, `BAW`, `DLH`, `UAE`). |
| **`get_flights_over_region`** | `latitude: float`, `longitude: float`, `radius_km: float?` | Scans airspace within a given radius (km) around specified coordinates (e.g. Istanbul airspace). |
| **`get_most_tracked_flights`** | `limit: int?` | Fetches the top live most-tracked flights in the world on FlightRadar24. |
| **`get_airport_info`** | `airport_code: str` | Retrieves airport coordinates, city, country, and elevation by IATA/ICAO code (`IST`, `SAW`, `ESB`, `LHR`, `JFK`, etc.). |

---

## 🛠️ Project Structure

```text
Semalar/
├── flight_service.py      # Live FlightRadar24 API & telemetry search engine
├── server.py              # Streamable HTTP MCP Server & Web Dashboard (/ & /mcp)
├── gemini_client.py       # Gemini AI client with MCP tool calling
├── test_flight_mcp.py     # Automated test script for MCP flight tools
├── requirements.txt       # Python dependencies
├── .env                   # API keys and server configuration
├── .env.example           # Example environment template
├── .gitignore             # Git ignored files
└── README.md              # Project documentation
```

---

## 🚀 Getting Started

### Step 1: Install Dependencies
```bash
# Activate virtual environment (macOS / Linux)
source .venv/bin/activate

# Install requirements
pip install -r requirements.txt
```

### Step 2: Configure Environment (`.env`)
Create/edit `.env` and add your [Google AI Studio](https://aistudio.google.com/app/apikey) API key:
```env
GEMINI_API_KEY=your_gemini_api_key_here
GEMINI_MODEL=gemini-3.7-flash
PUBLIC_MCP_URL=http://localhost:8000/mcp
```

---

### Step 3: Run the System

#### 💻 Terminal 1: Start MCP Server
```bash
python server.py
```
* **MCP Endpoint:** `http://localhost:8000/mcp`
* **Live Web Dashboard:** `http://localhost:8000`

#### 💻 Terminal 2: Start Gemini Client
```bash
python gemini_client.py
```

---

## 💬 Sample Queries

* `What are the details of flight THY9UC and what city is it currently flying over?` ➡️ `get_flight_info(query="THY9UC")`
* `What are the top 3 most tracked flights in the world right now and what aircraft are they flying?` ➡️ `get_most_tracked_flights(limit=3)`
* `List 3 active airborne flights operated by Pegasus (PGT)` ➡️ `search_airline_flights(airline_code="PGT")`
* `Show flights within 100 km of Istanbul (41.0082, 28.9784)` ➡️ `get_flights_over_region(...)`
AQ.Ab8RN6KJkiUu2Bq6ovfhHTZslvQ5Q_hVR4nFvQ4RpIApNOYDNQ
