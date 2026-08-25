# ✈️ Semalar — Live Flight Radar & Apache Kafka Telemetry Platform

<p align="center">
  <b>Real-Time ADS-B Aircraft Tracking, High-Throughput Apache Kafka Stream Engine, and AI Aviation Assistant powered by Streamable HTTP Model Context Protocol (MCP)</b>
</p>

---

## 🌟 Overview

**Semalar** is a distributed, high-performance aviation intelligence platform that integrates:
1. **Live ADS-B Aircraft Telemetry**: Direct integration with the global FlightRadar24 network for live airborne flight tracking, airport info, and regional radar scans.
2. **Apache Kafka Stream Engine**: High-throughput telemetry ingestion, buffering 1200+ active flights in-memory for sub-millisecond querying, statistical analytics, and supersonic flight filtering.
3. **Streamable HTTP MCP Server**: RFC-compliant Model Context Protocol server exposing 11 specialized aviation and Kafka tools.
4. **Multi-Provider AI Flight Agent**: Zero-hallucination conversational AI agent supporting **Google Gemini (3.7 Flash / 2.5 Flash)**, **Groq**, **OpenAI**, **DeepSeek**, **OpenRouter**, and **Local Ollama**.
5. **Dual Web Cockpits & Terminal CLI**:
   - 🔴 **1. Project — Live Radar & AI Chat UI**: `http://localhost:8000`
   - ⚡ **2. Project — Kafka Telemetry Cockpit**: `http://localhost:8000/kafka`
   - 📊 **Apache Kafka UI Panel**: `http://localhost:8080`
   - 💻 **Interactive Terminal CLI**: `python backend/flight_cli.py`

---

## 🏗️ Distributed Architecture

The system is decoupled into independent nodes, enabling it to run seamlessly on a single machine or distributed across multiple physical machines over local network / internet:

```mermaid
graph TD
    subgraph ClientNode [Client Node / PC-B: AI Agent & UI]
        CLI[Terminal CLI: flight_cli.py]
        WebRadar[Live Radar UI: index.html]
        WebKafka[Kafka Cockpit UI: kafka.html]
        Agent[AI Agent: flight_agent.py]
        LLM[Gemini / Groq / OpenAI API]
        
        Agent <--> LLM
        CLI --> Agent
        WebRadar -->|/api/chat| ServerNode
        WebKafka -->|/api/chat| ServerNode
    end

    subgraph ServerNode [Server Node / PC-A: Telemetry & MCP Engine]
        Server[Starlette / FastMCP Server: server.py]
        KStore[In-Memory Store: FlightKafkaStore]
        Producer[Stream Producer: FlightKafkaProducer]
        Collector[Data Collector: FlightDataCollector]
        Audit[Audit Logger: Topic mcp-requests]
        
        Server --> KStore
        Server --> Audit
        Producer --> Collector
    end

    subgraph ExternalServices [External Data & Streaming Layer]
        Kafka[Apache Kafka Cluster: 9092]
        KafkaUI[Kafka UI: 8080]
        FR24[FlightRadar24 Live Network]
    end

    Agent -->|HTTP RPC /api/tools/execute| Server
    Producer -->|Publish 1200+ flights| Kafka
    KStore -->|Consume live-flights| Kafka
    Collector -->|Fetch ADS-B Telemetry| FR24
    Audit -->|Log Tool Executions| Kafka
```

---

## 📁 Repository Structure

```text
Semalar/
├── backend/
│   ├── server.py              # Starlette ASGI & FastMCP Server (Port 8000)
│   ├── flight_agent.py        # Multi-provider AI Agent (Remote HTTP Tool Dispatcher)
│   ├── flight_kafka_store.py  # In-memory Kafka stream consumer & indexer
│   ├── flight_producer.py     # Batch & streaming Kafka producer (1200+ flights)
│   ├── flight_collector.py    # FlightRadar24 raw data scraper & normalizer
│   ├── flight_service.py      # Direct live FlightRadar24 querying service
│   ├── flight_cli.py          # Interactive terminal aviation AI client
│   └── test_flight_mcp.py     # Automated MCP protocol verification script
├── frontend/
│   ├── index.html             # 1. Project: Live Flight Radar & AI Assistant
│   ├── kafka.html             # 2. Project: Apache Kafka 1200+ Telemetry Cockpit
│   ├── css/
│   │   └── style.css          # Dark-mode glassmorphic aviation design system
│   └── js/
│       ├── app.jsx            # React Live Radar frontend
│       ├── kafka_app.jsx      # React Kafka Dashboard frontend
│       ├── api.js             # API client service
│       └── components/        # Reusable React components
├── docker-compose.yml         # Apache Kafka (KRaft mode) & Kafka UI stack
├── requirements.txt           # Python dependencies
├── .env                       # API keys & configuration
└── README.md                  # Comprehensive documentation
```

---

## 📋 Registered MCP Aviation Tools (11 Tools)

Every tool is decorated with `@audit_tool`, which automatically records execution time ($ms$), argument payloads, and record counts to the Kafka `mcp-requests` audit topic in real-time.

| Tool Name | Parameters | Description |
| :--- | :--- | :--- |
| **`get_flights_above_speed`** | `min_speed_kmh: float`, `limit: int` | Filters supersonic / high-speed flights from Kafka buffer exceeding threshold (e.g. 850 km/h, 900 km/h). |
| **`get_flight_from_kafka`** | `flight_code: str` | Sub-millisecond instant lookup for a flight in Kafka memory by flight number, callsign, or tail registration. |
| **`get_flights_over_region_from_kafka`** | `latitude`, `longitude`, `radius_km` | Regional airspace radar scan against buffered Kafka telemetry using Haversine calculation. |
| **`search_airline_from_kafka`** | `airline_code: str`, `limit: int` | Instant airline fleet search from Kafka stream (e.g. THY, PGT, DLH, BAW). |
| **`get_kafka_stream_stats`** | *None* | Statistical summary across Kafka stream: total aircraft, unique airlines, max & average speed and altitude. |
| **`refresh_kafka_stream`** | *None* | Synchronizes the in-memory cache with the latest messages from the Kafka `live-flights` topic. |
| **`get_flight_info`** | `query: str` | Live global search on FlightRadar24 network by flight number (`TK10`), callsign (`THY9UC`), or registration (`TC-JYA`). |
| **`search_airline_flights`** | `airline_code: str`, `limit: int` | Direct live FlightRadar query for active airborne flights of a given airline. |
| **`get_flights_over_region`** | `latitude`, `longitude`, `radius_km` | Direct live FlightRadar regional radar scan around coordinates. |
| **`get_most_tracked_flights`** | `limit: int` | Returns top most-tracked live flights in the world on FlightRadar24. |
| **`get_airport_info`** | `airport_code: str` | Retrieves airport metadata, coordinates, elevation, and ground traffic (e.g. `IST`, `SAW`, `LHR`, `JFK`). |

---

## 🚀 Quick Start Guide

### 1. Prerequisites
- Python 3.10+
- Docker & Docker Compose (for Kafka cluster)
- Google Gemini API Key (or OpenAI / Groq / OpenRouter key)

### 2. Environment Configuration
Create a `.env` file in the project root:
```env
# LLM Provider: "gemini", "groq", "openrouter", "openai", "deepseek", "ollama"
LLM_PROVIDER=gemini

# Google Gemini API Key
GEMINI_API_KEY=AIzaSy...your_gemini_api_key_here
GEMINI_MODEL=gemini-3.7-flash

# Groq / OpenAI (Optional)
GROQ_API_KEY=gsk_...
GROQ_MODEL=qwen/qwen3.6-27b

# MCP Server Endpoint
MCP_SERVER_URL=http://localhost:8000
```

### 3. Start Apache Kafka & Kafka UI
```bash
docker compose up -d
```
- **Apache Kafka Broker**: `localhost:9092`
- **Apache Kafka UI Panel**: 👉 **`http://localhost:8080`**

### 4. Start the Application Server
```bash
python backend/server.py
```

### 5. Access the Web Interfaces
- **1. Project (Live Radar & AI Assistant)**: 👉 **`http://localhost:8000`**
- **2. Project (Kafka Telemetry Cockpit)**: 👉 **`http://localhost:8000/kafka`**
- **MCP Protocol Endpoint**: 👉 **`http://localhost:8000/mcp`**

---

## 🖥️ Distributed Deployment (Running Across 2 Different PCs)

Because the AI Agent (`flight_agent.py`) communicates with the MCP server via standard HTTP RPC, you can separate the workload across multiple machines:

```text
[PC-A: Server Node (192.168.1.100)]               [PC-B: AI Agent Client Node]
  - Docker (Kafka :9092)                             - Only needs Python + .env
  - python backend/server.py (port 8000)             - MCP_SERVER_URL=http://192.168.1.100:8000
                                                     - python backend/flight_cli.py
```

1. **On Server PC (PC-A)**: Run `python backend/server.py`.
2. **On Client PC (PC-B)**: Set `MCP_SERVER_URL=http://<PC-A-IP>:8000` in `.env` and run `python backend/flight_cli.py`.

---

## 💬 Sample AI Assistant Queries

You can ask questions in natural language (English or Turkish) in the Web Chat or Terminal CLI:

- *"Show me all flights in the Kafka buffer flying faster than 900 km/h and their aircraft models."*
- *"What are the current Kafka stream telemetry stats? What is the maximum speed recorded?"*
- *"Where is flight THY10 right now, what is its altitude, ground speed, and route?"*
- *"What are the top 3 most tracked flights in the world right now?"*
- *"Scan the airspace around Istanbul coordinates (41.0082, 28.9784) with a 200 km radius."*
- *"List all airborne Pegasus (PGT) flights currently in flight."*
- *"Give me full airport information for IST and SAW."*

---

## 🛡️ License

This project is open-source and available under the **MIT License**.
