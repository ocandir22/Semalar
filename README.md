# ✈️ Semalar — Apache Kafka Live Flight Telemetry & AI Cockpit Platform

<p align="center">
  <b>Real-Time ADS-B Aircraft Tracking, High-Throughput Apache Kafka Stream Engine, 81 Turkish Province Polygon Engine, and AI Aviation Assistant powered by Streamable HTTP Model Context Protocol (MCP)</b>
</p>

---

## 🌟 Overview

**Semalar** is a distributed, high-performance aviation intelligence platform that integrates:
1. **Live ADS-B Telemetry Pipeline**: Continuous ingestion of all active flights in Turkish airspace (~500+ aircraft) from FlightRadar24.
2. **Apache Kafka Stream Engine**: High-throughput streaming to Kafka topic `live-flights`, buffering records in-memory for sub-millisecond querying, statistical analytics, and supersonic flight filtering.
3. **81-Province Polygon Geospatial Engine**: Exact boundary containment evaluations via sub-millisecond Ray-Casting Point-in-Polygon (PIP) algorithms across all 81 Turkish provinces and 7 macro-regions.
4. **Streamable HTTP FastMCP Server**: Official RFC-compliant Model Context Protocol server exposing `query_kafka_stream` over HTTP JSON-RPC (`http://localhost:8000/mcp`).
5. **Multi-Provider AI Flight Agent**: Zero-hallucination conversational AI agent supporting **Groq (Qwen 2.5 / Llama 3.3)**, **Google Gemini (3.7 Flash / 2.5 Flash)**, **OpenAI**, **DeepSeek**, **OpenRouter**, and **Local Ollama**.
6. **Real-Time Kafka Audit Logging**: Automatic telemetry pipeline pushing every MCP tool call, execution metrics, and latency to the Kafka `mcp-requests` topic.
7. **Web Cockpit & Terminal CLI**:
   - ⚡ **Kafka Telemetry Cockpit UI**: `http://localhost:8000`
   - 📊 **Apache Kafka UI Panel**: `http://localhost:8080`
   - 💻 **Interactive Terminal CLI**: `python backend/project_kafka/kafka_cli.py`

---

## 🏗️ Distributed Architecture

```mermaid
graph TD
    subgraph ClientNode [Client Node / AI Agent & Cockpit UI]
        CLI[Terminal CLI: kafka_cli.py]
        WebKafka[Kafka Cockpit UI: kafka.html]
        Agent[AI Agent: kafka_agent.py]
        LLM[Groq / Gemini / OpenAI API]
        
        Agent <--> LLM
        CLI --> Agent
        WebKafka -->|/api/chat| ServerNode
    end

    subgraph ServerNode [Server Node: Telemetry & MCP Engine]
        Server[Starlette / FastMCP Server: server.py]
        KStore[In-Memory Store: FlightKafkaStore]
        GeoEngine[Geospatial Engine: TurkeyGeoEngine]
        Producer[Stream Producer: FlightKafkaProducer]
        Collector[Data Collector: FlightDataCollector]
        Audit[Audit Logger: Topic mcp-requests]
        
        Server --> KStore
        Server --> Audit
        KStore --> GeoEngine
        Producer --> Collector
    end

    subgraph ExternalServices [External Data & Streaming Layer]
        Kafka[Apache Kafka Cluster: 9092]
        KafkaUI[Kafka UI: 8080]
        FR24[FlightRadar24 ADS-B Live Network]
    end

    Agent -->|Dynamic FastMCP Tools| Server
    Producer -->|Publish Turkey flights| Kafka
    KStore -->|Consume live-flights| Kafka
    Collector -->|Fetch ADS-B Telemetry| FR24
    Audit -->|Log Tool Executions| Kafka
```

---

## 📁 Repository Structure

```text
Semalar/
├── backend/
│   ├── core/                  # Shared core infrastructure
│   │   ├── geo_service.py     # 81-Province GeoJSON boundary & ray-casting PIP engine
│   │   ├── audit_logger.py    # Real-time Kafka 'mcp-requests' audit producer & ring buffer
│   │   ├── llm_client.py      # Multi-provider LLM caller (Groq/Gemini/OpenAI) & Thinking timeline
│   │   └── data/              # tr-cities.json & tr-provinces-catalog.json
│   ├── project_kafka/         # Apache Kafka Telemetry Cockpit
│   │   ├── flight_collector.py# FlightRadar24 live scraper & normalizer
│   │   ├── flight_producer.py # Real-time streaming Kafka producer daemon (15s cycle)
│   │   ├── flight_kafka_store.py # In-memory Kafka stream consumer, indexer & polygon filter
│   │   ├── kafka_agent.py     # FastMCP-powered Telemetry AI Agent
│   │   └── kafka_cli.py       # Dedicated Kafka Cockpit terminal CLI
│   ├── server.py              # Central Unified Starlette ASGI & FastMCP Server (Port 8000)
│   └── test_flight_mcp.py     # Automated MCP protocol verification script
├── frontend/
│   ├── kafka.html             # Apache Kafka Telemetry Cockpit & AI Thinking Chat UI
│   └── css/
│       └── style.css          # Dark-mode glassmorphic aviation HUD design system
├── docker-compose.yml         # Apache Kafka (KRaft mode) & Kafka UI stack
├── requirements.txt           # Python dependencies
├── .env                       # API keys & configuration
└── README.md                  # Documentation
```

---

## 📋 Unified FastMCP Tool: `query_kafka_stream`

Every tool execution is intercepted by `core.audit_logger`, recording execution time ($ms$), argument payloads, status, and matched record counts to the Kafka `mcp-requests` audit topic in real-time.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| **`query`** | `string` | Specific flight number (e.g. `TK10`, `MH21`), callsign (`THY10`, `PGT45K`), or tail registration (`TC-LJA`). |
| **`region`** | `string` | Target Turkish province (`İstanbul`, `Ankara`, `Erzurum`) or macro-region (`MARMARA`, `EGE`, `TR`). Evaluated via ray-casting PIP against exact 81-province boundary polygons. |
| **`airline`** | `string` | 3-letter ICAO (`THY`, `PGT`, `DLH`, `BAW`) or 2-letter IATA (`TK`, `PC`, `LH`, `BA`) airline code. |
| **`min_speed_kmh`** | `number` | Minimum ground speed filter in km/h (e.g. 800, 900 for high-speed aircraft). |
| **`min_altitude_feet`** | `number` | Minimum altitude filter in feet (e.g. 32800 for 10,000 meters and above). |
| **`get_stats`** | `boolean` | Pass `true` to retrieve stream statistical summary (max/avg speed and altitude, airline counts). |
| **`limit`** | `integer` | Maximum number of flight records to return (default: 15). |

---

## 🚀 Quickstart

### 1. Start Apache Kafka Cluster
```bash
docker compose up -d
```
Verify Kafka UI at [http://localhost:8080](http://localhost:8080).

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure `.env`
```env
GROQ_API_KEY=your_groq_api_key
# or GEMINI_API_KEY=your_gemini_api_key
```

### 4. Run the Unified Server
```bash
python backend/server.py
```
Open **[http://localhost:8000](http://localhost:8000)** in your browser.

### 5. (Optional) Run Interactive Terminal CLI
```bash
python backend/project_kafka/kafka_cli.py "Ankara semalarında 800 km/s üzeri uçaklar"
```
