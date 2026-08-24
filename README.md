# ✈️ Semalar — Live Flight Radar & AI Aviation Assistant (FlightRadar24 + MCP + React UI)

**Semalar** is a modern AI-powered aviation assistant that connects **FlightRadar24's live ADS-B telemetry network**, the **Model Context Protocol (MCP - Streamable HTTP)**, and **Multi-Provider AI (Gemini / Groq / OpenAI / Ollama)** to deliver real-time flight tracking, aircraft model lookup, airspace radar scanning, and airport telemetry through both a **modern React Web UI** and a **terminal CLI**.

---

## 🏗️ 1. Architecture & Overview

To prevent hallucination, the system relies entirely on **Tool Calling (Function Calling)**. The single source of truth is the live FlightRadar24 network.

```text
       ┌─────────────────────────────────────────────────────────────┐
       │                      KULLANICI ARAYÜZLERİ                   │
       │   ┌─────────────────────────┐    ┌──────────────────────┐   │
       │   │  React Web UI (Tarayıcı)│    │  Terminal CLI (Konsol)│   │
       │   │  http://localhost:8000  │    │  backend/flight_cli  │   │
       │   └────────────┬────────────┘    └──────────┬───────────┘   │
       └────────────────┼────────────────────────────┼───────────────┘
                        │ HTTP /api/chat             │ Doğrudan Çağrı
                        ▼                            ▼
       ┌─────────────────────────────────────────────────────────────┐
       │             Ortak AI Motoru (backend/flight_agent.py)       │
       │   - Gemini 3.7 Flash / Groq / OpenAI / Ollama / DeepSeek   │
       │   - MCP Tool Çağrıları (Zero-Hallucination Telemetri)       │
       └──────────────────────────────┬──────────────────────────────┘
                                      │ (2) Tool Call (örn: get_flight_info)
                                      ▼
       ┌─────────────────────────────────────────────────────────────┐
       │           MCP Server & Web Sunucusu (backend/server.py)     │
       │   - Streamable HTTP MCP (/mcp)                              │
       │   - REST API (/api/chat, /api/tracked, /api/status)         │
       │   - React Statik Web UI Sunumu                              │
       └──────────────────────────────┬──────────────────────────────┘
                                      │ (3) Canlı Telemetri Sorgusu
                                      ▼
       ┌─────────────────────────────────────────────────────────────┐
       │          Flight Service (backend/flight_service.py)         │
       │   - Canlı FlightRadar24 ADS-B Küresel Telemetri Ağı        │
       └─────────────────────────────────────────────────────────────┘
```

---

## 📁 2. Proje Yapısı

```text
Semalar/
├── backend/
│   ├── server.py              # MCP Sunucusu, REST API & Web UI Sunumu
│   ├── flight_agent.py        # Çoklu sağlayıcı destekli AI & MCP motoru
│   ├── flight_service.py      # Canlı FlightRadar24 API ve telemetri motoru
│   ├── flight_cli.py          # Terminal CLI interaktif istemcisi
│   └── test_flight_mcp.py     # MCP araçları otomatik testi
├── frontend/
│   ├── index.html             # React Web UI giriş noktası
│   ├── css/style.css          # Koyu mod kokpit/radar tasarım sistemi
│   └── js/
│       ├── app.jsx            # React ana uygulaması
│       ├── api.js             # Backend API istemcisi
│       └── components/        # Modüler React bileşenleri
├── requirements.txt           # Python bağımlılıkları
├── .env                       # API anahtarları ve model ayarları
└── README.md                  # Proje dokümantasyonu
```

---

## 📋 Kayıtlı MCP Havacılık Araçları

| Araç Adı | Parametreler | Açıklama |
| :--- | :--- | :--- |
| **`get_flight_info`** | `query: str` | Uçuş kodu (`TK10`), çağrı işareti (`THY9UC`) veya kuyruk tescili (`TC-JYA`) ile canlı koordinat, irtifa, hız ve uçak modeli getirir. |
| **`search_airline_flights`** | `airline_code: str`, `limit: int?` | Havayolu koduna göre (örn: `THY`, `PGT`, `BAW`, `DLH`) havadaki aktif uçuşları listeler. |
| **`get_flights_over_region`** | `latitude: float`, `longitude: float`, `radius_km: float?` | Belirli koordinatlar çevresindeki hava sahasını radar gibi tarar (örn: İstanbul hava sahası). |
| **`get_most_tracked_flights`** | `limit: int?` | Dünyada anlık olarak FlightRadar24'te en çok izlenen canlı uçuşları getirir. |
| **`get_airport_info`** | `airport_code: str` | IATA/ICAO koduyla (`IST`, `SAW`, `LHR`, `JFK`) havalimanı detayları ve koordinatlarını verir. |

---

## 🚀 Çalıştırma Yöntemleri

### 🌐 Seçenek 1: Web Arayüzü (React UI) ile Çalıştırma (Önerilen)

Sunucuyu başlatın:
```bash
python backend/server.py
```
Tarayıcınızda açın:
👉 **`http://localhost:8000`**

---

### 💻 Seçenek 2: Terminal (CLI) Modunda Çalıştırma

Terminalden yazışmak için:
```bash
python backend/flight_cli.py
```
Veya doğrudan tek soru sorup çıkmak için:
```bash
python backend/flight_cli.py "THY10 nolu uçak şu an nerede ve modeli ne?"
```

---

## 💬 Örnek Sorular

* `THY10 nolu uçak şu an nerede, irtifası kaç ve modeli ne?`
* `Dünyada şu an en çok takip edilen ilk 3 uçuş hangisi?`
* `İstanbul (41.0082, 28.9784) semalarında uçan uçakları göster.`
* `Pegasus'un (PGT) havadaki uçaklarını listele.`
* `IST ve SAW havalimanı bilgileri nelerdir?`
