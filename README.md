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
       │   │  http://localhost:8000  │    │  gemini_client.py    │   │
       │   └────────────┬────────────┘    └──────────┬───────────┘   │
       └────────────────┼────────────────────────────┼───────────────┘
                        │ HTTP /api/chat             │ Doğrudan Çağrı
                        ▼                            ▼
       ┌─────────────────────────────────────────────────────────────┐
       │               Ortak AI Motoru (flight_agent.py)             │
       │   - Gemini 3.7 Flash / Groq / OpenAI / Ollama / DeepSeek   │
       │   - MCP Tool Çağrıları (Zero-Hallucination Telemetri)       │
       └──────────────────────────────┬──────────────────────────────┘
                                      │ (2) Tool Call (örn: get_flight_info)
                                      ▼
       ┌─────────────────────────────────────────────────────────────┐
       │             MCP Server & Web Sunucusu (server.py)           │
       │   - Streamable HTTP MCP (/mcp)                              │
       │   - REST API (/api/chat, /api/tracked, /api/status)         │
       │   - React Statik Web UI Sunumu                              │
       └──────────────────────────────┬──────────────────────────────┘
                                      │ (3) Canlı Telemetri Sorgusu
                                      ▼
       ┌─────────────────────────────────────────────────────────────┐
       │             Flight Service (flight_service.py)              │
       │   - Canlı FlightRadar24 ADS-B Küresel Telemetri Ağı        │
       └─────────────────────────────────────────────────────────────┘
```

---

## ⚙️ 2. Katman Yapısı & Dosyalar

* **`flight_service.py`**: Canlı FlightRadar24 API motoru (uçuş arama, irtifa/hız birim dönüşümleri, radar koordinat taraması).
* **`flight_agent.py`**: Çoklu sağlayıcı destekli (Gemini, Groq, OpenRouter, Ollama, DeepSeek, OpenAI) ortak AI ve MCP araç çağırma motoru.
* **`server.py`**: Starlette/Uvicorn tabanlı MCP sunucusu, REST API'leri ve React Web arayüzünü tek porttan (`http://localhost:8000`) sunar.
* **`gemini_client.py`**: Terminalden canlı yazışma istemcisi (`FlightRadar [gemini] >`).
* **`frontend/`**: Modern kokpit/radar tasarımlı React Web Arayüzü (Chat balonları, tool trace rozetleri, canlı en çok izlenen uçuşlar paneli).

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

Yalnızca tek bir komutla sunucuyu ve web arayüzünü başlatın:
```bash
python server.py
```
Ardından tarayıcınızda açın:
👉 **`http://localhost:8000`**

* Canlı AI Chatbot ile mesajlaşın.
* Sağ panelde FlightRadar24'ün en çok izlenen canlı uçuşlarını anlık takip edin.
* Tek tıkla uçuş detaylarını asistanınıza sorun.

---

### 💻 Seçenek 2: Terminal (CLI) Modunda Çalıştırma

Eğer terminalden yazışmaya devam etmek isterseniz:

1. Birinci terminalde sunucuyu başlatın:
   ```bash
   python server.py
   ```
2. İkinci terminalde istemciyi açın:
   ```bash
   python gemini_client.py
   ```
   * Veya doğrudan tek soru sorup çıkın:
   ```bash
   python gemini_client.py "TK10 nolu uçak şu an nerede ve modeli ne?"
   ```

---

## 💬 Örnek Sorular

* `THY10 nolu uçak şu an nerede, irtifası kaç ve modeli ne?`
* `Dünyada şu an en çok takip edilen ilk 3 uçuş hangisi?`
* `İstanbul (41.0082, 28.9784) semalarında uçan uçakları göster.`
* `Pegasus'un (PGT) havadaki uçaklarını listele.`
* `IST ve SAW havalimanı bilgileri nelerdir?`
