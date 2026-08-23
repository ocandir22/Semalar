# ✈️ Semalar — Canlı Uçuş ve Uçak Takip MCP Sunucusu (FlightRadar24 + Gemini AI)

**Semalar**, **FlightRadar24 canlı ADS-B telemetrisini**, **Model Context Protocol (MCP - Streamable HTTP)** standartlarını ve **Google Gemini AI (`google-genai`)** modelini birleştirerek; doğal dilde uçuş takibi, uçak modeli sorgulaması, radar taraması ve havalimanı telemetrisi sunan yeni nesil bir yapay zeka havacılık asistanıdır.

---

## 🏗️ 1. Büyük Resim (Sistem Nasıl Çalışıyor?)

Sistem, yapay zekanın kendi genel bilgisinden uydurmasını (**halüsinasyon**) engellemek için **Tool Calling (Araç Çağırma)** mimarisiyle çalışır. Verinin tek gerçeği canlı FlightRadar24 ağıdır.

```text
 ┌─────────────────┐       (1) Doğal Dil Sorusu       ┌──────────────────┐
 │    Kullanıcı    │ ───────────────────────────────> │  Gemini API (AI) │
 └─────────────────┘                                  └─────────┬────────┘
          ▲                                                     │ (2) "get_flight_info(query='TK2167')"
          │ (5) Doğal Dil Yanıtı                                ▼     (MCP Tool Seçimi)
 ┌────────┴────────┐                                  ┌──────────────────┐
 │  gemini_client  │ <─────────────────────────────── │   server.py      │
 │    (İstemci)    │       (4) Canlı JSON Telemetri   │   (MCP Server)   │
 └─────────────────┘                                  └─────────┬────────┘
                                                                │ (3) Çok Katmanlı Arama
                                                                ▼
                                                      ┌──────────────────┐
                                                      │ flight_service.py│
                                                      │ (FlightRadar24)  │
                                                      └──────────────────┘
```

---

## ⚙️ 2. Çalışma Mekanizması ve Katmanlar (Adım Adım)

Proje 3 temel katmandan oluşur:

### 1. Katman: Veri Katmanı (`flight_service.py`)
FlightRadar24 canlı telemetri ağına bağlanır ve dünya genelindeki 15.000+ uçuş arasından verileri çeker:
* **Çok Katmanlı Global Arama:**
  1. `fr_api.search(query)` ile FlightRadar'ın global indeksinden uçağın canlı `flight_id` değeri yakalanır.
  2. `fr_api.get_flight_details()` ile anlık GPS koordinatları, irtifa izi (`trail`) ve uçak modeli çekilir.
  3. Çağrı kodu (`THY9UC`), sefer no (`TK2167`) ve tescil (`TC-JYA`) birbirine otomatik eşleştirilir.
* **Birim Dönüşümleri & Geometri:** Hız knot'tan km/s'ye, irtifa feet'ten metreye dönüştürülür. Haversine formülü ile şehir koordinatlarına olan mesafe anlık hesaplanır.

### 2. Katman: MCP Sunucu Katmanı (`server.py`)
Python fonksiyonlarını yapay zekanın anlayacağı evrensel MCP standardına dönüştürür:
* **`@mcp_server.tool()`:** Fonksiyon docstring ve tip tanımlarından otomatik JSON-RPC şeması üretir.
* **Streamable HTTP (`/mcp`):** Sunucu `http://localhost:8000/mcp` adresi üzerinden Streamable HTTP standardıyla yayın yapar.
* **Canlı Web Dashboard (`http://localhost:8000`):** Tarayıcı üzerinden sunucu durumunu ve dünyada en çok takip edilen ilk 5 uçuşu gösterir.

### 3. Katman: Gemini İstemci Katmanı (`gemini_client.py`)
Kullanıcı ile yapay zeka arasındaki iletişimi yönetir:
* **Dinamik Araç Keşfi:** MCP sunucusundaki araçları otomatik olarak Gemini `FunctionDeclaration` nesnelerine dönüştürür.
* **Sıfır Halüsinasyon (`temperature=0.0`):** Katı sistem talimatlarıyla modelin yalnızca gelen JSON verisini yorumlaması sağlanır.
* **Hata Toleransı (Fallback):** Model yoğunluklarında otomatik yeniden deneme ve model yedekleme (`gemini-3.7-flash` ➔ `gemini-3.5-flash-lite` ➔ `gemini-2.5-flash`) mekanizması çalışır.

---

## 📋 Tanımlı MCP Uçuş Araçları (Tools)

| Araç Adı | Parametreler | Açıklama |
| :--- | :--- | :--- |
| **`get_flight_info`** | `query: str` | Uçuş no (`TK10`), çağrı kodu (`THY9UC`) veya kuyruk tescilinden (`TC-JYA`) anlık konum, irtifa, hız, rota ve uçak modelini getirir. |
| **`search_airline_flights`** | `airline_code: str`, `limit: int?` | Havayolu koduyla (`THY`, `PGT`, `BAW`, `DLH`, `UAE`) havadaki tüm aktif uçakları listeler. |
| **`get_flights_over_region`** | `latitude: float`, `longitude: float`, `radius_km: float?` | Belirtilen koordinat ve yarıçap çevresindeki hava sahasını tarar (örn: İstanbul semaları). |
| **`get_most_tracked_flights`** | `limit: int?` | Dünyada anlık olarak Flightradar24'te en çok takip edilen ilk 10 uçuşu listeler. |
| **`get_airport_info`** | `airport_code: str` | IATA/ICAO koduna göre havalimanı detaylarını (`IST`, `SAW`, `ESB`, `LHR` vb.) getirir. |

---

## 🛠️ Proje Dosya Yapısı

```text
Semalar/
├── flight_service.py      # Canlı FlightRadar24 API entegrasyonu ve telemetri motoru
├── server.py              # Streamable HTTP MCP Server & Web Dashboard (/ & /mcp)
├── gemini_client.py       # Gemini API ile Remote MCP Server'ı bağlayan havacılık istemcisi
├── test_flight_mcp.py     # MCP araçlarını doğrudan test eden otomatik test betiği
├── requirements.txt       # Gerekli Python bağımlılıkları
├── .env                   # Gemini API Key ve Sunucu URL ayarları
├── .env.example           # Örnek konfigürasyon dosyası
├── .gitignore             # Git dışı bırakılacak dosyalar
└── README.md              # Proje dökümantasyonu
```

---

## 🚀 Kurulum ve Çalıştırma Adımları

### 1. Adım: Bağımlılıkları Yükleyin
```bash
# Sanal ortamı aktif edin (macOS / Linux)
source .venv/bin/activate

# Paketleri yükleyin
pip install -r requirements.txt
```

### 2. Adım: .env Dosyasını Yapılandırın
`.env` dosyasını açıp [Google AI Studio](https://aistudio.google.com/app/apikey)'dan aldığınız API anahtarınızı girin:
```env
GEMINI_API_KEY=your_gemini_api_key_here
GEMINI_MODEL=gemini-3.7-flash
PUBLIC_MCP_URL=http://localhost:8000/mcp
```

---

### 3. Adım: Sistemi Çalıştırma

#### 💻 1. Terminal: MCP Sunucusunu Başlatın
```bash
python server.py
```
* **MCP Endpoint:** `http://localhost:8000/mcp`
* **Canlı Web Dashboard:** `http://localhost:8000`

#### 💻 2. Terminal: Gemini İstemcisini Çalıştırın
```bash
python gemini_client.py
```

---

## 💬 Örnek Doğal Dil Soruları

* `THY9UC uçağının bilgileri nedir ve şu an hangi şehir üzerinde?` ➡️ `get_flight_info(query="THY9UC")`
* `Dünyada şu an en çok takip edilen ilk 3 uçuş hangisi ve modelleri ne?` ➡️ `get_most_tracked_flights(limit=3)`
* `Pegasus'un (PGT) havadaki aktif uçaklarından 3 tanesini listele` ➡️ `search_airline_flights(airline_code="PGT")`
* `İstanbul (41.0082, 28.9784) semalarında 100 km içinde uçan uçaklar hangileri?` ➡️ `get_flights_over_region(...)`
* `IST ve SAW havalimanları hakkında detaylı bilgi ver` ➡️ `get_airport_info(airport_code="IST")`
