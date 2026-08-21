# People MCP + Gemini API + SQLite + ngrok

Bu proje, **Python**, **Streamable HTTP MCP Server**, **SQLite**, **ngrok** ve **Google Gemini API (`google-genai`)** kullanarak sıfırdan geliştirilmiş bir Model Context Protocol (MCP) projesidir.

---

## 🏗️ Mimari ve Akış

```text
Kullanıcı ("Ali nerede doğmuş?")
       │
       ▼
Gemini API (google-genai)
       │ (MCP tool kararı: get_person(name="Ali"))
       ▼
ngrok Tüneli (https://xxxx.ngrok.app/mcp)
       │
       ▼
Local MCP Server (http://localhost:8000/mcp - Streamable HTTP)
       │
       ▼
SQLite Database (people.db - Doğum tarihinden dinamik yaş hesabı)
       │
       ▼
MCP Server ──(JSON Sonuç)──> Gemini API ──(Doğal Dil Yanıtı)──> Kullanıcı
```

---

## 🛠️ Proje Dosya Yapısı

```text
people-mcp/
├── server.py             # Streamable HTTP MCP Server (/mcp)
├── database.py           # SQLite veritabanı bağlantısı ve yaş hesaplama
├── seed.py               # 25 adet sahte kişiyi veritabanına ekleme
├── gemini_client.py      # Gemini API ile Remote MCP Server'ı bağlayan istemci
├── run_ngrok.py          # ngrok tünelini tek komutla açan yardımcı script
├── data/
│   └── people.db         # SQLite veritabanı
├── .env                  # API anahtarları ve URL ayarları
├── .env.example          # Örnek konfigürasyon dosyası
├── requirements.txt      # Gerekli Python paketleri
└── README.md
```

---

## 📋 MCP Server Tool'ları

| Tool | Parametreler | Açıklama |
| :--- | :--- | :--- |
| **`get_person`** | `name: str` | İsim ile kişi bilgilerini (doğum yeri, meslek, doğum tarihi ve hesaplanmış yaş) getirir. |
| **`list_people`** | - | Veritabanındaki tüm kişileri listeler. |
| **`search_people`** | `profession: str?`, `birth_place: str?` | Meslek ve/veya doğum yerine göre filtreleme yapar. |

> 💡 **Not:** Yaş veritabanında saklanmaz; her sorguda `birth_date` üzerinden dinamik olarak hesaplanır.

---

## 🚀 Kurulum ve Çalıştırma Adımları

### 1. Adım: Bağımlılıkları Yükleyin
```powershell
# Proje dizinine gidin
cd c:\Users\ocandir\Desktop\people-mcp

# Sanal ortamı aktif edin ve paketleri yükleyin
.\.venv\Scripts\pip install -r requirements.txt
```

### 2. Adım: Veritabanını Doldurun (Seed)
```powershell
.\.venv\Scripts\python seed.py
```

### 3. Adım: .env Dosyasını Düzenleyin
`.env` dosyasını açın ve `GEMINI_API_KEY` değerinizi girin:
```env
GEMINI_API_KEY=AIzaSy...
GEMINI_MODEL=gemini-2.5-flash
PUBLIC_MCP_URL=http://localhost:8000/mcp
```

---

### 4. Adım: Sistemi Çalıştırma (3 Terminal)

#### 💻 Terminal 1: MCP Server'ı Başlatın
```powershell
.\.venv\Scripts\python server.py
```
*(MCP Server `http://localhost:8000/mcp` üzerinde Streamable HTTP ile çalışacaktır).*

#### 💻 Terminal 2: ngrok ile Dışarı Açın (Opsiyonel / Remote Test)
```powershell
.\.venv\Scripts\python run_ngrok.py
```
*(Ekrana gelen `https://xxxx.ngrok-free.app/mcp` adresini kopyalayıp `.env` dosyasındaki `PUBLIC_MCP_URL` alanına yazın).*

#### 💻 Terminal 3: Gemini Client'ı Çalıştırın
```powershell
.\.venv\Scripts\python gemini_client.py
```

---

## 💬 Örnek Doğal Dil Soruları

* `Ali nerede doğmuş?` ➡️ `get_person(name="Ali")`
* `Ankara'da doğan kişileri göster` ➡️ `search_people(birth_place="Ankara")`
* `Doktor olan kişilerin yaşları kaç?` ➡️ `search_people(profession="Doktor")`
* `Veritabanındaki herkesi listele` ➡️ `list_people()`
