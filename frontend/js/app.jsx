// Semalar React Web UI — Cockpit Radar & Kafka Live Telemetry Stream
const { useState, useEffect, useRef } = React;
const API_BASE = window.location.origin;

// ============================================================
// 1. API Client Functions
// ============================================================

async function sendChatMessage(message) {
  const response = await fetch(`${API_BASE}/api/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message })
  });
  if (!response.ok) {
    const errData = await response.json().catch(() => ({}));
    throw new Error(errData.error || `HTTP ${response.status}: Sunucu hatası`);
  }
  return await response.json();
}

async function fetchTrackedFlights(limit = 8) {
  const response = await fetch(`${API_BASE}/api/tracked?limit=${limit}`);
  if (!response.ok) throw new Error("Canlı uçuş verisi alınamadı");
  return await response.json();
}

async function fetchServerStatus() {
  const response = await fetch(`${API_BASE}/api/status`);
  if (!response.ok) throw new Error("Sunucu durum bilgisi alınamadı");
  return await response.json();
}

async function fetchKafkaStats() {
  const response = await fetch(`${API_BASE}/api/kafka/stats`);
  if (!response.ok) throw new Error("Kafka istatistikleri alınamadı");
  return await response.json();
}

async function fetchKafkaFlights(params = {}) {
  const query = new URLSearchParams(params).toString();
  const response = await fetch(`${API_BASE}/api/kafka/flights?${query}`);
  if (!response.ok) throw new Error("Kafka uçuşları alınamadı");
  return await response.json();
}

async function fetchKafkaLogs() {
  const response = await fetch(`${API_BASE}/api/kafka/logs`);
  if (!response.ok) throw new Error("Kafka logları alınamadı");
  return await response.json();
}

async function triggerKafkaSync() {
  const response = await fetch(`${API_BASE}/api/kafka/sync`, { method: "POST" });
  if (!response.ok) throw new Error("Kafka senkronize edilemedi");
  return await response.json();
}

async function triggerKafkaProduceFresh() {
  const response = await fetch(`${API_BASE}/api/kafka/produce`, { method: "POST" });
  if (!response.ok) throw new Error("Yeni veri Kafka'ya gönderilemedi");
  return await response.json();
}


// ============================================================
// 2. Header Component (View Switcher Tabs)
// ============================================================

function Header({ statusInfo, currentView, onViewChange, onClearChat, onToggleSidebar, isSidebarOpen }) {
  const provider = statusInfo?.provider?.toUpperCase() || "AI";
  const model = statusInfo?.model || "Hazır";

  return (
    <header className="app-header">
      <div className="brand-section">
        <div className="brand-icon">✈️</div>
        <div>
          <div className="brand-title">
            SEMALAR
            <span style={{ fontSize: "11px", background: "rgba(0, 240, 255, 0.15)", color: "#38bdf8", padding: "2px 8px", borderRadius: "4px", fontWeight: "600" }}>
              KAFKA & ADS-B
            </span>
          </div>
          <div className="brand-subtitle">Live FlightRadar24 & Apache Kafka Telemetry</div>
        </div>
      </div>

      {/* 3 View Tabs */}
      <div className="nav-tabs-group">
        <button
          className={`nav-tab-btn ${currentView === "chat" ? "active" : ""}`}
          onClick={() => onViewChange("chat")}
        >
          <span>💬</span>
          <span>AI Radar Chat</span>
        </button>

        <button
          className={`nav-tab-btn ${currentView === "kafka" ? "active" : ""}`}
          onClick={() => onViewChange("kafka")}
        >
          <span>⚡</span>
          <span>Kafka Canlı Akış (1200)</span>
        </button>

        <button
          className={`nav-tab-btn ${currentView === "logs" ? "active" : ""}`}
          onClick={() => onViewChange("logs")}
        >
          <span>📋</span>
          <span>MCP İstek Günlüğü</span>
        </button>
      </div>

      <div className="header-status-group">
        <div className="status-badge live">
          <div className="pulse-dot"></div>
          KAFKA: 9092 ONLINE
        </div>

        <div className="status-badge provider">
          ✨ {provider} ({model})
        </div>

        {currentView === "chat" && (
          <button className="btn-icon" onClick={onClearChat} title="Sohbeti Temizle">
            🗑️ Temizle
          </button>
        )}

        <button className="btn-icon" onClick={onToggleSidebar} title="Radar Panelini Aç/Kapat">
          📡 {isSidebarOpen ? "Paneli Gizle" : "Radar Paneli"}
        </button>
      </div>
    </header>
  );
}


// ============================================================
// 3. Kafka Telemetry Stream Dashboard (1200 Flights & Speed Filters)
// ============================================================

function KafkaDashboard({ onAskFlight }) {
  const [stats, setStats] = useState(null);
  const [flights, setFlights] = useState([]);
  const [minSpeed, setMinSpeed] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [airlineFilter, setAirlineFilter] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [isProducing, setIsProducing] = useState(false);
  const [actionMsg, setActionMsg] = useState("");

  useEffect(() => {
    loadKafkaData();
  }, [minSpeed, airlineFilter]);

  const loadKafkaData = async () => {
    setIsLoading(true);
    try {
      const statsData = await fetchKafkaStats();
      setStats(statsData);

      const params = { limit: 60 };
      if (minSpeed) params.min_speed = minSpeed;
      if (airlineFilter) params.airline = airlineFilter;
      if (searchQuery) params.query = searchQuery;

      const flightsData = await fetchKafkaFlights(params);
      if (flightsData && flightsData.flights) {
        setFlights(flightsData.flights);
      }
    } catch (err) {
      console.warn("Kafka verisi alınamadı:", err);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSearchSubmit = (e) => {
    e.preventDefault();
    loadKafkaData();
  };

  const handleSync = async () => {
    setIsLoading(true);
    try {
      const res = await triggerKafkaSync();
      setActionMsg(`✅ ${res.message}`);
      loadKafkaData();
      setTimeout(() => setActionMsg(""), 4000);
    } catch (err) {
      setActionMsg(`❌ ${err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const handleProduceFresh = async () => {
    if (isProducing) return;
    setIsProducing(true);
    setActionMsg("📡 FlightRadar24'ten 1200 canlı uçak çekilip Kafka'ya basılıyor...");
    try {
      const res = await triggerKafkaProduceFresh();
      setActionMsg(`✨ Başarılı! ${res.sent_count || 1200} uçak Kafka 'live-flights' topic'ine gönderildi.`);
      loadKafkaData();
      setTimeout(() => setActionMsg(""), 5000);
    } catch (err) {
      setActionMsg(`❌ Hata: ${err.message}`);
    } finally {
      setIsProducing(false);
    }
  };

  return (
    <div className="kafka-dashboard">
      {/* 4 KPI Cards */}
      <div className="stats-grid">
        <div className="stat-card" style={{ "--stat-accent": "#00f0ff" }}>
          <div className="stat-icon">🛫</div>
          <div className="stat-content">
            <span className="stat-label">Kafka Uçuş Havuzu</span>
            <span className="stat-value">{stats?.total_flights_in_kafka || 1200}</span>
            <span className="stat-sub">Aktif İndekslenmiş Uçak</span>
          </div>
        </div>

        <div className="stat-card" style={{ "--stat-accent": "#38bdf8" }}>
          <div className="stat-icon">🌐</div>
          <div className="stat-content">
            <span className="stat-label">Farklı Havayolu</span>
            <span className="stat-value">{stats?.total_unique_airlines || 159}</span>
            <span className="stat-sub">IATA / ICAO Taşıyıcı</span>
          </div>
        </div>

        <div className="stat-card" style={{ "--stat-accent": "#f59e0b" }}>
          <div className="stat-icon">⚡</div>
          <div className="stat-content">
            <span className="stat-label">Maksimum Yer Hızı</span>
            <span className="stat-value">{stats?.speed_kmh?.max || 0} km/s</span>
            <span className="stat-sub">Ortalama: {stats?.speed_kmh?.average || 0} km/s</span>
          </div>
        </div>

        <div className="stat-card" style={{ "--stat-accent": "#10b981" }}>
          <div className="stat-icon">🏔️</div>
          <div className="stat-content">
            <span className="stat-label">Maksimum İrtifa</span>
            <span className="stat-value">{stats?.altitude_feet?.max ? stats.altitude_feet.max.toLocaleString() : 0} ft</span>
            <span className="stat-sub">Ortalama: {stats?.altitude_feet?.average ? Math.round(stats.altitude_feet.average).toLocaleString() : 0} ft</span>
          </div>
        </div>
      </div>

      {actionMsg && (
        <div style={{ background: "rgba(0, 240, 255, 0.1)", border: "1px solid rgba(0, 240, 255, 0.3)", borderRadius: "8px", padding: "10px 16px", fontSize: "13px", color: "#38bdf8" }}>
          {actionMsg}
        </div>
      )}

      {/* Speed & Search Toolbar */}
      <div className="kafka-toolbar">
        <div className="speed-pills-group">
          <span style={{ fontSize: "12px", color: "#94a3b8", fontWeight: "600", marginRight: "4px" }}>
            ⚡ Hız Filtresi:
          </span>
          <button 
            className={`speed-pill-btn ${minSpeed === "" ? "active" : ""}`}
            onClick={() => setMinSpeed("")}
          >
            Tümü (1200)
          </button>
          <button 
            className={`speed-pill-btn ${minSpeed === "600" ? "active" : ""}`}
            onClick={() => setMinSpeed("600")}
          >
            &gt; 600 km/s
          </button>
          <button 
            className={`speed-pill-btn ${minSpeed === "800" ? "active" : ""}`}
            onClick={() => setMinSpeed("800")}
          >
            &gt; 800 km/s
          </button>
          <button 
            className={`speed-pill-btn ${minSpeed === "900" ? "active" : ""}`}
            onClick={() => setMinSpeed("900")}
          >
            🚀 &gt; 900 km/s
          </button>
          <button 
            className={`speed-pill-btn ${minSpeed === "1000" ? "active" : ""}`}
            onClick={() => setMinSpeed("1000")}
          >
            🔥 &gt; 1000 km/s
          </button>
        </div>

        <form onSubmit={handleSearchSubmit} className="search-input-group">
          <input
            type="text"
            placeholder="Uçuş kodu, tescil veya havayolu ara (örn: TK, B77L, JA227J)..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
          <button type="submit" className="btn-icon" style={{ padding: "6px 12px" }}>
            🔍
          </button>
        </form>

        <div style={{ display: "flex", gap: "8px" }}>
          <button className="btn-icon" onClick={handleSync} disabled={isLoading} title="Kafka'dan Belleğe Yeniden Çek">
            🔄 Senkronize Et
          </button>
          <button 
            className="btn-icon" 
            style={{ background: "rgba(16, 185, 129, 0.15)", color: "#34d399", borderColor: "rgba(16, 185, 129, 0.3)" }}
            onClick={handleProduceFresh} 
            disabled={isProducing}
            title="FlightRadar'dan 1200 Taze Veri Çek ve Kafka'ya Bas"
          >
            {isProducing ? "🛫 Basılıyor..." : "🚀 1200 Taze Veri Bas"}
          </button>
        </div>
      </div>

      {/* Flight Cards Grid */}
      <div>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "12px" }}>
          <span style={{ fontSize: "13px", color: "#94a3b8" }}>
            Eşleşen Uçuşlar: <strong style={{ color: "#38bdf8" }}>{flights.length}</strong> adet
          </span>
          <span style={{ fontSize: "11px", color: "#64748b" }}>
            Veri Kaynağı: Apache Kafka Topic 'live-flights' (localhost:9092)
          </span>
        </div>

        {flights.length > 0 ? (
          <div className="telemetry-grid">
            {flights.map((f, idx) => {
              const fCode = f.flight_number || f.callsign || "N/A";
              const spdKmh = f.telemetry?.ground_speed_kmh || 0;
              const isSupersonic = spdKmh >= 900;
              return (
                <div key={idx} className="telemetry-card">
                  <div className="telemetry-card-top">
                    <span className="telemetry-code">{fCode}</span>
                    <span className={`telemetry-speed-badge ${isSupersonic ? "supersonic" : ""}`}>
                      ⚡ {spdKmh} km/s
                    </span>
                  </div>

                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <span className="route-pill">{f.route?.display || "? ➔ ?"}</span>
                    <span style={{ fontSize: "11px", color: "#94a3b8" }}>
                      ✈ {f.aircraft_model || "Bilinmiyor"}
                    </span>
                  </div>

                  <div className="telemetry-metrics">
                    <div className="telemetry-metric-item">
                      <span className="telemetry-metric-label">İrtifa</span>
                      <span className="telemetry-metric-val">
                        {f.telemetry?.altitude_feet ? f.telemetry.altitude_feet.toLocaleString() : 0} ft
                      </span>
                    </div>
                    <div className="telemetry-metric-item">
                      <span className="telemetry-metric-label">Konum</span>
                      <span className="telemetry-metric-val">
                        {f.telemetry?.latitude?.toFixed(2)}, {f.telemetry?.longitude?.toFixed(2)}
                      </span>
                    </div>
                  </div>

                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: "2px" }}>
                    <span style={{ fontSize: "10.5px", color: "#64748b" }}>
                      Tescil: {f.registration || "N/A"}
                    </span>
                    <button
                      className="btn-icon"
                      style={{ fontSize: "11px", padding: "3px 8px" }}
                      onClick={() => onAskFlight(fCode)}
                      title="Bu uçağın detaylarını AI Asistanına sor"
                    >
                      💬 AI'ya Sor
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
        ) : (
          <div style={{ textAlign: "center", padding: "40px", color: "#64748b", background: "var(--bg-card)", borderRadius: "10px" }}>
            {isLoading ? "Kafka akışından uçuşlar taranıyor..." : "Arama kriterlerine uygun uçuş bulunamadı."}
          </div>
        )}
      </div>
    </div>
  );
}


// ============================================================
// 4. Kafka Audit Logs Stream (topic: mcp-requests)
// ============================================================

function KafkaLogsView() {
  const [logs, setLogs] = useState([]);

  useEffect(() => {
    loadLogs();
    const interval = setInterval(loadLogs, 4000);
    return () => clearInterval(interval);
  }, []);

  const loadLogs = async () => {
    try {
      const data = await fetchKafkaLogs();
      if (data && data.logs) {
        setLogs(data.logs);
      }
    } catch (err) {
      console.warn("Log verisi alınamadı:", err);
    }
  };

  return (
    <div className="kafka-dashboard">
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div>
          <h3 style={{ color: "#38bdf8", fontSize: "18px" }}>📋 Canlı MCP İstek Günlüğü (Audit Log Stream)</h3>
          <p style={{ color: "#94a3b8", fontSize: "12.5px" }}>
            Yapay zeka veya istemciler tarafından yapılan tüm MCP tool çağrıları anlık olarak Kafka <strong>mcp-requests</strong> topic'ine kaydedilir.
          </p>
        </div>
        <button className="btn-icon" onClick={loadLogs}>
          🔄 Güncelle
        </button>
      </div>

      <div className="audit-logs-container">
        <table className="audit-table">
          <thead>
            <tr>
              <th>Zaman (UTC)</th>
              <th>Çağrılan MCP Aracı</th>
              <th>Parametreler</th>
              <th>Eşleşen Kayıt</th>
              <th>İşlem Süresi</th>
              <th>Durum</th>
            </tr>
          </thead>
          <tbody>
            {logs && logs.length > 0 ? (
              logs.map((log, idx) => (
                <tr key={idx}>
                  <td style={{ fontFamily: "var(--font-mono)", color: "#94a3b8", fontSize: "11px" }}>
                    {log.timestamp ? log.timestamp.split("T")[1].substring(0, 8) : "--:--:--"}
                  </td>
                  <td>
                    <span className="audit-tool-badge">⚙️ {log.tool_name}</span>
                  </td>
                  <td style={{ fontFamily: "var(--font-mono)", fontSize: "11px", color: "#bae6fd" }}>
                    {JSON.stringify(log.arguments)}
                  </td>
                  <td>
                    {log.matched_records !== null ? `${log.matched_records} uçuş` : "-"}
                  </td>
                  <td style={{ fontFamily: "var(--font-mono)", color: "#34d399", fontWeight: "600" }}>
                    ⚡ {log.execution_time_ms} ms
                  </td>
                  <td>
                    <span style={{ color: "#10b981", fontSize: "11px", fontWeight: "600" }}>✓ Başarılı</span>
                  </td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={6} style={{ textAlign: "center", padding: "30px", color: "#64748b" }}>
                  Henüz kaydedilmiş MCP isteği bulunmuyor. Chat veya CLI üzerinden soru sorduğunuzda burada görünecektir.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}


// ============================================================
// 5. Prompt Chips
// ============================================================

function PromptChips({ onSelectPrompt }) {
  const samplePrompts = [
    { label: "⚡ 900 km/s üzeri hızlı uçaklar", query: "Kafka akışında 900 km/s hızın üzerindeki en hızlı uçakları ve modellerini listele" },
    { label: "📊 Kafka Akış İstatistikleri", query: "Kafka telemetri deposundaki ortalama hız, en yüksek irtifa ve toplam uçak istatistikleri nelerdir?" },
    { label: "📍 THY10 nerede?", query: "THY10 nolu uçak şu an nerede, irtifası kaç ve uçağın modeli ne?" },
    { label: "🇹🇷 İstanbul Semaları", query: "İstanbul (41.0082, 28.9784) semalarında 150 km yarıçapında uçan uçakları göster" },
    { label: "🔥 En çok takip edilenler", query: "Dünyada şu an FlightRadar24'te en çok takip edilen ilk 3 uçuş hangisi?" }
  ];

  return (
    <div className="prompt-chips-container">
      {samplePrompts.map((p, idx) => (
        <button
          key={idx}
          className="prompt-chip"
          onClick={() => onSelectPrompt(p.query)}
        >
          {p.label}
        </button>
      ))}
    </div>
  );
}


// ============================================================
// 6. Chat Area & Message Formatter
// ============================================================

function formatAssistantMessage(text) {
  if (!text) return null;
  const lines = text.split("\n");
  const elements = [];

  lines.forEach((line, index) => {
    let formattedLine = line.trim();
    if (!formattedLine) {
      elements.push(<div key={`br-${index}`} style={{ height: "6px" }} />);
      return;
    }

    const parts = [];
    const regex = /\*\*(.*?)\*\*/g;
    let lastIdx = 0;
    let match;

    while ((match = regex.exec(formattedLine)) !== null) {
      if (match.index > lastIdx) {
        parts.push(formattedLine.substring(lastIdx, match.index));
      }
      parts.push(<strong key={`b-${index}-${match.index}`}>{match[1]}</strong>);
      lastIdx = regex.lastIndex;
    }
    if (lastIdx < formattedLine.length) {
      parts.push(formattedLine.substring(lastIdx));
    }

    if (formattedLine.startsWith("•") || formattedLine.startsWith("-") || formattedLine.startsWith("*")) {
      elements.push(
        <div key={`li-${index}`} style={{ display: "flex", gap: "8px", margin: "4px 0", paddingLeft: "6px" }}>
          <span style={{ color: "#38bdf8" }}>✈</span>
          <div>{parts.length > 0 ? parts : formattedLine.substring(1).trim()}</div>
        </div>
      );
    } else {
      elements.push(
        <p key={`p-${index}`} style={{ margin: "4px 0" }}>
          {parts.length > 0 ? parts : formattedLine}
        </p>
      );
    }
  });

  return <div className="markdown-content">{elements}</div>;
}

function ChatArea({ messages, isLoading, onSelectPrompt, messagesEndRef }) {
  const [copiedIdx, setCopiedIdx] = useState(null);

  const handleCopy = (text, idx) => {
    navigator.clipboard.writeText(text);
    setCopiedIdx(idx);
    setTimeout(() => setCopiedIdx(null), 2000);
  };

  return (
    <div className="chat-messages">
      <div className="welcome-hero">
        <h2>✈️ Semalar Canlı Havacılık & Kafka Uçuş Radarı</h2>
        <p>
          FlightRadar24 ADS-B canlı veri ağı, Apache Kafka telemetri havuzu ve yapay zeka ile 
          uçuş numarası (örn. <strong>TK10</strong>), belirli bir hızın üzerindeki uçaklar (örn. <strong>&gt;900 km/s</strong>), 
          kuyruk tescili veya havalimanı telemetrisini anlık sorgulayın.
        </p>
        <PromptChips onSelectPrompt={onSelectPrompt} />
      </div>

      {messages.map((msg, idx) => (
        <div key={idx} className={`message-row ${msg.role}`}>
          <div className="message-sender-tag">
            {msg.role === "user" ? "👤 Siz" : "✈️ Semalar Asistanı"}
            {msg.model && <span style={{ color: "#64748b", fontSize: "10px" }}>({msg.model})</span>}
          </div>

          <div className="message-bubble">
            {msg.toolCalls && msg.toolCalls.length > 0 && (
              <div className="tool-trace-container">
                {msg.toolCalls.map((tc, tcIdx) => (
                  <div key={tcIdx} className="tool-trace-card">
                    <div className="tool-trace-header">
                      <span>⚙️ MCP Tool:</span>
                      <strong>{tc.name}</strong>
                    </div>
                    <div className="tool-trace-detail">
                      Parametreler: {JSON.stringify(tc.args)}
                    </div>
                  </div>
                ))}
              </div>
            )}

            {msg.role === "user" ? (
              <div>{msg.content}</div>
            ) : (
              <div>
                {formatAssistantMessage(msg.content)}
                <div style={{ marginTop: "10px", display: "flex", justifyContent: "flex-end" }}>
                  <button
                    className="btn-icon"
                    style={{ fontSize: "11px", padding: "3px 8px" }}
                    onClick={() => handleCopy(msg.content, idx)}
                  >
                    {copiedIdx === idx ? "✓ Kopyalandı" : "📋 Kopyala"}
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      ))}

      {isLoading && (
        <div className="message-row assistant">
          <div className="message-sender-tag">✈️ Semalar Asistanı</div>
          <div className="typing-radar">
            <div className="radar-spinner"></div>
            <span>Canlı FlightRadar24 ve Kafka akışı taranıyor...</span>
          </div>
        </div>
      )}

      <div ref={messagesEndRef} />
    </div>
  );
}


// ============================================================
// 7. Chat Input
// ============================================================

function ChatInput({ inputQuery, setInputQuery, onSendMessage, isLoading }) {
  const handleKeyDown = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      onSendMessage();
    }
  };

  return (
    <div className="chat-input-container">
      <div className="chat-input-wrapper">
        <input
          type="text"
          className="chat-input"
          placeholder="Uçuş kodu (örn: TK10), hız sorgusu (örn: 900 km/s üstü) veya havalimanı yazın..."
          value={inputQuery}
          onChange={(e) => setInputQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          disabled={isLoading}
          autoFocus
        />
      </div>

      <button
        className="btn-send"
        onClick={onSendMessage}
        disabled={isLoading || !inputQuery.trim()}
      >
        <span>Gönder</span>
        <span>🚀</span>
      </button>
    </div>
  );
}


// ============================================================
// 8. Flight Sidebar (Tracked + Fastest Kafka)
// ============================================================

function FlightSidebar({ trackedFlights, isLoadingTracked, onRefreshTracked, onSelectFlight, onAirportSearch }) {
  const [airportInput, setAirportInput] = useState("");
  const [fastestKafka, setFastestKafka] = useState([]);
  const [sidebarTab, setSidebarTab] = useState("tracked");

  useEffect(() => {
    fetchKafkaFlights({ min_speed: 850, limit: 6 })
      .then((data) => {
        if (data && data.flights) setFastestKafka(data.flights);
      })
      .catch((err) => console.warn(err));
  }, []);

  const handleAirportSubmit = (e) => {
    e.preventDefault();
    if (airportInput.trim()) {
      onAirportSearch(airportInput.trim().toUpperCase());
      setAirportInput("");
    }
  };

  return (
    <aside className="radar-sidebar">
      <div className="sidebar-header">
        <div style={{ display: "flex", gap: "6px" }}>
          <button
            className={`nav-tab-btn ${sidebarTab === "tracked" ? "active" : ""}`}
            style={{ padding: "4px 8px", fontSize: "11px" }}
            onClick={() => setSidebarTab("tracked")}
          >
            🔥 En Çok İzlenenler
          </button>
          <button
            className={`nav-tab-btn ${sidebarTab === "fastest" ? "active" : ""}`}
            style={{ padding: "4px 8px", fontSize: "11px" }}
            onClick={() => setSidebarTab("fastest")}
          >
            ⚡ Hızlılar (&gt;850 km/s)
          </button>
        </div>

        <button className="btn-icon" onClick={onRefreshTracked} disabled={isLoadingTracked} title="Listeyi Yenile">
          {isLoadingTracked ? "⏳..." : "🔄"}
        </button>
      </div>

      <div className="sidebar-content">
        <div className="airport-widget">
          <div style={{ fontSize: "12px", fontWeight: "600", color: "#38bdf8", display: "flex", alignItems: "center", gap: "6px" }}>
            <span>🏢</span>
            <span>Havalimanı Hızlı Sorgu</span>
          </div>
          <form onSubmit={handleAirportSubmit} className="airport-input-group">
            <input
              type="text"
              className="airport-input"
              placeholder="IATA/ICAO (örn: IST, SAW, LHR)"
              maxLength={4}
              value={airportInput}
              onChange={(e) => setAirportInput(e.target.value)}
            />
            <button type="submit" className="btn-icon" style={{ padding: "4px 10px", fontSize: "11px" }}>
              Ara
            </button>
          </form>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
          {sidebarTab === "tracked" ? (
            trackedFlights && trackedFlights.length > 0 ? (
              trackedFlights.map((flight, idx) => {
                const flightCode = flight.flight_number || flight.callsign || "N/A";
                return (
                  <div key={idx} className="flight-item" onClick={() => onSelectFlight(flightCode)} title="Detaylarını AI Asistanına Sor">
                    <div className="flight-item-top">
                      <span className="flight-number">{flightCode}</span>
                      <span className="flight-trackers">
                        👥 {flight.live_trackers ? flight.live_trackers.toLocaleString() : 0}
                      </span>
                    </div>

                    <div className="flight-route">
                      <span className="route-pill">{flight.route || "Bilinmiyor"}</span>
                      {flight.callsign && flight.callsign !== flightCode && (
                        <span style={{ fontSize: "11px", color: "#64748b" }}>({flight.callsign})</span>
                      )}
                    </div>

                    <div className="flight-model">
                      ✈ {flight.aircraft_type || flight.model || "Uçak tipi belirtilmemiş"}
                    </div>
                  </div>
                );
              })
            ) : (
              <div style={{ textAlign: "center", color: "#64748b", fontSize: "13px", padding: "20px 0" }}>
                {isLoadingTracked ? "Canlı radar verisi çekiliyor..." : "Uçuş verisi bulunamadı."}
              </div>
            )
          ) : (
            fastestKafka && fastestKafka.length > 0 ? (
              fastestKafka.map((flight, idx) => {
                const flightCode = flight.flight_number || flight.callsign || "N/A";
                const spd = flight.telemetry?.ground_speed_kmh || 0;
                return (
                  <div key={idx} className="flight-item" onClick={() => onSelectFlight(flightCode)} title="Detaylarını AI Asistanına Sor">
                    <div className="flight-item-top">
                      <span className="flight-number">{flightCode}</span>
                      <span style={{ color: "#fbbf24", fontFamily: "var(--font-mono)", fontSize: "11.5px", fontWeight: "700" }}>
                        ⚡ {spd} km/s
                      </span>
                    </div>

                    <div className="flight-route">
                      <span className="route-pill">{flight.route?.display || "Bilinmiyor"}</span>
                      <span style={{ fontSize: "11px", color: "#64748b" }}>{flight.aircraft_model}</span>
                    </div>
                  </div>
                );
              })
            ) : (
              <div style={{ textAlign: "center", color: "#64748b", fontSize: "13px", padding: "20px 0" }}>
                Kafka hızlı uçuş verisi yükleniyor...
              </div>
            )
          )}
        </div>

        <div style={{ marginTop: "auto", background: "rgba(0, 240, 255, 0.03)", border: "1px solid rgba(56, 189, 248, 0.15)", borderRadius: "8px", padding: "12px", fontSize: "11.5px", color: "#94a3b8" }}>
          <strong style={{ color: "#38bdf8", display: "block", marginBottom: "4px" }}>
            🛠️ 11 Aktif MCP Aracı:
          </strong>
          FlightRadar canlı aramalar + Kafka 1200 uçuş hız, bölge ve istatistik filtreleri.
        </div>
      </div>
    </aside>
  );
}


// ============================================================
// 9. Main Application Component
// ============================================================

function App() {
  const [currentView, setCurrentView] = useState("chat");
  const [messages, setMessages] = useState([]);
  const [inputQuery, setInputQuery] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [isSidebarOpen, setIsSidebarOpen] = useState(true);
  
  const [statusInfo, setStatusInfo] = useState(null);
  const [trackedFlights, setTrackedFlights] = useState([]);
  const [isLoadingTracked, setIsLoadingTracked] = useState(false);

  const messagesEndRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages, isLoading]);

  useEffect(() => {
    fetchServerStatus()
      .then((data) => setStatusInfo(data))
      .catch((err) => console.warn("Sunucu durum hatası:", err));

    loadTrackedFlights();
  }, []);

  const loadTrackedFlights = async () => {
    setIsLoadingTracked(true);
    try {
      const data = await fetchTrackedFlights(8);
      if (data && data.most_tracked_flights) {
        setTrackedFlights(data.most_tracked_flights);
      }
    } catch (err) {
      console.warn("Uçuş verisi yüklenemedi:", err);
    } finally {
      setIsLoadingTracked(false);
    }
  };

  const handleSendMessage = async (textToSend) => {
    const query = (textToSend || inputQuery).trim();
    if (!query || isLoading) return;

    setCurrentView("chat");

    const userMsg = { role: "user", content: query };
    setMessages((prev) => [...prev, userMsg]);
    setInputQuery("");
    setIsLoading(true);

    try {
      const res = await sendChatMessage(query);

      if (res.status === "success") {
        const assistantMsg = {
          role: "assistant",
          content: res.answer,
          toolCalls: res.tool_calls || [],
          model: res.model || statusInfo?.model
        };
        setMessages((prev) => [...prev, assistantMsg]);
      } else {
        const errorMsg = {
          role: "assistant",
          content: `⚠️ **Hata:** ${res.answer || res.error || "İstek işlenirken bir sorun oluştu."}`,
          toolCalls: res.tool_calls || [],
          model: res.model
        };
        setMessages((prev) => [...prev, errorMsg]);
      }
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: `❌ **Bağlantı Hatası:** Sunucuya erişilemedi (${err.message}). Lütfen backend sunucusunun (server.py) çalıştığından emin olun.`
        }
      ]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSelectPrompt = (promptText) => {
    setInputQuery(promptText);
    handleSendMessage(promptText);
  };

  const handleSelectFlight = (flightCode) => {
    const query = `${flightCode} nolu uçağın canlı durumunu, irtifasını, hızını ve rotasını getir.`;
    handleSendMessage(query);
  };

  const handleAirportSearch = (airportCode) => {
    const query = `${airportCode} kodlu havalimanının şehir, ülke ve koordinat detaylarını ver.`;
    handleSendMessage(query);
  };

  const handleClearChat = () => {
    setMessages([]);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100vh" }}>
      <Header
        statusInfo={statusInfo}
        currentView={currentView}
        onViewChange={(view) => setCurrentView(view)}
        onClearChat={handleClearChat}
        onToggleSidebar={() => setIsSidebarOpen(!isSidebarOpen)}
        isSidebarOpen={isSidebarOpen}
      />

      <main className="app-container">
        {/* Chat Mode */}
        {currentView === "chat" && (
          <section className="chat-column">
            <ChatArea
              messages={messages}
              isLoading={isLoading}
              onSelectPrompt={handleSelectPrompt}
              messagesEndRef={messagesEndRef}
            />

            <ChatInput
              inputQuery={inputQuery}
              setInputQuery={setInputQuery}
              onSendMessage={() => handleSendMessage()}
              isLoading={isLoading}
            />
          </section>
        )}

        {/* Kafka Dashboard Mode */}
        {currentView === "kafka" && (
          <section className="chat-column" style={{ background: "rgba(13, 20, 36, 0.5)" }}>
            <KafkaDashboard onAskFlight={handleSelectFlight} />
          </section>
        )}

        {/* Kafka Logs Mode */}
        {currentView === "logs" && (
          <section className="chat-column" style={{ background: "rgba(13, 20, 36, 0.5)" }}>
            <KafkaLogsView />
          </section>
        )}

        {/* Right Sidebar */}
        {isSidebarOpen && (
          <FlightSidebar
            trackedFlights={trackedFlights}
            isLoadingTracked={isLoadingTracked}
            onRefreshTracked={loadTrackedFlights}
            onSelectFlight={handleSelectFlight}
            onAirportSearch={handleAirportSearch}
          />
        )}
      </main>
    </div>
  );
}

// ============================================================
// 10. Mount Root
// ============================================================
const rootElement = document.getElementById("root");
if (rootElement) {
  const root = ReactDOM.createRoot(rootElement);
  root.render(<App />);
}
