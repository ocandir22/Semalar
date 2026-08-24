// Semalar React Web UI — Unified Standalone Application

const { useState, useEffect, useRef } = React;
const API_BASE = window.location.origin;

// ============================================================
// API Client Functions
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
  if (!response.ok) {
    throw new Error("Canlı uçuş verisi alınamadı");
  }
  return await response.json();
}

async function fetchServerStatus() {
  const response = await fetch(`${API_BASE}/api/status`);
  if (!response.ok) {
    throw new Error("Sunucu durum bilgisi alınamadı");
  }
  return await response.json();
}


// ============================================================
// Header Component
// ============================================================

function Header({ statusInfo, onClearChat, onToggleSidebar, isSidebarOpen }) {
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
              AI RADAR
            </span>
          </div>
          <div className="brand-subtitle">FlightRadar24 Telemetry & MCP Assistant</div>
        </div>
      </div>

      <div className="header-status-group">
        <div className="status-badge live">
          <div className="pulse-dot"></div>
          ADS-B RADAR LIVE
        </div>

        <div className="status-badge provider">
          ✨ {provider} ({model})
        </div>

        <button 
          className="btn-icon" 
          onClick={onClearChat} 
          title="Sohbeti Temizle"
        >
          🗑️ Temizle
        </button>

        <button 
          className="btn-icon" 
          onClick={onToggleSidebar} 
          title="Radar Panelini Aç/Kapat"
        >
          📡 {isSidebarOpen ? "Paneli Gizle" : "Radar Paneli"}
        </button>
      </div>
    </header>
  );
}


// ============================================================
// Prompt Chips Component
// ============================================================

function PromptChips({ onSelectPrompt }) {
  const samplePrompts = [
    { label: "📍 THY10 nerede?", query: "THY10 nolu uçak şu an nerede, irtifası kaç ve uçağın modeli ne?" },
    { label: "🔥 En çok takip edilenler", query: "Dünyada şu an FlightRadar24'te en çok takip edilen ilk 3 uçuş hangisi?" },
    { label: "🇹🇷 İstanbul Semaları", query: "İstanbul (41.0082, 28.9784) semalarında 80 km yarıçapında uçan uçakları göster" },
    { label: "✈️ Pegasus Uçuşları", query: "Pegasus'un (PGT) şu an havadaki uçaklarını listele" },
    { label: "🏢 IST & SAW Havalimanı", query: "İstanbul Havalimanı (IST) ve Sabiha Gökçen (SAW) hakkında bilgi ve koordinat ver" }
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
// Chat Area Component
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
        <h2>✈️ Semalar Canlı Havacılık & Uçuş Takip Asistanı</h2>
        <p>
          FlightRadar24 küresel ADS-B canlı veri ağı, Model Context Protocol (MCP) ve yapay zeka ile 
          uçuş numarası (örn. <strong>TK10</strong>), kuyruk tescili (örn. <strong>TC-LJA</strong>), bölgesel radar taraması 
          veya havalimanı telemetrisini anlık sorgulayın.
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
            <span>Canlı FlightRadar24 ADS-B telemetrisi taranıyor...</span>
          </div>
        </div>
      )}

      <div ref={messagesEndRef} />
    </div>
  );
}


// ============================================================
// Chat Input Component
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
          placeholder="Uçuş kodu (örn: TK10), bölge radar sorusu veya havalimanı yazın..."
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
// Flight Sidebar Component
// ============================================================

function FlightSidebar({ trackedFlights, isLoadingTracked, onRefreshTracked, onSelectFlight, onAirportSearch }) {
  const [airportInput, setAirportInput] = useState("");

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
        <div className="sidebar-title">
          <span>🔥</span>
          <span>En Çok İzlenen Uçuşlar</span>
        </div>
        <button 
          className="btn-icon" 
          onClick={onRefreshTracked} 
          disabled={isLoadingTracked}
          title="Listeyi Yenile"
        >
          {isLoadingTracked ? "⏳..." : "🔄 Yenile"}
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
          {trackedFlights && trackedFlights.length > 0 ? (
            trackedFlights.map((flight, idx) => {
              const flightCode = flight.flight_number || flight.callsign || "N/A";
              return (
                <div 
                  key={idx} 
                  className="flight-item"
                  onClick={() => onSelectFlight(flightCode)}
                  title="Detaylarını AI Asistanına Sor"
                >
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
          )}
        </div>

        <div style={{ marginTop: "auto", background: "rgba(0, 240, 255, 0.03)", border: "1px solid rgba(56, 189, 248, 0.15)", borderRadius: "8px", padding: "12px", fontSize: "11.5px", color: "#94a3b8" }}>
          <strong style={{ color: "#38bdf8", display: "block", marginBottom: "4px" }}>
            🛠️ 5 Canlı Havacılık Aracı:
          </strong>
          Uçuş telemetrisi, havayolu arama, hava sahası tarama, global izlenme ve havalimanı verisi.
        </div>
      </div>
    </aside>
  );
}


// ============================================================
// Main Application Component
// ============================================================

function App() {
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
        onClearChat={handleClearChat}
        onToggleSidebar={() => setIsSidebarOpen(!isSidebarOpen)}
        isSidebarOpen={isSidebarOpen}
      />

      <main className="app-container">
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

// Mount the React Application
const rootElement = document.getElementById("root");
if (rootElement) {
  const root = ReactDOM.createRoot(rootElement);
  root.render(<App />);
}
