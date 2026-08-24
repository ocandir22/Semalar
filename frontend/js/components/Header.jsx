export function Header({ statusInfo, onClearChat, onToggleSidebar, isSidebarOpen }) {
  const provider = statusInfo?.provider?.toUpperCase() || "GEMINI";
  const model = statusInfo?.model || "gemini-3.7-flash";

  return (
    <header className="app-header">
      <div className="brand-section">
        <div className="brand-icon">✈️</div>
        <div>
          <div className="brand-title">
            SEMALAR
            <span style={{ fontSize: "12px", background: "rgba(0, 240, 255, 0.15)", color: "#38bdf8", padding: "2px 8px", borderRadius: "4px" }}>
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
