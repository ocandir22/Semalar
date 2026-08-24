export function FlightSidebar({ trackedFlights, isLoadingTracked, onRefreshTracked, onSelectFlight, onAirportSearch }) {
  const [airportInput, setAirportInput] = React.useState("");

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
        {/* Airport Search Widget */}
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

        {/* Live Top Tracked Flight Items */}
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

        {/* MCP Tools Guide */}
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
