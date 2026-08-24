import { PromptChips } from "./PromptChips.jsx";

// Lightweight Markdown / Text Formatter for Aviation Telemetry
function formatAssistantMessage(text) {
  if (!text) return null;

  // Split lines
  const lines = text.split("\n");
  const elements = [];

  lines.forEach((line, index) => {
    let formattedLine = line.trim();
    if (!formattedLine) {
      elements.push(<div key={`br-${index}`} style={{ height: "6px" }} />);
      return;
    }

    // Bold text replacements
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
        <div key={`li-${index}`} style={{ display: "flex", gap: "8px", margin: "4px 0", paddingLeft: "8px" }}>
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

export function ChatArea({ messages, isLoading, onSelectPrompt, messagesEndRef }) {
  const [copiedIdx, setCopiedIdx] = React.useState(null);

  const handleCopy = (text, idx) => {
    navigator.clipboard.writeText(text);
    setCopiedIdx(idx);
    setTimeout(() => setCopiedIdx(null), 2000);
  };

  return (
    <div className="chat-messages">
      {/* Welcome Hero */}
      <div className="welcome-hero">
        <h2>✈️ Semalar Canlı Havacılık & Uçuş Takip Asistanı</h2>
        <p>
          FlightRadar24 küresel ADS-B canlı veri ağı, Model Context Protocol (MCP) ve yapay zeka ile 
          uçuş numarası (örn. <strong>TK10</strong>), kuyruk tescili (örn. <strong>TC-LJA</strong>), bölgesel radar taraması 
          veya havalimanı telemetrisini anlık sorgulayın.
        </p>
        <PromptChips onSelectPrompt={onSelectPrompt} />
      </div>

      {/* Message List */}
      {messages.map((msg, idx) => (
        <div key={idx} className={`message-row ${msg.role}`}>
          <div className="message-sender-tag">
            {msg.role === "user" ? "👤 Siz" : "✈️ Semalar Asistanı"}
            {msg.model && <span style={{ color: "#64748b", fontSize: "10px" }}>({msg.model})</span>}
          </div>

          <div className="message-bubble">
            {/* Tool Calls Execution Traces */}
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

            {/* Message Body */}
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

      {/* Typing & Radar Scanning Indicator */}
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
