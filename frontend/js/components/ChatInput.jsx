export function ChatInput({ inputQuery, setInputQuery, onSendMessage, isLoading }) {
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
