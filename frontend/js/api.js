// Semalar Backend API Client (FlightRadar24 & Apache Kafka)

const API_BASE = window.location.origin;

export async function sendChatMessage(message) {
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

export async function fetchTrackedFlights(limit = 8) {
  const response = await fetch(`${API_BASE}/api/tracked?limit=${limit}`);
  if (!response.ok) {
    throw new Error("Canlı uçuş verisi alınamadı");
  }
  return await response.json();
}

export async function fetchServerStatus() {
  const response = await fetch(`${API_BASE}/api/status`);
  if (!response.ok) {
    throw new Error("Sunucu durum bilgisi alınamadı");
  }
  return await response.json();
}

// ============================================================
// KAFKA STREAM & AUDIT API
// ============================================================

export async function fetchKafkaStats() {
  const response = await fetch(`${API_BASE}/api/kafka/stats`);
  if (!response.ok) throw new Error("Kafka istatistikleri alınamadı");
  return await response.json();
}

export async function fetchKafkaFlights(params = {}) {
  const query = new URLSearchParams(params).toString();
  const response = await fetch(`${API_BASE}/api/kafka/flights?${query}`);
  if (!response.ok) throw new Error("Kafka uçuşları alınamadı");
  return await response.json();
}

export async function fetchKafkaFastest(minSpeed = 800, limit = 20) {
  const response = await fetch(`${API_BASE}/api/kafka/fastest?min_speed=${minSpeed}&limit=${limit}`);
  if (!response.ok) throw new Error("Kafka hızlı uçaklar alınamadı");
  return await response.json();
}

export async function fetchKafkaLogs() {
  const response = await fetch(`${API_BASE}/api/kafka/logs`);
  if (!response.ok) throw new Error("Kafka MCP logları alınamadı");
  return await response.json();
}

export async function triggerKafkaSync() {
  const response = await fetch(`${API_BASE}/api/kafka/sync`, { method: "POST" });
  if (!response.ok) throw new Error("Kafka senkronize edilemedi");
  return await response.json();
}

export async function triggerKafkaProduceFresh() {
  const response = await fetch(`${API_BASE}/api/kafka/produce`, { method: "POST" });
  if (!response.ok) throw new Error("Yeni veri Kafka'ya gönderilemedi");
  return await response.json();
}
