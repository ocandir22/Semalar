export function PromptChips({ onSelectPrompt }) {
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
