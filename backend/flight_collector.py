import time
import sys
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

try:
    from FlightRadarAPI import FlightRadar24API
except ImportError:
    from FlightRadar24 import FlightRadar24API

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


class FlightDataCollector:
    """FlightRadar24 üzerinden toplu ve anlık canlı uçuş verilerini çeken,
    verileri temizleyen ve normalize eden bağımsız veri toplayıcı sınıf.
    """

    def __init__(self):
        self.fr_api = FlightRadar24API()

    @staticmethod
    def knots_to_kmh(knots: Optional[float]) -> Optional[float]:
        """Knot cinsinden hızı km/s cinsine çevirir."""
        if knots is None:
            return None
        return round(knots * 1.852, 1)

    @staticmethod
    def feet_to_meters(feet: Optional[float]) -> Optional[float]:
        """Feet cinsinden irtifayı metre cinsine çevirir."""
        if feet is None:
            return None
        return round(feet * 0.3048, 1)

    def normalize_flight(self, flight_obj: Any) -> Dict[str, Any]:
        """Ham FlightRadar24 uçuş nesnesini standart JSON uyumlu sözlüğe dönüştürür."""
        # Uçuş numarasını güvenli al
        flight_number = getattr(flight_obj, "number", None) or getattr(flight_obj, "callsign", None)
        callsign = getattr(flight_obj, "callsign", None)
        registration = getattr(flight_obj, "registration", None)
        aircraft_code = getattr(flight_obj, "aircraft_code", None)
        
        # Konum ve telemetri
        lat = getattr(flight_obj, "latitude", None)
        lon = getattr(flight_obj, "longitude", None)
        alt_ft = getattr(flight_obj, "altitude", None)
        spd_kts = getattr(flight_obj, "ground_speed", None)
        heading = getattr(flight_obj, "heading", None)
        v_speed = getattr(flight_obj, "vertical_speed", None)
        squawk = getattr(flight_obj, "squawk", None)
        on_ground = bool(getattr(flight_obj, "on_ground", False))

        # Havalimanı kodları (IATA)
        origin_iata = getattr(flight_obj, "origin_airport_iata", None)
        dest_iata = getattr(flight_obj, "destination_airport_iata", None)

        # Havayolu kodları
        airline_iata = getattr(flight_obj, "airline_iata", None)
        airline_icao = getattr(flight_obj, "airline_icao", None)

        # Şimdi zaman damgası
        now_utc = datetime.now(timezone.utc).isoformat()

        return {
            "flight_id": getattr(flight_obj, "id", None),
            "icao_24bit": getattr(flight_obj, "icao_24bit", None),
            "flight_number": flight_number,
            "callsign": callsign,
            "registration": registration,
            "aircraft_model": aircraft_code or "Unknown",
            "airline_iata": airline_iata,
            "airline_icao": airline_icao,
            "telemetry": {
                "latitude": lat,
                "longitude": lon,
                "altitude_feet": alt_ft,
                "altitude_meters": self.feet_to_meters(alt_ft),
                "ground_speed_knots": spd_kts,
                "ground_speed_kmh": self.knots_to_kmh(spd_kts),
                "heading_degrees": heading,
                "vertical_speed_fpm": v_speed,
                "squawk": squawk,
                "on_ground": on_ground,
            },
            "route": {
                "origin_iata": origin_iata or "N/A",
                "destination_iata": dest_iata or "N/A",
                "display": f"{origin_iata or '?'} ➔ {dest_iata or '?'}"
            },
            "collected_at": now_utc,
            "timestamp": time.time()
        }

    def fetch_bulk_flights(self, target_count: int = 1200, bounds: Optional[str] = None, airline: Optional[str] = None) -> List[Dict[str, Any]]:
        """FlightRadar24'ten belirtilen hedef adet kadar canlı uçak verisi çeker.
        
        Args:
            target_count: Çekilmek istenen hedef uçak sayısı (varsayılan: 1200)
            bounds: Özel koordinat sınırları (isteğe bağlı)
            airline: Özel havayolu kodu filtresi (isteğe bağlı)
            
        Returns:
            Normalize edilmiş uçuş sözlükleri listesi
        """
        print(f"📡 FlightRadar24 canlı verileri çekiliyor (Hedef: {target_count} uçuş)...")
        start_time = time.time()
        
        raw_flights = []
        try:
            if bounds:
                raw_flights = self.fr_api.get_flights(bounds=bounds)
            elif airline:
                raw_flights = self.fr_api.get_flights(airline=airline)
            else:
                # Dünya geneli anlık uçuşlar
                raw_flights = self.fr_api.get_flights()
        except Exception as e:
            print(f"❌ FlightRadar24 API çağrısı sırasında hata: {e}")
            return []

        elapsed = round(time.time() - start_time, 2)
        total_available = len(raw_flights)
        print(f"✅ Toplam {total_available} canlı uçuş tespit edildi ({elapsed} sn).")

        # Hedef sayı kadar filtrele ve normalize et
        selected_raw = raw_flights[:target_count]
        normalized_flights = [self.normalize_flight(f) for f in selected_raw]

        print(f"✨ {len(normalized_flights)} uçuş verisi başarıyla normalize edildi.")
        return normalized_flights


if __name__ == "__main__":
    collector = FlightDataCollector()
    flights = collector.fetch_bulk_flights(target_count=1200)
    
    if flights:
        print("\n--- ÖRNEK TOPLANAN UÇUŞ VERİSİ (1. Uçuş) ---")
        import json
        print(json.dumps(flights[0], indent=2, ensure_ascii=False))
        print(f"\nToplam çekilen: {len(flights)} adet uçuş.")
