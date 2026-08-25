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
    """Independent collector class that fetches bulk live flight telemetry
    from FlightRadar24, cleans, and normalizes them into structured JSON payloads.
    """

    def __init__(self):
        self.fr_api = FlightRadar24API()

    @staticmethod
    def knots_to_kmh(knots: Optional[float]) -> Optional[float]:
        """Converts speed from knots to km/h."""
        if knots is None:
            return None
        return round(knots * 1.852, 1)

    @staticmethod
    def feet_to_meters(feet: Optional[float]) -> Optional[float]:
        """Converts altitude from feet to meters."""
        if feet is None:
            return None
        return round(feet * 0.3048, 1)

    def normalize_flight(self, flight_obj: Any) -> Dict[str, Any]:
        """Converts raw FlightRadar24 flight object into a standardized JSON telemetry dictionary."""
        flight_number = getattr(flight_obj, "number", None) or getattr(flight_obj, "callsign", None)
        callsign = getattr(flight_obj, "callsign", None)
        registration = getattr(flight_obj, "registration", None)
        aircraft_code = getattr(flight_obj, "aircraft_code", None)
        
        # Position & Telemetry
        lat = getattr(flight_obj, "latitude", None)
        lon = getattr(flight_obj, "longitude", None)
        alt_ft = getattr(flight_obj, "altitude", None)
        spd_kts = getattr(flight_obj, "ground_speed", None)
        heading = getattr(flight_obj, "heading", None)
        v_speed = getattr(flight_obj, "vertical_speed", None)
        squawk = getattr(flight_obj, "squawk", None)
        on_ground = bool(getattr(flight_obj, "on_ground", False))

        # Airport Codes (IATA)
        origin_iata = getattr(flight_obj, "origin_airport_iata", None)
        dest_iata = getattr(flight_obj, "destination_airport_iata", None)

        # Airline Codes
        airline_iata = getattr(flight_obj, "airline_iata", None)
        airline_icao = getattr(flight_obj, "airline_icao", None)

        # Timestamp
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
        """Fetches live flight telemetry up to the specified target count from FlightRadar24.
        
        Args:
            target_count: Target number of flights to collect (default: 1200)
            bounds: Custom geographic bounds (optional)
            airline: Custom airline code filter (optional)
            
        Returns:
            List of normalized flight telemetry dictionaries
        """
        print(f"📡 Fetching live data from FlightRadar24 (Target: {target_count} flights)...")
        start_time = time.time()
        
        raw_flights = []
        try:
            if bounds:
                raw_flights = self.fr_api.get_flights(bounds=bounds)
            elif airline:
                raw_flights = self.fr_api.get_flights(airline=airline)
            else:
                raw_flights = self.fr_api.get_flights()
        except Exception as e:
            print(f"❌ Error during FlightRadar24 API call: {e}")
            return []

        elapsed = round(time.time() - start_time, 2)
        total_available = len(raw_flights)
        print(f"✅ Detected {total_available} live flights worldwide ({elapsed}s).")

        # Slice to target count and normalize
        selected_raw = raw_flights[:target_count]
        normalized_flights = [self.normalize_flight(f) for f in selected_raw]

        print(f"✨ Successfully normalized {len(normalized_flights)} flight telemetry records.")
        return normalized_flights


if __name__ == "__main__":
    collector = FlightDataCollector()
    flights = collector.fetch_bulk_flights(target_count=1200)
    
    if flights:
        print("\n--- SAMPLE NORMALIZED FLIGHT TELEMETRY (Flight 1) ---")
        import json
        print(json.dumps(flights[0], indent=2, ensure_ascii=False))
        print(f"\nTotal collected: {len(flights)} flights.")
