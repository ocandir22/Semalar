import os
import sys
import json
import math
import time
from typing import Dict, Any, List, Optional
from kafka import KafkaConsumer, TopicPartition

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


def calculate_haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculates spherical distance between two coordinates in kilometers using Haversine formula."""
    R = 6371.0  # Earth's radius (km)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(R * c, 2)


class FlightKafkaStore:
    """Store class that consumes live flight telemetry from Kafka topic 'live-flights',
    indexes them in-memory for sub-millisecond querying, and provides query interfaces for MCP tools.
    """

    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "live-flights"):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        
        # In-Memory Indexes
        self.flights: Dict[str, Dict[str, Any]] = {}          # flight_id -> flight
        self.by_flight_number: Dict[str, str] = {}            # FLIGHT_NUM -> flight_id
        self.by_callsign: Dict[str, str] = {}                 # CALLSIGN -> flight_id
        self.by_registration: Dict[str, str] = {}             # REG -> flight_id
        self.last_sync_time: float = 0.0

        # Load initial telemetry records from Kafka
        self.sync_from_kafka()

    def sync_from_kafka(self, max_records: int = 5000, timeout_ms: int = 3000) -> int:
        """Consumes latest records from Kafka topic and synchronizes in-memory search indexes."""
        print(f"📥 Consuming telemetry from Kafka (Topic: {self.topic}, Server: {self.bootstrap_servers})...")
        start_time = time.time()
        
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=timeout_ms,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
        except Exception as e:
            print(f"⚠️ Kafka connection error: {e}")
            return len(self.flights)

        consumed_count = 0
        try:
            for message in consumer:
                flight = message.value
                if not isinstance(flight, dict):
                    continue

                flight_id = flight.get("flight_id") or str(message.offset)
                self.flights[flight_id] = flight

                # Update search indexes
                f_num = str(flight.get("flight_number") or "").strip().upper()
                if f_num:
                    self.by_flight_number[f_num] = flight_id

                callsign = str(flight.get("callsign") or "").strip().upper()
                if callsign:
                    self.by_callsign[callsign] = flight_id

                reg = str(flight.get("registration") or "").strip().upper()
                if reg:
                    self.by_registration[reg] = flight_id

                consumed_count += 1
                if consumed_count >= max_records:
                    break
        finally:
            consumer.close()

        self.last_sync_time = time.time()
        elapsed = round(self.last_sync_time - start_time, 2)
        print(f"✅ Kafka Synchronization Complete: {len(self.flights)} flights indexed in memory ({elapsed}s).")
        return len(self.flights)

    def query_flights(
        self,
        query: Optional[str] = None,
        airline: Optional[str] = None,
        min_speed_kmh: Optional[float] = None,
        max_speed_kmh: Optional[float] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        radius_km: Optional[float] = None,
        min_altitude_feet: Optional[float] = None,
        get_stats: bool = False,
        limit: int = 15
    ) -> Dict[str, Any]:
        """Unified multi-filter query method for the Kafka telemetry buffer.
        Evaluates all provided constraints (speed, location, airline, query, stats) simultaneously in a single pass.
        """
        # If statistics requested, return stream summary
        if get_stats:
            return self.get_telemetry_stats()

        clean_q = query.strip().upper() if query and str(query).strip() else None
        clean_airline = airline.strip().upper() if airline and str(airline).strip() else None
        has_coords = (latitude is not None and longitude is not None)
        effective_radius = float(radius_km) if radius_km is not None else (150.0 if has_coords else None)

        matched = []
        for f in self.flights.values():
            telemetry = f.get("telemetry", {})
            spd = telemetry.get("ground_speed_kmh")
            alt = telemetry.get("altitude_feet")
            f_lat = telemetry.get("latitude")
            f_lon = telemetry.get("longitude")

            # 1. Speed Filters
            if min_speed_kmh is not None and (spd is None or spd < min_speed_kmh):
                continue
            if max_speed_kmh is not None and (spd is None or spd > max_speed_kmh):
                continue

            # 2. Altitude Filter
            if min_altitude_feet is not None and (alt is None or alt < min_altitude_feet):
                continue

            # 3. Airline Filter
            if clean_airline:
                f_iata = str(f.get("airline_iata") or "").upper()
                f_icao = str(f.get("airline_icao") or "").upper()
                callsign = str(f.get("callsign") or "").upper()
                f_num = str(f.get("flight_number") or "").upper()
                if (clean_airline not in [f_iata, f_icao] and 
                    not callsign.startswith(clean_airline) and 
                    not f_num.startswith(clean_airline)):
                    continue

            # 4. Flight Query Filter (number / callsign / registration / ID)
            if clean_q:
                f_num = str(f.get("flight_number") or "").upper()
                c_sign = str(f.get("callsign") or "").upper()
                reg = str(f.get("registration") or "").upper()
                f_id = str(f.get("flight_id") or "").upper()
                if clean_q not in f_num and clean_q not in c_sign and clean_q not in reg and clean_q != f_id:
                    continue

            # 5. Geographic Proximity Filter
            dist = None
            if has_coords:
                if f_lat is None or f_lon is None:
                    continue
                dist = calculate_haversine_distance(latitude, longitude, f_lat, f_lon)
                if effective_radius is not None and dist > effective_radius:
                    continue

            # Record match
            if dist is not None:
                item = dict(f)
                item["distance_to_center_km"] = dist
                matched.append(item)
            else:
                matched.append(f)

        # Smart Sorting:
        # If coordinates provided without specific speed filter, sort by distance
        if has_coords and min_speed_kmh is None:
            matched.sort(key=lambda x: x.get("distance_to_center_km", 999999))
        else:
            # Default sort by ground speed descending
            matched.sort(key=lambda x: x.get("telemetry", {}).get("ground_speed_kmh", 0) or 0, reverse=True)

        return {
            "status": "success",
            "source": "kafka_in_memory_stream",
            "total_matches": len(matched),
            "returned_count": min(len(matched), limit),
            "flights": matched[:limit]
        }

    def find_flight(self, query: str) -> Dict[str, Any]:
        """Finds a flight in Kafka memory by flight number, callsign, registration or flight ID."""
        res = self.query_flights(query=query, limit=1)
        if res.get("flights"):
            return {
                "status": "success",
                "source": "kafka_in_memory_stream",
                "flight": res["flights"][0]
            }
        return {
            "status": "not_found",
            "source": "kafka_in_memory_stream",
            "message": f"No active flight record found in Kafka stream for '{query}' (Total indexed: {len(self.flights)} flights)."
        }

    def find_by_airline(self, airline_code: str, limit: int = 15) -> Dict[str, Any]:
        """Filters flights in Kafka memory belonging to a specific airline (IATA/ICAO e.g. THY, TK, PGT, DLH)."""
        res = self.query_flights(airline=airline_code, limit=limit)
        res["airline_code"] = airline_code.strip().upper()
        return res

    def find_nearby(self, latitude: float, longitude: float, radius_km: float = 150.0, limit: int = 15) -> Dict[str, Any]:
        """Finds flights within a specified radius (km) around center coordinates, ordered by distance."""
        res = self.query_flights(latitude=latitude, longitude=longitude, radius_km=radius_km, limit=limit)
        res["center"] = {"latitude": latitude, "longitude": longitude}
        res["radius_km"] = radius_km
        return res

    def find_flights_above_speed(self, min_speed_kmh: float = 800.0, limit: int = 15) -> Dict[str, Any]:
        """Filters high-speed / supersonic flights in the Kafka buffer exceeding specified ground speed (km/h)."""
        res = self.query_flights(min_speed_kmh=min_speed_kmh, limit=limit)
        res["min_speed_kmh"] = min_speed_kmh
        return res

    def get_telemetry_stats(self) -> Dict[str, Any]:
        """Calculates statistical summary across all flights in Kafka (speed, altitude, airlines)."""
        if not self.flights:
            return {"status": "empty", "message": "No data in Kafka store."}

        speeds = []
        altitudes = []
        airlines = set()

        for f in self.flights.values():
            t = f.get("telemetry", {})
            spd = t.get("ground_speed_kmh")
            alt = t.get("altitude_feet")
            airline = f.get("airline_iata") or f.get("airline_icao")

            if spd is not None and spd > 50:  # Airborne aircraft
                speeds.append(spd)
            if alt is not None and alt > 1000:
                altitudes.append(alt)
            if airline and airline != "N/A":
                airlines.add(airline)

        max_speed = max(speeds) if speeds else 0
        avg_speed = round(sum(speeds) / len(speeds), 1) if speeds else 0
        max_alt = max(altitudes) if altitudes else 0
        avg_alt = round(sum(altitudes) / len(altitudes), 1) if altitudes else 0

        return {
            "status": "success",
            "source": "kafka_in_memory_stream",
            "total_flights_in_kafka": len(self.flights),
            "total_unique_airlines": len(airlines),
            "speed_kmh": {
                "max": max_speed,
                "average": avg_speed
            },
            "altitude_feet": {
                "max": max_alt,
                "average": avg_alt
            },
            "last_synced_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.last_sync_time))
        }


# Global Singleton Store Instance
kafka_store = FlightKafkaStore()


if __name__ == "__main__":
    store = FlightKafkaStore()
    print("\n--- 1. OVERALL TELEMETRY STATS ---")
    print(json.dumps(store.get_telemetry_stats(), indent=2, ensure_ascii=False))

    print("\n--- 2. FLIGHTS ABOVE 850 KM/H ---")
    fast = store.find_flights_above_speed(min_speed_kmh=850.0, limit=3)
    print(json.dumps(fast, indent=2, ensure_ascii=False))

    print("\n--- 3. FLIGHTS NEAR ISTANBUL (41.0082, 28.9784) ---")
    nearby = store.find_nearby(41.0082, 28.9784, radius_km=300, limit=2)
    print(json.dumps(nearby, indent=2, ensure_ascii=False))
