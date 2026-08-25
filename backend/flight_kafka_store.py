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

    def find_flight(self, query: str) -> Dict[str, Any]:
        """Finds a flight in Kafka memory by flight number, callsign, registration or flight ID."""
        clean_q = query.strip().upper()

        # 1. Exact Matches
        flight_id = (
            self.by_flight_number.get(clean_q) or
            self.by_callsign.get(clean_q) or
            self.by_registration.get(clean_q) or
            clean_q.lower() if clean_q.lower() in self.flights else None
        )

        if flight_id and flight_id in self.flights:
            return {
                "status": "success",
                "source": "kafka_in_memory_stream",
                "flight": self.flights[flight_id]
            }

        # 2. Substring Search
        for f_id, f in self.flights.items():
            f_num = str(f.get("flight_number") or "").upper()
            c_sign = str(f.get("callsign") or "").upper()
            reg = str(f.get("registration") or "").upper()

            if clean_q in f_num or clean_q in c_sign or clean_q in reg:
                return {
                    "status": "success",
                    "source": "kafka_in_memory_stream",
                    "flight": f
                }

        return {
            "status": "not_found",
            "source": "kafka_in_memory_stream",
            "message": f"No active flight record found in Kafka stream for '{query}' (Total indexed: {len(self.flights)} flights)."
        }

    def find_by_airline(self, airline_code: str, limit: int = 15) -> Dict[str, Any]:
        """Filters flights in Kafka memory belonging to a specific airline (IATA/ICAO e.g. THY, TK, PGT, DLH)."""
        clean_code = airline_code.strip().upper()
        matched = []

        for f in self.flights.values():
            iata = str(f.get("airline_iata") or "").upper()
            icao = str(f.get("airline_icao") or "").upper()
            callsign = str(f.get("callsign") or "").upper()
            f_num = str(f.get("flight_number") or "").upper()

            if (clean_code in [iata, icao]) or (callsign.startswith(clean_code)) or (f_num.startswith(clean_code)):
                matched.append(f)

        return {
            "status": "success",
            "source": "kafka_in_memory_stream",
            "airline_code": clean_code,
            "total_matches": len(matched),
            "returned_count": min(len(matched), limit),
            "flights": matched[:limit]
        }

    def find_nearby(self, latitude: float, longitude: float, radius_km: float = 150.0, limit: int = 15) -> Dict[str, Any]:
        """Finds flights within a specified radius (km) around center coordinates, ordered by distance."""
        nearby = []

        for f in self.flights.values():
            telemetry = f.get("telemetry", {})
            f_lat = telemetry.get("latitude")
            f_lon = telemetry.get("longitude")

            if f_lat is not None and f_lon is not None:
                dist = calculate_haversine_distance(latitude, longitude, f_lat, f_lon)
                if dist <= radius_km:
                    flight_copy = dict(f)
                    flight_copy["distance_to_center_km"] = dist
                    nearby.append(flight_copy)

        # Sort by distance ascending
        nearby.sort(key=lambda x: x["distance_to_center_km"])

        return {
            "status": "success",
            "source": "kafka_in_memory_stream",
            "center": {"latitude": latitude, "longitude": longitude},
            "radius_km": radius_km,
            "total_in_radius": len(nearby),
            "returned_count": min(len(nearby), limit),
            "flights": nearby[:limit]
        }

    def find_flights_above_speed(self, min_speed_kmh: float = 800.0, limit: int = 15) -> Dict[str, Any]:
        """Filters high-speed / supersonic flights in the Kafka buffer exceeding specified ground speed (km/h).
        
        Args:
            min_speed_kmh: Minimum ground speed threshold (in km/h, e.g. 800 or 900)
            limit: Maximum number of flights to return (default: 15)
        """
        fast_flights = []

        for f in self.flights.values():
            telemetry = f.get("telemetry", {})
            speed_kmh = telemetry.get("ground_speed_kmh")

            if speed_kmh is not None and speed_kmh >= min_speed_kmh:
                fast_flights.append(f)

        # Sort descending by speed
        fast_flights.sort(
            key=lambda x: x.get("telemetry", {}).get("ground_speed_kmh", 0) or 0,
            reverse=True
        )

        return {
            "status": "success",
            "source": "kafka_in_memory_stream",
            "min_speed_kmh": min_speed_kmh,
            "total_matches": len(fast_flights),
            "returned_count": min(len(fast_flights), limit),
            "flights": fast_flights[:limit]
        }

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
