import os
import sys
import json
import math
import time
from typing import Dict, Any, List, Optional
from kafka import KafkaConsumer, TopicPartition
from geo_service import geo_engine

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


# Geographic presets and national airspace bounding boxes
GEO_REGIONS: Dict[str, Dict[str, Any]] = {
    # Turkey National Airspace Bounding Box (~35.8° - 42.2° N, ~25.6° - 44.8° E)
    "TR": {"type": "bbox", "min_lat": 35.8, "max_lat": 42.2, "min_lon": 25.6, "max_lon": 44.8, "name": "Türkiye"},
    "TURKEY": {"type": "bbox", "min_lat": 35.8, "max_lat": 42.2, "min_lon": 25.6, "max_lon": 44.8, "name": "Türkiye"},
    "TURKIYE": {"type": "bbox", "min_lat": 35.8, "max_lat": 42.2, "min_lon": 25.6, "max_lon": 44.8, "name": "Türkiye"},
    "TÜRKIYE": {"type": "bbox", "min_lat": 35.8, "max_lat": 42.2, "min_lon": 25.6, "max_lon": 44.8, "name": "Türkiye"},
    # Major Geographic Regions
    "MARMARA": {"type": "bbox", "min_lat": 40.0, "max_lat": 42.1, "min_lon": 26.0, "max_lon": 31.0, "name": "Marmara Bölgesi"},
    "EGE": {"type": "bbox", "min_lat": 36.5, "max_lat": 40.2, "min_lon": 26.0, "max_lon": 30.2, "name": "Ege Bölgesi"},
    "AEGEAN": {"type": "bbox", "min_lat": 36.5, "max_lat": 40.2, "min_lon": 26.0, "max_lon": 30.2, "name": "Ege Bölgesi"},
    "AKDENIZ": {"type": "bbox", "min_lat": 36.0, "max_lat": 38.0, "min_lon": 29.0, "max_lon": 36.5, "name": "Akdeniz Bölgesi"},
    "MEDITERRANEAN": {"type": "bbox", "min_lat": 36.0, "max_lat": 38.0, "min_lon": 29.0, "max_lon": 36.5, "name": "Akdeniz Bölgesi"},
    "KARADENIZ": {"type": "bbox", "min_lat": 40.5, "max_lat": 42.2, "min_lon": 31.0, "max_lon": 42.0, "name": "Karadeniz Bölgesi"},
    "BLACK_SEA": {"type": "bbox", "min_lat": 40.5, "max_lat": 42.2, "min_lon": 31.0, "max_lon": 42.0, "name": "Karadeniz Bölgesi"},
    "IC_ANADOLU": {"type": "bbox", "min_lat": 37.5, "max_lat": 40.5, "min_lon": 30.5, "max_lon": 37.0, "name": "İç Anadolu Bölgesi"},
}


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

    def sync_from_kafka(self, target_count: int = 1200, max_wait_seconds: float = 4.0) -> int:
        """Consumes latest messages from Kafka topic 'live-flights' into memory cache."""
        print(f"📥 Consuming telemetry from Kafka (Topic: {self.topic}, Server: {self.bootstrap_servers})...")
        start_time = time.time()
        
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=int(max_wait_seconds * 1000),
                value_deserializer=lambda x: json.loads(x.decode("utf-8"))
            )
            
            partitions = consumer.partitions_for_topic(self.topic)
            if not partitions:
                print(f"⚠️ Topic '{self.topic}' not found or empty.")
                consumer.close()
                return 0

            # Seek to near end to retrieve latest flight records
            topic_partitions = [TopicPartition(self.topic, p) for p in partitions]
            end_offsets = consumer.end_offsets(topic_partitions)

            for tp in topic_partitions:
                end_off = end_offsets.get(tp, 0)
                start_off = max(0, end_off - target_count)
                consumer.assign([tp])
                consumer.seek(tp, start_off)

            new_flights = {}
            new_by_num = {}
            new_by_callsign = {}
            new_by_reg = {}

            consumed = 0
            for message in consumer:
                record = message.value
                flight_id = record.get("flight_id")
                if flight_id:
                    new_flights[flight_id] = record

                    f_num = (record.get("flight_number") or "").upper().strip()
                    callsign = (record.get("callsign") or "").upper().strip()
                    reg = (record.get("registration") or "").upper().strip()

                    if f_num: new_by_num[f_num] = flight_id
                    if callsign: new_by_callsign[callsign] = flight_id
                    if reg: new_by_reg[reg] = flight_id

                    consumed += 1

                if time.time() - start_time > max_wait_seconds:
                    break

            consumer.close()
            if new_flights:
                self.flights = new_flights
                self.by_flight_number = new_by_num
                self.by_callsign = new_by_callsign
                self.by_registration = new_by_reg
        except Exception as e:
            print(f"⚠️ Kafka connection warning: {e}")

        self.last_sync_time = time.time()
        elapsed = round(self.last_sync_time - start_time, 2)
        print(f"✅ Kafka Synchronization Complete: {len(self.flights)} flights indexed in memory ({elapsed}s).")
        return len(self.flights)

    def refresh_turkey_telemetry(self, flights: List[Dict[str, Any]]):
        """Refreshes in-memory store atomically with freshly streamed Turkey airspace flights."""
        new_flights = {}
        new_by_num = {}
        new_by_callsign = {}
        new_by_reg = {}

        for record in flights:
            flight_id = record.get("flight_id")
            if flight_id:
                new_flights[flight_id] = record

                f_num = (record.get("flight_number") or "").upper().strip()
                callsign = (record.get("callsign") or "").upper().strip()
                reg = (record.get("registration") or "").upper().strip()

                if f_num: new_by_num[f_num] = flight_id
                if callsign: new_by_callsign[callsign] = flight_id
                if reg: new_by_reg[reg] = flight_id

        self.flights = new_flights
        self.by_flight_number = new_by_num
        self.by_callsign = new_by_callsign
        self.by_registration = new_by_reg
        self.last_sync_time = time.time()

    def query_flights(
        self,
        query: Optional[str] = None,
        region: Optional[str] = None,
        airline: Optional[str] = None,
        min_speed_kmh: Optional[float] = None,
        min_altitude_feet: Optional[float] = None,
        get_stats: bool = False,
        limit: int = 15,
        **kwargs
    ) -> Dict[str, Any]:
        """Unified multi-filter query method for the Kafka telemetry buffer.
        Evaluates all provided constraints (speed, region/province, airline, query, stats) simultaneously in a single pass.
        """
        # If statistics requested, return stream summary
        if get_stats:
            return self.get_telemetry_stats()

        clean_q = query.strip().upper() if query and str(query).strip() else None
        clean_airline = airline.strip().upper() if airline and str(airline).strip() else None

        # Resolve country or region preset if specified
        geo_filter = None
        region_name = None
        raw_geo = (region or kwargs.get("country") or "").strip()

        # 1. First check if raw_geo corresponds to one of Turkey's 81 provinces (Exact GeoJSON Polygon)
        if raw_geo:
            matched_province = geo_engine.resolve_province_name(raw_geo)
            if matched_province:
                prov_info = geo_engine.get_province_info(matched_province)
                region_name = matched_province
                geo_filter = {
                    "type": "polygon",
                    "province": matched_province,
                    "name": matched_province,
                    "center": prov_info.get("center") if prov_info else None
                }

        # 2. Fall back to national/regional preset bounding boxes (e.g. TR, Marmara, Aegean)
        if not geo_filter and raw_geo:
            geo_target = raw_geo.upper().replace("İ", "I")
            if geo_target in GEO_REGIONS:
                geo_filter = GEO_REGIONS[geo_target]
                region_name = geo_filter.get("name", geo_target)
            elif "TURK" in geo_target or geo_target == "TR":
                geo_filter = GEO_REGIONS["TR"]
                region_name = "Türkiye"

        matched = []
        for f in self.flights.values():
            telemetry = f.get("telemetry", {})
            spd = telemetry.get("ground_speed_kmh")
            alt = telemetry.get("altitude_feet")
            f_lat = telemetry.get("latitude")
            f_lon = telemetry.get("longitude")

            # 1. Speed Filter
            if min_speed_kmh is not None and (spd is None or spd < min_speed_kmh):
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

            # 5. Geographic Proximity & National Airspace Filtering
            dist = None
            if geo_filter:
                if f_lat is None or f_lon is None:
                    continue
                if geo_filter["type"] == "polygon":
                    if not geo_engine.is_point_in_province(f_lat, f_lon, geo_filter["province"]):
                        continue
                    if geo_filter.get("center"):
                        dist = calculate_haversine_distance(
                            geo_filter["center"]["lat"], geo_filter["center"]["lon"], f_lat, f_lon
                        )
                elif geo_filter["type"] == "bbox":
                    if not (geo_filter["min_lat"] <= f_lat <= geo_filter["max_lat"] and
                            geo_filter["min_lon"] <= f_lon <= geo_filter["max_lon"]):
                        continue

            # Record match
            item = dict(f)
            if dist is not None:
                item["distance_to_center_km"] = dist
            if region_name:
                item["filtered_region"] = region_name
            matched.append(item)

        # Sort by ground speed descending
        matched.sort(key=lambda x: x.get("telemetry", {}).get("ground_speed_kmh", 0) or 0, reverse=True)

        resp = {
            "status": "success",
            "source": "kafka_in_memory_stream",
            "total_matches": len(matched),
            "returned_count": min(len(matched), limit),
            "flights": matched[:limit]
        }
        if region_name:
            resp["applied_region"] = region_name
            if geo_filter and geo_filter.get("type") == "polygon":
                prov_info = geo_engine.get_province_info(region_name)
                if prov_info:
                    resp["province_details"] = {
                        "name": prov_info["name"],
                        "plate": prov_info["plate_code"],
                        "geographic_region": prov_info["region"],
                        "center_coords": prov_info["center"],
                        "summary": prov_info["summary"]
                    }
        return resp

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
    fast = store.query_flights(min_speed_kmh=850.0, limit=3)
    print(json.dumps(fast, indent=2, ensure_ascii=False))

    print("\n--- 3. FLIGHTS IN ERZURUM PROVINCE ---")
    erz = store.query_flights(region="Erzurum", limit=3)
    print(json.dumps(erz, indent=2, ensure_ascii=False))
