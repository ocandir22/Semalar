import os
import sys
import json
import math
import time
from typing import Dict, Any, List, Optional
from kafka import KafkaConsumer, TopicPartition

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(BASE_DIR)
ROOT_DIR = os.path.dirname(BACKEND_DIR)
for p in [ROOT_DIR, BACKEND_DIR, BASE_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    from core.geo_service import geo_engine
except ImportError:
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


TURKISH_AIRPORTS: Dict[str, Dict[str, Any]] = {
    # Major Hubs
    "IST": {"iata": "IST", "icao": "LTFM", "name": "İstanbul Havalimanı", "city": "İstanbul", "lat": 41.2753, "lon": 28.7519},
    "SAW": {"iata": "SAW", "icao": "LTFJ", "name": "Sabiha Gökçen Havalimanı", "city": "İstanbul", "lat": 40.8986, "lon": 29.3092},
    "ESB": {"iata": "ESB", "icao": "LTAC", "name": "Esenboğa Havalimanı", "city": "Ankara", "lat": 40.1281, "lon": 32.9951},
    "AYT": {"iata": "AYT", "icao": "LTAI", "name": "Antalya Havalimanı", "city": "Antalya", "lat": 36.8987, "lon": 30.8005},
    "ADB": {"iata": "ADB", "icao": "LTBJ", "name": "İzmir Adnan Menderes Havalimanı", "city": "İzmir", "lat": 38.2924, "lon": 27.1570},
    "DLM": {"iata": "DLM", "icao": "LTBS", "name": "Dalaman Havalimanı", "city": "Muğla", "lat": 36.7131, "lon": 28.7925},
    "BJV": {"iata": "BJV", "icao": "LTFE", "name": "Milas-Bodrum Havalimanı", "city": "Muğla", "lat": 37.2506, "lon": 27.6644},
    "TZX": {"iata": "TZX", "icao": "LTCG", "name": "Trabzon Havalimanı", "city": "Trabzon", "lat": 40.9951, "lon": 39.7897},
    "GZT": {"iata": "GZT", "icao": "LTAJ", "name": "Gaziantep Havalimanı", "city": "Gaziantep", "lat": 36.9472, "lon": 37.4786},
    "ADA": {"iata": "ADA", "icao": "LTAF", "name": "Adana Şakirpaşa Havalimanı", "city": "Adana", "lat": 36.9822, "lon": 35.2804},
    "COV": {"iata": "COV", "icao": "LTCO", "name": "Çukurova Uluslararası Havalimanı", "city": "Mersin", "lat": 36.8994, "lon": 35.0664},
    "KYA": {"iata": "KYA", "icao": "LTAN", "name": "Konya Havalimanı", "city": "Konya", "lat": 37.9790, "lon": 32.5619},
    "ASR": {"iata": "ASR", "icao": "LTAU", "name": "Kayseri Erkilet Havalimanı", "city": "Kayseri", "lat": 38.7704, "lon": 35.4954},
    "ERZ": {"iata": "ERZ", "icao": "LTCE", "name": "Erzurum Havalimanı", "city": "Erzurum", "lat": 39.9562, "lon": 41.1702},
    "DIY": {"iata": "DIY", "icao": "LTCC", "name": "Diyarbakır Havalimanı", "city": "Diyarbakır", "lat": 37.8939, "lon": 40.2010},
    "VAN": {"iata": "VAN", "icao": "LTCI", "name": "Van Ferit Melen Havalimanı", "city": "Van", "lat": 38.4682, "lon": 43.3323},
    "SZF": {"iata": "SZF", "icao": "LTFH", "name": "Samsun Çarşamba Havalimanı", "city": "Samsun", "lat": 41.2583, "lon": 36.5486},
    "VAS": {"iata": "VAS", "icao": "LTAR", "name": "Sivas Nuri Demirağ Havalimanı", "city": "Sivas", "lat": 39.8139, "lon": 36.9036},
    "GNY": {"iata": "GNY", "icao": "LTBG", "name": "Şanlıurfa GAP Havalimanı", "city": "Şanlıurfa", "lat": 37.4589, "lon": 38.9044},
    "MLX": {"iata": "MLX", "icao": "LTAT", "name": "Malatya Erhaç Havalimanı", "city": "Malatya", "lat": 38.4353, "lon": 38.0908},
    "EZS": {"iata": "EZS", "icao": "LTCA", "name": "Elazığ Havalimanı", "city": "Elazığ", "lat": 38.6067, "lon": 39.2931},
    "HTY": {"iata": "HTY", "icao": "LTDA", "name": "Hatay Havalimanı", "city": "Hatay", "lat": 36.3628, "lon": 36.2822},
    "OGU": {"iata": "OGU", "icao": "LTCB", "name": "Ordu-Giresun Havalimanı", "city": "Ordu", "lat": 40.9672, "lon": 37.8997},
    "RZV": {"iata": "RZV", "icao": "LTFO", "name": "Rize-Artvin Havalimanı", "city": "Rize", "lat": 41.1714, "lon": 40.8306},
    "NAV": {"iata": "NAV", "icao": "LTAZ", "name": "Kapadokya Havalimanı", "city": "Nevşehir", "lat": 38.7719, "lon": 34.5344},
    "AOE": {"iata": "AOE", "icao": "LTBY", "name": "Eskişehir Hasan Polatkan Havalimanı", "city": "Eskişehir", "lat": 39.8122, "lon": 30.5256},
    "BAL": {"iata": "BAL", "icao": "LTCJ", "name": "Batman Havalimanı", "city": "Batman", "lat": 37.9294, "lon": 41.1167},
    "MQM": {"iata": "MQM", "icao": "LTCR", "name": "Mardin Prof. Dr. Aziz Sancar Havalimanı", "city": "Mardin", "lat": 37.2239, "lon": 40.6319},
    "KZR": {"iata": "KZR", "icao": "LTCN", "name": "Zafer Havalimanı", "city": "Kütahya", "lat": 39.1128, "lon": 30.1306},
    "TEQ": {"iata": "TEQ", "icao": "LTBU", "name": "Tekirdağ Çorlu Havalimanı", "city": "Tekirdağ", "lat": 41.1381, "lon": 27.9219},
    "EDO": {"iata": "EDO", "icao": "LTFD", "name": "Balıkesir Koca Seyit Havalimanı", "city": "Balıkesir", "lat": 39.5544, "lon": 27.0139},
    "BZI": {"iata": "BZI", "icao": "LTBK", "name": "Balıkesir Merkez Havalimanı", "city": "Balıkesir", "lat": 39.6192, "lon": 27.9258},
    "CKZ": {"iata": "CKZ", "icao": "LTBH", "name": "Çanakkale Havalimanı", "city": "Çanakkale", "lat": 40.1378, "lon": 26.4267},
    "GKD": {"iata": "GKD", "icao": "LTBI", "name": "Gökçeada Havalimanı", "city": "Çanakkale", "lat": 40.2000, "lon": 25.9000},
    "KFS": {"iata": "KFS", "icao": "LTCV", "name": "Kastamonu Havalimanı", "city": "Kastamonu", "lat": 41.3142, "lon": 33.7958},
    "NOP": {"iata": "NOP", "icao": "LTCM", "name": "Sinop Havalimanı", "city": "Sinop", "lat": 42.0158, "lon": 35.0664},
    "MZH": {"iata": "MZH", "icao": "LTAP", "name": "Amasya Merzifon Havalimanı", "city": "Amasya", "lat": 40.8294, "lon": 35.5222},
    "TJK": {"iata": "TJK", "icao": "LTAW", "name": "Tokat Havalimanı", "city": "Tokat", "lat": 40.3086, "lon": 36.3683},
    "AJI": {"iata": "AJI", "icao": "LTCO", "name": "Ağrı Ahmed-i Hani Havalimanı", "city": "Ağrı", "lat": 39.6547, "lon": 43.0278},
    "KSY": {"iata": "KSY", "icao": "LTCF", "name": "Kars Harakani Havalimanı", "city": "Kars", "lat": 40.5622, "lon": 43.1150},
    "IGD": {"iata": "IGD", "icao": "LTCT", "name": "Iğdır Şehit Bülent Aydın Havalimanı", "city": "Iğdır", "lat": 39.9889, "lon": 43.8653},
    "YKO": {"iata": "YKO", "icao": "LTCW", "name": "Hakkari Yüksekova Selahaddin Eyyubi Havalimanı", "city": "Hakkari", "lat": 37.5492, "lon": 44.2389},
    "NKT": {"iata": "NKT", "icao": "LTCL", "name": "Şırnak Şerafettin Elçi Havalimanı", "city": "Şırnak", "lat": 37.3653, "lon": 42.1794},
    "SXZ": {"iata": "SXZ", "icao": "LTCS", "name": "Siirt Havalimanı", "city": "Siirt", "lat": 37.9789, "lon": 41.8389},
    "MSR": {"iata": "MSR", "icao": "LTCK", "name": "Muş Sultan Alparslan Havalimanı", "city": "Muş", "lat": 38.7483, "lon": 41.6622},
    "BGG": {"iata": "BGG", "icao": "LTCU", "name": "Bingöl Havalimanı", "city": "Bingöl", "lat": 38.8611, "lon": 40.5925},
    "ERC": {"iata": "ERC", "icao": "LTCD", "name": "Erzincan Yıldırım Akbulut Havalimanı", "city": "Erzincan", "lat": 39.7103, "lon": 39.5261},
    "KCM": {"iata": "KCM", "icao": "LTCN", "name": "Kahramanmaraş Havalimanı", "city": "Kahramanmaraş", "lat": 37.5386, "lon": 36.9536},
    "ADF": {"iata": "ADF", "icao": "LTCP", "name": "Adıyaman Havalimanı", "city": "Adıyaman", "lat": 37.7317, "lon": 38.4689},
    "USQ": {"iata": "USQ", "icao": "LTBO", "name": "Uşak Havalimanı", "city": "Uşak", "lat": 38.6806, "lon": 29.4719},
    "DNZ": {"iata": "DNZ", "icao": "LTAY", "name": "Denizli Çardak Havalimanı", "city": "Denizli", "lat": 37.7856, "lon": 29.7014},
    "ISE": {"iata": "ISE", "icao": "LTFC", "name": "Isparta Süleyman Demirel Havalimanı", "city": "Isparta", "lat": 37.8553, "lon": 30.3689},
    "GZP": {"iata": "GZP", "icao": "LTFG", "name": "Gazipaşa-Alanya Havalimanı", "city": "Antalya", "lat": 36.2992, "lon": 32.3014}
}


def resolve_airport(airport_str: str) -> Optional[Dict[str, Any]]:
    """Resolves airport string (IATA, ICAO, City, or Name) against Turkish airports catalog."""
    if not airport_str:
        return None
    code = airport_str.strip().upper()
    if code in TURKISH_AIRPORTS:
        return TURKISH_AIRPORTS[code]
    for ap in TURKISH_AIRPORTS.values():
        if ap["icao"] == code or code in ap["name"].upper() or code in ap["city"].upper():
            return ap
    return None


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
        if get_stats and str(get_stats).lower() in ["true", "1", "yes"]:
            return self.get_telemetry_stats()

        clean_q = query.strip().upper() if query and str(query).strip() else None
        clean_airline = airline.strip().upper() if airline and str(airline).strip() else None

        # Resolve geographical filter via core geo_engine (81 provinces or 7 macro regions)
        raw_geo = (region or kwargs.get("country") or "").strip()
        geo_filter = geo_engine.resolve_geo_filter(raw_geo) if raw_geo else None
        region_name = geo_filter.get("name", raw_geo) if geo_filter else None

        # Coerce numeric filters safely (handles str/int/float inputs from LLMs)
        if min_speed_kmh is not None:
            try:
                min_speed_kmh = float(min_speed_kmh)
            except (ValueError, TypeError):
                min_speed_kmh = None

        if min_altitude_feet is not None:
            try:
                min_altitude_feet = float(min_altitude_feet)
            except (ValueError, TypeError):
                min_altitude_feet = None

        if limit is not None:
            try:
                limit = int(limit)
            except (ValueError, TypeError):
                limit = 15

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

    # ============================================================
    # 🚨 1. EMERGENCY & SQUAWK ALERTS TOOL
    # ============================================================
    def find_emergency_flights(
        self,
        emergency_type: str = "ALL",
        include_rapid_descent: bool = True,
        limit: int = 15
    ) -> Dict[str, Any]:
        """Detects aircraft broadcasting emergency squawk codes (7700, 7600, 7500)
        or experiencing severe rapid descent rates in the Kafka live stream.
        """
        emergency_squawks = {
            "7700": "General Emergency (Genel Acil Durum)",
            "7600": "Radio Failure / Lost Comms (Telsiz/İletişim Kaybı)",
            "7500": "Unlawful Interference / Hijacking (Güvenlik / Kaçırılma)"
        }
        clean_type = (emergency_type or "ALL").strip().upper()

        matched = []
        for f in self.flights.values():
            t = f.get("telemetry", {})
            squawk = str(t.get("squawk") or "").strip()
            v_speed = t.get("vertical_speed_fpm")
            is_emergency = False
            reasons = []

            # Squawk check
            if squawk in emergency_squawks:
                if clean_type == "ALL" or clean_type == squawk:
                    is_emergency = True
                    reasons.append(f"Squawk {squawk}: {emergency_squawks[squawk]}")

            # Rapid descent check (alarming vertical speed < -3000 fpm)
            if include_rapid_descent and v_speed is not None and v_speed < -3000:
                is_emergency = True
                reasons.append(f"Rapid Emergency Descent: {v_speed} fpm (Ani İrtifa Kaybı)")

            if is_emergency:
                item = dict(f)
                item["emergency_alert"] = {
                    "squawk": squawk,
                    "reasons": reasons,
                    "severity": "CRITICAL" if squawk == "7700" or (v_speed and v_speed < -4000) else "HIGH"
                }
                matched.append(item)

        return {
            "status": "success",
            "source": "kafka_in_memory_stream",
            "emergency_detected": len(matched) > 0,
            "total_matches": len(matched),
            "returned_count": min(len(matched), limit),
            "emergency_flights": matched[:limit],
            "note": "Airspace is normal with zero active emergency squawks." if not matched else f"⚠️ ALERT: {len(matched)} aircraft with emergency/alert condition detected!"
        }

    # ============================================================
    # 📍 2. NEARBY AIRCRAFT RADIUS TOOL
    # ============================================================
    def find_nearby_aircraft(
        self,
        location: str = "",
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        radius_km: float = 50.0,
        min_altitude_feet: Optional[float] = None,
        limit: int = 15
    ) -> Dict[str, Any]:
        """Finds aircraft within a specified geographic radius (km) around a target city,
        airport, or exact coordinate (lat, lon) sorted by distance.
        """
        center_lat = latitude
        center_lon = longitude
        resolved_name = location or "Custom Coordinate"

        # Resolve location if string provided
        if location and (center_lat is None or center_lon is None):
            # Check airport catalog
            ap = resolve_airport(location)
            if ap:
                center_lat = ap["lat"]
                center_lon = ap["lon"]
                resolved_name = f"{ap['name']} ({ap['iata']})"
            else:
                # Check province catalog
                prov_info = geo_engine.get_province_info(location)
                if prov_info:
                    center_lat = prov_info["center"]["lat"]
                    center_lon = prov_info["center"]["lon"]
                    resolved_name = f"{prov_info['name']} İl Merkezi"
                else:
                    # Check macro region
                    macro = geo_engine.resolve_macro_region(location)
                    if macro:
                        center_lat = (macro["min_lat"] + macro["max_lat"]) / 2.0
                        center_lon = (macro["min_lon"] + macro["max_lon"]) / 2.0
                        resolved_name = macro["name"]

        if center_lat is None or center_lon is None:
            return {
                "status": "error",
                "error": f"Could not resolve center coordinates for location '{location}'. Please provide valid city, airport code, or lat/lon."
            }

        matched = []
        for f in self.flights.values():
            t = f.get("telemetry", {})
            f_lat = t.get("latitude")
            f_lon = t.get("longitude")
            f_alt = t.get("altitude_feet")

            if f_lat is None or f_lon is None:
                continue

            if min_altitude_feet is not None and (f_alt is None or f_alt < min_altitude_feet):
                continue

            dist = calculate_haversine_distance(center_lat, center_lon, f_lat, f_lon)
            if dist <= radius_km:
                item = dict(f)
                item["distance_km"] = dist
                matched.append(item)

        # Sort by distance ascending (nearest first)
        matched.sort(key=lambda x: x.get("distance_km", 999999))

        return {
            "status": "success",
            "source": "kafka_in_memory_stream",
            "center": {
                "name": resolved_name,
                "latitude": round(center_lat, 4),
                "longitude": round(center_lon, 4),
                "radius_km": radius_km
            },
            "total_matches": len(matched),
            "returned_count": min(len(matched), limit),
            "flights": matched[:limit]
        }

    # ============================================================
    # 🛫 3. AIRPORT TRAFFIC & APPROACH TOOL
    # ============================================================
    def get_airport_traffic(
        self,
        airport_code: str,
        traffic_type: str = "ALL",
        airline: str = "",
        limit: int = 15
    ) -> Dict[str, Any]:
        """Retrieves arriving, departing, or proximate approach traffic for a given Turkish or global airport."""
        ap = resolve_airport(airport_code)
        target_iata = (ap["iata"] if ap else airport_code).upper().strip()
        target_icao = (ap["icao"] if ap else airport_code).upper().strip()
        clean_traffic = (traffic_type or "ALL").upper().strip()
        clean_airline = (airline or "").upper().strip()

        matched_arrivals = []
        matched_departures = []
        matched_approach = []

        for f in self.flights.values():
            route = f.get("route", {})
            orig = str(route.get("origin_iata") or "").upper().strip()
            dest = str(route.get("destination_iata") or "").upper().strip()
            t = f.get("telemetry", {})
            f_lat = t.get("latitude")
            f_lon = t.get("longitude")
            f_alt = t.get("altitude_feet")
            v_speed = t.get("vertical_speed_fpm")

            if clean_airline:
                f_iata = str(f.get("airline_iata") or "").upper()
                f_icao = str(f.get("airline_icao") or "").upper()
                callsign = str(f.get("callsign") or "").upper()
                if clean_airline not in [f_iata, f_icao] and not callsign.startswith(clean_airline):
                    continue

            is_arrival = (dest == target_iata or dest == target_icao)
            is_departure = (orig == target_iata or orig == target_icao)

            # Spatial approach check if airport coordinates known
            is_near_approach = False
            if ap and f_lat and f_lon:
                dist = calculate_haversine_distance(ap["lat"], ap["lon"], f_lat, f_lon)
                if dist <= 60.0 and f_alt and f_alt <= 12000 and (v_speed and v_speed < 0 or is_arrival):
                    is_near_approach = True

            item = dict(f)
            if ap and f_lat and f_lon:
                item["distance_to_airport_km"] = calculate_haversine_distance(ap["lat"], ap["lon"], f_lat, f_lon)

            if is_arrival:
                matched_arrivals.append(item)
            elif is_departure:
                matched_departures.append(item)
            elif is_near_approach:
                matched_approach.append(item)

        if clean_traffic in ["ARR", "ARRIVALS", "INBOUND", "LANDING"]:
            final_list = matched_arrivals + [a for a in matched_approach if a not in matched_arrivals]
        elif clean_traffic in ["DEP", "DEPARTURES", "OUTBOUND", "TAKEOFF"]:
            final_list = matched_departures
        else:
            final_list = matched_arrivals + matched_departures + [a for a in matched_approach if a not in matched_arrivals]

        return {
            "status": "success",
            "source": "kafka_in_memory_stream",
            "airport": ap or {"iata": target_iata, "name": f"Airport {target_iata}"},
            "traffic_filter": clean_traffic,
            "counts": {
                "arrivals": len(matched_arrivals),
                "departures": len(matched_departures),
                "active_approach": len(matched_approach),
                "total_matched": len(final_list)
            },
            "returned_count": min(len(final_list), limit),
            "flights": final_list[:limit]
        }

    # ============================================================
    # 📈 4. VERTICAL TELEMETRY & CLIMB/DESCENT TOOL
    # ============================================================
    def get_vertical_rate_flights(
        self,
        flight_phase: str = "ALL",
        min_vertical_speed_fpm: Optional[float] = None,
        region: str = "",
        airline: str = "",
        limit: int = 15
    ) -> Dict[str, Any]:
        """Filters aircraft by vertical speed rates and flight phases: climbing (> +500 fpm),
        descending (< -500 fpm), or cruising level flight (|fpm| <= 500).
        """
        clean_phase = (flight_phase or "ALL").upper().strip()
        clean_airline = (airline or "").upper().strip()
        geo_filter = geo_engine.resolve_geo_filter(region) if region else None

        matched = []
        for f in self.flights.values():
            t = f.get("telemetry", {})
            v_spd = t.get("vertical_speed_fpm")
            f_lat = t.get("latitude")
            f_lon = t.get("longitude")

            if v_spd is None:
                continue

            # Airline check
            if clean_airline:
                f_iata = str(f.get("airline_iata") or "").upper()
                f_icao = str(f.get("airline_icao") or "").upper()
                callsign = str(f.get("callsign") or "").upper()
                if clean_airline not in [f_iata, f_icao] and not callsign.startswith(clean_airline):
                    continue

            # Geo check
            if geo_filter and f_lat and f_lon:
                if geo_filter["type"] == "polygon" and not geo_engine.is_point_in_province(f_lat, f_lon, geo_filter["province"]):
                    continue
                elif geo_filter["type"] == "bbox" and not (geo_filter["min_lat"] <= f_lat <= geo_filter["max_lat"] and geo_filter["min_lon"] <= f_lon <= geo_filter["max_lon"]):
                    continue

            # Phase classification
            if v_spd > 500:
                current_phase = "CLIMBING"
            elif v_spd < -500:
                current_phase = "DESCENDING"
            else:
                current_phase = "CRUISING"

            if clean_phase != "ALL" and clean_phase not in current_phase:
                continue

            if min_vertical_speed_fpm is not None and abs(v_spd) < min_vertical_speed_fpm:
                continue

            item = dict(f)
            item["vertical_profile"] = {
                "phase": current_phase,
                "vertical_speed_fpm": v_spd,
                "vertical_speed_mps": round(v_spd * 0.00508, 1),
                "altitude_feet": t.get("altitude_feet"),
                "altitude_meters": t.get("altitude_meters")
            }
            matched.append(item)

        # Sort by absolute vertical speed descending
        matched.sort(key=lambda x: abs(x.get("telemetry", {}).get("vertical_speed_fpm") or 0), reverse=True)

        return {
            "status": "success",
            "source": "kafka_in_memory_stream",
            "phase_filter": clean_phase,
            "total_matches": len(matched),
            "returned_count": min(len(matched), limit),
            "flights": matched[:limit]
        }

    # ============================================================
    # 🌍 5. TRANSIT OVERFLIGHT CORRIDOR TOOL
    # ============================================================
    def get_transit_flights(
        self,
        min_altitude_feet: Optional[float] = 28000.0,
        airline: str = "",
        limit: int = 15
    ) -> Dict[str, Any]:
        """Identifies international transit overflights crossing Turkish airspace without landing or taking off in Turkey."""
        turkish_airports_iata = set(TURKISH_AIRPORTS.keys())
        clean_airline = (airline or "").upper().strip()

        matched = []
        for f in self.flights.values():
            t = f.get("telemetry", {})
            f_alt = t.get("altitude_feet")
            route = f.get("route", {})
            orig = str(route.get("origin_iata") or "").upper().strip()
            dest = str(route.get("destination_iata") or "").upper().strip()

            if min_altitude_feet is not None and (f_alt is None or f_alt < min_altitude_feet):
                continue

            if clean_airline:
                f_iata = str(f.get("airline_iata") or "").upper()
                f_icao = str(f.get("airline_icao") or "").upper()
                callsign = str(f.get("callsign") or "").upper()
                if clean_airline not in [f_iata, f_icao] and not callsign.startswith(clean_airline):
                    continue

            # Transit condition: Neither origin nor destination is a known Turkish airport, but flight is in TR airspace
            is_turkish_flight = (orig in turkish_airports_iata or dest in turkish_airports_iata)
            is_transit = not is_turkish_flight and orig not in ["", "N/A", "?"] and dest not in ["", "N/A", "?"]

            if is_transit:
                item = dict(f)
                item["transit_corridor"] = {
                    "corridor_display": f"{orig} ➔ [Türkiye Semaları Üst Geçiş] ➔ {dest}",
                    "is_overflight": True,
                    "cruising_altitude_ft": f_alt
                }
                matched.append(item)

        # Sort by ground speed descending
        matched.sort(key=lambda x: x.get("telemetry", {}).get("ground_speed_kmh", 0) or 0, reverse=True)

        return {
            "status": "success",
            "source": "kafka_in_memory_stream",
            "corridor_type": "International Transit Overflight",
            "total_matches": len(matched),
            "returned_count": min(len(matched), limit),
            "transit_flights": matched[:limit]
        }

    # ============================================================
    # 📊 6. FLEET & AIRCRAFT MODEL ANALYTICS TOOL
    # ============================================================
    def get_fleet_aircraft_analytics(
        self,
        aircraft_family: str = "",
        airline: str = "",
        include_breakdown: bool = True
    ) -> Dict[str, Any]:
        """Aggregates aircraft models, wide-body vs narrow-body distributions, and active airline fleet shares."""
        wide_body_models = {"B77W", "B772", "B773", "B788", "B789", "B78X", "A332", "A333", "A359", "A35K", "A388", "B744", "B748"}
        clean_family = (aircraft_family or "").upper().strip()
        clean_airline = (airline or "").upper().strip()

        model_counts: Dict[str, int] = {}
        airline_counts: Dict[str, int] = {}
        body_type_counts = {"wide_body": 0, "narrow_body_or_regional": 0, "unknown": 0}
        total_analyzed = 0

        for f in self.flights.values():
            model = str(f.get("aircraft_model") or "Unknown").upper().strip()
            al = str(f.get("airline_iata") or f.get("airline_icao") or "Unknown").upper().strip()

            if clean_airline and clean_airline not in al:
                continue
            if clean_family and clean_family not in model:
                continue

            total_analyzed += 1
            model_counts[model] = model_counts.get(model, 0) + 1
            airline_counts[al] = airline_counts.get(al, 0) + 1

            if model in wide_body_models:
                body_type_counts["wide_body"] += 1
            elif model != "UNKNOWN":
                body_type_counts["narrow_body_or_regional"] += 1
            else:
                body_type_counts["unknown"] += 1

        top_models = sorted([{"model": k, "count": v, "percentage": round(v / total_analyzed * 100, 1)} for k, v in model_counts.items() if k != "UNKNOWN"], key=lambda x: x["count"], reverse=True)[:10] if total_analyzed else []
        top_airlines = sorted([{"airline": k, "count": v, "percentage": round(v / total_analyzed * 100, 1)} for k, v in airline_counts.items() if k != "UNKNOWN"], key=lambda x: x["count"], reverse=True)[:10] if total_analyzed else []

        return {
            "status": "success",
            "source": "kafka_in_memory_stream",
            "total_aircraft_analyzed": total_analyzed,
            "body_type_distribution": body_type_counts,
            "top_aircraft_models": top_models,
            "top_active_airlines": top_airlines,
            "filter_applied": {
                "aircraft_family": clean_family or "ALL",
                "airline": clean_airline or "ALL"
            }
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


def query_kafka_stream(**kwargs) -> Dict[str, Any]:
    """Unified entrypoint for querying the Kafka flight stream."""
    return kafka_store.query_flights(**kwargs)
