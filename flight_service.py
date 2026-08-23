import math
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from FlightRadarAPI import FlightRadar24API


fr_api = FlightRadar24API()


class FlightRef:
    """Helper class to pass flight ID to get_flight_details."""
    def __init__(self, fid: str):
        self.id = fid


def calculate_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculates haversine distance in km between two lat/lon coordinates."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(R * c, 2)


def format_flight_summary(flight) -> Dict[str, Any]:
    """Formats a basic Flight object into a clean dictionary."""
    altitude_ft = getattr(flight, "altitude", 0) or 0
    ground_speed_kts = getattr(flight, "ground_speed", 0) or 0
    
    return {
        "id": getattr(flight, "id", ""),
        "callsign": getattr(flight, "callsign", "") or "",
        "flight_number": getattr(flight, "number", "") or getattr(flight, "callsign", ""),
        "registration": getattr(flight, "registration", "") or "",
        "aircraft_code": getattr(flight, "aircraft_code", "") or "",
        "airline_icao": getattr(flight, "airline_icao", "") or "",
        "airline_iata": getattr(flight, "airline_iata", "") or "",
        "origin_airport": getattr(flight, "origin_airport_iata", "") or "N/A",
        "destination_airport": getattr(flight, "destination_airport_iata", "") or "N/A",
        "latitude": getattr(flight, "latitude", 0.0),
        "longitude": getattr(flight, "longitude", 0.0),
        "altitude_feet": altitude_ft,
        "altitude_meters": round(altitude_ft * 0.3048, 1),
        "ground_speed_knots": ground_speed_kts,
        "ground_speed_kmh": round(ground_speed_kts * 1.852, 1),
        "heading_degrees": getattr(flight, "heading", 0),
        "vertical_speed_fpm": getattr(flight, "vertical_speed", 0),
        "on_ground": bool(getattr(flight, "on_ground", 0)),
        "squawk": getattr(flight, "squawk", "")
    }


def parse_rich_details(details: dict, basic_summary: Optional[dict] = None) -> Dict[str, Any]:
    """Extracts structured telemetry, aircraft model, route, airline, and status from get_flight_details."""
    if basic_summary is None:
        basic_summary = {}

    aircraft = details.get("aircraft", {}) or {}
    model = aircraft.get("model", {}) or {}
    airline = details.get("airline", {}) or {}
    airport = details.get("airport", {}) or {}
    orig = airport.get("origin", {}) or {}
    dest = airport.get("destination", {}) or {}
    status = details.get("status", {}) or {}
    time_info = details.get("time", {}) or {}
    trail = details.get("trail", []) or []

    # Get latest real-time telemetry from trail if available
    latest_pt = trail[0] if trail else {}
    lat = latest_pt.get("lat", basic_summary.get("latitude", 0.0))
    lon = latest_pt.get("lng", basic_summary.get("longitude", 0.0))
    alt_ft = latest_pt.get("alt", basic_summary.get("altitude_feet", 0))
    spd_kts = latest_pt.get("spd", basic_summary.get("ground_speed_knots", 0))
    heading = latest_pt.get("hd", basic_summary.get("heading_degrees", 0))

    identification = details.get("identification", {}) or {}
    callsign = identification.get("callsign", basic_summary.get("callsign", ""))
    flight_num = identification.get("number", {}).get("default", basic_summary.get("flight_number", callsign))

    return {
        "callsign": callsign,
        "flight_number": flight_num,
        "registration": aircraft.get("registration", basic_summary.get("registration", "")),
        "aircraft_details": {
            "model_name": model.get("text", model.get("code", basic_summary.get("aircraft_code", "N/A"))),
            "model_code": model.get("code", basic_summary.get("aircraft_code", "")),
            "registration": aircraft.get("registration", ""),
            "serial_number": aircraft.get("serialNo", ""),
            "age_years": aircraft.get("age", ""),
            "country": aircraft.get("country", {}).get("name", "") if isinstance(aircraft.get("country"), dict) else ""
        },
        "airline_details": {
            "name": airline.get("name", ""),
            "short_name": airline.get("short", ""),
            "icao": airline.get("code", {}).get("icao", basic_summary.get("airline_icao", "")) if isinstance(airline.get("code"), dict) else "",
            "iata": airline.get("code", {}).get("iata", basic_summary.get("airline_iata", "")) if isinstance(airline.get("code"), dict) else ""
        },
        "route": {
            "origin": {
                "name": orig.get("name", ""),
                "city": orig.get("position", {}).get("region", {}).get("city", "") if isinstance(orig.get("position"), dict) else "",
                "country": orig.get("position", {}).get("country", {}).get("name", "") if isinstance(orig.get("position"), dict) else "",
                "iata": orig.get("code", {}).get("iata", basic_summary.get("origin_airport", "")) if isinstance(orig.get("code"), dict) else "",
                "icao": orig.get("code", {}).get("icao", "") if isinstance(orig.get("code"), dict) else ""
            },
            "destination": {
                "name": dest.get("name", ""),
                "city": dest.get("position", {}).get("region", {}).get("city", "") if isinstance(dest.get("position"), dict) else "",
                "country": dest.get("position", {}).get("country", {}).get("name", "") if isinstance(dest.get("position"), dict) else "",
                "iata": dest.get("code", {}).get("iata", basic_summary.get("destination_airport", "")) if isinstance(dest.get("code"), dict) else "",
                "icao": dest.get("code", {}).get("icao", "") if isinstance(dest.get("code"), dict) else ""
            }
        },
        "telemetry": {
            "latitude": lat,
            "longitude": lon,
            "altitude_feet": alt_ft,
            "altitude_meters": round(alt_ft * 0.3048, 1) if alt_ft else 0,
            "ground_speed_knots": spd_kts,
            "ground_speed_kmh": round(spd_kts * 1.852, 1) if spd_kts else 0,
            "heading_degrees": heading,
            "on_ground": (alt_ft == 0 and spd_kts < 40)
        },
        "flight_status": {
            "text": status.get("text", "En route"),
            "color": status.get("color", "green"),
            "scheduled_departure": time_info.get("scheduled", {}).get("departure") if isinstance(time_info.get("scheduled"), dict) else None,
            "scheduled_arrival": time_info.get("scheduled", {}).get("arrival") if isinstance(time_info.get("scheduled"), dict) else None,
            "estimated_arrival": time_info.get("estimated", {}).get("arrival") if isinstance(time_info.get("estimated"), dict) else None
        }
    }


def get_flight_info(query: str) -> Dict[str, Any]:
    """Finds a live flight by flight number (e.g. TK2584, TK10, PC2020), callsign (e.g. THY1TE, THY10, UAE152), or tail registration (e.g. TC-LBV, TC-LJA).
    Uses FlightRadar24's global search engine + ADS-B telemetry to return exact live coordinates, altitude, speed, route, and aircraft model.
    """
    clean_query = query.strip().upper().replace(" ", "")
    if not clean_query:
        return {"status": "error", "message": "Uçuş numarası veya çağrı kodu boş olamaz."}

    # Strategy 1: FlightRadar24 Global Search (Exact & Live)
    try:
        search_res = fr_api.search(clean_query)
        if isinstance(search_res, dict):
            live_items = search_res.get("live", [])
            if live_items:
                best_live = live_items[0]
                flight_id = best_live.get("id")
                if flight_id:
                    details = fr_api.get_flight_details(FlightRef(flight_id))
                    if isinstance(details, dict):
                        parsed = parse_rich_details(details)
                        return {
                            "status": "success",
                            "source": "live_global_search",
                            "flight": parsed
                        }
    except Exception as e:
        print(f"[Warning] Global search exception: {e}")

    # Strategy 2: Airline Fleet Search (e.g. THY, PGT, UAE, BAW, DLH)
    for prefix_len in [3, 2]:
        if len(clean_query) > prefix_len:
            code = clean_query[:prefix_len]
            try:
                airline_flights = fr_api.get_flights(airline=code)
                for f in airline_flights:
                    c = (getattr(f, "callsign", "") or "").upper()
                    n = (getattr(f, "number", "") or "").upper()
                    r = (getattr(f, "registration", "") or "").upper()
                    if clean_query in [c, n, r] or clean_query in c or (n and clean_query in n):
                        summary = format_flight_summary(f)
                        try:
                            details = fr_api.get_flight_details(f)
                            if isinstance(details, dict):
                                return {"status": "success", "source": "airline_fleet", "flight": parse_rich_details(details, summary)}
                        except Exception:
                            pass
                        return {"status": "success", "source": "airline_fleet_telemetry", "flight": summary}
            except Exception:
                pass

    # Strategy 3: Global Snapshot Search
    try:
        flights = fr_api.get_flights()
        for f in flights:
            c = (getattr(f, "callsign", "") or "").upper()
            n = (getattr(f, "number", "") or "").upper()
            r = (getattr(f, "registration", "") or "").upper()
            if clean_query in [c, n, r] or clean_query in c or (n and clean_query in n):
                summary = format_flight_summary(f)
                try:
                    details = fr_api.get_flight_details(f)
                    if isinstance(details, dict):
                        return {"status": "success", "source": "global_snapshot", "flight": parse_rich_details(details, summary)}
                except Exception:
                    pass
                return {"status": "success", "source": "global_snapshot_telemetry", "flight": summary}
    except Exception as e:
        return {"status": "error", "message": f"FlightRadar24 API'ye erişilemedi: {str(e)}"}

    return {
        "status": "not_found",
        "message": f"'{query}' kodlu uçuş şu anda FlightRadar üzerinde havada veya aktif olarak bulunamadı. Uçuş henüz kalkmamış veya inmiş olabilir."
    }


def search_airline_flights(airline_code: str, limit: int = 15) -> Dict[str, Any]:
    """Searches live airborne flights for a specific airline by ICAO/IATA code (e.g. THY, TK, PGT, BAW, DLH)."""
    clean_code = airline_code.strip().upper()
    try:
        flights = fr_api.get_flights(airline=clean_code)
        if not flights and len(clean_code) == 2:
            all_flights = fr_api.get_flights()
            flights = [f for f in all_flights if getattr(f, "airline_iata", "").upper() == clean_code]
    except Exception as e:
        return {"status": "error", "message": f"Havayolu uçuşları çekilemedi: {str(e)}"}

    if not flights:
        return {
            "status": "not_found",
            "message": f"'{airline_code}' havayolu için şu anda havada aktif uçuş bulunamadı."
        }

    results = [format_flight_summary(f) for f in flights[:limit]]
    return {
        "status": "success",
        "airline": clean_code,
        "total_active_flights": len(flights),
        "returned_count": len(results),
        "flights": results
    }


def get_flights_over_region(latitude: float, longitude: float, radius_km: float = 100.0, limit: int = 15) -> Dict[str, Any]:
    """Finds live flights within a radius around a geographic coordinate (latitude, longitude)."""
    try:
        radius_meters = int(radius_km * 1000)
        bounds = fr_api.get_bounds_by_point(latitude, longitude, radius_meters)
        flights = fr_api.get_flights(bounds=bounds)
    except Exception as e:
        return {"status": "error", "message": f"Bölge uçuşları çekilemedi: {str(e)}"}

    if not flights:
        return {
            "status": "not_found",
            "message": f"({latitude}, {longitude}) koordinatının {radius_km} km çevresinde şu an uçuş bulunamadı."
        }

    enriched = []
    for f in flights:
        item = format_flight_summary(f)
        f_lat = item["latitude"]
        f_lon = item["longitude"]
        dist = calculate_distance_km(latitude, longitude, f_lat, f_lon)
        item["distance_to_center_km"] = dist
        enriched.append(item)

    enriched.sort(key=lambda x: x["distance_to_center_km"])
    results = enriched[:limit]

    return {
        "status": "success",
        "center_point": {"latitude": latitude, "longitude": longitude},
        "radius_km": radius_km,
        "total_flights_in_radius": len(enriched),
        "returned_count": len(results),
        "flights": results
    }


def get_most_tracked_flights(limit: int = 10) -> Dict[str, Any]:
    """Fetches the top live most tracked flights globally from FlightRadar24."""
    try:
        data = fr_api.get_most_tracked()
        tracked_list = []
        if isinstance(data, dict):
            raw_items = data.get("data", [])
        elif isinstance(data, list):
            raw_items = data
        else:
            raw_items = []

        for item in raw_items[:limit]:
            tracked_list.append({
                "flight_id": item.get("flight_id", ""),
                "flight_number": item.get("flight", ""),
                "callsign": item.get("callsign", ""),
                "squawk": item.get("squawk", ""),
                "clicks": item.get("clicks", 0),
                "route": f"{item.get('from_iata', 'N/A')} -> {item.get('to_iata', 'N/A')}",
                "aircraft_type": item.get("type", ""),
                "model": item.get("model", ""),
                "live_trackers": item.get("clicks", 0)
            })

        return {
            "status": "success",
            "count": len(tracked_list),
            "most_tracked_flights": tracked_list
        }
    except Exception as e:
        return {"status": "error", "message": f"En çok takip edilen uçuşlar çekilemedi: {str(e)}"}


def get_airport_info(airport_code: str) -> Dict[str, Any]:
    """Retrieves airport details (name, city, country, coordinates, elevation) by IATA or ICAO code."""
    clean_code = airport_code.strip().upper()
    try:
        airports = fr_api.get_airports()
        matched = None
        for a in airports:
            iata = getattr(a, "iata", "") or ""
            icao = getattr(a, "icao", "") or ""
            if clean_code in [iata.upper(), icao.upper()]:
                matched = a
                break

        if not matched:
            return {
                "status": "not_found",
                "message": f"'{airport_code}' IATA/ICAO koduna sahip havalimanı bulunamadı."
            }

        return {
            "status": "success",
            "airport": {
                "name": getattr(matched, "name", ""),
                "iata": getattr(matched, "iata", ""),
                "icao": getattr(matched, "icao", ""),
                "city": getattr(matched, "city", ""),
                "country": getattr(matched, "country", ""),
                "latitude": getattr(matched, "latitude", 0.0),
                "longitude": getattr(matched, "longitude", 0.0),
                "altitude_feet": getattr(matched, "altitude", 0)
            }
        }
    except Exception as e:
        return {"status": "error", "message": f"Havalimanı bilgisi çekilemedi: {str(e)}"}
