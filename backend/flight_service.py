import math
import sys
from typing import Optional, List, Dict, Any
from geo_service import geo_engine
try:
    from FlightRadarAPI import FlightRadar24API
except ImportError:
    from FlightRadar24 import FlightRadar24API

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

fr_api = FlightRadar24API()


def knots_to_kmh(knots: Optional[float]) -> Optional[float]:
    if knots is None:
        return None
    return round(knots * 1.852, 1)


def feet_to_meters(feet: Optional[float]) -> Optional[float]:
    if feet is None:
        return None
    return round(feet * 0.3048, 1)


def calculate_haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculates distance between two coordinates in kilometers."""
    R = 6371.0  # Earth's radius in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(R * c, 2)


class _FlightWrapper:
    """Wrapper object with an .id property for FlightRadar24API.get_flight_details compatibility."""
    def __init__(self, flight_id: str):
        self.id = flight_id


def get_flight_info(query: str) -> Dict[str, Any]:
    """Finds a live flight by flight number, callsign, or aircraft registration.
    Uses multi-tiered lookup via FlightRadar24 live database.
    """
    raw_query = query.strip()
    clean_query = raw_query.upper()
    no_space_query = clean_query.replace(" ", "").replace("-", "")

    search_candidates = [clean_query, no_space_query]
    if no_space_query.startswith("TK") and len(no_space_query) > 2:
        search_candidates.append("THY" + no_space_query[2:])
    elif no_space_query.startswith("THY") and len(no_space_query) > 3:
        search_candidates.append("TK" + no_space_query[3:])
    elif no_space_query.startswith("PC") and len(no_space_query) > 2:
        search_candidates.append("PGT" + no_space_query[2:])
    elif no_space_query.startswith("PGT") and len(no_space_query) > 3:
        search_candidates.append("PC" + no_space_query[3:])

    target_flight_id = None
    target_flight_obj = None

    # Tier 1: Search API
    for cand in search_candidates:
        try:
            search_results = fr_api.search(cand)
            live_results = search_results.get("live", []) if isinstance(search_results, dict) else []
            for item in live_results:
                f_id = item.get("id")
                detail = item.get("detail", {})
                callsign = detail.get("callsign", "").upper()
                flight_num = detail.get("flight", "").upper()
                reg = detail.get("reg", "").upper()

                if any(c in [callsign, flight_num, reg] or c in flight_num or c in callsign for c in search_candidates):
                    target_flight_id = f_id
                    break
            if not target_flight_id and live_results:
                target_flight_id = live_results[0].get("id")
            if target_flight_id:
                break
        except Exception:
            pass

    # Tier 2: Global get_flights() scan
    if not target_flight_id:
        try:
            flights = fr_api.get_flights()
            for f in flights:
                f_call = (f.callsign or "").upper()
                f_num = (f.number or "").upper()
                f_reg = (f.registration or "").upper()
                if any(c in [f_call, f_num, f_reg] for c in search_candidates) or \
                   any(c in f_call or c in f_num for c in search_candidates if len(c) >= 3):
                    target_flight_id = f.id
                    target_flight_obj = f
                    break
        except Exception:
            pass

    if not target_flight_id:
        return {
            "status": "not_found",
            "message": f"No active live telemetry found for flight '{query}'. The aircraft may not have departed yet, has landed, or is outside radar coverage."
        }

    # Tier 3: Fetch Comprehensive Details
    details = {}
    try:
        details = fr_api.get_flight_details(_FlightWrapper(target_flight_id))
    except Exception:
        pass

    trail = details.get("trail", []) if isinstance(details, dict) else []
    current_point = trail[0] if trail else {}

    latitude = current_point.get("lat")
    longitude = current_point.get("lng")
    altitude_ft = current_point.get("alt")
    speed_kts = current_point.get("spd")
    heading = current_point.get("hd")

    if latitude is None:
        if target_flight_obj:
            latitude = target_flight_obj.latitude
            longitude = target_flight_obj.longitude
            altitude_ft = target_flight_obj.altitude
            speed_kts = target_flight_obj.ground_speed
            heading = target_flight_obj.heading
        else:
            try:
                flights = fr_api.get_flights()
                for f in flights:
                    if f.id == target_flight_id:
                        latitude = f.latitude
                        longitude = f.longitude
                        altitude_ft = f.altitude
                        speed_kts = f.ground_speed
                        heading = f.heading
                        break
            except Exception:
                pass

    # Extract Flight & Airline Metadata
    identification = details.get("identification", {}) if isinstance(details, dict) else {}
    flight_number = identification.get("number", {}).get("default")
    callsign = identification.get("callsign")

    aircraft = details.get("aircraft", {}) if isinstance(details, dict) else {}
    aircraft_model = aircraft.get("model", {}).get("text") or aircraft.get("model", {}).get("code")
    registration = aircraft.get("registration")

    airline = details.get("airline", {}).get("name") if isinstance(details, dict) else None
    flight_status = details.get("status", {}).get("text") if isinstance(details, dict) else "En Route (Live Radar)"

    airport_info = details.get("airport") if isinstance(details, dict) and isinstance(details.get("airport"), dict) else {}
    origin_airport = airport_info.get("origin") if isinstance(airport_info.get("origin"), dict) else {}
    dest_airport = airport_info.get("destination") if isinstance(airport_info.get("destination"), dict) else {}

    orig_pos = origin_airport.get("position") if isinstance(origin_airport.get("position"), dict) else {}
    orig_region = orig_pos.get("region") if isinstance(orig_pos.get("region"), dict) else {}
    orig_country = orig_pos.get("country") if isinstance(orig_pos.get("country"), dict) else {}
    orig_code = origin_airport.get("code") if isinstance(origin_airport.get("code"), dict) else {}

    dest_pos = dest_airport.get("position") if isinstance(dest_airport.get("position"), dict) else {}
    dest_region = dest_pos.get("region") if isinstance(dest_pos.get("region"), dict) else {}
    dest_country = dest_pos.get("country") if isinstance(dest_pos.get("country"), dict) else {}
    dest_code = dest_airport.get("code") if isinstance(dest_airport.get("code"), dict) else {}

    origin = {
        "name": origin_airport.get("name"),
        "city": orig_region.get("city"),
        "country": orig_country.get("name"),
        "code_iata": orig_code.get("iata"),
        "code_icao": orig_code.get("icao")
    }

    destination = {
        "name": dest_airport.get("name"),
        "city": dest_region.get("city"),
        "country": dest_country.get("name"),
        "code_iata": dest_code.get("iata"),
        "code_icao": dest_code.get("icao")
    }


    return {
        "status": "success",
        "flight_number": flight_number or no_space_query,
        "callsign": callsign or (target_flight_obj.callsign if target_flight_obj else None),
        "airline": airline or (target_flight_obj.airline_icao if target_flight_obj else None),
        "aircraft": {
            "model": aircraft_model or (target_flight_obj.aircraft_code if target_flight_obj else "Unspecified"),
            "registration": registration or (target_flight_obj.registration if target_flight_obj else None)
        },
        "flight_status": flight_status,
        "telemetry": {
            "latitude": latitude,
            "longitude": longitude,
            "altitude_feet": altitude_ft,
            "altitude_meters": feet_to_meters(altitude_ft),
            "ground_speed_knots": speed_kts,
            "ground_speed_kmh": knots_to_kmh(speed_kts),
            "heading_degrees": heading
        },
        "route": {
            "origin": origin,
            "destination": destination
        }
    }



def search_airline_flights(airline_code: str, limit: int = 15) -> Dict[str, Any]:
    """Searches active flights for a given airline code (ICAO/IATA, e.g. THY, PGT, DLH, BAW)."""
    clean_code = airline_code.strip().upper()
    try:
        flights = fr_api.get_flights(airline=clean_code)
    except Exception as e:
        return {"status": "error", "message": f"Error retrieving airline flights: {str(e)}"}

    if not flights:
        return {
            "status": "not_found",
            "airline_code": clean_code,
            "message": f"No active airborne flights found for airline code '{clean_code}'."
        }

    results = []
    for f in flights[:limit]:
        results.append({
            "flight_number": f.number,
            "callsign": f.callsign,
            "registration": f.registration,
            "latitude": f.latitude,
            "longitude": f.longitude,
            "altitude_feet": f.altitude,
            "altitude_meters": feet_to_meters(f.altitude),
            "ground_speed_kmh": knots_to_kmh(f.ground_speed),
            "heading": f.heading,
            "origin_airport": f.origin_airport_iata,
            "destination_airport": f.destination_airport_iata
        })

    return {
        "status": "success",
        "airline_code": clean_code,
        "total_active_flights_found": len(flights),
        "returned_count": len(results),
        "flights": results
    }


# Geographic presets and national airspace bounding boxes
GEO_REGIONS: Dict[str, Dict[str, Any]] = {
    # Turkey National Airspace Bounding Box (~35.8° - 42.2° N, ~25.6° - 44.8° E)
    "TR": {"type": "bbox", "bounds": "42.2,35.8,25.6,44.8", "name": "Türkiye"},
    "TURKEY": {"type": "bbox", "bounds": "42.2,35.8,25.6,44.8", "name": "Türkiye"},
    "TURKIYE": {"type": "bbox", "bounds": "42.2,35.8,25.6,44.8", "name": "Türkiye"},
    "TÜRKIYE": {"type": "bbox", "bounds": "42.2,35.8,25.6,44.8", "name": "Türkiye"},
    # Major Geographic Regions
    "MARMARA": {"type": "bbox", "bounds": "42.1,40.0,26.0,31.0", "name": "Marmara Bölgesi"},
    "EGE": {"type": "bbox", "bounds": "40.2,36.5,26.0,30.2", "name": "Ege Bölgesi"},
    "AEGEAN": {"type": "bbox", "bounds": "40.2,36.5,26.0,30.2", "name": "Ege Bölgesi"},
    "AKDENIZ": {"type": "bbox", "bounds": "38.0,36.0,29.0,36.5", "name": "Akdeniz Bölgesi"},
    "MEDITERRANEAN": {"type": "bbox", "bounds": "38.0,36.0,29.0,36.5", "name": "Akdeniz Bölgesi"},
    "KARADENIZ": {"type": "bbox", "bounds": "42.2,40.5,31.0,42.0", "name": "Karadeniz Bölgesi"},
    "BLACK_SEA": {"type": "bbox", "bounds": "42.2,40.5,31.0,42.0", "name": "Karadeniz Bölgesi"},
    "IC_ANADOLU": {"type": "bbox", "bounds": "40.5,37.5,30.5,37.0", "name": "İç Anadolu Bölgesi"},
}


def get_flights_over_region(
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
    radius_km: float = 100.0,
    country: Optional[str] = None,
    region: Optional[str] = None,
    min_speed_kmh: Optional[float] = None,
    limit: int = 15
) -> Dict[str, Any]:
    """Finds live flights within a given radius around specified coordinates, or within official national / regional borders (e.g. Turkey / Ankara)."""
    geo_filter = None
    region_name = None
    raw_geo = (region or country or "").strip()

    # 1. First check if raw_geo matches one of Turkey's 81 provinces (Exact GeoJSON Polygon)
    if raw_geo:
        matched_province = geo_engine.resolve_province_name(raw_geo)
        if matched_province:
            prov_info = geo_engine.get_province_info(matched_province)
            if prov_info:
                bbox = prov_info["bbox"]
                center = prov_info["center"]
                bounds = f"{bbox['max_lat']},{bbox['min_lat']},{bbox['min_lon']},{bbox['max_lon']}"
                try:
                    flights = fr_api.get_flights(bounds=bounds)
                except Exception as e:
                    return {"status": "error", "message": f"Error during provincial radar scan: {str(e)}"}

                filtered = []
                for f in flights:
                    # Precise Point-in-Polygon test
                    if not geo_engine.is_point_in_province(f.latitude, f.longitude, matched_province):
                        continue
                    spd_kmh = knots_to_kmh(f.ground_speed) or 0
                    if min_speed_kmh is not None and spd_kmh < min_speed_kmh:
                        continue
                    dist = calculate_haversine_distance(center["lat"], center["lon"], f.latitude, f.longitude)
                    filtered.append((dist, f))

                # Sort by distance to provincial center or speed
                if min_speed_kmh is not None:
                    filtered.sort(key=lambda x: knots_to_kmh(x[1].ground_speed) or 0, reverse=True)
                else:
                    filtered.sort(key=lambda x: x[0])

                results = []
                for dist, f in filtered[:limit]:
                    results.append({
                        "flight_number": f.number,
                        "callsign": f.callsign,
                        "aircraft_model": f.aircraft_code,
                        "province": matched_province,
                        "distance_km": dist,
                        "latitude": f.latitude,
                        "longitude": f.longitude,
                        "altitude_feet": f.altitude,
                        "altitude_meters": feet_to_meters(f.altitude),
                        "ground_speed_kmh": knots_to_kmh(f.ground_speed),
                        "heading": f.heading,
                        "route": f"{f.origin_airport_iata or '?'} ➔ {f.destination_airport_iata or '?'}"
                    })

                prov_info = geo_engine.get_province_info(matched_province)
                return {
                    "status": "success",
                    "source": "flightradar24_live_radar",
                    "applied_province": matched_province,
                    "province_details": {
                        "name": prov_info["name"],
                        "plate": prov_info["plate_code"],
                        "geographic_region": prov_info["region"],
                        "center_coords": prov_info["center"],
                        "summary": prov_info["summary"]
                    } if prov_info else None,
                    "min_speed_kmh": min_speed_kmh,
                    "total_flights_found": len(filtered),
                    "returned_flights": results
                }

    # 2. Fall back to national/regional preset bounding boxes (e.g. TR, Marmara, Aegean)
    geo_target = raw_geo.upper().replace("İ", "I")
    if geo_target:
        if geo_target in GEO_REGIONS:
            geo_filter = GEO_REGIONS[geo_target]
            region_name = geo_filter.get("name", geo_target)
        elif "TURK" in geo_target or geo_target == "TR":
            geo_filter = GEO_REGIONS["TR"]
            region_name = "Türkiye"

    # Case A: Predefined Bounding Box (e.g. Turkey national borders)
    if geo_filter and geo_filter.get("type") == "bbox":
        bounds = geo_filter["bounds"]
        try:
            flights = fr_api.get_flights(bounds=bounds)
        except Exception as e:
            return {"status": "error", "message": f"Error during regional radar scan: {str(e)}"}

        filtered = []
        for f in flights:
            spd_kmh = knots_to_kmh(f.ground_speed) or 0
            if min_speed_kmh is not None and spd_kmh < min_speed_kmh:
                continue
            filtered.append(f)

        # Sort by speed descending
        filtered.sort(key=lambda x: knots_to_kmh(x.ground_speed) or 0, reverse=True)

        results = []
        for f in filtered[:limit]:
            results.append({
                "flight_number": f.number,
                "callsign": f.callsign,
                "aircraft_model": f.aircraft_code,
                "latitude": f.latitude,
                "longitude": f.longitude,
                "altitude_feet": f.altitude,
                "altitude_meters": feet_to_meters(f.altitude),
                "ground_speed_kmh": knots_to_kmh(f.ground_speed),
                "heading": f.heading,
                "route": f"{f.origin_airport_iata or '?'} ➔ {f.destination_airport_iata or '?'}"
            })

        return {
            "status": "success",
            "source": "flightradar24_live_radar",
            "applied_region": region_name,
            "min_speed_kmh": min_speed_kmh,
            "total_flights_found": len(filtered),
            "returned_flights": results
        }

    # Case B: Radial coordinates (either from preset e.g. Ankara or explicit lat/lon)
    center_lat = geo_filter["lat"] if (geo_filter and geo_filter.get("type") == "radial") else latitude
    center_lon = geo_filter["lon"] if (geo_filter and geo_filter.get("type") == "radial") else longitude
    target_radius = geo_filter["radius_km"] if (geo_filter and geo_filter.get("type") == "radial") else (radius_km or 100.0)

    if center_lat is None or center_lon is None:
        return {"status": "error", "message": "Please specify latitude/longitude or a valid region/country (e.g. 'TR', 'Ankara', 'Istanbul')."}

    lat_deg_delta = (target_radius / 111.0) * 1.15
    lon_deg_delta = (target_radius / (111.0 * max(0.1, math.cos(math.radians(center_lat))))) * 1.15

    bounds = f"{center_lat + lat_deg_delta},{center_lat - lat_deg_delta},{center_lon - lon_deg_delta},{center_lon + lon_deg_delta}"

    try:
        flights = fr_api.get_flights(bounds=bounds)
    except Exception as e:
        return {"status": "error", "message": f"Error during regional radar scan: {str(e)}"}

    flights_in_radius = []
    for f in flights:
        dist = calculate_haversine_distance(center_lat, center_lon, f.latitude, f.longitude)
        if dist <= target_radius:
            spd_kmh = knots_to_kmh(f.ground_speed) or 0
            if min_speed_kmh is not None and spd_kmh < min_speed_kmh:
                continue
            flights_in_radius.append((dist, f))

    if min_speed_kmh is not None:
        flights_in_radius.sort(key=lambda x: knots_to_kmh(x[1].ground_speed) or 0, reverse=True)
    else:
        flights_in_radius.sort(key=lambda x: x[0])

    results = []
    for dist, f in flights_in_radius[:limit]:
        results.append({
            "flight_number": f.number,
            "callsign": f.callsign,
            "aircraft_model": f.aircraft_code,
            "distance_km": dist,
            "altitude_feet": f.altitude,
            "altitude_meters": feet_to_meters(f.altitude),
            "ground_speed_kmh": knots_to_kmh(f.ground_speed),
            "heading": f.heading,
            "route": f"{f.origin_airport_iata or '?'} ➔ {f.destination_airport_iata or '?'}"
        })

    resp = {
        "status": "success",
        "source": "flightradar24_live_radar",
        "center_coordinates": {"latitude": center_lat, "longitude": center_lon},
        "radius_km": target_radius,
        "total_flights_in_radius": len(flights_in_radius),
        "returned_flights": results
    }
    if region_name:
        resp["applied_region"] = region_name
    if min_speed_kmh:
        resp["min_speed_kmh"] = min_speed_kmh
    return resp


def get_most_tracked_flights(limit: int = 10) -> Dict[str, Any]:
    """Fetches the top live most-tracked flights in the world on FlightRadar24."""
    try:
        tracked = fr_api.get_most_tracked()
        flights_data = tracked.get("data", []) if isinstance(tracked, dict) else []
    except Exception as e:
        return {"status": "error", "message": f"Failed to fetch most-tracked flights: {str(e)}"}

    results = []
    for item in flights_data[:limit]:
        results.append({
            "flight_number": item.get("flight"),
            "callsign": item.get("callsign"),
            "squawk": item.get("squawk"),
            "route": f"{item.get('from_iata') or item.get('from_city') or '?'} ➔ {item.get('to_iata') or item.get('to_city') or '?'}",
            "model": item.get("model"),
            "aircraft_type": item.get("type"),
            "live_trackers": item.get("clicks")
        })

    return {
        "status": "success",
        "total_tracked": len(results),
        "most_tracked_flights": results
    }


def get_airport_info(airport_code: str) -> Dict[str, Any]:
    """Fetches airport details and coordinates for an IATA/ICAO code."""
    clean_code = airport_code.strip().upper()
    try:
        airport = fr_api.get_airport_details(clean_code)
    except Exception as e:
        return {"status": "error", "message": f"Failed to fetch airport info ({clean_code}): {str(e)}"}

    if not airport or not isinstance(airport, dict) or "details" not in airport:
        return {
            "status": "not_found",
            "message": f"Airport '{clean_code}' was not found in the FlightRadar database."
        }

    details = airport.get("details", {})
    position = details.get("position", {})

    return {
        "status": "success",
        "airport_code": clean_code,
        "name": details.get("name"),
        "city": position.get("region", {}).get("city"),
        "country": position.get("country", {}).get("name"),
        "elevation_feet": position.get("elevation"),
        "elevation_meters": feet_to_meters(position.get("elevation")),
        "latitude": position.get("latitude"),
        "longitude": position.get("longitude"),
        "timezone": details.get("timezone", {}).get("name"),
        "visible_aircraft_ground": airport.get("aircraft", {}).get("ground", {}).get("total", 0)
    }
