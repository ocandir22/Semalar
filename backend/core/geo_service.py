"""
GeoService — 81-Province Administrative Boundary & Spatial Engine for Turkey
Provides sub-millisecond Point-in-Polygon (PIP) spatial filtering for all 81 Turkish provinces
using official GeoJSON geometries with clean formatting and zero external GIS dependencies.
"""

import os
import json
import unicodedata
import re
from typing import Optional, Dict, Any, List, Tuple

# Paths to geo data (checks core/data first, then backend/data)
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
if not os.path.exists(DATA_DIR):
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
GEOJSON_FILE = os.path.join(DATA_DIR, "tr-cities.json")


def _normalize_key(text: str) -> str:
    """Normalizes Turkish characters and accents to ASCII uppercase for robust matching."""
    if not text:
        return ""
    mapping = {
        "İ": "I", "I": "I", "ı": "I", "i": "I",
        "Ş": "S", "ş": "S",
        "Ğ": "G", "ğ": "G",
        "Ü": "U", "ü": "U",
        "Ö": "O", "ö": "O",
        "Ç": "C", "ç": "C"
    }
    res = "".join(mapping.get(c, c) for c in text.strip())
    nfkd = unicodedata.normalize("NFKD", res)
    ascii_text = "".join([c for c in nfkd if not unicodedata.combining(c)])
    return ascii_text.upper()


# Canonical catalog of all 81 Turkish provinces with plate numbers and geographic regions
TURKISH_PROVINCES_CATALOG: Dict[int, Dict[str, str]] = {
    1: {"name": "Adana", "region": "Akdeniz"},
    2: {"name": "Adıyaman", "region": "Güneydoğu Anadolu"},
    3: {"name": "Afyonkarahisar", "region": "Ege"},
    4: {"name": "Ağrı", "region": "Doğu Anadolu"},
    5: {"name": "Amasya", "region": "Karadeniz"},
    6: {"name": "Ankara", "region": "İç Anadolu"},
    7: {"name": "Antalya", "region": "Akdeniz"},
    8: {"name": "Artvin", "region": "Karadeniz"},
    9: {"name": "Aydın", "region": "Ege"},
    10: {"name": "Balıkesir", "region": "Marmara"},
    11: {"name": "Bilecik", "region": "Marmara"},
    12: {"name": "Bingöl", "region": "Doğu Anadolu"},
    13: {"name": "Bitlis", "region": "Doğu Anadolu"},
    14: {"name": "Bolu", "region": "Karadeniz"},
    15: {"name": "Burdur", "region": "Akdeniz"},
    16: {"name": "Bursa", "region": "Marmara"},
    17: {"name": "Çanakkale", "region": "Marmara"},
    18: {"name": "Çankırı", "region": "İç Anadolu"},
    19: {"name": "Çorum", "region": "Karadeniz"},
    20: {"name": "Denizli", "region": "Ege"},
    21: {"name": "Diyarbakır", "region": "Güneydoğu Anadolu"},
    22: {"name": "Edirne", "region": "Marmara"},
    23: {"name": "Elazığ", "region": "Doğu Anadolu"},
    24: {"name": "Erzincan", "region": "Doğu Anadolu"},
    25: {"name": "Erzurum", "region": "Doğu Anadolu"},
    26: {"name": "Eskişehir", "region": "İç Anadolu"},
    27: {"name": "Gaziantep", "region": "Güneydoğu Anadolu"},
    28: {"name": "Giresun", "region": "Karadeniz"},
    29: {"name": "Gümüşhane", "region": "Karadeniz"},
    30: {"name": "Hakkari", "region": "Doğu Anadolu"},
    31: {"name": "Hatay", "region": "Akdeniz"},
    32: {"name": "Isparta", "region": "Akdeniz"},
    33: {"name": "Mersin", "region": "Akdeniz"},
    34: {"name": "İstanbul", "region": "Marmara"},
    35: {"name": "İzmir", "region": "Ege"},
    36: {"name": "Kars", "region": "Doğu Anadolu"},
    37: {"name": "Kastamonu", "region": "Karadeniz"},
    38: {"name": "Kayseri", "region": "İç Anadolu"},
    39: {"name": "Kırklareli", "region": "Marmara"},
    40: {"name": "Kırşehir", "region": "İç Anadolu"},
    41: {"name": "Kocaeli", "region": "Marmara"},
    42: {"name": "Konya", "region": "İç Anadolu"},
    43: {"name": "Kütahya", "region": "Ege"},
    44: {"name": "Malatya", "region": "Doğu Anadolu"},
    45: {"name": "Manisa", "region": "Ege"},
    46: {"name": "Kahramanmaraş", "region": "Akdeniz"},
    47: {"name": "Mardin", "region": "Güneydoğu Anadolu"},
    48: {"name": "Muğla", "region": "Ege"},
    49: {"name": "Muş", "region": "Doğu Anadolu"},
    50: {"name": "Nevşehir", "region": "İç Anadolu"},
    51: {"name": "Niğde", "region": "İç Anadolu"},
    52: {"name": "Ordu", "region": "Karadeniz"},
    53: {"name": "Rize", "region": "Karadeniz"},
    54: {"name": "Sakarya", "region": "Marmara"},
    55: {"name": "Samsun", "region": "Karadeniz"},
    56: {"name": "Siirt", "region": "Güneydoğu Anadolu"},
    57: {"name": "Sinop", "region": "Karadeniz"},
    58: {"name": "Sivas", "region": "İç Anadolu"},
    59: {"name": "Tekirdağ", "region": "Marmara"},
    60: {"name": "Tokat", "region": "Karadeniz"},
    61: {"name": "Trabzon", "region": "Karadeniz"},
    62: {"name": "Tunceli", "region": "Doğu Anadolu"},
    63: {"name": "Şanlıurfa", "region": "Güneydoğu Anadolu"},
    64: {"name": "Uşak", "region": "Ege"},
    65: {"name": "Van", "region": "Doğu Anadolu"},
    66: {"name": "Yozgat", "region": "İç Anadolu"},
    67: {"name": "Zonguldak", "region": "Karadeniz"},
    68: {"name": "Aksaray", "region": "İç Anadolu"},
    69: {"name": "Bayburt", "region": "Karadeniz"},
    70: {"name": "Karaman", "region": "İç Anadolu"},
    71: {"name": "Kırıkkale", "region": "İç Anadolu"},
    72: {"name": "Batman", "region": "Güneydoğu Anadolu"},
    73: {"name": "Şırnak", "region": "Güneydoğu Anadolu"},
    74: {"name": "Bartın", "region": "Karadeniz"},
    75: {"name": "Ardahan", "region": "Doğu Anadolu"},
    76: {"name": "Iğdır", "region": "Doğu Anadolu"},
    77: {"name": "Yalova", "region": "Marmara"},
    78: {"name": "Karabük", "region": "Karadeniz"},
    79: {"name": "Kilis", "region": "Güneydoğu Anadolu"},
    80: {"name": "Osmaniye", "region": "Akdeniz"},
    81: {"name": "Düzce", "region": "Karadeniz"}
}

# Common user aliases / alternative names
PROVINCE_ALIASES: Dict[str, str] = {
    "URFA": "Şanlıurfa",
    "SANLIURFA": "Şanlıurfa",
    "ANTEP": "Gaziantep",
    "GAZIANTEP": "Gaziantep",
    "MARAS": "Kahramanmaraş",
    "KAHRAMANMARAS": "Kahramanmaraş",
    "AFYON": "Afyonkarahisar",
    "ICEL": "Mersin",
    "İÇEL": "Mersin",
    "IZMIT": "Kocaeli",
    "İZMİT": "Kocaeli",
    "ADAPAZARI": "Sakarya",
    "ANTAKYA": "Hatay",
    "DERSIM": "Tunceli",
}

# National airspace and macro geographic regions
MACRO_REGIONS: Dict[str, Dict[str, Any]] = {
    # Turkey National Airspace (~35.8° - 42.2° N, ~25.6° - 44.8° E)
    "TR": {"type": "bbox", "min_lat": 35.8, "max_lat": 42.2, "min_lon": 25.6, "max_lon": 44.8, "bounds": "42.2,35.8,25.6,44.8", "name": "Türkiye"},
    "TURKEY": {"type": "bbox", "min_lat": 35.8, "max_lat": 42.2, "min_lon": 25.6, "max_lon": 44.8, "bounds": "42.2,35.8,25.6,44.8", "name": "Türkiye"},
    "TURKIYE": {"type": "bbox", "min_lat": 35.8, "max_lat": 42.2, "min_lon": 25.6, "max_lon": 44.8, "bounds": "42.2,35.8,25.6,44.8", "name": "Türkiye"},
    "TÜRKIYE": {"type": "bbox", "min_lat": 35.8, "max_lat": 42.2, "min_lon": 25.6, "max_lon": 44.8, "bounds": "42.2,35.8,25.6,44.8", "name": "Türkiye"},
    # 7 Geographic Regions of Turkey
    "MARMARA": {"type": "bbox", "min_lat": 40.0, "max_lat": 42.1, "min_lon": 26.0, "max_lon": 31.0, "bounds": "42.1,40.0,26.0,31.0", "name": "Marmara Bölgesi"},
    "EGE": {"type": "bbox", "min_lat": 36.5, "max_lat": 40.2, "min_lon": 26.0, "max_lon": 30.2, "bounds": "40.2,36.5,26.0,30.2", "name": "Ege Bölgesi"},
    "AEGEAN": {"type": "bbox", "min_lat": 36.5, "max_lat": 40.2, "min_lon": 26.0, "max_lon": 30.2, "bounds": "40.2,36.5,26.0,30.2", "name": "Ege Bölgesi"},
    "AKDENIZ": {"type": "bbox", "min_lat": 36.0, "max_lat": 38.0, "min_lon": 29.0, "max_lon": 36.5, "bounds": "38.0,36.0,29.0,36.5", "name": "Akdeniz Bölgesi"},
    "MEDITERRANEAN": {"type": "bbox", "min_lat": 36.0, "max_lat": 38.0, "min_lon": 29.0, "max_lon": 36.5, "bounds": "38.0,36.0,29.0,36.5", "name": "Akdeniz Bölgesi"},
    "KARADENIZ": {"type": "bbox", "min_lat": 40.5, "max_lat": 42.2, "min_lon": 31.0, "max_lon": 42.0, "bounds": "42.2,40.5,31.0,42.0", "name": "Karadeniz Bölgesi"},
    "BLACK_SEA": {"type": "bbox", "min_lat": 40.5, "max_lat": 42.2, "min_lon": 31.0, "max_lon": 42.0, "bounds": "42.2,40.5,31.0,42.0", "name": "Karadeniz Bölgesi"},
    "IC_ANADOLU": {"type": "bbox", "min_lat": 37.5, "max_lat": 40.5, "min_lon": 30.5, "max_lon": 37.0, "bounds": "40.5,37.5,30.5,37.0", "name": "İç Anadolu Bölgesi"},
    "DOGU_ANADOLU": {"type": "bbox", "min_lat": 37.0, "max_lat": 42.0, "min_lon": 38.0, "max_lon": 44.8, "bounds": "42.0,37.0,38.0,44.8", "name": "Doğu Anadolu Bölgesi"},
    "GUNEYDOGU_ANADOLU": {"type": "bbox", "min_lat": 36.5, "max_lat": 38.5, "min_lon": 36.5, "max_lon": 43.0, "bounds": "38.5,36.5,36.5,43.0", "name": "Güneydoğu Anadolu Bölgesi"},
}


def _point_in_ring(x: float, y: float, ring: List[List[float]]) -> bool:
    """Ray-casting Point-in-Polygon test for linear ring. x=lon, y=lat."""
    inside = False
    n = len(ring)
    if n < 3:
        return False
    j = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


class TurkeyGeoEngine:
    """In-memory GIS spatial engine indexing all 81 provinces of Turkey with human-readable formatting."""

    def __init__(self, geojson_path: str = GEOJSON_FILE):
        self.geojson_path = geojson_path
        self.provinces: Dict[str, Dict[str, Any]] = {}
        self.provinces_by_plate: Dict[int, Dict[str, Any]] = {}
        self.lookup_map: Dict[str, str] = {}  # normalized key -> canonical name
        self.sync_and_load()

    def sync_and_load(self):
        """Validates tr-cities.json, repairs Turkish character encoding and formatting, and indexes all 81 provinces."""
        if not os.path.exists(self.geojson_path):
            print(f"[GeoEngine] tr-cities.json not found at: {self.geojson_path}")
            return

        try:
            with open(self.geojson_path, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)

            features = data.get("features", [])
            raw_by_plate = {}
            for feat in features:
                props = feat.get("properties", {})
                num = props.get("plate") or props.get("number") or feat.get("id")
                if num and isinstance(num, int):
                    raw_by_plate[num] = feat

            formatted_features = []
            self.provinces.clear()
            self.provinces_by_plate.clear()
            self.lookup_map.clear()

            for plate in range(1, 82):
                feat = raw_by_plate.get(plate)
                if not feat:
                    continue

                catalog_entry = TURKISH_PROVINCES_CATALOG.get(plate, {"name": f"İl {plate}", "region": "Bilinmiyor"})
                canonical_name = catalog_entry["name"]
                region_name = catalog_entry["region"]

                geom = feat.get("geometry", {})
                geom_type = geom.get("type")
                coords = geom.get("coordinates", [])

                # Gather all coordinates to compute bounding box and geographic center
                all_lons = []
                all_lats = []

                if geom_type == "Polygon":
                    for ring in coords:
                        for pt in ring:
                            all_lons.append(round(pt[0], 5))
                            all_lats.append(round(pt[1], 5))
                elif geom_type == "MultiPolygon":
                    for poly in coords:
                        for ring in poly:
                            for pt in ring:
                                all_lons.append(round(pt[0], 5))
                                all_lats.append(round(pt[1], 5))

                if not all_lons or not all_lats:
                    continue

                bbox = {
                    "min_lat": round(min(all_lats), 4),
                    "max_lat": round(max(all_lats), 4),
                    "min_lon": round(min(all_lons), 4),
                    "max_lon": round(max(all_lons), 4)
                }

                center = {
                    "latitude": round(sum(all_lats) / len(all_lats), 4),
                    "longitude": round(sum(all_lons) / len(all_lons), 4)
                }

                prov_record = {
                    "plate": plate,
                    "plate_code": f"{plate:02d}",
                    "name": canonical_name,
                    "region": region_name,
                    "geometry_type": geom_type,
                    "coordinates": coords,
                    "bbox": bbox,
                    "center": center,
                    "readable_summary": f"[İl] {canonical_name} (Plaka: {plate:02d} | {region_name} Bölgesi) — Merkez: {center['latitude']}° N, {center['longitude']}° E"
                }

                self.provinces[canonical_name] = prov_record
                self.provinces_by_plate[plate] = prov_record

                # Index normalized name for fast lookup
                self.lookup_map[_normalize_key(canonical_name)] = canonical_name

                # Add to formatted GeoJSON feature collection
                formatted_features.append({
                    "type": "Feature",
                    "id": plate,
                    "properties": {
                        "plate": plate,
                        "plate_code": f"{plate:02d}",
                        "name": canonical_name,
                        "region": region_name,
                        "center": center,
                        "bbox": bbox
                    },
                    "geometry": geom
                })

            # Register well-known aliases
            for alias, target in PROVINCE_ALIASES.items():
                norm_alias = _normalize_key(alias)
                norm_target = _normalize_key(target)
                if norm_target in self.lookup_map:
                    self.lookup_map[norm_alias] = self.lookup_map[norm_target]

            # Re-save formatted, readable tr-cities.json with inline coordinate points and UTF-8
            clean_output = {
                "type": "FeatureCollection",
                "metadata": {
                    "dataset": "Turkey Administrative Provinces",
                    "total_provinces": len(formatted_features),
                    "encoding": "UTF-8",
                    "precision": "High Precision Polygons"
                },
                "features": formatted_features
            }
            raw_json = json.dumps(clean_output, ensure_ascii=False, indent=2)
            compact_json = re.sub(r'\[\s*(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)\s*\]', r'[\1, \2]', raw_json)
            with open(self.geojson_path, "w", encoding="utf-8") as f_out:
                f_out.write(compact_json)

            # Export lightweight, 100% human-readable province catalog (without polygon clutter)
            catalog_file = os.path.join(DATA_DIR, "tr-provinces-catalog.json")
            catalog_data = [feat["properties"] for feat in formatted_features]
            with open(catalog_file, "w", encoding="utf-8") as f_cat:
                json.dump(catalog_data, f_cat, ensure_ascii=False, indent=2)

            print(f"[GeoEngine] Synchronized and indexed {len(self.provinces)} Turkish provinces in clean, readable format.")
        except Exception as e:
            print(f"[GeoEngine] Sync error: {e}")

    def resolve_province_name(self, query_str: Optional[str]) -> Optional[str]:
        """Resolves province name against canonical 81 Turkish provinces and registered aliases."""
        if not query_str:
            return None
        norm = _normalize_key(query_str.strip())

        # 1. Exact normalized match (e.g. 'ERZURUM' -> 'Erzurum', 'ISTANBUL' -> 'İstanbul')
        if norm in self.lookup_map:
            return self.lookup_map[norm]

        # 2. Token / Substring match (handles composite terms like 'Erzurum Şehri')
        for key, canonical in self.lookup_map.items():
            if key in norm or norm in key:
                return canonical

        return None

    def is_point_in_province(self, lat: float, lon: float, province_query: str) -> bool:
        """
        Sub-millisecond Point-in-Polygon test.
        First rejects points outside the province bounding box in O(1),
        then evaluates ray-casting against the exact polygon.
        """
        canonical = self.resolve_province_name(province_query)
        if not canonical or canonical not in self.provinces:
            return False

        prov = self.provinces[canonical]
        bbox = prov["bbox"]

        # Fast Bounding Box Pre-filter
        if lon < bbox["min_lon"] or lon > bbox["max_lon"] or lat < bbox["min_lat"] or lat > bbox["max_lat"]:
            return False

        # Exact Polygon check
        geom_type = prov["geometry_type"]
        coords = prov["coordinates"]

        if geom_type == "Polygon":
            if not _point_in_ring(lon, lat, coords[0]):
                return False
            for hole in coords[1:]:
                if _point_in_ring(lon, lat, hole):
                    return False
            return True

        elif geom_type == "MultiPolygon":
            for poly in coords:
                if _point_in_ring(lon, lat, poly[0]):
                    in_hole = False
                    for hole in poly[1:]:
                        if _point_in_ring(lon, lat, hole):
                            in_hole = True
                            break
                    if not in_hole:
                        return True
            return False

        return False

    def find_province_for_point(self, lat: float, lon: float) -> Optional[str]:
        """Identifies which Turkish province contains the given coordinate (lat, lon)."""
        if not (35.8 <= lat <= 42.2 and 25.6 <= lon <= 44.8):
            return None

        for canonical, prov in self.provinces.items():
            bbox = prov["bbox"]
            if bbox["min_lon"] <= lon <= bbox["max_lon"] and bbox["min_lat"] <= lat <= bbox["max_lat"]:
                if self.is_point_in_province(lat, lon, canonical):
                    return canonical
        return None

    def get_province_info(self, name_or_query: str) -> Optional[Dict[str, Any]]:
        """Returns clean, human-readable province metadata."""
        canonical = self.resolve_province_name(name_or_query)
        if not canonical or canonical not in self.provinces:
            return None
        p = self.provinces[canonical]
        return {
            "name": p["name"],
            "plate": p["plate"],
            "plate_code": p["plate_code"],
            "region": p["region"],
            "center": {
                "lat": p["center"]["latitude"],
                "lon": p["center"]["longitude"]
            },
            "bounds": {
                "min_lat": p["bbox"]["min_lat"],
                "max_lat": p["bbox"]["max_lat"],
                "min_lon": p["bbox"]["min_lon"],
                "max_lon": p["bbox"]["max_lon"]
            },
            "summary": p["readable_summary"]
        }

    def get_province_polygon(self, province_name: str) -> Optional[Dict[str, Any]]:
        """Returns the GeoJSON geometry (Polygon / MultiPolygon with coordinates) for a given province."""
        canonical = self.resolve_province_name(province_name)
        if not canonical or canonical not in self.provinces:
            return None
        p = self.provinces[canonical]
        return {
            "name": p["name"],
            "plate": p["plate_code"],
            "geometry_type": p["geometry_type"],
            "coordinates": p["coordinates"],
            "center": p["center"],
            "bbox": p["bbox"]
        }

    def list_provinces(self, region_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Returns a clean list of all 81 provinces formatted with plate, name, and region."""
        results = []
        norm_filter = _normalize_key(region_filter) if region_filter else None

        for plate in range(1, 82):
            if plate not in self.provinces_by_plate:
                continue
            p = self.provinces_by_plate[plate]
            if norm_filter and norm_filter not in _normalize_key(p["region"]):
                continue
            results.append({
                "plate": p["plate_code"],
                "name": p["name"],
                "region": p["region"],
                "center": p["center"]
            })
        return results

    def resolve_macro_region(self, region_str: Optional[str]) -> Optional[Dict[str, Any]]:
        """Resolves region string to macro-region bounding box (e.g. 'Marmara', 'Ege', 'TR')."""
        if not region_str:
            return None
        norm = _normalize_key(region_str.strip()).replace(" ", "_")
        if norm in MACRO_REGIONS:
            return MACRO_REGIONS[norm]
        if "TURK" in norm or norm == "TR":
            return MACRO_REGIONS["TR"]
        return None

    def resolve_geo_filter(self, query_str: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        Unified geographical filter resolver:
        1. Checks for 81 Turkish Provinces (Exact GeoJSON polygon & bounding box)
        2. Falls back to Macro Geographic Regions (Marmara, Ege, Karadeniz, TR)
        """
        if not query_str:
            return None
        # Check province first
        matched_prov = self.resolve_province_name(query_str)
        if matched_prov:
            prov_info = self.get_province_info(matched_prov)
            prov_poly = self.get_province_polygon(matched_prov)
            return {
                "type": "polygon",
                "province": matched_prov,
                "name": matched_prov,
                "center": prov_info.get("center") if prov_info else None,
                "bbox": prov_info.get("bounds") if prov_info else None,
                "polygon": prov_poly,
                "summary": prov_info.get("summary") if prov_info else ""
            }
        # Fall back to macro region
        macro = self.resolve_macro_region(query_str)
        if macro:
            return macro
        return None


# Global singleton instance
geo_engine = TurkeyGeoEngine()
