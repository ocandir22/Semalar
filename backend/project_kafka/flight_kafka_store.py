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

GLOBAL_AIRPORTS: Dict[str, Dict[str, Any]] = {
    # Europe
    "FRA": {"iata": "FRA", "icao": "EDDF", "name": "Frankfurt Havalimanı", "city": "Frankfurt", "lat": 50.0379, "lon": 8.5622},
    "MUC": {"iata": "MUC", "icao": "EDDM", "name": "Münih Havalimanı", "city": "Münih", "lat": 48.3537, "lon": 11.7860},
    "BER": {"iata": "BER", "icao": "EDDB", "name": "Berlin Brandenburg", "city": "Berlin", "lat": 52.3667, "lon": 13.5033},
    "HAM": {"iata": "HAM", "icao": "EDDH", "name": "Hamburg Havalimanı", "city": "Hamburg", "lat": 53.6304, "lon": 9.9882},
    "DUS": {"iata": "DUS", "icao": "EDDL", "name": "Düsseldorf Havalimanı", "city": "Düsseldorf", "lat": 51.2895, "lon": 6.7668},
    "CGN": {"iata": "CGN", "icao": "EDDK", "name": "Köln Bonn Havalimanı", "city": "Köln", "lat": 50.8659, "lon": 7.1427},
    "STR": {"iata": "STR", "icao": "EDDS", "name": "Stuttgart Havalimanı", "city": "Stuttgart", "lat": 48.6899, "lon": 9.2220},
    "LHR": {"iata": "LHR", "icao": "EGLL", "name": "Londra Heathrow", "city": "Londra", "lat": 51.4700, "lon": -0.4543},
    "LGW": {"iata": "LGW", "icao": "EGKK", "name": "Londra Gatwick", "city": "Londra", "lat": 51.1537, "lon": -0.1821},
    "STN": {"iata": "STN", "icao": "EGSS", "name": "Londra Stansted", "city": "Londra", "lat": 51.8860, "lon": 0.2389},
    "MAN": {"iata": "MAN", "icao": "EGCC", "name": "Manchester Havalimanı", "city": "Manchester", "lat": 53.3537, "lon": -2.2750},
    "EDI": {"iata": "EDI", "icao": "EGPH", "name": "Edinburgh Havalimanı", "city": "Edinburgh", "lat": 55.9500, "lon": -3.3725},
    "CDG": {"iata": "CDG", "icao": "LFPG", "name": "Paris Charles de Gaulle", "city": "Paris", "lat": 49.0097, "lon": 2.5479},
    "ORY": {"iata": "ORY", "icao": "LFPO", "name": "Paris Orly", "city": "Paris", "lat": 48.7262, "lon": 2.3652},
    "NCE": {"iata": "NCE", "icao": "LFMN", "name": "Nice Côte d'Azur", "city": "Nice", "lat": 43.6584, "lon": 7.2159},
    "LYS": {"iata": "LYS", "icao": "LFLL", "name": "Lyon-Saint Exupéry", "city": "Lyon", "lat": 45.7256, "lon": 5.0811},
    "AMS": {"iata": "AMS", "icao": "EHAM", "name": "Amsterdam Schiphol", "city": "Amsterdam", "lat": 52.3105, "lon": 4.7683},
    "BRU": {"iata": "BRU", "icao": "EBBR", "name": "Brüksel Havalimanı", "city": "Brüksel", "lat": 50.9014, "lon": 4.4844},
    "VIE": {"iata": "VIE", "icao": "LOWW", "name": "Viyana Uluslararası Havalimanı", "city": "Viyana", "lat": 48.1103, "lon": 16.5697},
    "ZRH": {"iata": "ZRH", "icao": "LSZH", "name": "Zürih Havalimanı", "city": "Zürih", "lat": 47.4582, "lon": 8.5555},
    "GVA": {"iata": "GVA", "icao": "LSGG", "name": "Cenevre Havalimanı", "city": "Cenevre", "lat": 46.2370, "lon": 6.1092},
    "BSL": {"iata": "BSL", "icao": "LFSB", "name": "EuroAirport Basel", "city": "Basel", "lat": 47.5896, "lon": 7.5299},
    "FCO": {"iata": "FCO", "icao": "LIRF", "name": "Roma Fiumicino", "city": "Roma", "lat": 41.8003, "lon": 12.2389},
    "MXP": {"iata": "MXP", "icao": "LIMC", "name": "Milano Malpensa", "city": "Milano", "lat": 45.6301, "lon": 8.7255},
    "LIN": {"iata": "LIN", "icao": "LIML", "name": "Milano Linate", "city": "Milano", "lat": 45.4451, "lon": 9.2767},
    "BGY": {"iata": "BGY", "icao": "LIME", "name": "Milano Bergamo", "city": "Bergamo", "lat": 45.6739, "lon": 9.7042},
    "VCE": {"iata": "VCE", "icao": "LIPZ", "name": "Venedik Marco Polo", "city": "Venedik", "lat": 45.5053, "lon": 12.3519},
    "NAP": {"iata": "NAP", "icao": "LIRN", "name": "Napoli Havalimanı", "city": "Napoli", "lat": 40.8860, "lon": 14.2908},
    "BLQ": {"iata": "BLQ", "icao": "LIPE", "name": "Bologna Guglielmo Marconi", "city": "Bologna", "lat": 44.5354, "lon": 11.2887},
    "MAD": {"iata": "MAD", "icao": "LEMD", "name": "Madrid-Barajas", "city": "Madrid", "lat": 40.4839, "lon": -3.5680},
    "BCN": {"iata": "BCN", "icao": "LEBL", "name": "Barselona El Prat", "city": "Barselona", "lat": 41.2974, "lon": 2.0833},
    "AGP": {"iata": "AGP", "icao": "LEMG", "name": "Málaga-Costa del Sol", "city": "Malaga", "lat": 36.6749, "lon": -4.4991},
    "VLC": {"iata": "VLC", "icao": "LEVC", "name": "Valensiya Havalimanı", "city": "Valensiya", "lat": 39.4893, "lon": -0.4816},
    "PMI": {"iata": "PMI", "icao": "LEPA", "name": "Palma de Mallorca", "city": "Mallorca", "lat": 39.5517, "lon": 2.7388},
    "LIS": {"iata": "LIS", "icao": "LPPT", "name": "Lizbon Humberto Delgado", "city": "Lizbon", "lat": 38.7756, "lon": -9.1354},
    "OPO": {"iata": "OPO", "icao": "LPPR", "name": "Porto Francisco Sá Carneiro", "city": "Porto", "lat": 41.2421, "lon": -8.6781},
    "ATH": {"iata": "ATH", "icao": "LGAV", "name": "Atina Eleftherios Venizelos", "city": "Atina", "lat": 37.9364, "lon": 23.9475},
    "SKG": {"iata": "SKG", "icao": "LGTS", "name": "Selanik Makedonya", "city": "Selanik", "lat": 40.5197, "lon": 22.9709},
    "HER": {"iata": "HER", "icao": "LGIR", "name": "Girit Kandiye Nikos Kazantzakis", "city": "Girit", "lat": 35.3397, "lon": 25.1803},
    "RHO": {"iata": "RHO", "icao": "LGRP", "name": "Rodos Diagoras", "city": "Rodos", "lat": 36.4054, "lon": 28.0862},
    "LCA": {"iata": "LCA", "icao": "LCLK", "name": "Larnaka Havalimanı", "city": "Larnaka", "lat": 34.8751, "lon": 33.6249},
    "ECN": {"iata": "ECN", "icao": "LCEN", "name": "Ercan Uluslararası Havalimanı", "city": "Lefkoşa", "lat": 35.1597, "lon": 33.5039},
    "WAW": {"iata": "WAW", "icao": "EPWA", "name": "Varşova Chopin", "city": "Varşova", "lat": 52.1672, "lon": 20.9679},
    "KRK": {"iata": "KRK", "icao": "EPKK", "name": "Krakov John Paul II", "city": "Krakov", "lat": 50.0777, "lon": 19.7848},
    "PRG": {"iata": "PRG", "icao": "LKPR", "name": "Prag Václav Havel", "city": "Prag", "lat": 50.1008, "lon": 14.2600},
    "BUD": {"iata": "BUD", "icao": "LHBP", "name": "Budapeşte Ferenc Liszt", "city": "Budapeşte", "lat": 47.4369, "lon": 19.2556},
    "OTP": {"iata": "OTP", "icao": "LROP", "name": "Bükreş Henri Coandă", "city": "Bükreş", "lat": 44.5711, "lon": 26.0858},
    "SOF": {"iata": "SOF", "icao": "LBSF", "name": "Sofya Havalimanı", "city": "Sofya", "lat": 42.6951, "lon": 23.4061},
    "VAR": {"iata": "VAR", "icao": "LBWN", "name": "Varna Havalimanı", "city": "Varna", "lat": 43.2321, "lon": 27.8251},
    "BOJ": {"iata": "BOJ", "icao": "LBBG", "name": "Burgaz Havalimanı", "city": "Burgaz", "lat": 42.5696, "lon": 27.5152},
    "BEG": {"iata": "BEG", "icao": "LYBE", "name": "Belgrad Nikola Tesla", "city": "Belgrad", "lat": 44.8184, "lon": 20.3091},
    "ZAG": {"iata": "ZAG", "icao": "LDZA", "name": "Zagreb Franjo Tuđman", "city": "Zagreb", "lat": 45.7431, "lon": 16.0688},
    "SJJ": {"iata": "SJJ", "icao": "LQSA", "name": "Saraybosna Havalimanı", "city": "Saraybosna", "lat": 43.8247, "lon": 18.3314},
    "SKP": {"iata": "SKP", "icao": "LWSK", "name": "Üsküp Uluslararası Havalimanı", "city": "Üsküp", "lat": 41.9616, "lon": 21.6214},
    "TIA": {"iata": "TIA", "icao": "LATI", "name": "Tiran Rahibe Teresa", "city": "Tiran", "lat": 41.4147, "lon": 19.7206},
    "PRN": {"iata": "PRN", "icao": "BKPR", "name": "Priştine Adem Jashari", "city": "Priştine", "lat": 42.5728, "lon": 20.9278},
    "OSL": {"iata": "OSL", "icao": "ENGM", "name": "Oslo Gardermoen", "city": "Oslo", "lat": 60.1975, "lon": 11.1004},
    "ARN": {"iata": "ARN", "icao": "ESSA", "name": "Stockholm Arlanda", "city": "Stockholm", "lat": 59.6519, "lon": 17.9186},
    "CPH": {"iata": "CPH", "icao": "EKCH", "name": "Kopenhag Kastrup", "city": "Kopenhag", "lat": 55.6180, "lon": 12.6508},
    "HEL": {"iata": "HEL", "icao": "EFHK", "name": "Helsinki-Vantaa", "city": "Helsinki", "lat": 60.3172, "lon": 24.9633},
    "DUB": {"iata": "DUB", "icao": "EIDW", "name": "Dublin Havalimanı", "city": "Dublin", "lat": 53.4213, "lon": -6.2701},

    # Middle East & Gulf
    "DXB": {"iata": "DXB", "icao": "OMDB", "name": "Dubai Uluslararası", "city": "Dubai", "lat": 25.2532, "lon": 55.3657},
    "DWC": {"iata": "DWC", "icao": "OMDW", "name": "Dubai Al Maktoum", "city": "Dubai", "lat": 24.8960, "lon": 55.1749},
    "AUH": {"iata": "AUH", "icao": "OMAA", "name": "Abu Dabi Zayed", "city": "Abu Dabi", "lat": 24.4330, "lon": 54.6511},
    "SHJ": {"iata": "SHJ", "icao": "OMSJ", "name": "Şarika Havalimanı", "city": "Şarika", "lat": 25.3286, "lon": 55.5172},
    "DOH": {"iata": "DOH", "icao": "OTHH", "name": "Doha Hamad", "city": "Doha", "lat": 25.2731, "lon": 51.6081},
    "RUH": {"iata": "RUH", "icao": "OERK", "name": "Riyad Kral Halid", "city": "Riyad", "lat": 24.9576, "lon": 46.6988},
    "JED": {"iata": "JED", "icao": "OEJN", "name": "Cidde Kral Abdülaziz", "city": "Cidde", "lat": 21.6796, "lon": 39.1565},
    "MED": {"iata": "MED", "icao": "OEMA", "name": "Medine Prens Muhammed", "city": "Medine", "lat": 24.5534, "lon": 39.7051},
    "DMM": {"iata": "DMM", "icao": "OEDF", "name": "Dammam Kral Fahd", "city": "Dammam", "lat": 26.4712, "lon": 49.7978},
    "KWI": {"iata": "KWI", "icao": "OKBK", "name": "Kuveyt Uluslararası", "city": "Kuveyt", "lat": 29.2267, "lon": 47.9689},
    "BAH": {"iata": "BAH", "icao": "OBBI", "name": "Bahreyn Uluslararası", "city": "Bahreyn", "lat": 26.2708, "lon": 50.6336},
    "MCT": {"iata": "MCT", "icao": "OOMS", "name": "Maskat Uluslararası", "city": "Maskat", "lat": 23.5933, "lon": 58.2844},
    "AMM": {"iata": "AMM", "icao": "OJAI", "name": "Amman Kraliçe Aliye", "city": "Amman", "lat": 31.7226, "lon": 35.9932},
    "BEY": {"iata": "BEY", "icao": "OLBA", "name": "Beyrut Refik Hariri", "city": "Beyrut", "lat": 33.8209, "lon": 35.4884},
    "TLV": {"iata": "TLV", "icao": "LLBG", "name": "Tel Aviv Ben Gurion", "city": "Tel Aviv", "lat": 32.0055, "lon": 34.8854},
    "BGW": {"iata": "BGW", "icao": "ORBI", "name": "Bağdat Uluslararası", "city": "Bağdat", "lat": 33.2625, "lon": 44.2344},
    "EBL": {"iata": "EBL", "icao": "ORER", "name": "Erbil Uluslararası", "city": "Erbil", "lat": 36.2372, "lon": 43.9631},
    "BSR": {"iata": "BSR", "icao": "ORMM", "name": "Basra Uluslararası", "city": "Basra", "lat": 30.5492, "lon": 47.6622},
    "IKA": {"iata": "IKA", "icao": "OIIE", "name": "Tahran İmam Humeyni", "city": "Tahran", "lat": 35.4161, "lon": 51.1522},
    "THR": {"iata": "THR", "icao": "OIII", "name": "Tahran Mehrabad", "city": "Tahran", "lat": 35.6892, "lon": 51.3134},
    "TBZ": {"iata": "TBZ", "icao": "OITT", "name": "Tebriz Şehit Medeni", "city": "Tebriz", "lat": 38.1331, "lon": 46.2350},
    "SYZ": {"iata": "SYZ", "icao": "OISS", "name": "Şiraz Şehit Destğayb", "city": "Şiraz", "lat": 29.5392, "lon": 52.5898},
    "MHD": {"iata": "MHD", "icao": "OIMM", "name": "Meşhed Şehit Haşiminejad", "city": "Meşhed", "lat": 36.2352, "lon": 59.6409},

    # Eurasia, Caucasus & Central Asia
    "GYD": {"iata": "GYD", "icao": "UBBB", "name": "Bakü Heydar Aliyev", "city": "Bakü", "lat": 40.4675, "lon": 50.0467},
    "TBS": {"iata": "TBS", "icao": "UGTB", "name": "Tiflis Şota Rustaveli", "city": "Tiflis", "lat": 41.6692, "lon": 44.9547},
    "BUS": {"iata": "BUS", "icao": "UGSB", "name": "Batum Aleksandre Kartveli", "city": "Batum", "lat": 41.6103, "lon": 41.5997},
    "EVN": {"iata": "EVN", "icao": "UDYZ", "name": "Erivan Zvartnots", "city": "Erivan", "lat": 40.1473, "lon": 44.3959},
    "TAS": {"iata": "TAS", "icao": "UTTT", "name": "Taşkent İslam Kerimov", "city": "Taşkent", "lat": 41.2579, "lon": 69.2812},
    "SKD": {"iata": "SKD", "icao": "UTSS", "name": "Semerkant Havalimanı", "city": "Semerkant", "lat": 39.7005, "lon": 66.9842},
    "ALA": {"iata": "ALA", "icao": "UAAA", "name": "Almatı Uluslararası", "city": "Almatı", "lat": 43.3521, "lon": 77.0405},
    "NQZ": {"iata": "NQZ", "icao": "UACC", "name": "Astana Nursultan Nazarbayev", "city": "Astana", "lat": 51.0222, "lon": 71.4669},
    "FRU": {"iata": "FRU", "icao": "UCFM", "name": "Bişkek Manas", "city": "Bişkek", "lat": 43.0613, "lon": 74.4776},
    "DYU": {"iata": "DYU", "icao": "UTDD", "name": "Duşanbe Havalimanı", "city": "Duşanbe", "lat": 38.5433, "lon": 68.8250},
    "ASB": {"iata": "ASB", "icao": "UTAA", "name": "Aşkabat Oğuz Han", "city": "Aşkabat", "lat": 37.9868, "lon": 58.3610},
    "SCO": {"iata": "SCO", "icao": "UATE", "name": "Aktau Havalimanı", "city": "Aktau", "lat": 43.8602, "lon": 51.0920},
    "GUW": {"iata": "GUW", "icao": "UATG", "name": "Atırau Havalimanı", "city": "Atırau", "lat": 47.1219, "lon": 51.8214},
    "SVO": {"iata": "SVO", "icao": "UUEE", "name": "Moskova Şeremetyevo", "city": "Moskova", "lat": 55.9726, "lon": 37.4146},
    "DME": {"iata": "DME", "icao": "UUDD", "name": "Moskova Domodedovo", "city": "Moskova", "lat": 55.4088, "lon": 37.9063},
    "VKO": {"iata": "VKO", "icao": "UUWW", "name": "Moskova Vnukovo", "city": "Moskova", "lat": 55.5915, "lon": 37.2615},
    "LED": {"iata": "LED", "icao": "ULLI", "name": "Sankt-Peterburg Pulkovo", "city": "St. Petersburg", "lat": 59.8003, "lon": 30.2625},
    "AER": {"iata": "AER", "icao": "URSS", "name": "Soçi Uluslararası", "city": "Soçi", "lat": 43.4499, "lon": 39.9566},
    "MRV": {"iata": "MRV", "icao": "URMM", "name": "Mineralnye Vody", "city": "Mineralnye Vody", "lat": 44.2251, "lon": 43.0819},
    "KZN": {"iata": "KZN", "icao": "UWKD", "name": "Kazan Gabdulla Tuqay", "city": "Kazan", "lat": 55.6062, "lon": 49.2787},

    # Asia & Indian Subcontinent
    "DEL": {"iata": "DEL", "icao": "VIDP", "name": "Yeni Delhi Indira Gandhi", "city": "Delhi", "lat": 28.5562, "lon": 77.1000},
    "BOM": {"iata": "BOM", "icao": "VABB", "name": "Mumbai Chhatrapati Shivaji", "city": "Mumbai", "lat": 19.0896, "lon": 72.8656},
    "BLR": {"iata": "BLR", "icao": "VOBL", "name": "Bengaluru Kempegowda", "city": "Bangalore", "lat": 13.1986, "lon": 77.7066},
    "HYD": {"iata": "HYD", "icao": "VOHS", "name": "Haydarabad Rajiv Gandhi", "city": "Haydarabad", "lat": 17.2403, "lon": 78.4294},
    "MAA": {"iata": "MAA", "icao": "VOMM", "name": "Chennai Havalimanı", "city": "Chennai", "lat": 12.9941, "lon": 80.1709},
    "KHI": {"iata": "KHI", "icao": "OPKC", "name": "Karaçi Cinnah", "city": "Karaçi", "lat": 24.9065, "lon": 67.1608},
    "LHE": {"iata": "LHE", "icao": "OPLA", "name": "Lahor Allama Iqbal", "city": "Lahor", "lat": 31.5216, "lon": 74.4036},
    "ISB": {"iata": "ISB", "icao": "OPIS", "name": "İslamabad Uluslararası", "city": "İslamabad", "lat": 33.5494, "lon": 72.8258},
    "DAC": {"iata": "DAC", "icao": "VGHS", "name": "Dakka Şahcelal", "city": "Dakka", "lat": 23.8433, "lon": 90.3978},
    "CMB": {"iata": "CMB", "icao": "VCBI", "name": "Kolombo Bandaranaike", "city": "Kolombo", "lat": 7.1808, "lon": 79.8841},
    "MLE": {"iata": "MLE", "icao": "VRMM", "name": "Malé Velana", "city": "Malé", "lat": 4.1918, "lon": 73.5291},
    "BKK": {"iata": "BKK", "icao": "VTBS", "name": "Bangkok Suvarnabhumi", "city": "Bangkok", "lat": 13.6900, "lon": 100.7501},
    "HKT": {"iata": "HKT", "icao": "VTSP", "name": "Phuket Uluslararası", "city": "Phuket", "lat": 8.1132, "lon": 98.3169},
    "KUL": {"iata": "KUL", "icao": "WMKK", "name": "Kuala Lumpur Uluslararası", "city": "Kuala Lumpur", "lat": 2.7456, "lon": 101.7099},
    "SIN": {"iata": "SIN", "icao": "WSSS", "name": "Singapur Changi", "city": "Singapur", "lat": 1.3644, "lon": 103.9915},
    "CGK": {"iata": "CGK", "icao": "WIII", "name": "Cakarta Soekarno-Hatta", "city": "Cakarta", "lat": -6.1256, "lon": 106.6559},
    "DPS": {"iata": "DPS", "icao": "WADD", "name": "Bali Ngurah Rai", "city": "Bali", "lat": -8.7482, "lon": 115.1672},
    "MNL": {"iata": "MNL", "icao": "RPLL", "name": "Manila Ninoy Aquino", "city": "Manila", "lat": 14.5086, "lon": 121.0194},
    "SGN": {"iata": "SGN", "icao": "VVTS", "name": "Ho Chi Minh Tan Son Nhat", "city": "Ho Chi Minh", "lat": 10.8188, "lon": 106.6519},
    "HAN": {"iata": "HAN", "icao": "VVNB", "name": "Hanoi Noi Bai", "city": "Hanoi", "lat": 21.2212, "lon": 105.8072},
    "HKG": {"iata": "HKG", "icao": "VHHH", "name": "Hong Kong Uluslararası", "city": "Hong Kong", "lat": 22.3080, "lon": 113.9185},
    "TPE": {"iata": "TPE", "icao": "RCTP", "name": "Taipei Taoyuan", "city": "Taipei", "lat": 25.0797, "lon": 121.2342},
    "PEK": {"iata": "PEK", "icao": "ZBAA", "name": "Pekin Başkent", "city": "Pekin", "lat": 40.0799, "lon": 116.6031},
    "PKX": {"iata": "PKX", "icao": "ZBAD", "name": "Pekin Daxing", "city": "Pekin", "lat": 39.5098, "lon": 116.4105},
    "PVG": {"iata": "PVG", "icao": "ZSPD", "name": "Şanghay Pudong", "city": "Şanghay", "lat": 31.1443, "lon": 121.8083},
    "SHA": {"iata": "SHA", "icao": "ZSSS", "name": "Şanghay Hongqiao", "city": "Şanghay", "lat": 31.1979, "lon": 121.3363},
    "CAN": {"iata": "CAN", "icao": "ZGGG", "name": "Guangzhou Baiyun", "city": "Guangzhou", "lat": 23.3924, "lon": 113.2988},
    "SZX": {"iata": "SZX", "icao": "ZGSZ", "name": "Shenzhen Bao'an", "city": "Shenzhen", "lat": 22.6393, "lon": 113.8107},
    "ICN": {"iata": "ICN", "icao": "RKSI", "name": "Seul Incheon", "city": "Seul", "lat": 37.4602, "lon": 126.4407},
    "NRT": {"iata": "NRT", "icao": "RJAA", "name": "Tokyo Narita", "city": "Tokyo", "lat": 35.7720, "lon": 140.3929},
    "HND": {"iata": "HND", "icao": "RJTT", "name": "Tokyo Haneda", "city": "Tokyo", "lat": 35.5494, "lon": 139.7798},
    "KIX": {"iata": "KIX", "icao": "RJBB", "name": "Osaka Kansai", "city": "Osaka", "lat": 34.4320, "lon": 135.2304},
    "SYD": {"iata": "SYD", "icao": "YSSY", "name": "Sidney Kingsford Smith", "city": "Sidney", "lat": -33.9399, "lon": 151.1753},
    "MEL": {"iata": "MEL", "icao": "YMML", "name": "Melbourne Tullamarine", "city": "Melbourne", "lat": -37.6690, "lon": 144.8410},

    # Americas
    "JFK": {"iata": "JFK", "icao": "KJFK", "name": "New York John F. Kennedy", "city": "New York", "lat": 40.6413, "lon": -73.7781},
    "EWR": {"iata": "EWR", "icao": "KEWR", "name": "Newark Liberty", "city": "New York", "lat": 40.6895, "lon": -74.1745},
    "ORD": {"iata": "ORD", "icao": "KORD", "name": "Chicago O'Hare", "city": "Chicago", "lat": 41.9742, "lon": -87.9073},
    "LAX": {"iata": "LAX", "icao": "KLAX", "name": "Los Angeles Uluslararası", "city": "Los Angeles", "lat": 33.9416, "lon": -118.4085},
    "SFO": {"iata": "SFO", "icao": "KSFO", "name": "San Francisco Uluslararası", "city": "San Francisco", "lat": 37.6213, "lon": -122.3790},
    "MIA": {"iata": "MIA", "icao": "KMIA", "name": "Miami Uluslararası", "city": "Miami", "lat": 25.7959, "lon": -80.2870},
    "ATL": {"iata": "ATL", "icao": "KATL", "name": "Atlanta Hartsfield-Jackson", "city": "Atlanta", "lat": 33.6407, "lon": -84.4277},
    "DFW": {"iata": "DFW", "icao": "KDFW", "name": "Dallas/Fort Worth", "city": "Dallas", "lat": 32.8998, "lon": -97.0403},
    "IAD": {"iata": "IAD", "icao": "KIAD", "name": "Washington Dulles", "city": "Washington", "lat": 38.9531, "lon": -77.4565},
    "BOS": {"iata": "BOS", "icao": "KBOS", "name": "Boston Logan", "city": "Boston", "lat": 42.3656, "lon": -71.0096},
    "IAH": {"iata": "IAH", "icao": "KIAH", "name": "Houston George Bush", "city": "Houston", "lat": 29.9902, "lon": -95.3368},
    "SEA": {"iata": "SEA", "icao": "KSEA", "name": "Seattle-Tacoma", "city": "Seattle", "lat": 47.4502, "lon": -122.3088},
    "DEN": {"iata": "DEN", "icao": "KDEN", "name": "Denver Uluslararası", "city": "Denver", "lat": 39.8561, "lon": -104.6737},
    "YYZ": {"iata": "YYZ", "icao": "CYYZ", "name": "Toronto Pearson", "city": "Toronto", "lat": 43.6777, "lon": -79.6248},
    "YVR": {"iata": "YVR", "icao": "CYVR", "name": "Vancouver Uluslararası", "city": "Vancouver", "lat": 49.1967, "lon": -123.1815},
    "YUL": {"iata": "YUL", "icao": "CYUL", "name": "Montreal Pierre Elliott Trudeau", "city": "Montreal", "lat": 45.4706, "lon": -73.7408},
    "MEX": {"iata": "MEX", "icao": "MMMX", "name": "Mexico City Benito Juárez", "city": "Mexico City", "lat": 19.4361, "lon": -99.0719},
    "CUN": {"iata": "CUN", "icao": "MMUN", "name": "Cancún Uluslararası", "city": "Cancun", "lat": 21.0365, "lon": -86.8771},
    "GRU": {"iata": "GRU", "icao": "SBGR", "name": "São Paulo/Guarulhos", "city": "Sao Paulo", "lat": -23.4356, "lon": -46.4731},
    "GIG": {"iata": "GIG", "icao": "SBGL", "name": "Rio de Janeiro/Galeão", "city": "Rio de Janeiro", "lat": -22.8134, "lon": -43.2494},
    "EZE": {"iata": "EZE", "icao": "SAEZ", "name": "Buenos Aires Ezeiza", "city": "Buenos Aires", "lat": -34.8222, "lon": -58.5358},
    "BOG": {"iata": "BOG", "icao": "SKBO", "name": "Bogotá El Dorado", "city": "Bogota", "lat": 4.7016, "lon": -74.1469},
    "PTY": {"iata": "PTY", "icao": "MPTO", "name": "Panama City Tocumen", "city": "Panama", "lat": 9.0714, "lon": -79.3835},

    # Africa
    "CAI": {"iata": "CAI", "icao": "HECA", "name": "Kahire Uluslararası", "city": "Kahire", "lat": 30.1219, "lon": 31.4056},
    "HRG": {"iata": "HRG", "icao": "HEGN", "name": "Hurgada Uluslararası", "city": "Hurgada", "lat": 27.1783, "lon": 33.7994},
    "SSH": {"iata": "SSH", "icao": "HESH", "name": "Şarm El-Şeyh Uluslararası", "city": "Şarm El-Şeyh", "lat": 27.9772, "lon": 34.3950},
    "CMN": {"iata": "CMN", "icao": "GMMN", "name": "Kazablanka V. Muhammed", "city": "Kazablanka", "lat": 33.3675, "lon": -7.5899},
    "RAK": {"iata": "RAK", "icao": "GMMX", "name": "Marakeş Menara", "city": "Marakeş", "lat": 31.6069, "lon": -8.0363},
    "TUN": {"iata": "TUN", "icao": "DTTA", "name": "Tunus-Kartaca", "city": "Tunus", "lat": 36.8510, "lon": 10.2272},
    "ALG": {"iata": "ALG", "icao": "DAAG", "name": "Cezayir Huvârî Bumedyen", "city": "Cezayir", "lat": 36.6910, "lon": 3.2154},
    "ADD": {"iata": "ADD", "icao": "HAAB", "name": "Addis Ababa Bole", "city": "Addis Ababa", "lat": 8.9779, "lon": 38.7993},
    "NBO": {"iata": "NBO", "icao": "HKJK", "name": "Nairobi Jomo Kenyatta", "city": "Nairobi", "lat": -1.3192, "lon": 36.9278},
    "JNB": {"iata": "JNB", "icao": "FAOR", "name": "Johannesburg O. R. Tambo", "city": "Johannesburg", "lat": -26.1392, "lon": 28.2460},
    "CPT": {"iata": "CPT", "icao": "FACT", "name": "Cape Town Uluslararası", "city": "Cape Town", "lat": -33.9715, "lon": 18.6021}
}

ALL_AIRPORTS: Dict[str, Dict[str, Any]] = {**GLOBAL_AIRPORTS, **TURKISH_AIRPORTS}


def resolve_airport(airport_str: str) -> Optional[Dict[str, Any]]:
    """Resolves airport string (IATA, ICAO, City, or Name) against Turkish & Global airports catalog."""
    if not airport_str:
        return None
    code = airport_str.strip().upper()
    if code in ALL_AIRPORTS:
        return ALL_AIRPORTS[code]
    for ap in ALL_AIRPORTS.values():
        if ap.get("icao") == code or code in ap.get("name", "").upper() or code in ap.get("city", "").upper():
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
        city: Optional[str] = None,
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
        raw_geo = (city or region or kwargs.get("province") or kwargs.get("country") or kwargs.get("city") or "").strip()
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

            # Record match with origin & destination coordinates for map flight paths
            item = dict(f)
            if dist is not None:
                item["distance_to_center_km"] = dist
            if region_name:
                item["filtered_region"] = region_name

            orig_code = str(f.get("route", {}).get("origin_iata") or "").upper().strip()
            dest_code = str(f.get("route", {}).get("destination_iata") or "").upper().strip()
            orig_ap = resolve_airport(orig_code) if orig_code else None
            dest_ap = resolve_airport(dest_code) if dest_code else None
            if orig_ap or dest_ap:
                item["route_endpoints"] = {
                    "origin": {"lat": orig_ap["lat"], "lon": orig_ap["lon"], "iata": orig_ap["iata"], "name": orig_ap["name"]} if orig_ap else None,
                    "destination": {"lat": dest_ap["lat"], "lon": dest_ap["lon"], "iata": dest_ap["iata"], "name": dest_ap["name"]} if dest_ap else None
                }
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
                if geo_filter.get("polygon"):
                    resp["geo_overlay"] = geo_filter["polygon"]
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

    # ============================================================
    # 🛡️ 8. TCAS & AIRSPACE CONFLICT DETECTION TOOL
    # ============================================================
    def detect_airspace_conflicts(
        self,
        min_horizontal_km: float = 10.0,
        min_vertical_feet: float = 1000.0,
        limit: int = 10
    ) -> Dict[str, Any]:
        """Scans active aircraft pairs across the airspace to detect loss of separation,
        potential TCAS traffic advisories (TA/RA), and close-proximity conflicts.
        """
        try: min_horizontal_km = float(min_horizontal_km)
        except (ValueError, TypeError): min_horizontal_km = 10.0
        try: min_vertical_feet = float(min_vertical_feet)
        except (ValueError, TypeError): min_vertical_feet = 1000.0
        try: limit = int(limit)
        except (ValueError, TypeError): limit = 10

        flights_list = list(self.flights.values())
        n = len(flights_list)
        conflicts = []

        for i in range(n):
            f1 = flights_list[i]
            t1 = f1.get("telemetry", {})
            lat1 = t1.get("latitude")
            lon1 = t1.get("longitude")
            alt1 = t1.get("altitude_feet")
            if lat1 is None or lon1 is None or alt1 is None or t1.get("on_ground"):
                continue

            for j in range(i + 1, n):
                f2 = flights_list[j]
                t2 = f2.get("telemetry", {})
                lat2 = t2.get("latitude")
                lon2 = t2.get("longitude")
                alt2 = t2.get("altitude_feet")
                if lat2 is None or lon2 is None or alt2 is None or t2.get("on_ground"):
                    continue

                vert_diff = abs(alt1 - alt2)
                if vert_diff > min_vertical_feet:
                    continue

                horiz_dist = calculate_haversine_distance(lat1, lon1, lat2, lon2)
                if horiz_dist <= min_horizontal_km:
                    if horiz_dist <= 3.0 and vert_diff <= 500:
                        severity = "CRITICAL_TCAS_RA"
                    elif horiz_dist <= 6.0 and vert_diff <= 800:
                        severity = "HIGH_TCAS_TA"
                    else:
                        severity = "PROXIMITY_ADVISORY"

                    f1_code = f1.get("flight_number") or f1.get("callsign") or "Uçuş-1"
                    f2_code = f2.get("flight_number") or f2.get("callsign") or "Uçuş-2"

                    conflicts.append({
                        "severity": severity,
                        "horizontal_distance_km": round(horiz_dist, 2),
                        "vertical_separation_feet": int(vert_diff),
                        "aircraft_1": {
                            "flight_number": f1_code,
                            "model": f1.get("aircraft_model", "Unknown"),
                            "registration": f1.get("registration", "N/A"),
                            "airline": f1.get("airline_iata") or f1.get("airline_icao", "N/A"),
                            "altitude_feet": alt1,
                            "speed_kmh": t1.get("ground_speed_kmh"),
                            "heading": t1.get("heading_degrees"),
                            "latitude": lat1,
                            "longitude": lon1,
                            "route": f1.get("route", {}).get("display", "---")
                        },
                        "aircraft_2": {
                            "flight_number": f2_code,
                            "model": f2.get("aircraft_model", "Unknown"),
                            "registration": f2.get("registration", "N/A"),
                            "airline": f2.get("airline_iata") or f2.get("airline_icao", "N/A"),
                            "altitude_feet": alt2,
                            "speed_kmh": t2.get("ground_speed_kmh"),
                            "heading": t2.get("heading_degrees"),
                            "latitude": lat2,
                            "longitude": lon2,
                            "route": f2.get("route", {}).get("display", "---")
                        }
                    })

        conflicts.sort(key=lambda x: x["horizontal_distance_km"])

        conflict_flights = []
        seen = set()
        for c in conflicts[:limit]:
            for key in ["aircraft_1", "aircraft_2"]:
                ac = c[key]
                f_code = ac["flight_number"]
                if f_code and f_code not in seen:
                    seen.add(f_code)
                    conflict_flights.append({
                        "flight_number": f_code,
                        "callsign": f_code,
                        "aircraft_model": ac["model"],
                        "registration": ac["registration"],
                        "telemetry": {
                            "latitude": ac["latitude"],
                            "longitude": ac["longitude"],
                            "altitude_feet": ac["altitude_feet"],
                            "ground_speed_kmh": ac["speed_kmh"],
                            "heading_degrees": ac["heading"]
                        },
                        "route": {"display": ac["route"]},
                        "tcas_alert": {
                            "severity": c["severity"],
                            "separation_km": c["horizontal_distance_km"],
                            "vert_diff_ft": c["vertical_separation_feet"]
                        }
                    })

        return {
            "status": "success",
            "source": "kafka_in_memory_stream",
            "total_conflicts_detected": len(conflicts),
            "returned_conflicts": conflicts[:limit],
            "flights": conflict_flights,
            "airspace_status": "NORMAL" if not conflicts else f"⚠️ ALERT: {len(conflicts)} close-proximity aircraft pairs detected in active airspace!"
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
