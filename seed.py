import sys
from database import init_db, get_db_connection

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

DUMMY_PEOPLE = [
    {"name": "Ali Yılmaz", "birth_date": "1990-05-14", "profession": "Yazılım Mühendisi", "birth_place": "Ankara"},
    {"name": "Ayşe Kaya", "birth_date": "1988-11-23", "profession": "Doktor", "birth_place": "İstanbul"},
    {"name": "Mehmet Demir", "birth_date": "1995-02-10", "profession": "Mimar", "birth_place": "İzmir"},
    {"name": "Fatma Çelik", "birth_date": "1992-09-18", "profession": "Öğretmen", "birth_place": "Bursa"},
    {"name": "Ahmet Öztürk", "birth_date": "1985-07-04", "profession": "Avukat", "birth_place": "Ankara"},
    {"name": "Zeynep Aydın", "birth_date": "1998-12-30", "profession": "Veri Analisti", "birth_place": "Antalya"},
    {"name": "Mustafa Koç", "birth_date": "1991-03-15", "profession": "Yazılım Mühendisi", "birth_place": "Eskişehir"},
    {"name": "Elif Şahin", "birth_date": "1996-08-22", "profession": "Grafik Tasarımcı", "birth_place": "İstanbul"},
    {"name": "Can Arslan", "birth_date": "1989-06-11", "profession": "İnşaat Mühendisi", "birth_place": "Trabzon"},
    {"name": "Deniz Güneş", "birth_date": "2000-01-05", "profession": "Ürün Yöneticisi", "birth_place": "İzmir"},
    {"name": "Burak Doğan", "birth_date": "1993-10-19", "profession": "Finans Uzmanı", "birth_place": "Ankara"},
    {"name": "Gamze Aslan", "birth_date": "1987-04-27", "profession": "Doktor", "birth_place": "Adana"},
    {"name": "Emre Polat", "birth_date": "1994-12-08", "profession": "Siber Güvenlik Uzmanı", "birth_place": "İstanbul"},
    {"name": "Seda Kurt", "birth_date": "1997-09-03", "profession": "Psikolog", "birth_place": "Bursa"},
    {"name": "Tolga Yıldız", "birth_date": "1986-05-19", "profession": "Elektrik Mühendisi", "birth_place": "Kayseri"},
    {"name": "Hande Yavuz", "birth_date": "1999-07-14", "profession": "İç Mimar", "birth_place": "Muğla"},
    {"name": "Kemal Aksoy", "birth_date": "1984-02-28", "profession": "Öğretmen", "birth_place": "Konya"},
    {"name": "Yasemin Korkmaz", "birth_date": "1992-11-09", "profession": "Eczacı", "birth_place": "Antalya"},
    {"name": "Ozan Çetin", "birth_date": "1995-06-25", "profession": "Yazılım Mühendisi", "birth_place": "İstanbul"},
    {"name": "Selin Bulut", "birth_date": "1990-08-16", "profession": "Pazarlama Müdürü", "birth_place": "İzmir"},
    {"name": "Murat Erdem", "birth_date": "1983-03-21", "profession": "Avukat", "birth_place": "Ankara"},
    {"name": "Ebru Taş", "birth_date": "1996-01-12", "profession": "Hemşire", "birth_place": "Samsun"},
    {"name": "Arda Tekin", "birth_date": "2001-04-02", "profession": "Veri Analisti", "birth_place": "Gaziantep"},
    {"name": "Leyla Avcı", "birth_date": "1993-09-29", "profession": "Biyolog", "birth_place": "Denizli"},
    {"name": "Berk Özkan", "birth_date": "1998-10-15", "profession": "Oyun Geliştirici", "birth_place": "İstanbul"}
]


def seed_database():
    """Seeds the SQLite database with dummy people records."""
    init_db()
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM people")  # Clean start
        for person in DUMMY_PEOPLE:
            cursor.execute("""
                INSERT INTO people (name, birth_date, profession, birth_place)
                VALUES (?, ?, ?, ?)
            """, (person["name"], person["birth_date"], person["profession"], person["birth_place"]))
        conn.commit()
    print(f"Successfully seeded {len(DUMMY_PEOPLE)} people into SQLite database.")


if __name__ == "__main__":
    seed_database()
