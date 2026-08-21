import sqlite3
import os
from datetime import date, datetime
from typing import Optional, List, Dict, Any

DB_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_PATH = os.path.join(DB_DIR, "people.db")


def get_db_connection() -> sqlite3.Connection:
    """Returns a SQLite connection with row factory enabled."""
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initializes SQLite database schema."""
    with get_db_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS people (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL COLLATE NOCASE,
                birth_date TEXT NOT NULL, -- YYYY-MM-DD
                profession TEXT NOT NULL COLLATE NOCASE,
                birth_place TEXT NOT NULL COLLATE NOCASE
            );
        """)
        conn.commit()


def calculate_age(birth_date_str: str) -> int:
    """Calculates age accurately given birth_date in YYYY-MM-DD format."""
    bdate = datetime.strptime(birth_date_str, "%Y-%m-%d").date()
    today = date.today()
    return today.year - bdate.year - ((today.month, today.day) < (bdate.month, bdate.day))


def format_person_row(row: sqlite3.Row) -> Dict[str, Any]:
    """Formats a database row into a dictionary with calculated age."""
    return {
        "id": row["id"],
        "name": row["name"],
        "birth_date": row["birth_date"],
        "age": calculate_age(row["birth_date"]),
        "profession": row["profession"],
        "birth_place": row["birth_place"]
    }


def get_person_by_name(name: str) -> Optional[Dict[str, Any]]:
    """Retrieves a single person by name (case-insensitive)."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM people WHERE name = ? COLLATE NOCASE", (name.strip(),))
        row = cursor.fetchone()
        if row:
            return format_person_row(row)
        return None


def list_all_people() -> List[Dict[str, Any]]:
    """Lists all people in the database."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM people ORDER BY name ASC")
        return [format_person_row(row) for row in cursor.fetchall()]


def search_people_db(profession: Optional[str] = None, birth_place: Optional[str] = None) -> List[Dict[str, Any]]:
    """Searches people filtered by profession and/or birth_place (case-insensitive)."""
    query = "SELECT * FROM people WHERE 1=1"
    params = []

    if profession and profession.strip():
        query += " AND profession LIKE ?"
        params.append(f"%{profession.strip()}%")

    if birth_place and birth_place.strip():
        query += " AND birth_place LIKE ?"
        params.append(f"%{birth_place.strip()}%")

    query += " ORDER BY name ASC"

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return [format_person_row(row) for row in cursor.fetchall()]
