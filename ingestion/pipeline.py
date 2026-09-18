"""Extract and atomically upsert one logical day's unmodified API responses."""

from contextlib import contextmanager
from datetime import date, timedelta
import math
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
API_URL = "https://archive-api.open-meteo.com/v1/archive"
DAILY_UNITS = {
    "temperature_2m_max": "\u00b0C",
    "temperature_2m_min": "\u00b0C",
    "temperature_2m_mean": "\u00b0C",
    "precipitation_sum": "mm",
    "wind_speed_10m_max": "km/h",
}


def parse_date(logical_date):
    if type(logical_date) is date:
        return logical_date
    if not isinstance(logical_date, str):
        raise ValueError("Logical date must be a date or YYYY-MM-DD string")
    parsed = date.fromisoformat(logical_date)
    if parsed.isoformat() != logical_date:
        raise ValueError("Logical date must use YYYY-MM-DD")
    return parsed


def load_cities(path=None):
    with open(path or PROJECT_ROOT / "config" / "cities.yml", encoding="utf-8") as stream:
        cities = yaml.safe_load(stream)["cities"]
    if not cities or len({city["id"] for city in cities}) != len(cities):
        raise ValueError("City configuration must contain unique city IDs")
    for city in cities:
        if not all(city.get(field) for field in ("id", "name", "timezone")):
            raise ValueError("Each city needs an ID, name and timezone")
        if not -90 <= city["latitude"] <= 90 or not -180 <= city["longitude"] <= 180:
            raise ValueError(f"Invalid coordinates for {city['id']}")
    return cities


def http_session():
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def validate_payload(payload, logical_date, timezone):
    expected_date = parse_date(logical_date).isoformat()
    daily = payload.get("daily", {})
    if daily.get("time") != [expected_date]:
        raise ValueError(f"Expected exactly one API day: {expected_date}")
    if payload.get("timezone") != timezone:
        raise ValueError(f"Unexpected API timezone for {expected_date}")
    for field, unit in DAILY_UNITS.items():
        if payload.get("daily_units", {}).get(field) != unit:
            raise ValueError(f"Unexpected unit for {field}")
        values = daily.get(field)
        if not isinstance(values, list) or len(values) != 1:
            raise ValueError(f"Expected exactly one value for {field}")
        value = values[0]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"Missing or invalid {field} for {expected_date}; retry when archive data is available")


def extract_date(logical_date):
    requested_date = parse_date(logical_date).isoformat()
    records = []
    with http_session() as session:
        for city in load_cities():
            response = session.get(
                API_URL,
                params={
                    "latitude": city["latitude"],
                    "longitude": city["longitude"],
                    "start_date": requested_date,
                    "end_date": requested_date,
                    "daily": ",".join(DAILY_UNITS),
                    "timezone": city["timezone"],
                    "temperature_unit": "celsius",
                    "precipitation_unit": "mm",
                    "wind_speed_unit": "kmh",
                },
                timeout=(5, 45),
            )
            response.raise_for_status()
            payload = response.json()
            validate_payload(payload, requested_date, city["timezone"])
            records.append({
                "city_id": city["id"],
                "city_name": city["name"],
                "logical_date": requested_date,
                "payload": payload,
            })
    return records


@contextmanager
def warehouse_connection():
    connection = psycopg2.connect(
        host=os.environ.get("WAREHOUSE_HOST", "localhost"),
        port=os.environ.get("WAREHOUSE_PORT", "5432"),
        dbname=os.environ.get("WAREHOUSE_DB", "warehouse"),
        user=os.environ.get("WAREHOUSE_USER", "de"),
        password=os.environ.get("WAREHOUSE_PASSWORD", "de"),
        connect_timeout=10,
        options="-c statement_timeout=60000 -c lock_timeout=30000",
    )
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def query_rows(statement, parameters=None):
    with warehouse_connection() as connection, connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(statement, parameters)
        return [dict(row) for row in cursor.fetchall()]


def load_date(logical_date, records):
    requested_date = parse_date(logical_date).isoformat()
    cities = {city["id"]: city for city in load_cities()}
    if len(records) != len(cities) or {row["city_id"] for row in records} != set(cities):
        raise ValueError("Load requires exactly one response per configured city")
    values = []
    for row in records:
        city = cities[row["city_id"]]
        if row["logical_date"] != requested_date or row["city_name"] != city["name"]:
            raise ValueError("Record metadata does not match requested date/city")
        validate_payload(row["payload"], requested_date, city["timezone"])
        values.append((row["city_id"], row["city_name"], requested_date, Json(row["payload"])))
    with warehouse_connection() as connection, connection.cursor() as cursor:
        execute_values(cursor, """
            insert into raw.weather_daily (city_id, city_name, logical_date, payload)
            values %s
            on conflict (city_id, logical_date) do update
            set city_name = excluded.city_name,
                payload = excluded.payload,
                ingested_at = now()
            where weather_daily.city_name is distinct from excluded.city_name
               or weather_daily.payload is distinct from excluded.payload
        """, values)
        cursor.execute("select count(*) from raw.weather_daily where logical_date = %s", (requested_date,))
        count = cursor.fetchone()[0]
    return {"logical_date": requested_date, "rows_loaded": len(values), "rows_for_date": count}


def backfill(start_date, end_date):
    current_date = parse_date(start_date)
    final_date = parse_date(end_date)
    if current_date > final_date:
        raise ValueError("Backfill start date must be on or before end date")
    results = []
    while current_date <= final_date:
        results.append(load_date(current_date, extract_date(current_date)))
        current_date += timedelta(days=1)
    return results