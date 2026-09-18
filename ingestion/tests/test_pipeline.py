from copy import deepcopy
from datetime import date, datetime
import unittest
from unittest.mock import MagicMock, patch

from ingestion.pipeline import (
    DAILY_UNITS, backfill, extract_date, http_session, load_cities, load_date,
    parse_date, validate_payload,
)


def sample_payload(logical_date="2026-09-16"):
    return {
        "timezone": "Asia/Kolkata",
        "daily_units": dict(DAILY_UNITS),
        "daily": {
            "time": [logical_date],
            "temperature_2m_max": [31.5],
            "temperature_2m_min": [23.0],
            "temperature_2m_mean": [27.4],
            "precipitation_sum": [2.5],
            "wind_speed_10m_max": [12.8],
        },
    }


class PipelineTests(unittest.TestCase):
    def test_dates_are_explicit_and_canonical(self):
        self.assertEqual(parse_date("2026-09-16"), date(2026, 9, 16))
        for invalid in ("20260916", "2026-02-30", datetime(2026, 9, 16), None):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                parse_date(invalid)

    def test_valid_payload_is_not_modified(self):
        payload = sample_payload()
        original = deepcopy(payload)
        validate_payload(payload, "2026-09-16", "Asia/Kolkata")
        self.assertEqual(payload, original)

    def test_missing_nonfinite_or_wrong_day_fails(self):
        for invalid in (None, float("nan"), float("inf"), "31.5", True):
            payload = sample_payload()
            payload["daily"]["temperature_2m_max"] = [invalid]
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                validate_payload(payload, "2026-09-16", "Asia/Kolkata")
        with self.assertRaises(ValueError):
            validate_payload(sample_payload(), "2026-09-17", "Asia/Kolkata")

    def test_unexpected_units_or_array_length_fails(self):
        payload = sample_payload()
        payload["daily_units"]["precipitation_sum"] = "inch"
        with self.assertRaises(ValueError):
            validate_payload(payload, "2026-09-16", "Asia/Kolkata")
        payload = sample_payload()
        payload["daily"]["precipitation_sum"] = [2.5, 3.0]
        with self.assertRaises(ValueError):
            validate_payload(payload, "2026-09-16", "Asia/Kolkata")

    def test_http_retries_transient_get_failures(self):
        with http_session() as session:
            retry = session.get_adapter("https://").max_retries
            self.assertEqual(retry.total, 3)
            self.assertTrue(retry.is_retry("GET", 429))
            self.assertTrue(retry.is_retry("GET", 503))
            self.assertFalse(retry.is_retry("GET", 400))

    @patch("ingestion.pipeline.http_session")
    def test_extract_uses_requested_date_and_preserves_payload(self, mock_session):
        response = MagicMock()
        response.json.return_value = sample_payload()
        session = mock_session.return_value.__enter__.return_value
        session.get.return_value = response
        records = extract_date("2026-09-16")
        self.assertEqual(len(records), len(load_cities()))
        self.assertEqual(records[0]["payload"], sample_payload())
        for call in session.get.call_args_list:
            self.assertEqual(call.kwargs["params"]["start_date"], "2026-09-16")
            self.assertEqual(call.kwargs["params"]["end_date"], "2026-09-16")
            self.assertEqual(call.kwargs["timeout"], (5, 45))

    @patch("ingestion.pipeline.warehouse_connection")
    def test_incomplete_load_fails_before_database_write(self, connection):
        with self.assertRaises(ValueError):
            load_date("2026-09-16", [])
        connection.assert_not_called()

    @patch("ingestion.pipeline.warehouse_connection")
    def test_wrong_date_fails_before_database_write(self, connection):
        records = [
            {"city_id": city["id"], "city_name": city["name"],
             "logical_date": "2026-09-16", "payload": sample_payload()}
            for city in load_cities()
        ]
        with self.assertRaises(ValueError):
            load_date("2026-09-17", records)
        connection.assert_not_called()

    @patch("ingestion.pipeline.load_date")
    @patch("ingestion.pipeline.extract_date")
    def test_backfill_is_inclusive(self, extract, load):
        self.assertEqual(len(backfill("2026-09-14", "2026-09-16")), 3)
        self.assertEqual([call.args[0].isoformat() for call in extract.call_args_list],
                         ["2026-09-14", "2026-09-15", "2026-09-16"])
        self.assertEqual(load.call_count, 3)

    def test_backfill_rejects_reversed_range(self):
        with self.assertRaises(ValueError):
            backfill("2026-09-17", "2026-09-16")


if __name__ == "__main__":
    unittest.main()