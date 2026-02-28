import os
import tempfile
import unittest
import json
from pathlib import Path
from unittest import mock

from src.db import Database
from src.demo_data import seed_demo_data
from src.services import AppService, ValidationError


class AppGuardsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.db = Database(Path(self.tmp.name))
        self.service = AppService(self.db)

    def tearDown(self):
        try:
            os.unlink(self.tmp.name)
        except FileNotFoundError:
            pass

    def test_pan_is_sanitized_on_ingest(self):
        business_date = "2026-02-22"
        self.service.ingest_file(
            "admin",
            "127.0.0.1",
            {
                "source": "WAY4_EXPORT",
                "business_date": business_date,
                "file_name": "way4.json",
                "checksum_sha256": "c1",
                "parser_profile": "WAY4_v1",
                "records": [
                    {
                        "rrn": "123",
                        "arn": "A123",
                        "pan_masked": "4000123412341234",
                        "amount": 100.0,
                        "currency": "KZT",
                        "txn_time": f"{business_date}T01:00:00+06:00",
                        "op_type": "PURCHASE",
                        "merchant_id": "M1",
                        "channel_id": "ECOM",
                        "status_norm": "BOOKED",
                    }
                ],
            },
        )
        with self.db.connect() as conn:
            row = conn.execute("SELECT pan_masked FROM txns LIMIT 1").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["pan_masked"], "400012******1234")
        self.assertNotEqual(row["pan_masked"], "4000123412341234")

    def test_pan_is_sanitized_when_embedded_in_noisy_string(self):
        business_date = "2026-02-22"
        self.service.ingest_file(
            "admin",
            "127.0.0.1",
            {
                "source": "WAY4_EXPORT",
                "business_date": business_date,
                "file_name": "way4-noisy-pan.json",
                "checksum_sha256": "c1-noisy",
                "parser_profile": "WAY4_v1",
                "records": [
                    {
                        "rrn": "124",
                        "arn": "A124",
                        "pan_masked": "CARD:40001234-1234-1234",
                        "amount": 101.0,
                        "currency": "KZT",
                        "txn_time": f"{business_date}T01:05:00+06:00",
                        "op_type": "PURCHASE",
                        "merchant_id": "M1",
                        "channel_id": "ECOM",
                        "status_norm": "BOOKED",
                    }
                ],
            },
        )
        with self.db.connect() as conn:
            row = conn.execute("SELECT pan_masked FROM txns WHERE rrn='124'").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["pan_masked"], "400012******1234")
        self.assertNotIn("4000123412341234", row["pan_masked"])

    def test_matching_requires_both_sources(self):
        business_date = "2026-02-23"
        self.service.ingest_file(
            "admin",
            "127.0.0.1",
            {
                "source": "WAY4_EXPORT",
                "business_date": business_date,
                "file_name": "way4.json",
                "checksum_sha256": "c2",
                "parser_profile": "WAY4_v1",
                "records": [
                    {
                        "rrn": "555",
                        "arn": "A555",
                        "pan_masked": "400012******5555",
                        "amount": 10.0,
                        "currency": "KZT",
                        "txn_time": f"{business_date}T02:00:00+06:00",
                        "op_type": "PURCHASE",
                        "merchant_id": "M2",
                        "channel_id": "ECOM",
                        "status_norm": "BOOKED",
                    }
                ],
            },
        )
        with self.assertRaises(ValidationError) as ctx:
            self.service.run_matching("admin", "127.0.0.1", {"business_date": business_date, "scope_filter": "ALL"})
        self.assertIn("Both sources are required", str(ctx.exception))

    def test_latest_results_by_business_date(self):
        business_date = "2026-02-24"
        run = seed_demo_data(self.service, business_date=business_date)
        result = self.service.get_latest_results("admin", business_date, {"page": 1, "page_size": 20})
        self.assertTrue(result["has_run"])
        self.assertEqual(result["run"]["run_id"], run["run_id"])
        self.assertIn("summary", result)

    def test_validate_xlsx_requires_payload(self):
        with self.assertRaises(ValidationError):
            self.service.validate_xlsx("admin", {"file_name": "sample.xlsx"})

    def test_results_filters_validation(self):
        business_date = "2026-02-26"
        run = seed_demo_data(self.service, business_date=business_date)
        with self.assertRaises(ValidationError):
            self.service.get_run_results("admin", run["run_id"], {"page": "abc"})
        with self.assertRaises(ValidationError):
            self.service.get_run_results("admin", run["run_id"], {"amount_min": "bad"})
        with self.assertRaises(ValidationError):
            self.service.get_run_results("admin", run["run_id"], {"amount_min": "100", "amount_max": "1"})

    def test_exception_actions_validation(self):
        business_date = "2026-02-27"
        seed_demo_data(self.service, business_date=business_date)
        cases = self.service.list_exceptions("admin", {"business_date": business_date})
        case_id = cases["items"][0]["case_id"]

        with self.assertRaises(ValidationError):
            self.service.exception_action(
                "admin",
                "127.0.0.1",
                case_id,
                {"action_type": "status_change", "status": "INVALID"},
            )
        with self.assertRaises(ValidationError):
            self.service.exception_action(
                "admin",
                "127.0.0.1",
                case_id,
                {"action_type": "assign", "owner_user_id": "ghost"},
            )
        with self.assertRaises(ValidationError):
            self.service.exception_action(
                "admin",
                "127.0.0.1",
                case_id,
                {"action_type": "comment", "comment": "   "},
            )

    def test_run_marked_failed_when_matching_crashes(self):
        business_date = "2026-02-25"
        way4_record = {
            "rrn": "W001",
            "arn": "A-W001",
            "pan_masked": "400012******0001",
            "amount": 125.0,
            "currency": "KZT",
            "txn_time": f"{business_date}T10:00:00+06:00",
            "op_type": "PURCHASE",
            "merchant_id": "M1",
            "channel_id": "POS",
            "status_norm": "BOOKED",
        }
        visa_record = {
            "rrn": "V001",
            "arn": "A-V001",
            "pan_masked": "400012******0002",
            "amount": 125.0,
            "currency": "KZT",
            "txn_time": f"{business_date}T10:01:00+06:00",
            "op_type": "PURCHASE",
            "merchant_id": "M1",
            "channel_id": "POS",
            "status_norm": "BOOKED",
        }
        self.service.ingest_file(
            "admin",
            "127.0.0.1",
            {
                "source": "WAY4_EXPORT",
                "business_date": business_date,
                "file_name": "way4.json",
                "checksum_sha256": "wm1",
                "parser_profile": "WAY4_v1",
                "records": [way4_record],
            },
        )
        self.service.ingest_file(
            "admin",
            "127.0.0.1",
            {
                "source": "VISA_EXPORT",
                "business_date": business_date,
                "file_name": "visa.json",
                "checksum_sha256": "vm1",
                "parser_profile": "VISA_v1",
                "records": [visa_record],
            },
        )

        with mock.patch("src.services.match_transactions", side_effect=RuntimeError("forced_crash")):
            with self.assertRaises(RuntimeError):
                self.service.run_matching("admin", "127.0.0.1", {"business_date": business_date, "scope_filter": "ALL"})

        with self.db.connect() as conn:
            run = conn.execute(
                "SELECT status, finished_at FROM match_runs WHERE business_date=? ORDER BY started_at DESC LIMIT 1",
                (business_date,),
            ).fetchone()
            self.assertIsNotNone(run)
            self.assertEqual(run["status"], "FAILED")
            self.assertTrue(run["finished_at"])
            audit = conn.execute(
                """
                SELECT result, details FROM audit_events
                WHERE action='MATCH_RUN_EXECUTE'
                ORDER BY event_at DESC
                LIMIT 1
                """
            ).fetchone()
            self.assertIsNotNone(audit)
            self.assertEqual(audit["result"], "FAILURE")
            self.assertIn("forced_crash", audit["details"])

    def test_run_failure_rolls_back_partial_results(self):
        business_date = "2026-02-28"
        way4_records = [
            {
                "rrn": "R001",
                "arn": "AR001",
                "pan_masked": "400012******9001",
                "amount": 10.0,
                "currency": "KZT",
                "txn_time": f"{business_date}T09:00:00+06:00",
                "op_type": "PURCHASE",
                "merchant_id": "M1",
                "channel_id": "POS",
                "status_norm": "BOOKED",
            },
            {
                "rrn": "R002",
                "arn": "AR002",
                "pan_masked": "400012******9002",
                "amount": 20.0,
                "currency": "KZT",
                "txn_time": f"{business_date}T09:05:00+06:00",
                "op_type": "PURCHASE",
                "merchant_id": "M1",
                "channel_id": "POS",
                "status_norm": "BOOKED",
            },
        ]
        visa_records = [
            {
                "rrn": "R001",
                "arn": "AR001",
                "pan_masked": "400012******8001",
                "amount": 10.0,
                "currency": "KZT",
                "txn_time": f"{business_date}T09:01:00+06:00",
                "op_type": "CLEARING",
                "merchant_id": "M1",
                "channel_id": "POS",
                "status_norm": "BOOKED",
            },
            {
                "rrn": "R002",
                "arn": "AR002",
                "pan_masked": "400012******8002",
                "amount": 20.0,
                "currency": "KZT",
                "txn_time": f"{business_date}T09:06:00+06:00",
                "op_type": "CLEARING",
                "merchant_id": "M1",
                "channel_id": "POS",
                "status_norm": "BOOKED",
            },
        ]
        self.service.ingest_file(
            "admin",
            "127.0.0.1",
            {
                "source": "WAY4_EXPORT",
                "business_date": business_date,
                "file_name": "way4-roll.json",
                "checksum_sha256": "roll-way4",
                "parser_profile": "WAY4_v1",
                "records": way4_records,
            },
        )
        self.service.ingest_file(
            "admin",
            "127.0.0.1",
            {
                "source": "VISA_EXPORT",
                "business_date": business_date,
                "file_name": "visa-roll.json",
                "checksum_sha256": "roll-visa",
                "parser_profile": "VISA_v1",
                "records": visa_records,
            },
        )

        call_counter = {"n": 0}
        original_dumps = json.dumps

        def flaky_dumps(obj, *args, **kwargs):
            call_counter["n"] += 1
            if call_counter["n"] == 2:
                raise RuntimeError("forced_mid_insert_failure")
            return original_dumps(obj, *args, **kwargs)

        with mock.patch("src.services.json.dumps", side_effect=flaky_dumps):
            with self.assertRaises(RuntimeError):
                self.service.run_matching("admin", "127.0.0.1", {"business_date": business_date, "scope_filter": "ALL"})

        with self.db.connect() as conn:
            run = conn.execute(
                "SELECT run_id, status FROM match_runs WHERE business_date=? ORDER BY started_at DESC LIMIT 1",
                (business_date,),
            ).fetchone()
            self.assertIsNotNone(run)
            self.assertEqual(run["status"], "FAILED")
            matches = conn.execute("SELECT COUNT(*) AS c FROM match_results WHERE run_id=?", (run["run_id"],)).fetchone()["c"]
            exceptions = conn.execute("SELECT COUNT(*) AS c FROM exception_cases WHERE run_id=?", (run["run_id"],)).fetchone()["c"]
            self.assertEqual(matches, 0)
            self.assertEqual(exceptions, 0)


class UiNavigationSmokeTest(unittest.TestCase):
    def test_navigation_is_limited_to_ingestion_and_results(self):
        html = Path("src/web/index.html").read_text(encoding="utf-8")
        js = Path("src/web/app.js").read_text(encoding="utf-8")
        self.assertIn('id="tabIngestion"', html)
        self.assertIn('id="tabResults"', html)
        self.assertNotIn('id="tabDashboard"', html)
        self.assertNotIn('id="tabLite"', html)
        self.assertNotIn('id="viewLite"', html)
        self.assertNotIn('id="userSelect"', html)
        self.assertNotIn('id="xlsxUploadBtn"', html)
        self.assertIn('const normalized = view === "results" ? "results" : "ingestion";', js)


if __name__ == "__main__":
    unittest.main()
