import os
import tempfile
import unittest
from pathlib import Path

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


class UiOnboardingSmokeTest(unittest.TestCase):
    def test_onboarding_elements_exist(self):
        html = Path("src/web/index.html").read_text(encoding="utf-8")
        js = Path("src/web/app.js").read_text(encoding="utf-8")
        self.assertIn('id="firstRunCta"', html)
        self.assertIn('id="startOnboardingBtn"', html)
        self.assertIn("updateDashboardOnboarding", js)
        self.assertIn("setView(\"lite\")", js)


if __name__ == "__main__":
    unittest.main()
