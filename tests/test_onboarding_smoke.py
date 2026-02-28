import hashlib
import os
import tempfile
import unittest
from pathlib import Path

from src.db import Database
from src.services import AppService


class OnboardingSmokeTest(unittest.TestCase):
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

    def _ingest_payload(self, source, business_date, file_name, checksum_seed, records):
        return {
            "source": source,
            "business_date": business_date,
            "file_name": file_name,
            "checksum_sha256": hashlib.sha256(checksum_seed.encode("utf-8")).hexdigest(),
            "parser_profile": "AUTO_TEST",
            "records": records,
        }

    def test_onboarding_flow_from_empty_to_results(self):
        business_date = "2026-03-01"

        html = Path("src/web/index.html").read_text(encoding="utf-8")
        self.assertIn("Начните сверку за 1 минуту", html)
        self.assertIn('id="xlsxUploadRunBtn"', html)
        self.assertNotIn('id="viewLite"', html)

        balance0 = self.service.source_balance("admin", business_date)
        self.assertFalse(balance0["ready_for_matching"])

        way4_records = [
            {
                "rrn": "Q001",
                "arn": "AQ001",
                "pan_masked": "400012******1001",
                "amount": 100.0,
                "currency": "KZT",
                "txn_time": f"{business_date}T10:00:00+06:00",
                "op_type": "PURCHASE",
                "merchant_id": "M1",
                "channel_id": "POS",
                "status_norm": "BOOKED",
            }
        ]
        visa_records = [
            {
                "rrn": "Q001",
                "arn": "AQ001",
                "pan_masked": "400012******2001",
                "amount": 100.0,
                "currency": "KZT",
                "txn_time": f"{business_date}T10:01:00+06:00",
                "op_type": "CLEARING",
                "merchant_id": "M1",
                "channel_id": "POS",
                "status_norm": "BOOKED",
            }
        ]

        self.service.ingest_file(
            "admin",
            "127.0.0.1",
            self._ingest_payload("WAY4_EXPORT", business_date, "way4.json", "way4-onboarding", way4_records),
        )
        balance1 = self.service.source_balance("admin", business_date)
        self.assertFalse(balance1["ready_for_matching"])

        self.service.ingest_file(
            "admin",
            "127.0.0.1",
            self._ingest_payload("VISA_EXPORT", business_date, "visa.json", "visa-onboarding", visa_records),
        )
        balance2 = self.service.source_balance("admin", business_date)
        self.assertTrue(balance2["ready_for_matching"])

        run = self.service.run_matching("admin", "127.0.0.1", {"business_date": business_date, "scope_filter": "ALL"})
        self.assertTrue(run.get("run_id"))

        latest = self.service.get_latest_results("admin", business_date, {"page": 1, "page_size": 20})
        self.assertTrue(latest.get("has_run"))
        self.assertEqual(latest["run"]["run_id"], run["run_id"])
        self.assertIn("summary", latest)
        self.assertIn("items", latest)
