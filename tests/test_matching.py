import unittest
from pathlib import Path

from src.config import DATA_DIR
from src.db import Database
from src.demo_data import seed_demo_data
from src.services import AppService


class MatchingFlowTest(unittest.TestCase):
    def setUp(self):
        db_file = DATA_DIR / "test_reconciliation.db"
        if db_file.exists():
            db_file.unlink()
        self.db = Database(db_file)
        self.service = AppService(self.db)

    def test_end_to_end_matching_and_analytics(self):
        business_date = "2026-02-22"
        run = seed_demo_data(self.service, business_date=business_date)
        self.assertTrue(run["run_id"])

        run_info = self.service.get_run("admin", run["run_id"])
        self.assertGreaterEqual(sum(run_info["match_summary"].values()), 3)
        self.assertGreaterEqual(sum(run_info["exception_summary"].values()), 1)

        analytics = self.service.hardcoded_analytics("admin", business_date)
        self.assertEqual(analytics["business_date"], business_date)
        self.assertGreater(analytics["total_way4"], 0)
        self.assertGreaterEqual(analytics["match_rate_pct"], 50.0)

    def test_exception_workflow(self):
        run = seed_demo_data(self.service, business_date="2026-02-23")
        ex = self.service.list_exceptions("admin", {"business_date": "2026-02-23"})
        self.assertGreater(ex["count"], 0)

        case_id = ex["items"][0]["case_id"]
        self.service.exception_action(
            "admin", "127.0.0.1", case_id, {"action_type": "assign", "owner_user_id": "operator1"}
        )
        self.service.exception_action(
            "admin", "127.0.0.1", case_id, {"action_type": "comment", "comment": "Investigating"}
        )
        closed = self.service.exception_action(
            "admin", "127.0.0.1", case_id, {"action_type": "close", "resolution_code": "POSTED_LATE"}
        )
        self.assertEqual(closed["case"]["status"], "CLOSED")


if __name__ == "__main__":
    unittest.main()
