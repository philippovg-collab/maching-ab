import os
import tempfile
import threading
import unittest
from pathlib import Path

from src.db import Database
from src.demo_data import seed_demo_data
from src.server import ApiHandler
from src.services import AppService

try:
    from playwright.sync_api import sync_playwright

    HAS_PLAYWRIGHT = True
except Exception:
    HAS_PLAYWRIGHT = False


@unittest.skipUnless(HAS_PLAYWRIGHT, "playwright is not installed")
class UiE2EPlaywrightTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        cls.tmp.close()
        db = Database(Path(cls.tmp.name))
        service = AppService(db)
        cls.seed_date = "2026-03-11"
        seed_demo_data(service, business_date=cls.seed_date)
        ApiHandler.service = service

        from http.server import ThreadingHTTPServer

        try:
            cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), ApiHandler)
        except PermissionError as e:
            raise unittest.SkipTest(f"Socket bind is not allowed in this environment: {e}")
        cls.port = cls.httpd.server_address[1]
        cls.base_url = f"http://127.0.0.1:{cls.port}"
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        try:
            if hasattr(cls, "httpd"):
                cls.httpd.shutdown()
                cls.httpd.server_close()
            if hasattr(cls, "thread"):
                cls.thread.join(timeout=2)
        finally:
            try:
                os.unlink(cls.tmp.name)
            except FileNotFoundError:
                pass

    def _is_hidden(self, page, selector: str) -> bool:
        return bool(page.eval_on_selector(selector, "el => el.classList.contains('hidden')"))

    def test_ui_navigation_and_open_results(self):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(self.base_url, wait_until="networkidle")

            # Scenario A: app opens on ingestion view and legacy tabs are removed.
            page.fill("#businessDate", "2026-03-10")
            page.dispatch_event("#businessDate", "change")
            page.wait_for_timeout(350)
            self.assertFalse(self._is_hidden(page, "#viewIngestion"))
            self.assertTrue(self._is_hidden(page, "#viewResults"))
            self.assertEqual(page.locator("#tabDashboard").count(), 0)
            self.assertEqual(page.locator("#tabLite").count(), 0)
            self.assertEqual(page.locator("#viewLite").count(), 0)
            self.assertEqual(page.locator("#quickCompareBtn").count(), 0)

            # Scenario B: open results for seeded run via the Results tab.
            page.fill("#businessDate", self.seed_date)
            page.dispatch_event("#businessDate", "change")
            page.wait_for_timeout(400)
            page.click("#tabResults")
            page.wait_for_timeout(350)
            self.assertFalse(self._is_hidden(page, "#viewResults"))
            run_id_text = page.locator("#resRunId").inner_text().strip()
            self.assertNotEqual(run_id_text, "-")
            self.assertIn("...", run_id_text)

            browser.close()
