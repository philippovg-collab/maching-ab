from __future__ import annotations

import json
import mimetypes
import os
import re
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from .services import AppService, ForbiddenError, NotFoundError, ValidationError

WEB_DIR = Path(__file__).resolve().parent / "web"


class ApiHandler(BaseHTTPRequestHandler):
    service = AppService()

    def _json_response(self, status: int, payload: Dict[str, Any]):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _file_response(self, path: Path):
        if not path.exists() or not path.is_file():
            return self._json_response(HTTPStatus.NOT_FOUND, {"error": "not_found"})
        body = path.read_bytes()
        mime = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _text_response(self, status: int, text: str, content_type: str, filename: Optional[str] = None):
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        if filename:
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _stream_download_response(
        self,
        path: Path,
        filename: str,
        content_type: str = "application/octet-stream",
        delete_after: bool = False,
    ):
        if not path.exists() or not path.is_file():
            return self._json_response(HTTPStatus.NOT_FOUND, {"error": "not_found"})
        size = path.stat().st_size
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(size))
        self.end_headers()
        try:
            with path.open("rb") as f:
                while True:
                    chunk = f.read(1024 * 64)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
        finally:
            if delete_after:
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        return json.loads(raw)

    def _actor(self) -> str:
        return self.headers.get("X-User", "admin")

    def _source_ip(self) -> str:
        return self.client_address[0] if self.client_address else "unknown"

    def _dispatch(self, method: str):
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            if method == "GET" and path == "/health":
                return self._json_response(HTTPStatus.OK, {"status": "ok"})
            query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
            actor = self._actor()
            source_ip = self._source_ip()

            if method == "GET" and path in {"/", "/index.html"}:
                return self._file_response(WEB_DIR / "index.html")

            if method == "GET" and path == "/assets/styles.css":
                return self._file_response(WEB_DIR / "styles.css")

            if method == "GET" and path == "/assets/app.js":
                return self._file_response(WEB_DIR / "app.js")

            if method == "POST" and path == "/api/v1/ingest/files":
                payload = self._read_json()
                res = self.service.ingest_file(actor, source_ip, payload)
                return self._json_response(HTTPStatus.OK, res)

            if method == "POST" and path == "/api/v1/ingest/xlsx":
                payload = self._read_json()
                res = self.service.ingest_xlsx(actor, source_ip, payload)
                return self._json_response(HTTPStatus.OK, res)

            if method == "POST" and path == "/api/v1/ingest/xlsx/batch":
                payload = self._read_json()
                res = self.service.ingest_xlsx_batch(actor, source_ip, payload)
                return self._json_response(HTTPStatus.OK, res)

            if method == "POST" and path == "/api/v1/validate/xlsx":
                payload = self._read_json()
                res = self.service.validate_xlsx(actor, payload)
                return self._json_response(HTTPStatus.OK, res)

            if method == "POST" and path == "/api/v1/quick-compare":
                payload = self._read_json()
                res = self.service.quick_compare(actor, source_ip, payload)
                return self._json_response(HTTPStatus.OK, res)

            if method == "GET" and path == "/api/v1/match/runs":
                try:
                    limit = int(query.get("limit", "50"))
                except (TypeError, ValueError):
                    raise ValidationError("limit must be an integer")
                res = self.service.list_runs(actor, limit, business_date=query.get("business_date"))
                return self._json_response(HTTPStatus.OK, res)

            m = re.fullmatch(r"/api/v1/results/run/([a-f0-9\-]+)", path)
            if method == "GET" and m:
                res = self.service.get_run_results(actor, m.group(1), query)
                return self._json_response(HTTPStatus.OK, res)

            if method == "GET" and path == "/api/v1/results/latest":
                res = self.service.get_latest_results(actor, query.get("business_date", ""), query)
                return self._json_response(HTTPStatus.OK, res)

            m = re.fullmatch(r"/api/v1/results/details/(.+)", path)
            if method == "GET" and m:
                from urllib.parse import unquote
                res = self.service.get_result_details(actor, unquote(m.group(1)))
                return self._json_response(HTTPStatus.OK, res)

            if method == "GET" and path == "/api/v1/match/status":
                res = self.service.latest_run_status(actor, query.get("business_date", ""))
                return self._json_response(HTTPStatus.OK, res)

            m = re.fullmatch(r"/api/v1/ingest/files/([a-f0-9\-]+)/status", path)
            if method == "GET" and m:
                res = self.service.ingest_status(actor, m.group(1))
                return self._json_response(HTTPStatus.OK, res)

            if method == "POST" and path == "/api/v1/match/runs":
                payload = self._read_json()
                res = self.service.run_matching(actor, source_ip, payload)
                return self._json_response(HTTPStatus.OK, res)

            m = re.fullmatch(r"/api/v1/match/runs/([a-f0-9\-]+)", path)
            if method == "GET" and m:
                res = self.service.get_run(actor, m.group(1))
                return self._json_response(HTTPStatus.OK, res)

            if method == "GET" and path == "/api/v1/exceptions":
                res = self.service.list_exceptions(actor, query)
                return self._json_response(HTTPStatus.OK, res)

            m = re.fullmatch(r"/api/v1/exceptions/([a-f0-9\-]+)", path)
            if method == "GET" and m:
                res = self.service.get_exception(actor, m.group(1))
                return self._json_response(HTTPStatus.OK, res)

            m = re.fullmatch(r"/api/v1/exceptions/([a-f0-9\-]+)/actions", path)
            if method == "POST" and m:
                payload = self._read_json()
                res = self.service.exception_action(actor, source_ip, m.group(1), payload)
                return self._json_response(HTTPStatus.OK, res)

            if method == "GET" and path == "/api/v1/admin/rulesets":
                res = self.service.get_rulesets(actor)
                return self._json_response(HTTPStatus.OK, res)

            if method == "PUT" and path == "/api/v1/admin/rulesets":
                payload = self._read_json()
                res = self.service.put_ruleset(actor, source_ip, payload)
                return self._json_response(HTTPStatus.OK, res)

            if method == "GET" and path == "/api/v1/audit/events":
                res = self.service.list_audit(actor, query)
                return self._json_response(HTTPStatus.OK, res)

            if method == "GET" and path == "/api/v1/meta/users":
                res = self.service.list_users(actor)
                return self._json_response(HTTPStatus.OK, res)

            if method == "GET" and path == "/api/v1/analytics/hardcoded":
                res = self.service.hardcoded_analytics(actor, query.get("business_date", ""))
                return self._json_response(HTTPStatus.OK, res)

            if method == "GET" and path == "/api/v1/monitor/source-balance":
                res = self.service.source_balance(actor, query.get("business_date", ""))
                return self._json_response(HTTPStatus.OK, res)

            if method == "GET" and path == "/api/v1/export/unmatched.csv":
                business_date = query.get("business_date", "")
                run_id = query.get("run_id")
                csv_text = self.service.export_unmatched_csv(actor, business_date, run_id=run_id)
                fname = f"unmatched_{business_date or 'unknown'}.csv"
                return self._text_response(HTTPStatus.OK, csv_text, "text/csv; charset=utf-8", filename=fname)

            m = re.fullmatch(r"/api/v1/runs/([a-f0-9\-]+)/export\.xlsx", path)
            if method == "GET" and m:
                file_path, filename = self.service.export_run_xlsx_file(actor, m.group(1))
                return self._stream_download_response(
                    file_path,
                    filename,
                    content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    delete_after=True,
                )

            m = re.fullmatch(r"/api/v1/runs/([a-f0-9\-]+)/unmatched_way4\.csv", path)
            if method == "GET" and m:
                file_path, filename = self.service.export_run_unmatched_csv_file(actor, m.group(1), "way4")
                return self._stream_download_response(file_path, filename, content_type="text/csv; charset=utf-8", delete_after=True)

            m = re.fullmatch(r"/api/v1/runs/([a-f0-9\-]+)/unmatched_visa\.csv", path)
            if method == "GET" and m:
                file_path, filename = self.service.export_run_unmatched_csv_file(actor, m.group(1), "visa")
                return self._stream_download_response(file_path, filename, content_type="text/csv; charset=utf-8", delete_after=True)

            m = re.fullmatch(r"/api/v1/runs/([a-f0-9\-]+)/mismatches_partial\.xlsx", path)
            if method == "GET" and m:
                file_path, filename = self.service.export_run_mismatches_partial_xlsx_file(actor, m.group(1))
                return self._stream_download_response(
                    file_path,
                    filename,
                    content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    delete_after=True,
                )

            return self._json_response(HTTPStatus.NOT_FOUND, {"error": "not_found"})

        except ForbiddenError as e:
            return self._json_response(HTTPStatus.FORBIDDEN, {"error": str(e)})
        except ValidationError as e:
            text = str(e)
            try:
                payload = json.loads(text)
                if isinstance(payload, dict):
                    payload.setdefault("error", payload.get("message", "validation_error"))
                    return self._json_response(HTTPStatus.BAD_REQUEST, payload)
            except Exception:
                pass
            return self._json_response(HTTPStatus.BAD_REQUEST, {"error": text})
        except NotFoundError as e:
            return self._json_response(HTTPStatus.NOT_FOUND, {"error": str(e)})
        except json.JSONDecodeError:
            return self._json_response(HTTPStatus.BAD_REQUEST, {"error": "Invalid JSON body"})
        except Exception:  # pragma: no cover
            traceback.print_exc()
            return self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal_error"})

    def log_message(self, format: str, *args):
        return

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def do_PUT(self):
        self._dispatch("PUT")


def run_server(host: str = "127.0.0.1", port: int = 8080):
    server = ThreadingHTTPServer((host, port), ApiHandler)
    print(f"Reconciliation MVP server running on http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
