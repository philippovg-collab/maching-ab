from __future__ import annotations

import hashlib
import hmac
import json
import csv
import re
import uuid
import tempfile
from io import StringIO
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from .auth import has_permission
from .config import DEFAULT_RULESET, PAN_HASH_SECRET
from .db import Database
from .export_reports import SimpleXlsxBuilder, write_csv_file
from .matching import RuleSet, Txn, match_transactions
from .xlsx_ingest import XlsxParseError, parse_xlsx_ingest, parse_xlsx_ingest_detailed


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_op_type(value: str) -> str:
    mapped = {
        "PURCHASE": "PURCHASE",
        "CLEARING": "CLEARING",
        "SETTLEMENT": "SETTLEMENT",
        "REFUND": "REFUND",
        "REVERSAL": "REVERSAL",
        "CHARGEBACK": "CHARGEBACK",
        "ADJUSTMENT": "ADJUSTMENT",
    }
    return mapped.get((value or "").strip().upper(), "PURCHASE")


def normalize_currency(value: str) -> str:
    return (value or "").strip().upper()[:3]


def hash_pan(masked_pan: str) -> str:
    digest = hmac.new(PAN_HASH_SECRET.encode(), masked_pan.encode(), hashlib.sha256).hexdigest()
    return digest


def sanitize_pan_masked(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "****"

    compact = re.sub(r"[\s\-]", "", raw)
    if re.fullmatch(r"\d{12,19}", compact):
        stars = "*" * max(2, len(compact) - 10)
        return f"{compact[:6]}{stars}{compact[-4:]}"

    if re.fullmatch(r"[0-9Xx\*]{12,19}", compact):
        return compact.replace("X", "*").replace("x", "*")

    return raw


class ForbiddenError(Exception):
    pass


class ValidationError(Exception):
    pass


class NotFoundError(Exception):
    pass


class AppService:
    def __init__(self, db: Optional[Database] = None):
        self.db = db or Database()
        self.db.init()
        self._ensure_default_ruleset()

    def _ensure_default_ruleset(self) -> None:
        with self.db.connect() as conn:
            row = conn.execute("SELECT version FROM rulesets WHERE is_active=1 LIMIT 1").fetchone()
            if row:
                return
            conn.execute(
                "INSERT INTO rulesets(version, is_active, json_text, created_at) VALUES(?,?,?,?)",
                (
                    DEFAULT_RULESET["version"],
                    1,
                    json.dumps(DEFAULT_RULESET, ensure_ascii=False),
                    now_iso(),
                ),
            )

    def get_roles(self, login: str) -> Set[str]:
        with self.db.connect() as conn:
            rows = conn.execute("SELECT role_name FROM user_roles WHERE login=?", (login,)).fetchall()
        return {r["role_name"] for r in rows}

    def check_permission(self, login: str, permission: str):
        roles = self.get_roles(login)
        if not has_permission(roles, permission):
            raise ForbiddenError(f"Permission denied: {permission}")

    def _audit(
        self,
        conn,
        actor: str,
        source_ip: str,
        object_type: str,
        object_id: Optional[str],
        action: str,
        result: str,
        details: str,
    ):
        conn.execute(
            """
            INSERT INTO audit_events(audit_id, event_at, actor_login, source_ip, object_type, object_id, action, result, details)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (str(uuid.uuid4()), now_iso(), actor, source_ip, object_type, object_id, action, result, details),
        )

    def ingest_file(self, actor: str, source_ip: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.check_permission(actor, "ingest:write")

        required = ["source", "business_date", "file_name", "checksum_sha256", "parser_profile"]
        missing = [k for k in required if not payload.get(k)]
        if missing:
            raise ValidationError(f"Missing required fields: {', '.join(missing)}")

        source = payload["source"].strip().upper()
        business_date = payload["business_date"]
        checksum = payload["checksum_sha256"]

        with self.db.connect() as conn:
            duplicate = conn.execute(
                """
                SELECT file_id, status, record_count FROM ingest_files
                WHERE source_system=? AND business_date=? AND checksum_sha256=?
                """,
                (source, business_date, checksum),
            ).fetchone()
            if duplicate:
                self._audit(
                    conn,
                    actor,
                    source_ip,
                    "ingest_file",
                    duplicate["file_id"],
                    "INGEST_REGISTER",
                    "DUPLICATE",
                    "Idempotent duplicate request",
                )
                return {
                    "file_id": duplicate["file_id"],
                    "status": duplicate["status"],
                    "record_count": duplicate["record_count"],
                    "duplicate": True,
                }

            file_id = str(uuid.uuid4())
            records = payload.get("records", [])
            conn.execute(
                """
                INSERT INTO ingest_files(file_id, source_system, business_date, file_name, checksum_sha256,
                parser_profile, received_at, status, record_count, created_by)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    file_id,
                    source,
                    business_date,
                    payload["file_name"],
                    checksum,
                    payload["parser_profile"],
                    payload.get("received_at") or now_iso(),
                    "PARSED",
                    len(records),
                    actor,
                ),
            )

            inserted = 0
            for rec in records:
                self._insert_txn(conn, source, business_date, rec)
                inserted += 1

            self._audit(
                conn,
                actor,
                source_ip,
                "ingest_file",
                file_id,
                "INGEST_REGISTER",
                "SUCCESS",
                f"records={inserted}",
            )
            return {"file_id": file_id, "status": "PARSED", "record_count": inserted, "duplicate": False}

    def ingest_xlsx(self, actor: str, source_ip: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.check_permission(actor, "ingest:write")

        file_name = payload.get("file_name")
        file_base64 = payload.get("file_base64")
        if not file_name or not file_base64:
            raise ValidationError("file_name and file_base64 are required")

        preferred_profile = payload.get("parser_profile")
        requested_date = payload.get("business_date")
        try:
            parsed = parse_xlsx_ingest(
                file_b64=file_base64,
                preferred_profile=preferred_profile,
                business_date=requested_date,
            )
        except XlsxParseError as e:
            raise ValidationError(str(e))

        checksum = payload.get("checksum_sha256")
        if not checksum:
            raw_b64 = file_base64.split(",", 1)[1] if file_base64.startswith("data:") and "," in file_base64 else file_base64
            checksum = hashlib.sha256(raw_b64.encode("utf-8")).hexdigest()

        ingest_payload = {
            "source": payload.get("source") or parsed.source,
            "business_date": requested_date or parsed.business_date,
            "file_name": file_name,
            "checksum_sha256": checksum,
            "received_at": payload.get("received_at") or now_iso(),
            "parser_profile": preferred_profile or parsed.profile,
            "records": parsed.records,
        }
        result = self.ingest_file(actor, source_ip, ingest_payload)
        return {
            **result,
            "detected_profile": parsed.profile,
            "detected_source": parsed.source,
            "business_date": ingest_payload["business_date"],
            "preview": parsed.preview,
        }

    def validate_xlsx(self, actor: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.check_permission(actor, "ingest:write")

        file_name = payload.get("file_name")
        file_base64 = payload.get("file_base64")
        if not file_name or not file_base64:
            raise ValidationError("file_name and file_base64 are required")

        preferred_profile = payload.get("parser_profile")
        requested_date = payload.get("business_date")
        try:
            detailed = parse_xlsx_ingest_detailed(
                file_base64, preferred_profile=preferred_profile, business_date=requested_date
            )
        except XlsxParseError as e:
            raise ValidationError(str(e))
        errors = [{"row": e.row, "field": e.field, "message": e.message} for e in detailed.errors[:1000]]
        return {
            "ok": len(errors) == 0,
            "file_name": file_name,
            "detected_profile": detailed.parsed.profile,
            "detected_source": detailed.parsed.source,
            "business_date": requested_date or detailed.parsed.business_date,
            "record_count": len(detailed.parsed.records),
            "preview": detailed.parsed.preview,
            "errors": errors,
        }

    def _ingest_from_parsed(
        self,
        actor: str,
        source_ip: str,
        file_name: str,
        file_b64: str,
        parsed,
        business_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        checksum = hashlib.sha256(file_b64.encode("utf-8")).hexdigest()
        payload = {
            "source": parsed.source,
            "business_date": business_date or parsed.business_date,
            "file_name": file_name,
            "checksum_sha256": checksum,
            "received_at": now_iso(),
            "parser_profile": parsed.profile,
            "records": parsed.records,
        }
        res = self.ingest_file(actor, source_ip, payload)
        return {
            **res,
            "detected_profile": parsed.profile,
            "detected_source": parsed.source,
            "business_date": payload["business_date"],
            "preview": parsed.preview,
        }

    def quick_compare(self, actor: str, source_ip: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.check_permission(actor, "ingest:write")
        self.check_permission(actor, "match:execute")

        business_date = payload.get("business_date")
        way4 = payload.get("way4_file") or {}
        visa_files = payload.get("visa_files") or []
        if not way4.get("file_name") or not way4.get("file_base64"):
            raise ValidationError("way4_file is required")
        if not isinstance(visa_files, list) or not visa_files:
            raise ValidationError("visa_files[] is required")

        validation_errors = []
        parsed_files = []

        try:
            way4_d = parse_xlsx_ingest_detailed(way4["file_base64"], preferred_profile=None, business_date=business_date)
            if not way4_d.parsed.source.startswith("WAY4"):
                validation_errors.append(
                    {"file": way4["file_name"], "row": 0, "field": "profile", "message": "Файл не распознан как Way4"}
                )
            for e in way4_d.errors[:200]:
                validation_errors.append(
                    {"file": way4["file_name"], "row": e.row, "field": e.field, "message": e.message}
                )
            parsed_files.append((way4["file_name"], way4["file_base64"], way4_d.parsed))
        except XlsxParseError as e:
            validation_errors.append({"file": way4.get("file_name", "way4"), "row": 0, "field": "file", "message": str(e)})

        for vf in visa_files:
            name = vf.get("file_name")
            b64 = vf.get("file_base64")
            if not name or not b64:
                validation_errors.append({"file": name or "visa", "row": 0, "field": "file", "message": "Пустой файл"})
                continue
            try:
                d = parse_xlsx_ingest_detailed(b64, preferred_profile=None, business_date=business_date)
                if not d.parsed.source.startswith("VISA"):
                    validation_errors.append(
                        {"file": name, "row": 0, "field": "profile", "message": "Файл не распознан как VISA"}
                    )
                for e in d.errors[:200]:
                    validation_errors.append({"file": name, "row": e.row, "field": e.field, "message": e.message})
                parsed_files.append((name, b64, d.parsed))
            except XlsxParseError as e:
                validation_errors.append({"file": name, "row": 0, "field": "file", "message": str(e)})

        if validation_errors:
            raise ValidationError(
                json.dumps(
                    {
                        "message": "Ошибка валидации входных файлов",
                        "errors": validation_errors[:500],
                    },
                    ensure_ascii=False,
                )
            )

        ingested = []
        resolved_date = business_date
        for name, b64, parsed in parsed_files:
            res = self._ingest_from_parsed(actor, source_ip, name, b64, parsed, business_date=business_date)
            ingested.append({"file_name": name, "result": res})
            if not resolved_date:
                resolved_date = res["business_date"]

        if not resolved_date:
            raise ValidationError("Не удалось определить business_date")

        run = self.run_matching(actor, source_ip, {"business_date": resolved_date, "scope_filter": "ALL"})
        return {
            "business_date": resolved_date,
            "run_id": run["run_id"],
            "ruleset_version": run["ruleset_version"],
            "matches_created": run["matches_created"],
            "exceptions_created": run["exceptions_created"],
            "ingested_files": ingested,
        }

    def ingest_xlsx_batch(self, actor: str, source_ip: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.check_permission(actor, "ingest:write")
        files = payload.get("files") or []
        if not isinstance(files, list) or not files:
            raise ValidationError("files[] is required")

        business_date = payload.get("business_date")
        results = []
        imported_records = 0

        for item in files:
            try:
                per_file_payload = {
                    "file_name": item.get("file_name"),
                    "file_base64": item.get("file_base64"),
                    "parser_profile": item.get("parser_profile"),
                    "business_date": business_date,
                    "source": item.get("source"),
                    "checksum_sha256": item.get("checksum_sha256"),
                    "received_at": payload.get("received_at"),
                }
                res = self.ingest_xlsx(actor, source_ip, per_file_payload)
                imported_records += int(res.get("record_count", 0))
                results.append({"file_name": item.get("file_name"), "ok": True, "result": res})
            except (ValidationError, ForbiddenError) as e:
                results.append({"file_name": item.get("file_name"), "ok": False, "error": str(e)})

        failed = sum(1 for r in results if not r["ok"])
        final_business_date = business_date
        if not final_business_date:
            for r in results:
                if r["ok"]:
                    final_business_date = r["result"].get("business_date")
                    break

        return {
            "business_date": final_business_date,
            "total_files": len(files),
            "failed_files": failed,
            "imported_records": imported_records,
            "items": results,
        }

    def _insert_txn(self, conn, source_system: str, business_date: str, rec: Dict[str, Any]) -> str:
        required = ["rrn", "amount", "currency", "txn_time", "merchant_id", "channel_id"]
        missing = [k for k in required if rec.get(k) in (None, "")]
        if missing:
            raise ValidationError(f"Transaction missing required fields: {', '.join(missing)}")

        masked_pan = sanitize_pan_masked(rec.get("pan_masked"))
        txn_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO txns(
              txn_id, source_system, business_date, rrn, arn, pan_masked, pan_hash, amount,
              currency, txn_time, op_type, merchant_id, channel_id, status_norm, fee_amount, fee_currency
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                txn_id,
                source_system,
                business_date,
                str(rec["rrn"]).strip().upper(),
                (rec.get("arn") or "").strip().upper() or None,
                masked_pan,
                hash_pan(masked_pan),
                float(rec["amount"]),
                normalize_currency(rec["currency"]),
                rec["txn_time"],
                normalize_op_type(rec.get("op_type", "PURCHASE")),
                str(rec["merchant_id"]).strip().upper(),
                str(rec["channel_id"]).strip().upper(),
                str(rec.get("status_norm", "BOOKED")).strip().upper(),
                float(rec.get("fee_amount", 0) or 0),
                normalize_currency(rec.get("fee_currency", rec.get("currency", ""))),
            ),
        )
        return txn_id

    def ingest_status(self, actor: str, file_id: str) -> Dict[str, Any]:
        self.check_permission(actor, "ingest:read")
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT file_id, source_system, business_date, file_name, status, record_count FROM ingest_files WHERE file_id=?",
                (file_id,),
            ).fetchone()
            if not row:
                raise NotFoundError("Ingest file not found")
            return dict(row)

    def _active_ruleset(self) -> RuleSet:
        with self.db.connect() as conn:
            row = conn.execute("SELECT version, json_text FROM rulesets WHERE is_active=1 LIMIT 1").fetchone()
            if not row:
                raise ValidationError("No active ruleset")
            payload = json.loads(row["json_text"])
            return RuleSet(
                version=row["version"],
                amount_tolerance=float(payload["amount_tolerance"]),
                date_window_days=int(payload["date_window_days"]),
                score_threshold=float(payload["score_threshold"]),
            )

    def run_matching(self, actor: str, source_ip: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.check_permission(actor, "match:execute")
        business_date = payload.get("business_date")
        if not business_date:
            raise ValidationError("business_date is required")
        scope_filter = payload.get("scope_filter", "ALL")

        rules = self._active_ruleset()

        with self.db.connect() as conn:
            way4_rows = conn.execute(
                "SELECT * FROM txns WHERE source_system LIKE 'WAY4%' AND business_date=?",
                (business_date,),
            ).fetchall()
            visa_rows = conn.execute(
                "SELECT * FROM txns WHERE source_system LIKE 'VISA%' AND business_date=?",
                (business_date,),
            ).fetchall()
            if not way4_rows and not visa_rows:
                raise ValidationError("No transactions for selected business_date")
            if not way4_rows or not visa_rows:
                raise ValidationError("Both sources are required: load Way4 and VISA for selected business_date")

            way4 = [self._to_txn(r) for r in way4_rows]
            visa = [self._to_txn(r) for r in visa_rows]

            run_id = str(uuid.uuid4())
            started = now_iso()
            conn.execute(
                "INSERT INTO match_runs(run_id, business_date, scope_filter, ruleset_version, started_at, status, created_by) VALUES(?,?,?,?,?,?,?)",
                (run_id, business_date, scope_filter, rules.version, started, "RUNNING", actor),
            )
            # Persist RUNNING status early so UI polling can display live progress.
            conn.commit()
            try:
                matches, exceptions = match_transactions(way4, visa, rules)
                for m in matches:
                    conn.execute(
                        """
                        INSERT INTO match_results(match_id, run_id, left_txn_id, right_txn_id, match_type, score, reason_code, explain_json)
                        VALUES(?,?,?,?,?,?,?,?)
                        """,
                        (
                            str(uuid.uuid4()),
                            run_id,
                            m["left_txn_id"],
                            m["right_txn_id"],
                            m["match_type"],
                            float(m["score"]),
                            m["reason_code"],
                            json.dumps(m["explain"], ensure_ascii=False),
                        ),
                    )

                for e in exceptions:
                    conn.execute(
                        """
                        INSERT INTO exception_cases(case_id, run_id, business_date, category, severity, status, primary_txn_id,
                        owner_user_id, aging_days, resolution_code, created_at, closed_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            str(uuid.uuid4()),
                            run_id,
                            business_date,
                            e["category"],
                            e["severity"],
                            "NEW",
                            e["primary_txn_id"],
                            None,
                            0,
                            None,
                            now_iso(),
                            None,
                        ),
                    )

                finished = now_iso()
                conn.execute(
                    "UPDATE match_runs SET finished_at=?, status='FINISHED' WHERE run_id=?",
                    (finished, run_id),
                )
                self._audit(
                    conn,
                    actor,
                    source_ip,
                    "match_run",
                    run_id,
                    "MATCH_RUN_EXECUTE",
                    "SUCCESS",
                    f"matches={len(matches)} exceptions={len(exceptions)}",
                )

                return {
                    "run_id": run_id,
                    "business_date": business_date,
                    "ruleset_version": rules.version,
                    "matches_created": len(matches),
                    "exceptions_created": len(exceptions),
                    "started_at": started,
                    "finished_at": finished,
                }
            except Exception as e:
                finished = now_iso()
                conn.execute(
                    "UPDATE match_runs SET finished_at=?, status='FAILED' WHERE run_id=?",
                    (finished, run_id),
                )
                self._audit(
                    conn,
                    actor,
                    source_ip,
                    "match_run",
                    run_id,
                    "MATCH_RUN_EXECUTE",
                    "FAILURE",
                    f"{type(e).__name__}: {str(e)[:300]}",
                )
                # RUNNING was committed earlier for UI polling, so FAILED state
                # must also be explicitly committed before re-raising.
                conn.commit()
                raise

    def latest_run_status(self, actor: str, business_date: str) -> Dict[str, Any]:
        self.check_permission(actor, "match:read")
        if not business_date:
            raise ValidationError("business_date is required")
        with self.db.connect() as conn:
            run = conn.execute(
                """
                SELECT run_id, business_date, ruleset_version, started_at, finished_at, status, created_by
                FROM match_runs
                WHERE business_date=?
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (business_date,),
            ).fetchone()
            if not run:
                return {"business_date": business_date, "has_run": False}

            out = dict(run)
            out["has_run"] = True
            if run["status"] == "FINISHED":
                m = conn.execute("SELECT COUNT(*) c FROM match_results WHERE run_id=?", (run["run_id"],)).fetchone()["c"]
                e = conn.execute("SELECT COUNT(*) c FROM exception_cases WHERE run_id=?", (run["run_id"],)).fetchone()["c"]
                out["matches_created"] = int(m)
                out["exceptions_created"] = int(e)
            else:
                out["matches_created"] = None
                out["exceptions_created"] = None
            return out

    def _to_txn(self, row) -> Txn:
        return Txn(
            txn_id=row["txn_id"],
            source_system=row["source_system"],
            business_date=row["business_date"],
            rrn=row["rrn"],
            arn=row["arn"],
            amount=float(row["amount"]),
            currency=row["currency"],
            txn_time=row["txn_time"],
            op_type=row["op_type"],
            merchant_id=row["merchant_id"],
            channel_id=row["channel_id"],
        )

    def get_run(self, actor: str, run_id: str) -> Dict[str, Any]:
        self.check_permission(actor, "match:read")
        with self.db.connect() as conn:
            run = conn.execute("SELECT * FROM match_runs WHERE run_id=?", (run_id,)).fetchone()
            if not run:
                raise NotFoundError("Run not found")

            result_rows = conn.execute(
                "SELECT match_type, COUNT(*) as cnt FROM match_results WHERE run_id=? GROUP BY match_type",
                (run_id,),
            ).fetchall()
            exc_rows = conn.execute(
                "SELECT category, COUNT(*) as cnt FROM exception_cases WHERE run_id=? GROUP BY category",
                (run_id,),
            ).fetchall()

            return {
                "run": dict(run),
                "match_summary": {r["match_type"]: r["cnt"] for r in result_rows},
                "exception_summary": {r["category"]: r["cnt"] for r in exc_rows},
            }

    def list_runs(self, actor: str, limit: int = 50, business_date: Optional[str] = None) -> Dict[str, Any]:
        self.check_permission(actor, "match:read")
        safe_limit = max(1, min(int(limit), 500))
        with self.db.connect() as conn:
            if business_date:
                rows = conn.execute(
                    """
                    SELECT run_id, business_date, scope_filter, ruleset_version, started_at, finished_at, status, created_by
                    FROM match_runs
                    WHERE business_date=?
                    ORDER BY started_at DESC
                    LIMIT ?
                    """,
                    (business_date, safe_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT run_id, business_date, scope_filter, ruleset_version, started_at, finished_at, status, created_by
                    FROM match_runs
                    ORDER BY started_at DESC
                    LIMIT ?
                    """,
                    (safe_limit,),
                ).fetchall()
            return {"items": [dict(r) for r in rows], "count": len(rows)}

    def get_run_results(self, actor: str, run_id: str, filters: Dict[str, Any]) -> Dict[str, Any]:
        self.check_permission(actor, "match:read")
        if not run_id:
            raise ValidationError("run_id is required")

        try:
            page = max(1, int(filters.get("page", 1)))
        except (TypeError, ValueError):
            raise ValidationError("page must be an integer >= 1")
        try:
            page_size = max(1, min(int(filters.get("page_size", 50)), 200))
        except (TypeError, ValueError):
            raise ValidationError("page_size must be an integer")
        offset = (page - 1) * page_size

        status = (filters.get("status") or "").strip().upper()
        search = (filters.get("q") or "").strip()
        currency = (filters.get("currency") or "").strip().upper()
        amount_min = filters.get("amount_min")
        amount_max = filters.get("amount_max")
        sort_by = (filters.get("sort_by") or "txn_time").strip().lower()
        sort_dir = (filters.get("sort_dir") or "desc").strip().lower()

        sort_columns = {
            "txn_time": "u.txn_time",
            "delta": "COALESCE(u.delta, 0)",
            "match_score": "COALESCE(u.match_score, -1)",
        }
        order_col = sort_columns.get(sort_by, "u.txn_time")
        order_dir = "ASC" if sort_dir == "asc" else "DESC"

        where = ["1=1"]
        params: List[Any] = []
        if status:
            where.append("u.status = ?")
            params.append(status)
        if search:
            like = f"%{search}%"
            where.append("(u.rrn LIKE ? OR u.arn LIKE ? OR u.pan_masked LIKE ?)")
            params.extend([like, like, like])
        if currency:
            where.append("u.currency = ?")
            params.append(currency)
        if amount_min not in (None, ""):
            try:
                amount_min_value = float(amount_min)
            except (TypeError, ValueError):
                raise ValidationError("amount_min must be a number")
            where.append("COALESCE(u.amount_way4, u.amount_visa, 0) >= ?")
            params.append(amount_min_value)
        if amount_max not in (None, ""):
            try:
                amount_max_value = float(amount_max)
            except (TypeError, ValueError):
                raise ValidationError("amount_max must be a number")
            where.append("COALESCE(u.amount_way4, u.amount_visa, 0) <= ?")
            params.append(amount_max_value)
            if amount_min not in (None, "") and amount_min_value > amount_max_value:
                raise ValidationError("amount_min must be <= amount_max")
        where_sql = " AND ".join(where)

        base_cte = """
        WITH unified AS (
          SELECT
            ('M:' || m.match_id) AS row_id,
            m.run_id AS run_id,
            CASE
              WHEN m.match_type='MATCHED' THEN 'MATCHED'
              WHEN m.match_type='PARTIAL_MATCH' THEN 'PARTIAL'
              WHEN m.match_type='DUPLICATE_SUSPECT' THEN 'DUPLICATE'
              ELSE 'MISMATCH'
            END AS status,
            wl.rrn AS rrn,
            COALESCE(NULLIF(wl.arn,''), NULLIF(wr.arn,'')) AS arn,
            COALESCE(wl.txn_time, wr.txn_time) AS txn_time,
            wl.amount AS amount_way4,
            wr.amount AS amount_visa,
            CASE
              WHEN wl.amount IS NOT NULL AND wr.amount IS NOT NULL THEN ROUND(wr.amount - wl.amount, 2)
              ELSE NULL
            END AS delta,
            COALESCE(wl.currency, wr.currency) AS currency,
            m.score AS match_score,
            m.reason_code AS rule_reason,
            COALESCE(wl.pan_masked, wr.pan_masked, '') AS pan_masked
          FROM match_results m
          LEFT JOIN txns wl ON wl.txn_id = m.left_txn_id
          LEFT JOIN txns wr ON wr.txn_id = m.right_txn_id
          WHERE m.run_id = ?

          UNION ALL

          SELECT
            ('E:' || e.case_id) AS row_id,
            e.run_id AS run_id,
            CASE
              WHEN e.category='MISSING_IN_WAY4' THEN 'MISSING_IN_WAY4'
              WHEN e.category='MISSING_IN_VISA' THEN 'MISSING_IN_VISA'
              WHEN e.category='DUPLICATE' THEN 'DUPLICATE'
              WHEN e.category='AMOUNT_MISMATCH' THEN 'MISMATCH'
              WHEN e.category='DATE_MISMATCH' THEN 'MISMATCH'
              WHEN e.category='STATUS_MISMATCH' THEN 'MISMATCH'
              WHEN e.category='OPTYPE_MISMATCH' THEN 'MISMATCH'
              ELSE 'MISMATCH'
            END AS status,
            t.rrn AS rrn,
            t.arn AS arn,
            t.txn_time AS txn_time,
            CASE WHEN t.source_system LIKE 'WAY4%' THEN t.amount ELSE NULL END AS amount_way4,
            CASE WHEN t.source_system LIKE 'VISA%' THEN t.amount ELSE NULL END AS amount_visa,
            NULL AS delta,
            t.currency AS currency,
            NULL AS match_score,
            e.category AS rule_reason,
            COALESCE(t.pan_masked, '') AS pan_masked
          FROM exception_cases e
          LEFT JOIN txns t ON t.txn_id = e.primary_txn_id
          WHERE e.run_id = ?
        )
        """

        with self.db.connect() as conn:
            run = conn.execute(
                """
                SELECT run_id, business_date, ruleset_version, started_at, finished_at, status, created_by
                FROM match_runs
                WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
            if not run:
                raise NotFoundError("Run not found")

            summary_sql = (
                base_cte
                + """
                SELECT
                  SUM(CASE WHEN status='MATCHED' THEN 1 ELSE 0 END) AS matched,
                  SUM(CASE WHEN status='MISSING_IN_WAY4' THEN 1 ELSE 0 END) AS unmatched_way4,
                  SUM(CASE WHEN status='MISSING_IN_VISA' THEN 1 ELSE 0 END) AS unmatched_visa,
                  SUM(CASE WHEN status='PARTIAL' THEN 1 ELSE 0 END) AS partial,
                  SUM(CASE WHEN status='DUPLICATE' THEN 1 ELSE 0 END) AS duplicates,
                  ROUND(COALESCE(SUM(ABS(COALESCE(delta, 0))), 0), 2) AS amount_delta
                FROM unified
                """
            )
            summary = conn.execute(summary_sql, (run_id, run_id)).fetchone()

            filter_params = [run_id, run_id] + params
            total = conn.execute(base_cte + f" SELECT COUNT(*) AS c FROM unified u WHERE {where_sql} ", filter_params).fetchone()[
                "c"
            ]

            item_sql = (
                base_cte
                + f"""
                SELECT
                  u.row_id, u.status, u.rrn, u.arn, u.txn_time,
                  u.amount_way4, u.amount_visa, u.delta, u.currency, u.match_score, u.rule_reason, u.pan_masked
                FROM unified u
                WHERE {where_sql}
                ORDER BY {order_col} {order_dir}, u.row_id DESC
                LIMIT {page_size} OFFSET {offset}
                """
            )
            items = conn.execute(item_sql, filter_params).fetchall()
            return {
                "run": dict(run),
                "summary": {
                    "matched": int(summary["matched"] or 0),
                    "unmatched_way4": int(summary["unmatched_way4"] or 0),
                    "unmatched_visa": int(summary["unmatched_visa"] or 0),
                    "partial": int(summary["partial"] or 0),
                    "duplicates": int(summary["duplicates"] or 0),
                    "amount_delta": float(summary["amount_delta"] or 0),
                },
                "items": [dict(r) for r in items],
                "page": page,
                "page_size": page_size,
                "total": int(total or 0),
                "total_pages": int((int(total or 0) + page_size - 1) // page_size),
            }

    def get_latest_results(self, actor: str, business_date: str, filters: Dict[str, Any]) -> Dict[str, Any]:
        self.check_permission(actor, "match:read")
        if not business_date:
            raise ValidationError("business_date is required")

        with self.db.connect() as conn:
            run = conn.execute(
                """
                SELECT run_id
                FROM match_runs
                WHERE business_date=?
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (business_date,),
            ).fetchone()
            if not run:
                return {"has_run": False, "business_date": business_date}

        out = self.get_run_results(actor, run["run_id"], filters)
        out["has_run"] = True
        return out

    def _iter_unified_rows_for_run(self, conn, run_id: str):
        sql = """
        WITH unified AS (
          SELECT
            ('M:' || m.match_id) AS row_id,
            m.run_id AS run_id,
            CASE
              WHEN m.match_type='MATCHED' THEN 'MATCHED'
              WHEN m.match_type='PARTIAL_MATCH' THEN 'PARTIAL'
              WHEN m.match_type='DUPLICATE_SUSPECT' THEN 'DUPLICATE'
              ELSE 'MISMATCH'
            END AS status,
            wl.rrn AS rrn,
            COALESCE(NULLIF(wl.arn,''), NULLIF(wr.arn,'')) AS arn,
            COALESCE(wl.txn_time, wr.txn_time) AS txn_time,
            wl.amount AS amount_way4,
            wr.amount AS amount_visa,
            CASE
              WHEN wl.amount IS NOT NULL AND wr.amount IS NOT NULL THEN ROUND(wr.amount - wl.amount, 2)
              ELSE NULL
            END AS delta,
            COALESCE(wl.currency, wr.currency) AS currency,
            m.score AS match_score,
            m.reason_code AS rule_reason,
            COALESCE(wl.pan_masked, wr.pan_masked, '') AS pan_masked,
            m.left_txn_id AS left_txn_id,
            m.right_txn_id AS right_txn_id
          FROM match_results m
          LEFT JOIN txns wl ON wl.txn_id = m.left_txn_id
          LEFT JOIN txns wr ON wr.txn_id = m.right_txn_id
          WHERE m.run_id = ?

          UNION ALL

          SELECT
            ('E:' || e.case_id) AS row_id,
            e.run_id AS run_id,
            CASE
              WHEN e.category='MISSING_IN_WAY4' THEN 'MISSING_IN_WAY4'
              WHEN e.category='MISSING_IN_VISA' THEN 'MISSING_IN_VISA'
              WHEN e.category='DUPLICATE' THEN 'DUPLICATE'
              WHEN e.category='AMOUNT_MISMATCH' THEN 'MISMATCH'
              WHEN e.category='DATE_MISMATCH' THEN 'MISMATCH'
              WHEN e.category='STATUS_MISMATCH' THEN 'MISMATCH'
              WHEN e.category='OPTYPE_MISMATCH' THEN 'MISMATCH'
              ELSE 'MISMATCH'
            END AS status,
            t.rrn AS rrn,
            t.arn AS arn,
            t.txn_time AS txn_time,
            CASE WHEN t.source_system LIKE 'WAY4%' THEN t.amount ELSE NULL END AS amount_way4,
            CASE WHEN t.source_system LIKE 'VISA%' THEN t.amount ELSE NULL END AS amount_visa,
            NULL AS delta,
            t.currency AS currency,
            NULL AS match_score,
            e.category AS rule_reason,
            COALESCE(t.pan_masked, '') AS pan_masked,
            CASE WHEN t.source_system LIKE 'WAY4%' THEN t.txn_id ELSE NULL END AS left_txn_id,
            CASE WHEN t.source_system LIKE 'VISA%' THEN t.txn_id ELSE NULL END AS right_txn_id
          FROM exception_cases e
          LEFT JOIN txns t ON t.txn_id = e.primary_txn_id
          WHERE e.run_id = ?
        )
        SELECT *
        FROM unified
        ORDER BY txn_time DESC, row_id DESC
        """
        cur = conn.execute(sql, (run_id, run_id))
        while True:
            batch = cur.fetchmany(2000)
            if not batch:
                break
            for row in batch:
                yield dict(row)

    def _diff_fields_text(self, conn, left_txn_id: Optional[str], right_txn_id: Optional[str]) -> str:
        if not left_txn_id or not right_txn_id:
            return ""
        left = conn.execute("SELECT * FROM txns WHERE txn_id=?", (left_txn_id,)).fetchone()
        right = conn.execute("SELECT * FROM txns WHERE txn_id=?", (right_txn_id,)).fetchone()
        diffs = self._build_differences(dict(left) if left else None, dict(right) if right else None)
        return ",".join(d["field"] for d in diffs)

    def export_run_unmatched_csv_file(self, actor: str, run_id: str, side: str) -> tuple[Path, str]:
        self.check_permission(actor, "exceptions:read")
        side_norm = side.lower()
        if side_norm not in {"way4", "visa"}:
            raise ValidationError("side must be way4 or visa")

        target_status = "MISSING_IN_VISA" if side_norm == "way4" else "MISSING_IN_WAY4"
        with self.db.connect() as conn:
            run = conn.execute("SELECT run_id, business_date FROM match_runs WHERE run_id=?", (run_id,)).fetchone()
            if not run:
                raise NotFoundError("Run not found")
            tmp_path = Path(tempfile.mkstemp(prefix=f"unmatched_{side_norm}_", suffix=".csv")[1])

            def rows():
                for r in self._iter_unified_rows_for_run(conn, run_id):
                    if r["status"] != target_status:
                        continue
                    yield (
                        r["row_id"],
                        r["status"],
                        r["rrn"],
                        r["arn"],
                        r["txn_time"],
                        r["amount_way4"],
                        r["amount_visa"],
                        r["delta"],
                        r["currency"],
                        r["rule_reason"],
                        r["pan_masked"],
                    )

            write_csv_file(
                tmp_path,
                [
                    "row_id",
                    "status",
                    "rrn",
                    "arn",
                    "txn_time",
                    "amount_way4",
                    "amount_visa",
                    "delta",
                    "currency",
                    "reason",
                    "pan_masked",
                ],
                rows(),
            )
            filename = f"unmatched_{side_norm}_{run['business_date']}_{run_id[:8]}.csv"
            return tmp_path, filename

    def export_run_mismatches_partial_xlsx_file(self, actor: str, run_id: str) -> tuple[Path, str]:
        self.check_permission(actor, "exceptions:read")
        with self.db.connect() as conn:
            run = conn.execute("SELECT run_id, business_date FROM match_runs WHERE run_id=?", (run_id,)).fetchone()
            if not run:
                raise NotFoundError("Run not found")
            builder = SimpleXlsxBuilder()

            def rows():
                for r in self._iter_unified_rows_for_run(conn, run_id):
                    if r["status"] not in {"MISMATCH", "PARTIAL"}:
                        continue
                    yield (
                        r["status"],
                        r["rrn"],
                        r["arn"],
                        r["txn_time"],
                        r["amount_way4"],
                        r["amount_visa"],
                        r["delta"],
                        r["currency"],
                        r["match_score"],
                        r["rule_reason"],
                        self._diff_fields_text(conn, r.get("left_txn_id"), r.get("right_txn_id")),
                        r["pan_masked"],
                    )

            builder.add_sheet(
                "Mismatches_Partial",
                [
                    "status",
                    "rrn",
                    "arn",
                    "txn_time",
                    "amount_way4",
                    "amount_visa",
                    "delta",
                    "currency",
                    "match_score",
                    "reason",
                    "different_fields",
                    "pan_masked",
                ],
                rows(),
            )
            tmp_path = Path(tempfile.mkstemp(prefix="mismatch_partial_", suffix=".xlsx")[1])
            builder.build(tmp_path)
            builder.cleanup()
            filename = f"mismatches_partial_{run['business_date']}_{run_id[:8]}.xlsx"
            return tmp_path, filename

    def export_run_xlsx_file(self, actor: str, run_id: str) -> tuple[Path, str]:
        self.check_permission(actor, "exceptions:read")
        with self.db.connect() as conn:
            run = conn.execute(
                """
                SELECT run_id, business_date, ruleset_version, started_at, finished_at, status, created_by
                FROM match_runs WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
            if not run:
                raise NotFoundError("Run not found")

            files = conn.execute(
                "SELECT file_name, source_system FROM ingest_files WHERE business_date=? ORDER BY source_system, file_name",
                (run["business_date"],),
            ).fetchall()
            file_names = ", ".join(f"{f['source_system']}:{f['file_name']}" for f in files) or "-"

            summary = self.get_run_results(actor, run_id, {"page": 1, "page_size": 1})["summary"]
            builder = SimpleXlsxBuilder()

            builder.add_sheet(
                "Summary",
                ["metric", "value"],
                [
                    ("run_id", run["run_id"]),
                    ("business_date", run["business_date"]),
                    ("ruleset_version", run["ruleset_version"]),
                    ("status", run["status"]),
                    ("started_at", run["started_at"]),
                    ("finished_at", run["finished_at"]),
                    ("created_by", run["created_by"]),
                    ("input_files", file_names),
                    ("matched", summary["matched"]),
                    ("unmatched_way4", summary["unmatched_way4"]),
                    ("unmatched_visa", summary["unmatched_visa"]),
                    ("partial", summary["partial"]),
                    ("duplicates", summary["duplicates"]),
                    ("amount_delta", summary["amount_delta"]),
                ],
            )

            def rows_by_status(statuses: set[str], include_diff=False):
                for r in self._iter_unified_rows_for_run(conn, run_id):
                    if r["status"] not in statuses:
                        continue
                    base = [
                        r["status"],
                        r["rrn"],
                        r["arn"],
                        r["txn_time"],
                        r["amount_way4"],
                        r["amount_visa"],
                        r["delta"],
                        r["currency"],
                        r["match_score"],
                        r["rule_reason"],
                        r["pan_masked"],
                    ]
                    if include_diff:
                        base.append(self._diff_fields_text(conn, r.get("left_txn_id"), r.get("right_txn_id")))
                    yield tuple(base)

            headers = [
                "status",
                "rrn",
                "arn",
                "txn_time",
                "amount_way4",
                "amount_visa",
                "delta",
                "currency",
                "match_score",
                "reason",
                "pan_masked",
            ]
            builder.add_sheet("Matched", headers, rows_by_status({"MATCHED"}))
            builder.add_sheet("Unmatched_Way4", headers, rows_by_status({"MISSING_IN_VISA"}))
            builder.add_sheet("Unmatched_VISA", headers, rows_by_status({"MISSING_IN_WAY4"}))
            builder.add_sheet(
                "Mismatches_Partial",
                headers + ["different_fields"],
                rows_by_status({"MISMATCH", "PARTIAL"}, include_diff=True),
            )
            builder.add_sheet("Duplicates", headers, rows_by_status({"DUPLICATE"}))

            tmp_path = Path(tempfile.mkstemp(prefix="run_export_", suffix=".xlsx")[1])
            builder.build(tmp_path)
            builder.cleanup()
            filename = f"reconciliation_report_{run['business_date']}_{run_id[:8]}.xlsx"
            return tmp_path, filename

    def get_result_details(self, actor: str, row_id: str) -> Dict[str, Any]:
        self.check_permission(actor, "match:read")
        if not row_id or ":" not in row_id:
            raise ValidationError("Invalid row_id")
        prefix, raw_id = row_id.split(":", 1)
        prefix = prefix.upper()

        with self.db.connect() as conn:
            if prefix == "M":
                m = conn.execute(
                    """
                    SELECT m.match_id, m.run_id, m.left_txn_id, m.right_txn_id, m.reason_code, m.score, m.explain_json
                    FROM match_results m
                    WHERE m.match_id=?
                    """,
                    (raw_id,),
                ).fetchone()
                if not m:
                    raise NotFoundError("Result row not found")
                left = conn.execute("SELECT * FROM txns WHERE txn_id=?", (m["left_txn_id"],)).fetchone()
                right = conn.execute("SELECT * FROM txns WHERE txn_id=?", (m["right_txn_id"],)).fetchone()
                explain = {}
                try:
                    explain = json.loads(m["explain_json"] or "{}")
                except Exception:
                    explain = {"raw": m["explain_json"]}
                diffs = self._build_differences(dict(left) if left else None, dict(right) if right else None)
                return {
                    "row_id": row_id,
                    "run_id": m["run_id"],
                    "left_record": self._public_txn(left),
                    "right_record": self._public_txn(right),
                    "differences": diffs,
                    "reason_code": m["reason_code"],
                    "score": m["score"],
                    "explain_json": explain,
                }

            if prefix == "E":
                e = conn.execute(
                    """
                    SELECT e.case_id, e.run_id, e.category, e.primary_txn_id
                    FROM exception_cases e
                    WHERE e.case_id=?
                    """,
                    (raw_id,),
                ).fetchone()
                if not e:
                    raise NotFoundError("Result row not found")
                primary = conn.execute("SELECT * FROM txns WHERE txn_id=?", (e["primary_txn_id"],)).fetchone()
                if not primary:
                    raise NotFoundError("Primary transaction not found")
                primary_d = dict(primary)
                diag = self._build_exception_diagnostics(conn, primary, primary_d["business_date"])
                top = (diag.get("top_candidates") or [])
                candidate = None
                if top:
                    candidate = conn.execute("SELECT * FROM txns WHERE txn_id=?", (top[0]["txn_id"],)).fetchone()
                p_src = primary_d.get("source_system", "")
                if p_src.startswith("WAY4"):
                    left = primary
                    right = candidate
                else:
                    left = candidate
                    right = primary
                diffs = self._build_differences(dict(left) if left else None, dict(right) if right else None)
                return {
                    "row_id": row_id,
                    "run_id": e["run_id"],
                    "left_record": self._public_txn(left),
                    "right_record": self._public_txn(right),
                    "differences": diffs,
                    "reason_code": e["category"],
                    "score": None,
                    "explain_json": {"top_reasons": diag.get("top_reasons", []), "candidate_source": diag.get("candidate_source")},
                }

            raise ValidationError("Unsupported row_id prefix")

    def _public_txn(self, row) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        d = dict(row)
        d.pop("pan_hash", None)
        return d

    def _build_differences(self, left: Optional[Dict[str, Any]], right: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        fields = [
            "rrn",
            "arn",
            "pan_masked",
            "amount",
            "currency",
            "txn_time",
            "status_norm",
            "op_type",
            "merchant_id",
            "channel_id",
            "fee_amount",
            "fee_currency",
        ]
        high = {"rrn", "arn", "amount", "currency"}
        medium = {"txn_time", "status_norm", "op_type", "merchant_id", "channel_id"}
        out = []
        for f in fields:
            l = left.get(f) if left else None
            r = right.get(f) if right else None
            same = (l == r)
            if same:
                continue
            if f in high:
                sev = "HIGH"
            elif f in medium:
                sev = "MEDIUM"
            else:
                sev = "LOW"
            out.append({"field": f, "left": l, "right": r, "severity": sev})
        return out

    def source_balance(self, actor: str, business_date: str) -> Dict[str, Any]:
        self.check_permission(actor, "match:read")
        if not business_date:
            raise ValidationError("business_date is required")
        with self.db.connect() as conn:
            total_way4 = conn.execute(
                "SELECT COUNT(*) AS c FROM txns WHERE source_system LIKE 'WAY4%' AND business_date=?",
                (business_date,),
            ).fetchone()["c"]
            total_visa = conn.execute(
                "SELECT COUNT(*) AS c FROM txns WHERE source_system LIKE 'VISA%' AND business_date=?",
                (business_date,),
            ).fetchone()["c"]
            files_way4 = conn.execute(
                "SELECT COUNT(*) AS c FROM ingest_files WHERE source_system LIKE 'WAY4%' AND business_date=?",
                (business_date,),
            ).fetchone()["c"]
            files_visa = conn.execute(
                "SELECT COUNT(*) AS c FROM ingest_files WHERE source_system LIKE 'VISA%' AND business_date=?",
                (business_date,),
            ).fetchone()["c"]

            ready = total_way4 > 0 and total_visa > 0
            warnings = []
            if total_way4 <= 0:
                warnings.append("Нет данных Way4 за выбранную дату")
            if total_visa <= 0:
                warnings.append("Нет данных VISA за выбранную дату")
            if total_way4 > 0 and total_visa > 0:
                ratio = total_way4 / max(total_visa, 1)
                if ratio < 0.3 or ratio > 3.0:
                    warnings.append("Сильный перекос объема между Way4 и VISA")
            else:
                ratio = None

            return {
                "business_date": business_date,
                "way4_records": int(total_way4),
                "visa_records": int(total_visa),
                "way4_files": int(files_way4),
                "visa_files": int(files_visa),
                "ratio_way4_to_visa": round(ratio, 4) if ratio is not None else None,
                "ready_for_matching": ready,
                "warnings": warnings,
            }

    def export_unmatched_csv(self, actor: str, business_date: str, run_id: Optional[str] = None) -> str:
        self.check_permission(actor, "exceptions:read")
        if not business_date:
            raise ValidationError("business_date is required")

        with self.db.connect() as conn:
            clauses = ["e.business_date=?", "e.status!='CLOSED'"]
            params: List[Any] = [business_date]
            if run_id:
                clauses.append("e.run_id=?")
                params.append(run_id)
            where = " AND ".join(clauses)

            rows = conn.execute(
                f"""
                SELECT
                  e.case_id, e.run_id, e.business_date, e.category, e.severity, e.status, e.owner_user_id,
                  e.created_at, e.primary_txn_id,
                  t.source_system, t.rrn, t.arn, t.pan_masked, t.amount, t.currency, t.txn_time, t.op_type,
                  t.merchant_id, t.channel_id, t.status_norm
                FROM exception_cases e
                LEFT JOIN txns t ON t.txn_id = e.primary_txn_id
                WHERE {where}
                ORDER BY e.created_at DESC
                """,
                params,
            ).fetchall()

            out = StringIO()
            writer = csv.writer(out, delimiter=";")
            writer.writerow(
                [
                    "case_id",
                    "run_id",
                    "business_date",
                    "category",
                    "severity",
                    "status",
                    "owner_user_id",
                    "created_at",
                    "source_system",
                    "rrn",
                    "arn",
                    "pan_masked",
                    "amount",
                    "currency",
                    "txn_time",
                    "op_type",
                    "merchant_id",
                    "channel_id",
                    "status_norm",
                    "reason_1",
                    "reason_2",
                    "reason_3",
                    "candidate_source",
                    "candidate_1",
                    "candidate_2",
                    "candidate_3",
                ]
            )

            for r in rows:
                rd = dict(r)
                category = rd.get("category", "")
                if category == "MISSING_IN_VISA":
                    reasons = [
                        "Запись присутствует в Way4, но не найдена в VISA за выбранную дату",
                        "Проверьте корректность business_date и cut-off",
                        "Проверьте RRN/ARN и полноту выгрузки VISA",
                    ]
                elif category == "MISSING_IN_WAY4":
                    reasons = [
                        "Запись присутствует в VISA, но не найдена в Way4 за выбранную дату",
                        "Проверьте корректность business_date и cut-off",
                        "Проверьте RRN/ARN и полноту выгрузки Way4",
                    ]
                elif category == "DUPLICATE":
                    reasons = [
                        "Обнаружены множественные кандидаты (дубликаты)",
                        "Проверьте уникальность RRN/ARN в источниках",
                        "Требуется ручной разбор кейса",
                    ]
                elif category == "AMOUNT_MISMATCH":
                    reasons = [
                        "RRN/ARN совпал, но сумма отличается",
                        "Проверьте fee/FX и допуск по сумме",
                        "Проверьте one-to-many/partial сценарии",
                    ]
                elif category == "DATE_MISMATCH":
                    reasons = [
                        "Дата транзакции вне допускаемого окна",
                        "Проверьте timezone и cut-off",
                        "Проверьте позднюю проводку/поздний клиринг",
                    ]
                else:
                    reasons = [
                        "Запись не сопоставлена автоматически",
                        "Проверьте поля rrn/arn/amount/currency/date/op_type",
                        "Требуется ручной анализ в карточке исключения",
                    ]

                writer.writerow(
                    [
                        rd.get("case_id", ""),
                        rd.get("run_id", ""),
                        rd.get("business_date", ""),
                        rd.get("category", ""),
                        rd.get("severity", ""),
                        rd.get("status", ""),
                        rd.get("owner_user_id", ""),
                        rd.get("created_at", ""),
                        rd.get("source_system", ""),
                        rd.get("rrn", ""),
                        rd.get("arn", ""),
                        rd.get("pan_masked", ""),
                        rd.get("amount", ""),
                        rd.get("currency", ""),
                        rd.get("txn_time", ""),
                        rd.get("op_type", ""),
                        rd.get("merchant_id", ""),
                        rd.get("channel_id", ""),
                        rd.get("status_norm", ""),
                        reasons[0],
                        reasons[1],
                        reasons[2],
                        "N/A",
                        "",
                        "",
                        "",
                    ]
                )

            # BOM for better opening in Excel with Cyrillic headers.
            return "\ufeff" + out.getvalue()

    def list_exceptions(self, actor: str, filters: Dict[str, Any]) -> Dict[str, Any]:
        self.check_permission(actor, "exceptions:read")
        clauses = []
        params: List[Any] = []
        for key in ("business_date", "category", "status", "run_id"):
            if filters.get(key):
                clauses.append(f"{key}=?")
                params.append(filters[key])
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        with self.db.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM exception_cases {where} ORDER BY created_at DESC LIMIT 500",
                params,
            ).fetchall()
            return {"items": [dict(r) for r in rows], "count": len(rows)}

    def get_exception(self, actor: str, case_id: str) -> Dict[str, Any]:
        self.check_permission(actor, "exceptions:read")
        with self.db.connect() as conn:
            case = conn.execute("SELECT * FROM exception_cases WHERE case_id=?", (case_id,)).fetchone()
            if not case:
                raise NotFoundError("Exception case not found")
            txn = conn.execute("SELECT * FROM txns WHERE txn_id=?", (case["primary_txn_id"],)).fetchone()
            actions = conn.execute(
                "SELECT action_id, actor_user_id, action_at, action_type, action_payload FROM exception_actions WHERE case_id=? ORDER BY action_at",
                (case_id,),
            ).fetchall()
            diagnostics = self._build_exception_diagnostics(conn, txn, case["business_date"]) if txn else None
            return {
                "case": dict(case),
                "transaction": self._public_txn(txn),
                "actions": [dict(a) for a in actions],
                "diagnostics": diagnostics,
            }

    def _build_exception_diagnostics(self, conn, txn_row, business_date: str) -> Dict[str, Any]:
        rules = self._active_ruleset()
        tx = dict(txn_row)
        rrn = tx.get("rrn")
        currency = tx.get("currency")
        amount = float(tx.get("amount") or 0.0)
        op_type = tx.get("op_type")
        txn_time = tx.get("txn_time")
        source = tx.get("source_system") or ""

        opposite_like = "VISA%" if source.startswith("WAY4") else "WAY4%"
        opposite_label = "VISA" if opposite_like.startswith("VISA") else "Way4"

        opposite_rows = conn.execute(
            "SELECT * FROM txns WHERE source_system LIKE ? AND business_date=?",
            (opposite_like, business_date),
        ).fetchall()

        rrn_rows = [r for r in opposite_rows if (r["rrn"] or "") == rrn]
        rrn_cur_rows = [r for r in rrn_rows if (r["currency"] or "") == currency]

        reasons: List[str] = []
        if not rrn_rows:
            reasons.append(f"В источнике {opposite_label} не найдено записей с тем же RRN")
        else:
            if len(rrn_rows) > 1:
                reasons.append(f"Найдено несколько записей с одинаковым RRN в {opposite_label} ({len(rrn_rows)} шт.)")
            if not rrn_cur_rows:
                reasons.append(f"RRN найден, но валюта не совпадает (ожидалась {currency})")
            else:
                in_tolerance = [
                    r
                    for r in rrn_cur_rows
                    if abs(float(r["amount"] or 0.0) - amount) <= rules.amount_tolerance
                ]
                if not in_tolerance:
                    min_delta = min(abs(float(r["amount"] or 0.0) - amount) for r in rrn_cur_rows)
                    reasons.append(
                        f"RRN и валюта совпадают, но сумма вне допуска ±{rules.amount_tolerance} (мин. дельта {round(min_delta,2)})"
                    )

                date_window_rows = []
                base_dt = self._safe_parse_iso(txn_time)
                for r in rrn_cur_rows:
                    other_dt = self._safe_parse_iso(r["txn_time"])
                    if base_dt and other_dt:
                        diff_days = abs((base_dt - other_dt).total_seconds()) / 86400.0
                        if diff_days <= rules.date_window_days:
                            date_window_rows.append(r)
                if not date_window_rows and rrn_cur_rows:
                    reasons.append(f"Дата транзакции вне окна ±{rules.date_window_days} дней")

        if rrn_cur_rows and not reasons:
            reasons.append("Проверить правила op_type/fee и one-to-many комбинации")

        def candidate_score(r):
            score = 0
            if (r["rrn"] or "") == rrn:
                score += 50
            if (r["currency"] or "") == currency:
                score += 20
            score += max(0, 20 - int(abs(float(r["amount"] or 0.0) - amount)))
            if (r["op_type"] or "") == op_type:
                score += 10
            return score

        top_candidates = sorted(opposite_rows, key=candidate_score, reverse=True)[:3]
        compact_candidates = []
        for r in top_candidates:
            compact_candidates.append(
                {
                    "txn_id": r["txn_id"],
                    "rrn": r["rrn"],
                    "amount": float(r["amount"] or 0.0),
                    "currency": r["currency"],
                    "txn_time": r["txn_time"],
                    "op_type": r["op_type"],
                    "merchant_id": r["merchant_id"],
                    "score_hint": candidate_score(r),
                }
            )

        return {
            "top_reasons": reasons[:3],
            "ruleset": {
                "version": rules.version,
                "amount_tolerance": rules.amount_tolerance,
                "date_window_days": rules.date_window_days,
                "score_threshold": rules.score_threshold,
            },
            "candidate_source": opposite_label,
            "top_candidates": compact_candidates,
        }

    def _safe_parse_iso(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None

    def exception_action(self, actor: str, source_ip: str, case_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.check_permission(actor, "exceptions:write")
        action_type = payload.get("action_type")
        if action_type not in {"assign", "comment", "status_change", "close"}:
            raise ValidationError("Unsupported action_type")

        with self.db.connect() as conn:
            case = conn.execute("SELECT * FROM exception_cases WHERE case_id=?", (case_id,)).fetchone()
            if not case:
                raise NotFoundError("Exception case not found")

            action_payload = dict(payload)
            if action_type == "assign":
                owner = payload.get("owner_user_id")
                if not owner:
                    raise ValidationError("owner_user_id is required for assign")
                owner_row = conn.execute("SELECT login, status FROM users WHERE login=?", (owner,)).fetchone()
                if not owner_row or owner_row["status"] != "ACTIVE":
                    raise ValidationError("owner_user_id not found or inactive")
                conn.execute("UPDATE exception_cases SET owner_user_id=?, status='TRIAGED' WHERE case_id=?", (owner, case_id))
            elif action_type == "status_change":
                status = payload.get("status")
                if not status:
                    raise ValidationError("status is required for status_change")
                if status not in {"NEW", "TRIAGED", "IN_PROGRESS", "CLOSED"}:
                    raise ValidationError("Unsupported status value")
                conn.execute("UPDATE exception_cases SET status=? WHERE case_id=?", (status, case_id))
            elif action_type == "comment":
                comment = str(payload.get("comment") or "").strip()
                if not comment:
                    raise ValidationError("comment is required for comment action")
                if len(comment) > 1000:
                    raise ValidationError("comment is too long (max 1000 chars)")
                action_payload["comment"] = comment
            elif action_type == "close":
                resolution = payload.get("resolution_code")
                if not resolution:
                    raise ValidationError("resolution_code is required for close")
                conn.execute(
                    "UPDATE exception_cases SET status='CLOSED', resolution_code=?, closed_at=? WHERE case_id=?",
                    (resolution, now_iso(), case_id),
                )

            conn.execute(
                "INSERT INTO exception_actions(action_id, case_id, actor_user_id, action_at, action_type, action_payload) VALUES(?,?,?,?,?,?)",
                (str(uuid.uuid4()), case_id, actor, now_iso(), action_type, json.dumps(action_payload, ensure_ascii=False)),
            )
            self._audit(
                conn,
                actor,
                source_ip,
                "exception_case",
                case_id,
                f"EXCEPTION_{action_type.upper()}",
                "SUCCESS",
                json.dumps(action_payload, ensure_ascii=False),
            )
        return self.get_exception(actor, case_id)

    def get_rulesets(self, actor: str) -> Dict[str, Any]:
        self.check_permission(actor, "admin:rules")
        with self.db.connect() as conn:
            rows = conn.execute("SELECT version, is_active, json_text, created_at FROM rulesets ORDER BY created_at DESC").fetchall()
            return {
                "items": [
                    {
                        "version": r["version"],
                        "is_active": bool(r["is_active"]),
                        "rules": json.loads(r["json_text"]),
                        "created_at": r["created_at"],
                    }
                    for r in rows
                ]
            }

    def put_ruleset(self, actor: str, source_ip: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.check_permission(actor, "admin:rules")
        for key in ("amount_tolerance", "date_window_days", "score_threshold"):
            if payload.get(key) is None:
                raise ValidationError(f"{key} is required")

        version = payload.get("version") or datetime.now(timezone.utc).strftime("v%Y%m%d%H%M%S")
        rules = {
            "version": version,
            "amount_tolerance": float(payload["amount_tolerance"]),
            "date_window_days": int(payload["date_window_days"]),
            "score_threshold": float(payload["score_threshold"]),
        }

        with self.db.connect() as conn:
            conn.execute("UPDATE rulesets SET is_active=0")
            conn.execute(
                "INSERT OR REPLACE INTO rulesets(version, is_active, json_text, created_at) VALUES(?,?,?,?)",
                (version, 1, json.dumps(rules, ensure_ascii=False), now_iso()),
            )
            self._audit(conn, actor, source_ip, "ruleset", version, "RULESET_UPDATE", "SUCCESS", json.dumps(rules))
            return {"active_version": version, "rules": rules}

    def list_audit(self, actor: str, filters: Dict[str, Any]) -> Dict[str, Any]:
        self.check_permission(actor, "audit:read")
        clauses = []
        params: List[Any] = []
        for key in ("actor_login", "object_type", "action", "result"):
            if filters.get(key):
                clauses.append(f"{key}=?")
                params.append(filters[key])
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        with self.db.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM audit_events {where} ORDER BY event_at DESC LIMIT 1000",
                params,
            ).fetchall()
            return {"items": [dict(r) for r in rows], "count": len(rows)}

    def list_users(self, actor: str) -> Dict[str, Any]:
        # Needed by the exception workflow UI for assignee selection.
        self.check_permission(actor, "exceptions:write")
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT u.login, u.full_name, u.status, GROUP_CONCAT(ur.role_name, ',') AS roles
                FROM users u
                LEFT JOIN user_roles ur ON ur.login = u.login
                GROUP BY u.login, u.full_name, u.status
                ORDER BY u.login
                """
            ).fetchall()
            items = []
            for r in rows:
                items.append(
                    {
                        "login": r["login"],
                        "full_name": r["full_name"],
                        "status": r["status"],
                        "roles": (r["roles"] or "").split(",") if r["roles"] else [],
                    }
                )
            return {"items": items, "count": len(items)}

    def hardcoded_analytics(self, actor: str, business_date: str) -> Dict[str, Any]:
        self.check_permission(actor, "analytics:read")
        if not business_date:
            raise ValidationError("business_date is required")

        with self.db.connect() as conn:
            total_way4 = conn.execute(
                "SELECT COUNT(*) as c FROM txns WHERE source_system LIKE 'WAY4%' AND business_date=?",
                (business_date,),
            ).fetchone()["c"]
            total_visa = conn.execute(
                "SELECT COUNT(*) as c FROM txns WHERE source_system LIKE 'VISA%' AND business_date=?",
                (business_date,),
            ).fetchone()["c"]

            run = conn.execute(
                "SELECT run_id FROM match_runs WHERE business_date=? ORDER BY started_at DESC LIMIT 1",
                (business_date,),
            ).fetchone()
            if not run:
                return {
                    "business_date": business_date,
                    "total_way4": total_way4,
                    "total_visa": total_visa,
                    "message": "No match run for date",
                }

            run_id = run["run_id"]
            matched_unique = conn.execute(
                "SELECT COUNT(DISTINCT left_txn_id) as c FROM match_results WHERE run_id=?",
                (run_id,),
            ).fetchone()["c"]
            partial_count = conn.execute(
                "SELECT COUNT(*) as c FROM match_results WHERE run_id=? AND match_type='PARTIAL_MATCH'",
                (run_id,),
            ).fetchone()["c"]
            open_ex = conn.execute(
                "SELECT COUNT(*) as c FROM exception_cases WHERE run_id=? AND status!='CLOSED'",
                (run_id,),
            ).fetchone()["c"]
            avg_aging = conn.execute(
                "SELECT COALESCE(AVG(aging_days), 0) as a FROM exception_cases WHERE run_id=? AND status!='CLOSED'",
                (run_id,),
            ).fetchone()["a"]

            variance = conn.execute(
                """
                SELECT COALESCE(SUM(ABS(w.amount - v.amount)), 0) as variance
                FROM match_results m
                JOIN txns w ON w.txn_id = m.left_txn_id
                JOIN txns v ON v.txn_id = m.right_txn_id
                WHERE m.run_id=?
                """,
                (run_id,),
            ).fetchone()["variance"]

            match_rate = (matched_unique / total_way4 * 100.0) if total_way4 else 0.0
            return {
                "business_date": business_date,
                "run_id": run_id,
                "total_way4": total_way4,
                "total_visa": total_visa,
                "match_rate_pct": round(match_rate, 2),
                "matched_count": matched_unique,
                "unmatched_count": int(open_ex),
                "partial_count": int(partial_count),
                "avg_open_aging_days": round(float(avg_aging), 2),
                "variance_amount": round(float(variance), 2),
            }
