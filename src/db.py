import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .config import DB_PATH


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS ingest_files (
  file_id TEXT PRIMARY KEY,
  source_system TEXT NOT NULL,
  business_date TEXT NOT NULL,
  file_name TEXT NOT NULL,
  checksum_sha256 TEXT NOT NULL,
  parser_profile TEXT,
  received_at TEXT NOT NULL,
  status TEXT NOT NULL,
  record_count INTEGER NOT NULL DEFAULT 0,
  created_by TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_ingest_dedup
ON ingest_files(source_system, business_date, checksum_sha256);

CREATE TABLE IF NOT EXISTS txns (
  txn_id TEXT PRIMARY KEY,
  source_system TEXT NOT NULL,
  business_date TEXT NOT NULL,
  rrn TEXT NOT NULL,
  arn TEXT,
  pan_masked TEXT NOT NULL,
  pan_hash TEXT NOT NULL,
  amount REAL NOT NULL,
  currency TEXT NOT NULL,
  txn_time TEXT NOT NULL,
  op_type TEXT NOT NULL,
  merchant_id TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  status_norm TEXT NOT NULL,
  fee_amount REAL NOT NULL DEFAULT 0,
  fee_currency TEXT
);

CREATE INDEX IF NOT EXISTS ix_txn_source_date ON txns(source_system, business_date);
CREATE INDEX IF NOT EXISTS ix_txn_rrn_cur_date ON txns(rrn, currency, business_date);
CREATE INDEX IF NOT EXISTS ix_txn_arn ON txns(arn);

CREATE TABLE IF NOT EXISTS match_runs (
  run_id TEXT PRIMARY KEY,
  business_date TEXT NOT NULL,
  scope_filter TEXT,
  ruleset_version TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status TEXT NOT NULL,
  created_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS match_results (
  match_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  left_txn_id TEXT NOT NULL,
  right_txn_id TEXT,
  match_type TEXT NOT NULL,
  score REAL NOT NULL,
  reason_code TEXT NOT NULL,
  explain_json TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES match_runs(run_id)
);

CREATE INDEX IF NOT EXISTS ix_match_run ON match_results(run_id);
CREATE INDEX IF NOT EXISTS ix_match_left ON match_results(left_txn_id);

CREATE TABLE IF NOT EXISTS exception_cases (
  case_id TEXT PRIMARY KEY,
  run_id TEXT,
  business_date TEXT NOT NULL,
  category TEXT NOT NULL,
  severity TEXT NOT NULL,
  status TEXT NOT NULL,
  primary_txn_id TEXT NOT NULL,
  owner_user_id TEXT,
  aging_days INTEGER NOT NULL,
  resolution_code TEXT,
  created_at TEXT NOT NULL,
  closed_at TEXT,
  FOREIGN KEY(run_id) REFERENCES match_runs(run_id)
);

CREATE INDEX IF NOT EXISTS ix_exception_date_status ON exception_cases(business_date, status);
CREATE INDEX IF NOT EXISTS ix_exception_run ON exception_cases(run_id);

CREATE TABLE IF NOT EXISTS exception_actions (
  action_id TEXT PRIMARY KEY,
  case_id TEXT NOT NULL,
  actor_user_id TEXT NOT NULL,
  action_at TEXT NOT NULL,
  action_type TEXT NOT NULL,
  action_payload TEXT NOT NULL,
  FOREIGN KEY(case_id) REFERENCES exception_cases(case_id)
);

CREATE TABLE IF NOT EXISTS users (
  login TEXT PRIMARY KEY,
  full_name TEXT NOT NULL,
  status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_roles (
  login TEXT NOT NULL,
  role_name TEXT NOT NULL,
  PRIMARY KEY(login, role_name),
  FOREIGN KEY(login) REFERENCES users(login)
);

CREATE TABLE IF NOT EXISTS rulesets (
  version TEXT PRIMARY KEY,
  is_active INTEGER NOT NULL,
  json_text TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_events (
  audit_id TEXT PRIMARY KEY,
  event_at TEXT NOT NULL,
  actor_login TEXT NOT NULL,
  source_ip TEXT,
  object_type TEXT NOT NULL,
  object_id TEXT,
  action TEXT NOT NULL,
  result TEXT NOT NULL,
  details TEXT
);
"""

DEFAULT_USERS = [
    ("admin", "Platform Admin", "ACTIVE", "admin"),
    ("operator1", "Operator L1", "ACTIVE", "operator_l1"),
    ("supervisor", "Operator L2", "ACTIVE", "operator_l2"),
    ("auditor", "Internal Auditor", "ACTIVE", "auditor"),
    ("finance", "Finance Viewer", "ACTIVE", "finance_viewer"),
]


class Database:
    def __init__(self, path: Path = DB_PATH):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init(self):
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            for login, full_name, status, role_name in DEFAULT_USERS:
                conn.execute(
                    "INSERT OR IGNORE INTO users(login, full_name, status) VALUES(?,?,?)",
                    (login, full_name, status),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO user_roles(login, role_name) VALUES(?,?)",
                    (login, role_name),
                )
