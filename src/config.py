from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "reconciliation.db"

DEFAULT_RULESET = {
    "version": "v1",
    "amount_tolerance": 2.0,
    "date_window_days": 1,
    "score_threshold": 0.75,
}

PAN_HASH_SECRET = "change-me-in-production"
