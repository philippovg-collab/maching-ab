import os
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

APP_ENV = (os.getenv("APP_ENV") or "dev").lower()
PAN_HASH_SECRET = os.getenv("PAN_HASH_SECRET", "change-me-in-production")

if APP_ENV in {"prod", "production"} and PAN_HASH_SECRET == "change-me-in-production":
    raise RuntimeError("PAN_HASH_SECRET must be set in production")
