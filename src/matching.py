from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from itertools import combinations
from typing import Dict, List, Optional, Sequence, Tuple


@dataclass
class RuleSet:
    version: str
    amount_tolerance: float
    date_window_days: int
    score_threshold: float


@dataclass
class Txn:
    txn_id: str
    source_system: str
    business_date: str
    rrn: str
    arn: Optional[str]
    amount: float
    currency: str
    txn_time: str
    op_type: str
    merchant_id: str
    channel_id: str


def parse_date(value: str) -> datetime:
    if "T" in value:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.fromisoformat(value + "T00:00:00")


def date_diff_days(left: str, right: str) -> float:
    left_dt = parse_date(left)
    right_dt = parse_date(right)
    return abs((left_dt - right_dt).total_seconds()) / 86400.0


def amount_close(a: float, b: float, tol: float) -> bool:
    return abs(a - b) <= tol


def op_compat_score(left: str, right: str) -> float:
    if left == right:
        return 0.2
    pairs = {
        ("PURCHASE", "CLEARING"),
        ("REFUND", "CHARGEBACK"),
        ("REVERSAL", "REVERSAL"),
    }
    if (left, right) in pairs or (right, left) in pairs:
        return 0.1
    return 0.0


def fuzzy_score(left: Txn, right: Txn, rules: RuleSet) -> Tuple[float, Dict[str, float]]:
    amount_delta = abs(left.amount - right.amount)
    date_delta = date_diff_days(left.txn_time, right.txn_time)

    amount_penalty = min(amount_delta / max(rules.amount_tolerance, 0.01), 1.0) * 0.5
    date_penalty = min(date_delta / max(rules.date_window_days, 1), 1.0) * 0.3
    compat_bonus = op_compat_score(left.op_type, right.op_type)

    score = max(0.0, min(1.0, 1.0 - amount_penalty - date_penalty + compat_bonus))
    return score, {
        "amount_delta": round(amount_delta, 2),
        "date_delta_days": float(date_delta),
        "amount_penalty": round(amount_penalty, 4),
        "date_penalty": round(date_penalty, 4),
        "compat_bonus": round(compat_bonus, 4),
    }


def match_transactions(
    way4: Sequence[Txn],
    visa: Sequence[Txn],
    rules: RuleSet,
) -> Tuple[List[dict], List[dict]]:
    """Returns (match_records, exception_records)."""

    way4_left = {t.txn_id: t for t in way4}
    visa_left = {t.txn_id: t for t in visa}
    matches: List[dict] = []
    exceptions: List[dict] = []

    exact_idx: Dict[Tuple[str, float, str, str], List[Txn]] = {}
    rrn_cur_idx: Dict[Tuple[str, str], List[Txn]] = {}
    arn_idx: Dict[str, List[Txn]] = {}

    for t in visa:
        exact_idx.setdefault((t.rrn, round(t.amount, 2), t.currency, t.business_date), []).append(t)
        rrn_cur_idx.setdefault((t.rrn, t.currency), []).append(t)
        if t.arn:
            arn_idx.setdefault(t.arn, []).append(t)

    def record_match(left: Txn, right: Optional[Txn], match_type: str, score: float, reason: str, explain: dict):
        matches.append(
            {
                "left_txn_id": left.txn_id,
                "right_txn_id": right.txn_id if right else None,
                "match_type": match_type,
                "score": round(score, 4),
                "reason_code": reason,
                "explain": explain,
            }
        )
        way4_left.pop(left.txn_id, None)
        if right:
            visa_left.pop(right.txn_id, None)

    def record_exception(txn: Txn, category: str, severity: str, reason: str):
        exceptions.append(
            {
                "primary_txn_id": txn.txn_id,
                "category": category,
                "severity": severity,
                "status": "NEW",
                "reason": reason,
            }
        )

    for w in list(way4_left.values()):
        candidates = [
            c
            for c in exact_idx.get((w.rrn, round(w.amount, 2), w.currency, w.business_date), [])
            if c.txn_id in visa_left
        ]
        if len(candidates) == 1:
            record_match(
                w,
                candidates[0],
                "MATCHED",
                1.0,
                "EXACT_RRN_AMOUNT_CURR_DATE",
                {"stage": "exact"},
            )
        elif len(candidates) > 1:
            record_exception(w, "DUPLICATE", "HIGH", "MULTI_CANDIDATE_EXACT")

    for w in list(way4_left.values()):
        if not w.arn:
            continue
        candidates = [c for c in arn_idx.get(w.arn, []) if c.txn_id in visa_left]
        if len(candidates) == 1:
            c = candidates[0]
            score, details = fuzzy_score(w, c, rules)
            if score >= rules.score_threshold:
                m_type = "PARTIAL_MATCH" if abs(w.amount - c.amount) > 0 else "MATCHED"
                record_match(w, c, m_type, score, "ARN_MATCH_WITH_TOLERANCE", {"stage": "arn", **details})

    for w in list(way4_left.values()):
        candidates = [
            c
            for c in rrn_cur_idx.get((w.rrn, w.currency), [])
            if c.txn_id in visa_left and date_diff_days(w.txn_time, c.txn_time) <= rules.date_window_days
        ]
        if not candidates:
            continue

        scored = []
        for c in candidates:
            score, details = fuzzy_score(w, c, rules)
            scored.append((score, c, details))
        scored.sort(key=lambda x: x[0], reverse=True)

        top_score, top_c, top_details = scored[0]
        second = scored[1][0] if len(scored) > 1 else -1
        unique_best = top_score - second > 0.05

        if top_score >= rules.score_threshold and unique_best:
            m_type = "PARTIAL_MATCH" if abs(w.amount - top_c.amount) > 0 else "MATCHED"
            record_match(w, top_c, m_type, top_score, "FUZZY_SCORE", {"stage": "fuzzy", **top_details})

    for w in list(way4_left.values()):
        candidates = [
            c for c in visa_left.values() if c.rrn == w.rrn and c.currency == w.currency and c.merchant_id == w.merchant_id
        ]
        matched_combo = None
        for r in (2, 3):
            for combo in combinations(candidates, r):
                total = round(sum(c.amount for c in combo), 2)
                if amount_close(total, round(w.amount, 2), rules.amount_tolerance):
                    matched_combo = combo
                    break
            if matched_combo:
                break

        if matched_combo:
            for c in matched_combo:
                record_match(
                    w,
                    c,
                    "PARTIAL_MATCH",
                    0.8,
                    "ONE_TO_MANY_SUM_MATCH",
                    {"stage": "one_to_many", "combo_size": len(matched_combo)},
                )

    for w in way4_left.values():
        record_exception(w, "MISSING_IN_VISA", "MEDIUM", "No cross-source candidate found")

    for v in visa_left.values():
        exceptions.append(
            {
                "primary_txn_id": v.txn_id,
                "category": "MISSING_IN_WAY4",
                "severity": "MEDIUM",
                "status": "NEW",
                "reason": "No Way4 candidate found",
            }
        )

    return matches, exceptions
