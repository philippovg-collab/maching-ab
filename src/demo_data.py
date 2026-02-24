from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from .services import AppService


def seed_demo_data(service: AppService, business_date: str = "2026-02-22"):
    ts = datetime.now(timezone.utc).isoformat()

    way4_records = [
        {
            "rrn": "100001",
            "arn": "ARN100001",
            "pan_masked": "400000******1111",
            "amount": 100.00,
            "currency": "KZT",
            "txn_time": f"{business_date}T01:01:00+06:00",
            "op_type": "PURCHASE",
            "merchant_id": "M001",
            "channel_id": "ECOM",
            "status_norm": "BOOKED",
        },
        {
            "rrn": "100002",
            "arn": "ARN100002",
            "pan_masked": "400000******2222",
            "amount": 50.00,
            "currency": "KZT",
            "txn_time": f"{business_date}T01:05:00+06:00",
            "op_type": "PURCHASE",
            "merchant_id": "M001",
            "channel_id": "ECOM",
            "status_norm": "BOOKED",
        },
        {
            "rrn": "100003",
            "arn": "ARN100003",
            "pan_masked": "400000******3333",
            "amount": 200.00,
            "currency": "KZT",
            "txn_time": f"{business_date}T02:10:00+06:00",
            "op_type": "PURCHASE",
            "merchant_id": "M002",
            "channel_id": "ECOM",
            "status_norm": "BOOKED",
        },
        {
            "rrn": "100004",
            "arn": "ARN100004",
            "pan_masked": "400000******4444",
            "amount": 60.00,
            "currency": "KZT",
            "txn_time": f"{business_date}T03:10:00+06:00",
            "op_type": "PURCHASE",
            "merchant_id": "M003",
            "channel_id": "ECOM",
            "status_norm": "BOOKED",
        },
    ]

    visa_records = [
        {
            "rrn": "100001",
            "arn": "ARN100001",
            "pan_masked": "400000******1111",
            "amount": 100.00,
            "currency": "KZT",
            "txn_time": f"{business_date}T01:02:00+06:00",
            "op_type": "CLEARING",
            "merchant_id": "M001",
            "channel_id": "ECOM",
            "status_norm": "BOOKED",
        },
        {
            "rrn": "100002",
            "arn": "ARN100002",
            "pan_masked": "400000******2222",
            "amount": 49.50,
            "currency": "KZT",
            "txn_time": f"{business_date}T01:07:00+06:00",
            "op_type": "CLEARING",
            "merchant_id": "M001",
            "channel_id": "ECOM",
            "status_norm": "BOOKED",
        },
        {
            "rrn": "100003",
            "arn": "ARN100003",
            "pan_masked": "400000******3333",
            "amount": 120.00,
            "currency": "KZT",
            "txn_time": f"{business_date}T02:12:00+06:00",
            "op_type": "SETTLEMENT",
            "merchant_id": "M002",
            "channel_id": "ECOM",
            "status_norm": "BOOKED",
        },
        {
            "rrn": "100003",
            "arn": "ARN100003",
            "pan_masked": "400000******3333",
            "amount": 80.00,
            "currency": "KZT",
            "txn_time": f"{business_date}T02:13:00+06:00",
            "op_type": "SETTLEMENT",
            "merchant_id": "M002",
            "channel_id": "ECOM",
            "status_norm": "BOOKED",
        },
        {
            "rrn": "100005",
            "arn": "ARN100005",
            "pan_masked": "400000******5555",
            "amount": 75.00,
            "currency": "KZT",
            "txn_time": f"{business_date}T04:30:00+06:00",
            "op_type": "CLEARING",
            "merchant_id": "M004",
            "channel_id": "ECOM",
            "status_norm": "BOOKED",
        },
    ]

    service.ingest_file(
        actor="admin",
        source_ip="127.0.0.1",
        payload={
            "source": "WAY4_EXPORT",
            "business_date": business_date,
            "file_name": f"WAY4_{business_date}.json",
            "checksum_sha256": hashlib.sha256(f"way4:{business_date}".encode()).hexdigest(),
            "received_at": ts,
            "parser_profile": "WAY4_v1",
            "records": way4_records,
        },
    )

    service.ingest_file(
        actor="admin",
        source_ip="127.0.0.1",
        payload={
            "source": "VISA_CLEARING",
            "business_date": business_date,
            "file_name": f"VISA_{business_date}.json",
            "checksum_sha256": hashlib.sha256(f"visa:{business_date}".encode()).hexdigest(),
            "received_at": ts,
            "parser_profile": "VISA_v1",
            "records": visa_records,
        },
    )

    run = service.run_matching(
        actor="admin",
        source_ip="127.0.0.1",
        payload={"business_date": business_date, "scope_filter": "ALL"},
    )
    return run


if __name__ == "__main__":
    service = AppService()
    result = seed_demo_data(service)
    print("Seed completed:", result)
