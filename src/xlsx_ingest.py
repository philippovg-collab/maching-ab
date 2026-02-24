from __future__ import annotations

import base64
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from io import BytesIO
from typing import Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
NS = {"a": NS_MAIN}

ALMATY_TZ = timezone(timedelta(hours=6))


@dataclass
class ParsedXlsx:
    profile: str
    source: str
    business_date: str
    records: List[Dict[str, object]]
    preview: List[Dict[str, object]]


@dataclass
class ValidationIssue:
    row: int
    field: str
    message: str


@dataclass
class ParsedXlsxDetailed:
    parsed: ParsedXlsx
    errors: List[ValidationIssue]


class XlsxParseError(Exception):
    pass


def _col_idx(cell_ref: str) -> int:
    m = re.match(r"([A-Z]+)", cell_ref)
    if not m:
        return 0
    n = 0
    for ch in m.group(1):
        n = n * 26 + (ord(ch) - 64)
    return n


def _safe_decimal_str(value: str) -> str:
    s = str(value).strip()
    if not s:
        return ""
    try:
        d = Decimal(s)
    except InvalidOperation:
        return s
    # remove scientific notation
    txt = format(d, "f")
    if "." in txt:
        txt = txt.rstrip("0").rstrip(".")
    return txt


def _to_float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    s = str(value).strip()
    if not s:
        return default
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return default


def _excel_serial_to_iso(value: object) -> str:
    num = _to_float(value, 0.0)
    dt = datetime(1899, 12, 30, tzinfo=ALMATY_TZ) + timedelta(days=num)
    return dt.replace(microsecond=0).isoformat()


def _excel_serial_to_date(value: object) -> str:
    num = _to_float(value, 0.0)
    dt = datetime(1899, 12, 30, tzinfo=ALMATY_TZ) + timedelta(days=num)
    return dt.date().isoformat()


def _slug(text: str, fallback: str = "UNKNOWN") -> str:
    base = re.sub(r"[^A-Za-z0-9]+", "_", (text or "").strip().upper()).strip("_")
    return (base[:24] or fallback)


def _decode_base64_payload(payload: str) -> bytes:
    raw = payload.strip()
    if "," in raw and raw.startswith("data:"):
        raw = raw.split(",", 1)[1]
    return base64.b64decode(raw)


def _extract_sheet_rows(xlsx_bytes: bytes) -> Tuple[str, List[Tuple[int, Dict[str, str]]]]:
    with zipfile.ZipFile(BytesIO(xlsx_bytes)) as zf:
        try:
            wb_root = ET.fromstring(zf.read("xl/workbook.xml"))
            rels_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        except KeyError as e:
            raise XlsxParseError(f"Invalid xlsx structure: {e}")

        rel_map = {r.attrib.get("Id"): r.attrib.get("Target", "") for r in rels_root}
        sheets = wb_root.findall(".//a:sheets/a:sheet", NS)
        if not sheets:
            raise XlsxParseError("Workbook has no sheets")

        first = sheets[0]
        sheet_name = first.attrib.get("name", "Sheet1")
        rid = first.attrib.get("{%s}id" % NS_REL)
        target = rel_map.get(rid, "")
        sheet_path = "xl/" + target.lstrip("/")
        if sheet_path not in zf.namelist():
            raise XlsxParseError("First sheet xml not found")

        shared_strings = []
        if "xl/sharedStrings.xml" in zf.namelist():
            sroot = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in sroot.findall(".//a:si", NS):
                texts = [t.text or "" for t in si.findall(".//a:t", NS)]
                shared_strings.append("".join(texts))

        sroot = ET.fromstring(zf.read(sheet_path))
        raw_rows: List[List[str]] = []
        for row in sroot.findall(".//a:sheetData/a:row", NS):
            values: Dict[int, str] = {}
            for c in row.findall("a:c", NS):
                ref = c.attrib.get("r", "")
                idx = _col_idx(ref)
                ctype = c.attrib.get("t")
                v = c.find("a:v", NS)
                if idx <= 0 or v is None:
                    continue
                raw = v.text or ""
                if ctype == "s":
                    try:
                        values[idx] = shared_strings[int(raw)]
                    except (ValueError, IndexError):
                        values[idx] = raw
                else:
                    values[idx] = raw
            if values:
                max_col = max(values)
                raw_rows.append([values.get(i, "") for i in range(1, max_col + 1)])

        if not raw_rows:
            raise XlsxParseError("Sheet is empty")

        headers = [h.strip() for h in raw_rows[0]]
        rows: List[Tuple[int, Dict[str, str]]] = []
        for idx, r in enumerate(raw_rows[1:], start=2):
            if not any(str(x).strip() for x in r):
                continue
            row_dict = {headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers))}
            rows.append((idx, row_dict))

        return sheet_name, rows


def _detect_profile(headers: List[str], preferred: Optional[str]) -> str:
    if preferred:
        return preferred
    hs = {h.strip().upper() for h in headers}
    if {"TARGET_NUMBER", "TRANS_AMOUNT", "RET_REF_NUMBER", "REQUEST_CATEGORY"}.issubset(hs):
        return "WAY4_1552_V1"
    if {"CARD", "DEBIT_AMOUNT", "CREDIT_AMOUNT", "RET_REF_NUMBER"}.issubset(hs):
        return "VISA_MSPK_V1"
    raise XlsxParseError("Unsupported xlsx format: unknown header set")


def _map_way4_1552(rows: List[Tuple[int, Dict[str, str]]], business_date: Optional[str]) -> ParsedXlsxDetailed:
    records: List[Dict[str, object]] = []
    errors: List[ValidationIssue] = []
    inferred_date = business_date
    for row_no, r in rows:
        if not inferred_date:
            inferred_date = _excel_serial_to_date(r.get("POSTING_DATE") or r.get("TRANS_DATE"))

        merchant = r.get("TRANS_DETAILS") or "UNKNOWN"
        op = (r.get("REQUEST_CATEGORY") or r.get("TRANS_TYPE") or "PURCHASE").upper()
        rrn = _safe_decimal_str(r.get("RET_REF_NUMBER", ""))
        amount = _to_float(r.get("TRANS_AMOUNT"), 0.0)
        currency = (r.get("TRANS_CURR") or r.get("SETTL_CURR") or "").upper()
        if not rrn:
            errors.append(ValidationIssue(row=row_no, field="RET_REF_NUMBER", message="Пустой RRN"))
            continue
        if amount <= 0:
            errors.append(ValidationIssue(row=row_no, field="TRANS_AMOUNT", message="Сумма <= 0"))
        if not currency:
            errors.append(ValidationIssue(row=row_no, field="TRANS_CURR", message="Пустая валюта"))

        record = {
            "rrn": rrn,
            "arn": _safe_decimal_str(r.get("ACQ_REF_NUMBER", "")),
            "pan_masked": r.get("TARGET_NUMBER") or "****",
            "amount": amount,
            "currency": currency or "KZT",
            "txn_time": _excel_serial_to_iso(r.get("TRANS_DATE") or r.get("POSTING_DATE")),
            "op_type": op,
            "merchant_id": _slug(merchant, "WAY4_MERCHANT"),
            "channel_id": _slug(r.get("SOURCE_CHANNEL", "WAY4"), "WAY4"),
            "status_norm": (r.get("POSTING_STATUS") or r.get("OUTWARD_STATUS") or "BOOKED").upper(),
            "fee_amount": 0.0,
            "fee_currency": (currency or "KZT").upper(),
        }
        records.append(record)

    if not inferred_date:
        raise XlsxParseError("Unable to infer business_date from WAY4 file")

    return ParsedXlsxDetailed(
        parsed=ParsedXlsx(
            profile="WAY4_1552_V1",
            source="WAY4_EXPORT",
            business_date=inferred_date,
            records=records,
            preview=records[:5],
        ),
        errors=errors,
    )


def _map_visa_mspk(rows: List[Tuple[int, Dict[str, str]]], business_date: Optional[str]) -> ParsedXlsxDetailed:
    records: List[Dict[str, object]] = []
    errors: List[ValidationIssue] = []
    inferred_date = business_date
    for row_no, r in rows:
        if not inferred_date:
            inferred_date = _excel_serial_to_date(r.get("POSTING_DATE") or r.get("TRANS_DATE"))

        debit = _to_float(r.get("DEBIT_AMOUNT"), 0.0)
        credit = _to_float(r.get("CREDIT_AMOUNT"), 0.0)
        amount = credit if credit > 0 else debit
        if amount <= 0:
            amount = abs(credit - debit)

        debit_fee = _to_float(r.get("DEBIT_FEE"), 0.0)
        credit_fee = _to_float(r.get("CREDIT_FEE"), 0.0)

        merchant = r.get("TRANS_DETAILS") or "UNKNOWN"
        op = (r.get("REQUEST_CATEGORY") or r.get("TRANS_TYPE") or "PURCHASE").upper()
        rrn = _safe_decimal_str(r.get("RET_REF_NUMBER", ""))
        if not rrn:
            errors.append(ValidationIssue(row=row_no, field="RET_REF_NUMBER", message="Пустой RRN"))
            continue
        if amount <= 0:
            errors.append(ValidationIssue(row=row_no, field="DEBIT_AMOUNT/CREDIT_AMOUNT", message="Сумма <= 0"))

        record = {
            "rrn": rrn,
            "arn": _safe_decimal_str(r.get("ACQ_REF_NUMBER", "")),
            "pan_masked": r.get("CARD") or "****",
            "amount": amount,
            "currency": "KZT",
            "txn_time": _excel_serial_to_iso(r.get("TRANS_DATE") or r.get("POSTING_DATE")),
            "op_type": op,
            "merchant_id": _slug(merchant, "VISA_MERCHANT"),
            "channel_id": _slug(r.get("SOURCE_CHANNEL", "VISA"), "VISA"),
            "status_norm": (r.get("BUSINESS_TYPE") or "BOOKED").upper(),
            "fee_amount": credit_fee if credit_fee > 0 else debit_fee,
            "fee_currency": "KZT",
        }
        records.append(record)

    if not inferred_date:
        raise XlsxParseError("Unable to infer business_date from VISA file")

    return ParsedXlsxDetailed(
        parsed=ParsedXlsx(
            profile="VISA_MSPK_V1",
            source="VISA_CLEARING",
            business_date=inferred_date,
            records=records,
            preview=records[:5],
        ),
        errors=errors,
    )


def parse_xlsx_ingest(file_b64: str, preferred_profile: Optional[str] = None, business_date: Optional[str] = None) -> ParsedXlsx:
    xlsx_bytes = _decode_base64_payload(file_b64)
    _, rows = _extract_sheet_rows(xlsx_bytes)
    if not rows:
        raise XlsxParseError("No data rows found")
    headers = list(rows[0].keys())
    profile = _detect_profile(headers, preferred_profile)

    if profile == "WAY4_1552_V1":
        return _map_way4_1552(rows, business_date).parsed
    if profile == "VISA_MSPK_V1":
        return _map_visa_mspk(rows, business_date).parsed
    raise XlsxParseError(f"Unsupported parser profile: {profile}")


def parse_xlsx_ingest_detailed(
    file_b64: str, preferred_profile: Optional[str] = None, business_date: Optional[str] = None
) -> ParsedXlsxDetailed:
    xlsx_bytes = _decode_base64_payload(file_b64)
    _, rows = _extract_sheet_rows(xlsx_bytes)
    if not rows:
        raise XlsxParseError("No data rows found")
    headers = list(rows[0][1].keys())
    profile = _detect_profile(headers, preferred_profile)

    if profile == "WAY4_1552_V1":
        return _map_way4_1552(rows, business_date)
    if profile == "VISA_MSPK_V1":
        return _map_visa_mspk(rows, business_date)
    raise XlsxParseError(f"Unsupported parser profile: {profile}")
