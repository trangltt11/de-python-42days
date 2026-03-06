from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .types_def import Record
from .ops import dedupe_by_key, safe_float
from .validate import validate_records
from .file_io import read_jsonl, write_jsonl, write_parquet


@dataclass(frozen=True)
class PipelinePaths:
    input_jsonl: Path
    processed_root: Path
    bad_root: Path


def extract(paths: PipelinePaths) -> list[Record]:
    """Extract: đọc raw JSONL."""
    return read_jsonl(paths.input_jsonl)


def normalize_record(r: Record) -> Record:
    """
    Transform nhỏ: chuẩn hóa field.
    - event: lower + strip
    - amount: float an toàn
    """
    event = str(r.get("event", "")).strip().lower()
    amount = safe_float(r.get("amount"), 0.0)
    return {**r, "event": event, "amount": amount}


def extract_date(ts: Any) -> str:
    """Lấy YYYY-MM-DD từ ts ISO8601."""
    if isinstance(ts, str) and len(ts) >= 10:
        return ts[:10]
    return "unknown"


def transform(records: list[Record]) -> tuple[list[Record], list[Record], list[Record]]:
    """
    Transform: dedupe + normalize + validate.
    Return: (valid_records, invalid_records)
    """
    # 1) dedupe theo event_id để tránh double count
    deduped = dedupe_by_key(records, "event_id")

    # 2) normalize
    normalized = [normalize_record(r) for r in deduped]

    # 3) validate (non-strict)
    root = Path(__file__).resolve().parents[2]
    bad_root=root / "data" / "bad" / "day15.jsonl"
    valid, results = validate_records(normalized, bad_root)
    invalid = [r for r, res in zip(normalized, results) if not res.ok]
    return valid, invalid,deduped


def load(paths: PipelinePaths, valid: list[Record], invalid: list[Record]) -> None:
    """
    Load: ghi processed parquet theo date và bad records theo date.
    """
    # Partition theo date lấy từ ts
    by_date_valid: dict[str, list[Record]] = {}
    for r in valid:
        d = extract_date(r.get("ts"))
        r2 = {**r, "ingest_date": d}
        by_date_valid.setdefault(d, []).append(r2)

    by_date_bad: dict[str, list[Record]] = {}
    for r in invalid:
        d = extract_date(r.get("ts"))
        by_date_bad.setdefault(d, []).append(r)

    # Ghi processed
    for d, recs in by_date_valid.items():
        out_dir = paths.processed_root / f"date={d}"
        out_file = out_dir / "events.parquet"
        write_parquet(out_file, recs)

    # Ghi bad
    for d, recs in by_date_bad.items():
        out_dir = paths.bad_root / f"date={d}"
        out_file = out_dir / "bad.jsonl"
        write_jsonl(out_file, recs, append=False)

    """Level 2 (vừa)

    Thêm stats chi tiết hơn vào run():

    deduped_records (đếm sau dedupe)

    partitions_written (số date partitions ghi ra)     """
   


def run_def(paths: PipelinePaths) -> dict[str, int]:
    """Orchestrator: chạy ETL và trả về stats."""
    records = extract(paths)
    valid, invalid, deduped = transform(records)
    load(paths, valid, invalid)
    partitions_written=set()
    for r in valid:
        ts= extract_date(r.get("ts"))
        partitions_written.add(ts)

    return {
        "total_records": len(records),
        "valid_records": len(valid),
        "invalid_records": len(invalid),
        "deduped_records": len(deduped),
        "partitions_written": len(partitions_written)
    }