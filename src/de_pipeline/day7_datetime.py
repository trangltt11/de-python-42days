from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any

from de_pipeline.types_def import Record
from de_pipeline.file_io import read_jsonl, write_parquet
from de_pipeline.ops import dedupe_by_key
from de_pipeline.validate import validate_records


BKK = ZoneInfo("Asia/Bangkok")  # timezone của bạn


def parse_ts(ts: str) -> datetime:
    """
    Parse ISO8601 timestamp.
    Yêu cầu: phải có timezone (aware).
    """
    dt = datetime.fromisoformat(ts)  # parse được "...+07:00"
    if dt.tzinfo is None:
        raise ValueError("ts must include timezone offset, e.g. +07:00")
    return dt


def to_utc(dt: datetime) -> datetime:
    """Convert aware datetime -> UTC."""
    if dt.tzinfo is None:
        raise ValueError("dt must be timezone-aware")
    return dt.astimezone(timezone.utc)


def to_bangkok(dt: datetime) -> datetime:
    """Convert aware datetime -> Asia/Bangkok."""
    if dt.tzinfo is None:
        raise ValueError("dt must be timezone-aware")
    return dt.astimezone(BKK)


def enrich_datetime_fields(r: Record) -> Record:
    """
    Tạo record mới (không sửa record gốc) với các field:
    - event_dt_utc: ISO string UTC
    - event_date: YYYY-MM-DD theo Bangkok time
    - event_hour: HH theo Bangkok time
    """
    ts = r.get("ts")
    if not isinstance(ts, str):
        raise ValueError("ts must be a string")

    dt = parse_ts(ts)
    dt_bkk = to_bangkok(dt)
    dt_utc = to_utc(dt)

    event_date = dt_bkk.strftime("%Y-%m-%d")
    event_hour = dt_bkk.strftime("%H")

    return {
        **r,
        "event_dt_utc": dt_utc.isoformat(),
        "event_date": event_date,
        "event_hour": event_hour,
    }


def partition_by_date_hour(records: list[Record]) -> dict[tuple[str, str], list[Record]]:
    """
    Group records theo (event_date, event_hour)
    Output key là tuple: (date, hour)
    """
    grouped: dict[tuple[str, str], list[Record]] = {}
    for r in records:
        d = r["event_date"]
        h = r["event_hour"]
        key = (d, h)
        grouped.setdefault(key, []).append(r)
    return grouped


def main() -> None:
    # 1) đọc raw
    input_path = "data/raw/day2_events.jsonl"
    records = read_jsonl(input_path)

    # 2) dedupe
    deduped = dedupe_by_key(records, "event_id")

    # 3) validate (dùng rule Day 5)
    valid, _results = validate_records(deduped)

    # 4) enrich datetime fields
    enriched: list[Record] = [enrich_datetime_fields(r) for r in valid]

    # 5) partition theo date/hour rồi ghi parquet
    grouped = partition_by_date_hour(enriched)

    for (date, hour), recs in grouped.items():
        out_path = f"data/processed/date={date}/hour={hour}/events.parquet"
        write_parquet(out_path, recs)

    print("Total:", len(records))
    print("Deduped:", len(deduped))
    print("Valid:", len(valid))
    print("Partitions:", len(grouped))
    # in thử 1 key
    if grouped:
        one_key = next(iter(grouped.keys()))
        print("Example partition key:", one_key)


if __name__ == "__main__":
    main()
