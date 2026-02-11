from __future__ import annotations

from pathlib import Path

from de_pipeline.file_io import read_jsonl, write_jsonl, write_parquet
from de_pipeline.ops import dedupe_by_key
from de_pipeline.validate import validate_records


def extract_date(ts: str) -> str:
    # ts dạng "2026-01-13T09:00:00+07:00" -> "2026-01-13"
    return ts[:10]


def main() -> None:
    input_path = Path("data/raw/day2_events.jsonl")
    records = read_jsonl(input_path)

    deduped = dedupe_by_key(records, "event_id")
    valid, results = validate_records(deduped)

    bad = [r for r, res in zip(deduped, results) if not res.ok]

    # ghi bad records
    if bad:
        write_jsonl(Path("data/bad/day6_bad_records.jsonl"), bad, append=False)

    # partition theo date (processed)
    if valid:
        date = extract_date(valid[0]["ts"])  # demo: lấy date từ record đầu
        out_parquet = Path(f"data/processed/date={date}/events.parquet")
        write_parquet(out_parquet, valid)

    print("Total:", len(records))
    print("Deduped:", len(deduped))
    print("Valid:", len(valid))
    print("Bad:", len(bad))


if __name__ == "__main__":
    main()
