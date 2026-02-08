from __future__ import annotations

from pathlib import Path
import pandas as pd


def main() -> None:
    input_path = Path("data/raw/day2_events.jsonl")

    # 1) đọc JSONL
    df = pd.read_json(input_path, lines=True)

    print("=== HEAD ===")
    print(df.head(3))

    print("\n=== DTYPES (before) ===")
    print(df.dtypes)

    # 2) làm sạch kiểu dữ liệu
    # amount: có thể là int/float/str -> ép numeric
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

    # event: chuẩn hoá chữ thường + trim
    df["event"] = df["event"].astype(str).str.strip().str.lower()

    # ts: parse datetime có timezone (coerce lỗi -> NaT)
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")

    print("\n=== DTYPES (after) ===")
    print(df.dtypes)

    # 3) data quality checks cơ bản
    print("\n=== NULL COUNT ===")
    print(df.isna().sum())

    print("\n=== DUPLICATE EVENT_ID ===")
    dup_mask = df["event_id"].duplicated(keep=False)
    print(df.loc[dup_mask, ["event_id", "user_id", "event", "amount", "ts"]])

    # 4) dedupe (giữ record đầu)
    df = df.drop_duplicates(subset=["event_id"], keep="first")

    # 5) thêm partition columns
    df["event_date"] = df["ts"].dt.strftime("%Y-%m-%d")
    df["event_hour"] = df["ts"].dt.strftime("%H")

    # 6) ghi Parquet
    out_path = Path("data/processed/day8/events.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)

    print("\nWrote:", out_path)
    print("Rows:", len(df))


if __name__ == "__main__":
    main()
