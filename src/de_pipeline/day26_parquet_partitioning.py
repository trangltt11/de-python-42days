from __future__ import annotations

from pathlib import Path
import pandas as pd


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    in_path = root / "data" / "raw" / "day2_events.jsonl"

    # 1) read jsonl -> df
    df = pd.read_json(in_path, lines=True)

    # 2) clean tối thiểu
    df["event"] = df["event"].astype(str).str.strip().str.lower()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")

    # 3) dedupe event_id để tránh double count
    df = df.drop_duplicates(subset=["event_id"], keep="first").copy()

    # 4) tạo partition columns year/month/day (zero-pad)
    df["year"] = df["ts"].dt.strftime("%Y")
    df["month"] = df["ts"].dt.strftime("%m")
    df["day"] = df["ts"].dt.strftime("%d")

    # 5) ghi parquet theo partition year/month/day
    out_root = root / "data" / "processed" / "day26"
    out_root.mkdir(parents=True, exist_ok=True)

    # nếu ts bị lỗi -> year/month/day = NaN -> drop để khỏi tạo folder "nan"
    df = df.dropna(subset=["year", "month", "day"])

    for (y, m, d), part in df.groupby(["year", "month", "day"], dropna=False):
        out_dir = out_root / f"year={y}" / f"month={m}" / f"day={d}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / "events.parquet"
        part.to_parquet(out_file, index=False)

        print(f"Wrote {len(part)} rows -> {out_file}")

    print("DONE. Output root:", out_root)


if __name__ == "__main__":
    main()