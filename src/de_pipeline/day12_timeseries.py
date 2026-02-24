from __future__ import annotations

from pathlib import Path
import pandas as pd


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    path = root / "data" / "raw" / "day2_events.jsonl"

    # 1) load
    df = pd.read_json(path, lines=True)

    # 2) clean cơ bản
    df["event"] = df["event"].astype(str).str.strip().str.lower()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")

    # 3) dedupe để tránh double count
    df = df.drop_duplicates(subset=["event_id"], keep="first")

    # 4) set index = ts (bắt buộc để resample)
    df = df.set_index("ts").sort_index()

    # ===== A) KPI theo giờ =====
    hourly = df.resample("H").agg(
        total_events=("event_id", "count"),
        unique_users=("user_id", "nunique"),
        total_amount=("amount", "sum"),
    )

    # ===== B) KPI purchase theo giờ =====
    purchase = df[df["event"] == "purchase"].copy()
    hourly_purchase = purchase.resample("H").agg(
        purchase_events=("event_id", "count"),
        purchase_amount=("amount", "sum"),
    )

    # merge 2 bảng giờ
    hourly_kpi = hourly.join(hourly_purchase, how="left").fillna(0)

    # ===== C) Rolling window (3 giờ) trên purchase_amount =====
    # rolling(3) = 3 “bins” liên tiếp (3 giờ)
    hourly_kpi["purchase_amount_roll3h"] = hourly_kpi["purchase_amount"].rolling(3).sum()
    hourly_kpi["purchase_amount_ma3h"] = hourly_kpi["purchase_amount"].rolling(3).mean()

    print("=== HOURLY KPI ===")
    print(hourly_kpi)

    # ===== D) KPI theo ngày =====
    daily = df.resample("D").agg(
        total_events=("event_id", "count"),
        unique_users=("user_id", "nunique"),
        total_amount=("amount", "sum"),
    )
    print("\n=== DAILY KPI ===")
    print(daily)

    # 5) output parquet
    out_dir = root / "data" / "processed" / "day12"
    out_dir.mkdir(parents=True, exist_ok=True)

    hourly_kpi.reset_index().to_parquet(out_dir / "hourly_kpi.parquet", index=False)
    daily.reset_index().to_parquet(out_dir / "daily_kpi.parquet", index=False)

    print("\nWrote:")
    print(out_dir / "hourly_kpi.parquet")
    print(out_dir / "daily_kpi.parquet")


if __name__ == "__main__":
    main()
