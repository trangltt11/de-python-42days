from __future__ import annotations

from pathlib import Path
import pandas as pd


def main() -> None:
    root = Path(__file__).resolve().parents[2]

    events_path = root / "data" / "raw" / "day2_events.jsonl"
    users_path = root / "data" / "raw" / "day9_users.csv"  # từ Day 9

    # 1) Load
    events = pd.read_json(events_path, lines=True)
    users = pd.read_csv(users_path)

    # 2) Clean
    events["event"] = events["event"].astype(str).str.strip().str.lower()
    events["amount"] = pd.to_numeric(events["amount"], errors="coerce").fillna(0.0)
    events["ts"] = pd.to_datetime(events["ts"], errors="coerce")

    # 3) Dedupe event_id (tránh double count)
    events = events.drop_duplicates(subset=["event_id"], keep="first")

    # 4) Add date/hour
    events["event_date"] = events["ts"].dt.strftime("%Y-%m-%d")
    events["event_hour"] = events["ts"].dt.strftime("%H")

    # 5) Join segment (dimension)
    users=users.drop_duplicates(subset=["user_id"], keep="last")
    users["user_id"] = users["user_id"].astype(str).str.strip()
    joined = events.merge(users, on="user_id", how="left", validate="many_to_one")

    # 6) KPI 1: daily events KPI theo segment
    kpi_daily_segment = (
        joined.groupby(["event_date", "segment"], dropna=False)
        .agg(
            total_events=("event_id", "count"),
            unique_users=("user_id", "nunique"),
            total_amount=("amount", "sum"),
            purchase_amount=("amount", lambda s: s[joined.loc[s.index, "event"] == "purchase"].sum()),
            last_ts=("ts", "max"),
        )
        .reset_index()
        .sort_values(["event_date", "segment"])
    )

    # 7) KPI 2: purchase KPI theo user (đúng kiểu DE)
    purchases = joined[joined["event"] == "purchase"].copy()

    kpi_user_purchase = (
        purchases.groupby(["user_id", "user_name", "segment"], dropna=False)
        .agg(
            purchase_count=("event_id", "count"),
            purchase_total=("amount", "sum"),
            avg_purchase=("amount", "mean"),
            last_purchase_ts=("ts", "max"),
        )
        .reset_index()
        .sort_values("purchase_total", ascending=False)
    )

    # 8) Pivot: event counts theo event type (wide table)
    # rows: event_date, cols: event, values: count
    event_counts_wide = (
        joined.pivot_table(
            index="event_date",
            columns="event",
            values="event_id",
            aggfunc="count",
            fill_value=0,
        )
        .reset_index()
    )

    # 9) Save outputs
    out_dir = root / "data" / "processed" / "day10"
    out_dir.mkdir(parents=True, exist_ok=True)

    kpi_daily_segment.to_parquet(out_dir / "kpi_daily_segment.parquet", index=False)
    kpi_user_purchase.to_parquet(out_dir / "kpi_user_purchase.parquet", index=False)
    event_counts_wide.to_parquet(out_dir / "event_counts_wide.parquet", index=False)

    print("Wrote to:", out_dir)
    print("\nDaily KPI (sample):")
    print(kpi_daily_segment.head())
    print("\nUser Purchase KPI (sample):")
    print(kpi_user_purchase.head())
    print("\nWide event counts:")
    print(event_counts_wide.head())


if __name__ == "__main__":
    main()