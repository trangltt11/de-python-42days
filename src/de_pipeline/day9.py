from __future__ import annotations

from pathlib import Path
import pandas as pd


def main() -> None:
    root = Path(__file__).resolve().parents[2]  # de-python-42days
    events_path = root / "data" / "raw" / "day2_events.jsonl"
    users_path = root / "data" / "raw" / "day9_users.csv"

    # 1) Load events (fact)
    events = pd.read_json(events_path, lines=True)

    # Clean tối thiểu (giống Day 8)
    events["event"] = events["event"].astype(str).str.strip().str.lower()
    events["amount"] = pd.to_numeric(events["amount"], errors="coerce").fillna(0.0)
    events["ts"] = pd.to_datetime(events["ts"], errors="coerce")

    # Dedupe event_id để tránh double count
    events = events.drop_duplicates(subset=["event_id"], keep="first")

    # 2) Load users (dimension)
    users = pd.read_csv(users_path)
    users["user_id"] = users["user_id"].astype(str).str.strip()

    # 3) Data quality check trước khi join
    # users.user_id phải unique (dimension chuẩn)
    dup_users = users["user_id"].duplicated().sum()
    print("Duplicate user_id in users:", dup_users)

    # 4) Join (left join) + validate cardinality
    # events: many, users: one => many_to_one
    joined = events.merge(users, on="user_id", how="left", validate="many_to_one")

    # 5) Audit: có event nào không match user không?
    missing_users = joined["user_name"].isna().sum()
    print("Events missing user dimension:", missing_users)

    # 6) Aggregate purchase theo user (sau join vẫn ok vì join không nhân bản)
    purchases = joined[joined["event"] == "purchase"].copy()

    agg = (
        purchases.groupby(["user_id", "user_name", "segment"], dropna=False)
        .agg(
            purchase_count=("event_id", "count"),
            purchase_total=("amount", "sum"),
            last_purchase_ts=("ts", "max"),
        )
        .reset_index()
        .sort_values(["purchase_total"], ascending=False)
    )

    print("\n=== PURCHASE AGG ===")
    print(agg)

    # 7) (Nâng nhẹ) Rank purchase_total trong từng segment
    agg["rank_in_segment"] = agg.groupby("segment")["purchase_total"].rank(
        method="dense", ascending=False
    )

    # 8) Write output parquet
    out_dir = root / "data" / "processed" / "day9"
    out_dir.mkdir(parents=True, exist_ok=True)

    joined.to_parquet(out_dir / "events_joined.parquet", index=False)
    agg.to_parquet(out_dir / "purchase_by_user.parquet", index=False)

    print("\nWrote:")
    print(out_dir / "events_joined.parquet")
    print(out_dir / "purchase_by_user.parquet")


if __name__ == "__main__":
    main()
