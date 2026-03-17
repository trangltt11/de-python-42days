from __future__ import annotations

from pathlib import Path
import pandas as pd
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    String, Float, DateTime, select, func
)


def main() -> None:
    root = Path(__file__).resolve().parents[2]

    # 1) Input JSONL
    jsonl_path = root / "data" / "raw" / "day2_events.jsonl"
    df = pd.read_json(jsonl_path, lines=True)

    # 2) Clean nhẹ (giống Day 8)
    df["event"] = df["event"].astype(str).str.strip().str.lower()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")

    # Dedupe theo event_id để tránh double count
    df = df.drop_duplicates(subset=["event_id"], keep="first")

    # 3) Tạo SQLite DB file
    out_dir = root / "data" / "processed" / "day22"
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / "events.db"

    engine = create_engine(f"sqlite:///{db_path}")

    # 4) Define schema (SQLAlchemy Core)
    metadata = MetaData()

    events = Table(
        "events",
        metadata,
        Column("event_id", String, primary_key=True),
        Column("user_id", String, nullable=False),
        Column("event", String, nullable=False),
        Column("amount", Float, nullable=False),
        Column("ts", DateTime, nullable=True),
    )

    # 5) Create table (nếu chưa có)
    metadata.create_all(engine)

    # 6) Insert (batch) trong transaction
    # Convert df -> list[dict]
    rows = df[["event_id", "user_id", "event", "amount", "ts"]].to_dict(orient="records")

    with engine.begin() as conn:
        # Optional: xoá hết để chạy lại không bị trùng (idempotent cho demo)
        conn.execute(events.delete())

        # Insert batch
        conn.execute(events.insert(), rows)

    print("Wrote DB:", db_path)
    print("Inserted rows:", len(rows))

    # 7) Query lại: tổng số record
    with engine.connect() as conn:
        total = conn.execute(select(func.count()).select_from(events)).scalar_one()
        print("\nTotal rows in DB:", total)

        # 8) KPI: count & sum theo event
        stmt = (
            select(
                events.c.event,
                func.count(events.c.event_id).label("cnt"),
                func.sum(events.c.amount).label("total_amount"),
            )
            .group_by(events.c.event)
            .order_by(events.c.event)
        )

        result = conn.execute(stmt).all()
        print("\nKPI by event:")
        for row in result:
            print(dict(row._mapping))

        # 9) KPI: purchase_total theo user
        stmt2 = (
            select(
                events.c.user_id,
                func.count(events.c.event_id).label("purchase_count"),
                func.sum(events.c.amount).label("purchase_total"),
            )
            .where(events.c.event == "purchase")
            .group_by(events.c.user_id)
            .order_by(func.sum(events.c.amount).desc())
        )
        result2 = conn.execute(stmt2).all()
        print("\nPurchase KPI by user:")
        for row in result2:
            print(dict(row._mapping))

if __name__ == "__main__":
    main()