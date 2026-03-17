from __future__ import annotations

from pathlib import Path
import pandas as pd
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    String, Float, DateTime, select, func, text
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert


def get_engine(db_path: Path):
    return create_engine(f"sqlite:///{db_path}")


def define_schema(metadata: MetaData) -> Table:
    events = Table(
        "events",
        metadata,
        Column("event_id", String, primary_key=True),
        Column("user_id", String, nullable=False),
        Column("event", String, nullable=False),
        Column("amount", Float, nullable=False),
        Column("ts", DateTime, nullable=True),
    )
    return events


def load_events_from_jsonl(path: Path) -> list[dict]:
    df = pd.read_json(path, lines=True)
    df["event"] = df["event"].astype(str).str.strip().str.lower()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df = df.drop_duplicates(subset=["event_id"], keep="first")
    return df[["event_id", "user_id", "event", "amount", "ts"]].to_dict(orient="records")


def upsert_events(conn, events_table: Table, rows: list[dict]) -> int:
    """
    SQLite UPSERT:
    - Nếu event_id chưa có -> INSERT
    - Nếu event_id đã có -> UPDATE user_id/event/amount/ts
    """
    stmt = sqlite_insert(events_table).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[events_table.c.event_id],
        set_={
            "user_id": stmt.excluded.user_id,
            "event": stmt.excluded.event,
            "amount": stmt.excluded.amount,
            "ts": stmt.excluded.ts,
        },
    )
    result = conn.execute(stmt)
    return result.rowcount if result.rowcount is not None else 0


def get_user_events_safe(conn, user_id: str) -> list[dict]:
    """
    Parameterized query (text + bind param).
    """
    sql = text("""
        SELECT event_id, user_id, event, amount, ts
        FROM events
        WHERE user_id = :user_id
        ORDER BY ts
    """)
    rows = conn.execute(sql, {"user_id": user_id}).mappings().all()
    return [dict(r) for r in rows]


def get_purchase_kpi_by_user(conn) -> list[dict]:
    sql = text("""
        SELECT user_id,
               COUNT(*) AS purchase_count,
               SUM(amount) AS purchase_total
        FROM events
        WHERE event = :event
        GROUP BY user_id
        ORDER BY purchase_total DESC
    """)
    rows = conn.execute(sql, {"event": "purchase"}).mappings().all()
    return [dict(r) for r in rows]


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    jsonl_path = root / "data" / "raw" / "day2_events.jsonl"

    out_dir = root / "data" / "processed" / "day23"
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / "events.db"

    engine = get_engine(db_path)
    metadata = MetaData()
    events = define_schema(metadata)
    metadata.create_all(engine)

    rows = load_events_from_jsonl(jsonl_path)

    # ===== (A) Transaction demo: upsert trong transaction =====
    try:
        with engine.begin() as conn:
            # upsert: chạy lại nhiều lần không lỗi, không nhân bản
            n = upsert_events(conn, events, rows)
            # để demo rollback, bạn có thể cố tình raise lỗi ở đây:
            # raise RuntimeError("simulate crash after write")
        print("Upsert affected rows:", n)
    except Exception as e:
        print("Transaction failed, rolled back:", e)

    # ===== (B) Query demo: parameterized query =====
    with engine.connect() as conn:
        total = conn.execute(select(func.count()).select_from(events)).scalar_one()
        print("Total rows in DB:", total)

        u1 = get_user_events_safe(conn, "u1")
        print("\nUser u1 events:")
        for r in u1:
            print(r)

        kpi = get_purchase_kpi_by_user(conn)
        print("\nPurchase KPI by user:")
        for r in kpi:
            print(r)


if __name__ == "__main__":
    main()