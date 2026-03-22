from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta, timezone
import random
import time

from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    String, Float, DateTime
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert


def chunked(lst, size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def make_fake_rows(n: int) -> list[dict]:
    """
    Sinh dữ liệu giả n dòng:
    - event_id unique: e000001...
    - user_id: u1..u1000
    - event: view/purchase/refund
    - amount: view=0, purchase>0, refund<0
    - ts: tăng dần theo phút
    """
    base = datetime(2026, 1, 13, 9, 0, tzinfo=timezone(timedelta(hours=7)))
    events = ["view", "purchase", "refund"]
    rows = []

    for i in range(n):
        ev = random.choices(events, weights=[0.6, 0.35, 0.05])[0]
        if ev == "view":
            amt = 0.0
        elif ev == "purchase":
            amt = round(random.uniform(1, 300), 2)
        else:
            amt = -round(random.uniform(1, 100), 2)

        rows.append({
            "event_id": f"e{i:06d}",
            "user_id": f"u{random.randint(1, 1000)}",
            "event": ev,
            "amount": amt,
            "ts": base + timedelta(minutes=i),
        })
    return rows


def define_events_table(metadata: MetaData) -> Table:
    return Table(
        "events",
        metadata,
        Column("event_id", String, primary_key=True),
        Column("user_id", String, nullable=False),
        Column("event", String, nullable=False),
        Column("amount", Float, nullable=False),
        Column("ts", DateTime, nullable=True),
    )


def setup_db(db_path: Path):
    engine = create_engine(f"sqlite:///{db_path}")
    metadata = MetaData()
    events = define_events_table(metadata)
    metadata.create_all(engine)
    return engine, events


def insert_per_row(engine, events: Table, rows: list[dict]) -> float:
    """
    Cách 1 (xấu): insert từng dòng (nhưng vẫn trong 1 transaction).
    """
    t0 = time.perf_counter()
    with engine.begin() as conn:
        conn.execute(events.delete())
        for r in rows:
            conn.execute(events.insert().values(**r))
    t1 = time.perf_counter()
    return t1 - t0


def insert_batch(engine, events: Table, rows: list[dict]) -> float:
    """
    Cách 2 (tốt): executemany - insert batch 1 lần.
    """
    t0 = time.perf_counter()
    with engine.begin() as conn:
        conn.execute(events.delete())
        conn.execute(events.insert(), rows)  # executemany
    t1 = time.perf_counter()
    return t1 - t0


def upsert_chunked(engine, events: Table, rows: list[dict], batch_size: int = 5000) -> float:
    """
    Cách 3 (production): upsert theo chunks.
    Chạy lại không bị duplicate, update nếu event_id đã tồn tại.
    """
    t0 = time.perf_counter()

    stmt = sqlite_insert(events)
    stmt = stmt.on_conflict_do_update(
        index_elements=[events.c.event_id],
        set_={
            "user_id": stmt.excluded.user_id,
            "event": stmt.excluded.event,
            "amount": stmt.excluded.amount,
            "ts": stmt.excluded.ts,
        },
    )

    with engine.begin() as conn:
        conn.execute(events.delete())  # demo idempotent
        for batch in chunked(rows, batch_size):
            conn.execute(stmt, batch)

    t1 = time.perf_counter()
    return t1 - t0


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    out_dir = root / "data" / "processed" / "day24"
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / "events.db"

    engine, events = setup_db(db_path)

    rows = make_fake_rows(50_000)  # demo 50k (đủ thấy khác biệt). Bạn có thể đổi 100_000.

    t_row = insert_per_row(engine, events, rows)
    t_batch = insert_batch(engine, events, rows)
    t_upsert = upsert_chunked(engine, events, rows, batch_size=5000)

    print("DB:", db_path)
    print("rows:", len(rows))
    print(f"insert_per_row: {t_row:.3f}s")
    print(f"insert_batch  : {t_batch:.3f}s")
    print(f"upsert_chunked: {t_upsert:.3f}s")

    # Bonus: chạy upsert lần 2 để chứng minh idempotent
    t_upsert2 = upsert_chunked(engine, events, rows, batch_size=5000)
    print(f"upsert_chunked (run2): {t_upsert2:.3f}s  (should not increase row count)")

    # Check row count
    with engine.connect() as conn:
        cnt = conn.execute(events.count()).scalar()  # SQLAlchemy 2.x: có thể không có .count() tuỳ version
        # Nếu lỗi, thay bằng:
        # from sqlalchemy import select, func
        # cnt = conn.execute(select(func.count()).select_from(events)).scalar_one()
        print("row_count_in_db:", cnt)


if __name__ == "__main__":
    main()