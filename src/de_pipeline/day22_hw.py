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
    print("dataaaaaaaaaaaa rawwwwwwwwwwww")
    print(rows)

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
        #In ra 5 dòng đầu tiên trong DB (ORDER BY ts).

        stmt3=(select(events)
               .select_from(events)
               .order_by(events.c.ts)
               .limit(5))
        results3 = conn.execute(stmt3)
        print("\nIn ra 5 dòng đầu tiên trong DB:")
        for row in results3:
            print(dict(row._mapping))
        #Thêm filter theo user:query tất cả record của user_id="u1".
        stmt4=(select(events)
               .select_from(events)
               .where(events.c.user_id=="u1"))
        results4 = conn.execute(stmt4)
        print("\nquery tất cả record của user_id='u1'")
        for row in results4:
            print(dict(row._mapping))
        """Parameterized query (quan trọng):

            Viết function get_user_events(conn, user_id: str) trả list events cho user đó.

            Không dùng f-string để nhét thẳng user_id vào SQL."""
        def get_user_events(conn, user_id: str)-> list[dict]:
            stmt5=(select(events)
               .select_from(events)
               .where(events.c.user_id==user_id))
            list_out=[]
            results5 = conn.execute(stmt5)
            for row in results5:
                a=dict(row._mapping)
                list_out.append(a)
            return list_out
        u1_output=get_user_events(conn,"u1")
        print("\nqlist events cho user user_id='u1'")
        print(u1_output)
        """tạo thêm table daily_kpi:

        event_date, total_events, purchase_total

        Insert kết quả aggregate theo ngày vào bảng này."""
        daily_kpi = Table(
        "daily_kpi",
        metadata,
        Column("event_date", String, nullable=False),
        Column("total_events", Float, nullable=False),
        Column("purchase_total", Float, nullable=False),
        )
        #TAO TABLE NEU CHUA CO
        metadata.create_all(engine)
        before_cnt = conn.execute(select(func.count()).select_from(daily_kpi)).scalar_one()
        print("before insert:", before_cnt)
        
        stmt6 = daily_kpi.insert().from_select(
        ["event_date", "total_events", "purchase_total"],
        select(
           func.strftime("%d-%m-%Y", events.c.ts).label("event_date"),
           func.count(events.c.event_id).label("total_events"),
           func.sum(events.c.amount).label("purchase_total"),
        )
        .where(events.c.event == "purchase")
        .group_by(func.strftime("%d-%m-%Y", events.c.ts))
         )

        conn.execute(stmt6)
        

        after_cnt = conn.execute(select(func.count()).select_from(daily_kpi)).scalar_one()
        print("after insert:", after_cnt)

        stmt7=(select(daily_kpi)
               .select_from(daily_kpi)
               )
        results7 = conn.execute(stmt7)
        print("\nquery tất cả record của daily_kpi")
        for row in results7:
            print(dict(row._mapping))



if __name__ == "__main__":
    main()
