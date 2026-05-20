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


def main() -> None:
      #Build Parquet partition
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
    out_root = root / "data" / "processed" / "day28"
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

#------------------------------------Load SQLite bằng upsert
    root = Path(__file__).resolve().parents[2]
    in_path = root / "data" / "processed"/"day28"
    db_path = out_dir / "events.db"

    dfs = []

    for parquet_file in in_path.rglob("*.parquet"):
        df = pd.read_parquet(parquet_file)
        dfs.append(df)

    print("=======================================")
    print(dfs)

    all_df = pd.concat(dfs, ignore_index=True)
    print("=======================================")
    print(all_df)
    #
    engine = get_engine(db_path)
    metadata = MetaData()
    events = define_schema(metadata)
    metadata.create_all(engine)

    rows= all_df[["event_id", "user_id", "event", "amount", "ts"]].to_dict(orient="records")
    print(rows)
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
        
    with engine.connect() as conn:
        total = conn.execute(select(func.count()).select_from(events)).scalar_one()
        print("Total rows in DB:", total)
if __name__ == "__main__":
    main()