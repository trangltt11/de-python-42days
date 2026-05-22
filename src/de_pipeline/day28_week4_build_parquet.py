from __future__ import annotations
import json
from typing import Any
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    String, Float, DateTime, select, func, text
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from datetime import datetime

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


def query_kpi_by_event(engine)-> pd.DataFrame:
     sql= text("""
               select   event,
                        count(*) as cnt,
                        sum(amount) as total_amount
                from events
                group by event
                order by event
                 """)
     return pd.read_sql(sql,engine)

def query_purchase_by_user(engine) -> pd.DataFrame:
    """
    KPI purchase theo user:
    - purchase_count
    - purchase_total
    """
    sql = text("""
        SELECT
            user_id,
            COUNT(*) AS purchase_count,
            SUM(amount) AS purchase_total
        FROM events
        WHERE event = :event
        GROUP BY user_id
        ORDER BY purchase_total DESC
    """)
    return pd.read_sql(sql, engine, params={"event": "purchase"})

# -------------------------
# 2) DQ CHECKS
# -------------------------
@dataclass(frozen=True)
class DQCheck:
    name: str
    passed: bool
    severity: str  # "ERROR" / "WARN"
    details: dict[str, Any]


def dq_unique_event_id(engine) -> DQCheck:
    # total rows vs distinct event_id
    sql_total = text("SELECT COUNT(*) AS total FROM events")
    sql_distinct = text("SELECT COUNT(DISTINCT event_id) AS distinct_cnt FROM events")

    total = int(pd.read_sql(sql_total, engine).iloc[0]["total"])
    distinct_cnt = int(pd.read_sql(sql_distinct, engine).iloc[0]["distinct_cnt"])

    passed = (total == distinct_cnt)
    return DQCheck(
        name="unique_event_id",
        passed=passed,
        severity="ERROR",
        details={"total_rows": total, "distinct_event_id": distinct_cnt, "duplicates": total - distinct_cnt},
    )


def dq_event_domain(engine, allowed=("view", "purchase", "refund")) -> DQCheck:
    sql = text("SELECT DISTINCT event FROM events")
    values = pd.read_sql(sql, engine)["event"].astype(str).str.strip().str.lower().tolist()

    invalid = sorted(set(values) - set(allowed))
    passed = (len(invalid) == 0)
    return DQCheck(
        name="event_domain",
        passed=passed,
        severity="ERROR",
        details={"allowed": list(allowed), "invalid_values": invalid},
    )


def dq_amount_rules(engine) -> DQCheck:
    # purchase amount > 0
    sql_purchase_bad = text("""
        SELECT COUNT(*) AS n
        FROM events
        WHERE event = 'purchase' AND amount <= 0
    """)
    # view amount == 0
    sql_view_bad = text("""
        SELECT COUNT(*) AS n
        FROM events
        WHERE event = 'view' AND amount != 0
    """)
    # refund amount < 0
    sql_refund_bad = text("""
        SELECT COUNT(*) AS n
        FROM events
        WHERE event = 'refund' AND amount >= 0
    """)

    bad_purchase = int(pd.read_sql(sql_purchase_bad, engine).iloc[0]["n"])
    bad_view = int(pd.read_sql(sql_view_bad, engine).iloc[0]["n"])
    bad_refund = int(pd.read_sql(sql_refund_bad, engine).iloc[0]["n"])

    passed = (bad_purchase + bad_view + bad_refund) == 0
    return DQCheck(
        name="amount_rules",
        passed=passed,
        severity="ERROR",
        details={
            "bad_purchase_amount_le_0": bad_purchase,
            "bad_view_amount_ne_0": bad_view,
            "bad_refund_amount_ge_0": bad_refund,
        },
    )


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

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
    db_path = in_path / "events.db"

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
#------------------------------------
      # (A) KPI
    report_dir = root / "data" / "processed" / "week4_reports"
    ensure_dir(report_dir)
    kpi_by_event = query_kpi_by_event(engine)
    kpi_purchase_by_user = query_purchase_by_user(engine)

    kpi_by_event_path = report_dir / "kpi_by_event.parquet"
    kpi_purchase_path = report_dir / "kpi_purchase_by_user.parquet"

    kpi_by_event.to_parquet(kpi_by_event_path, index=False)
    kpi_purchase_by_user.to_parquet(kpi_purchase_path, index=False)

    # (B) DQ
    checks = [
        dq_unique_event_id(engine),
        dq_event_domain(engine),
        dq_amount_rules(engine),
    ]

    error_failed = [c for c in checks if c.severity == "ERROR" and not c.passed]
    status = "PASS" if len(error_failed) == 0 else "FAIL"

    dq_report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "input": {"db_path": str(db_path)},
        "summary": {
            "status": status,
            "error_failed": [c.name for c in error_failed],
        },
        "checks": [
            {"name": c.name, "passed": c.passed, "severity": c.severity, "details": c.details}
            for c in checks
        ],
        "outputs": {
            "kpi_by_event": str(kpi_by_event_path),
            "kpi_purchase_by_user": str(kpi_purchase_path),
            "dq_report": str(report_dir / "dq_report.json"),
        },
    }

    dq_report_path = report_dir / "dq_report.json"
    write_json(dq_report_path, dq_report)

    # Console summary
    print("Wrote:", kpi_by_event_path)
    print("Wrote:", kpi_purchase_path)
    print("Wrote:", dq_report_path)
    print("DQ status:", status)
    if status == "FAIL":
        print("ERROR failed:", dq_report["summary"]["error_failed"])
if __name__ == "__main__":
    main()