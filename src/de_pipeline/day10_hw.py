from __future__ import annotations

from pathlib import Path
import pandas as pd
def main() -> None:
    root=Path(__file__).resolve().parents[2]
    event_path=root/"data"/"raw"/ "day2_events.jsonl"
    #1: load
    events=pd.read_json(event_path, lines=True)
    """B1.1 Tính tổng amount theo event:

    joined.groupby("event")["amount"].sum()


    B1.2 Đếm số events theo event_hour."""
    #2: clean
    events["event"]=events["event"].astype(str).str.strip().str.lower()
    events["amount"]=pd.to_numeric(events["amount"],errors="coerce").fillna(0.0)
    events["ts"]=pd.to_datetime(events["ts"], errors="coerce")
    #3 deduplicate
    events_uni=events.drop_duplicates(subset=["event_id"], keep="first")
    
    event_amount=(events_uni.groupby("event")
                  .agg(total_amout=("amount", "sum"))
                  .reset_index()
                  )
    print(event_amount)
    events_uni["ts_hour"]= events_uni["ts"].dt.strftime("%d-%m-%Y %H").copy()
    events_uni["event_date"]=events_uni["ts"].dt.strftime("%d-%m-%Y ").copy()
    print(events_uni)
    ts_hour_events=(events_uni.groupby("ts_hour")
                    .agg(event_hour=("event_id","count"))
                    .reset_index()
                    )
    print(ts_hour_events)
    """ Level 2 (vừa)

        B2.1 Tạo KPI theo event_date:

        total_events

        purchase_events

        purchase_amount
        (gợi ý filter purchases rồi merge/concat)"""
    ts_event_date=(events_uni.groupby("event_date")
                   .agg(total_events=("event_id","count"),
                        purchase_events=("event_id", lambda s: s[events_uni.loc[events_uni["event"]=="purchase","event_id"]].count()),
                        purchase_amount=("amount", lambda s: s[events_uni.loc[events_uni["event"]=="purchase","amount"]].sum()))
                    .reset_index())
    print (ts_event_date)
    
if __name__ == "__main__":
    main()