from __future__ import annotations

from pathlib import Path
import pandas as pd
def main() -> None:
    root=Path(__file__).resolve().parents[2]
    event_path=root/"data"/"raw"/ "day2_events.jsonl"
    users_path = root / "data" / "raw" / "day9_users.csv"  # từ Day 9
    #1: load
    events=pd.read_json(event_path, lines=True)
    users = pd.read_csv(users_path)
    users["user_id"] = users["user_id"].astype(str).str.strip()
    """B1.1 Tính tổng amount theo event:

    joined.groupby("event")["amount"].sum()


    B1.2 Đếm số events theo event_hour."""
    #2: clean
    events["event"]=events["event"].astype(str).str.strip().str.lower()
    events["amount"]=pd.to_numeric(events["amount"],errors="coerce").fillna(0.0)
    events["ts"]=pd.to_datetime(events["ts"], errors="coerce")
    #3 deduplicate
    events_uni=events.drop_duplicates(subset=["event_id"], keep="first").copy()
    
    event_amount=(events_uni.groupby("event")
                  .agg(total_amout=("amount", "sum"))
                  .reset_index()
                  )
    print(event_amount)
    events_uni["ts_hour"]= events_uni["ts"].dt.strftime("%d-%m-%Y %H")
    events_uni["event_date"]=events_uni["ts"].dt.strftime("%d-%m-%Y ")
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
                        purchase_events=("event_id", lambda s: s[events_uni.loc[events_uni.index,"event"]=="purchase"].count()),
                        purchase_amount=("amount", lambda s: s[events_uni.loc[events_uni.index,"event"]=="purchase"].sum()))
                    .reset_index())
    print (ts_event_date)
    """"Level 3 (vừa → khó)

        B3.1 Tính conversion đơn giản theo ngày:

        conversion = purchase_users / total_users_that_day
        (purchase_users: nunique user_id trong purchases theo ngày)"""
    #events_uni_puchase= events_uni.loc[events_uni["event"]=="purchase"]
    conversion=(events_uni.groupby("event_date")
                .agg(purchase_users=("user_id", lambda s: s[events_uni.loc[events_uni.index,"event"]=="purchase"].nunique()),
                     total_users_that_day=("user_id","count"))
                .reset_index())
    conversion["conversion"]=conversion["purchase_users"]/conversion["total_users_that_day"]
    print(conversion)
    """Level 4 (khó)

        B4.1 Tạo bảng wide cho segment x event counts:

        index: segment

        columns: event

        values: count"""
    joined= events.merge(users, on="user_id", how="left", validate="many_to_one")
    pivot_table=pd.pivot_table(data=joined,index="segment", columns="event", values="event_id", aggfunc="count", fill_value=0,dropna=False)
    print(joined)
    print (pivot_table)

if __name__ == "__main__":
    main()