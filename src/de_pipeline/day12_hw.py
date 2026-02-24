from __future__ import annotations

from pathlib import Path
import pandas as pd
def main() -> None:
    root=Path(__file__).resolve().parents[2]
    event_path=root/"data"/"raw"/"day2_events.jsonl"
    #1: load
    events=pd.read_json(event_path, lines=True)
    
    #2: clean
    event_uni=events.drop_duplicates(subset="event_id", keep="first").copy()
    event_uni["ts"]=pd.to_datetime(events["ts"], errors="coerce")
    event_uni["hourly_ts"]= event_uni["ts"].dt.strftime("%d-%m-%Y %H")
    
    """Level 1 (dễ)

    B1.1 Tạo hourly_counts chỉ đếm số events theo giờ (1 cột).
    Gợi ý:

    hourly_counts = df.resample("H")["event_id"].count()


    B1.2 Fill giờ thiếu = 0 (nếu có):

    hourly_counts = hourly_counts.asfreq("H", fill_value=0)"""
    event_uni = event_uni.set_index("ts").sort_index()
    print(event_uni)
    hourly_counts = event_uni.resample("h")["event_id"].count()
    print(hourly_counts)
    print("======================")
    hourly_counts_v2 = hourly_counts.asfreq("h", fill_value=0)
    print(hourly_counts_v2)
    """B2.1 Tính KPI theo giờ cho từng event (purchase/view/refund) bằng pivot:

        resample count theo giờ và event
        Gợi ý:

        tmp = df.reset_index()  # để có cột ts lại
        table = tmp.pivot_table(index=pd.Grouper(key="ts", freq="H"),
                                columns="event", values="event_id", aggfunc="count", fill_value=0)"""
    
if __name__ == "__main__":
    main()