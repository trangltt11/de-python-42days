from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import pandas as pd

def main()-> None:
    root=Path(__file__).resolve().parents[2]
    root_path=root/"data"/"raw"/"day13_events.csv"
    # accumulators
    count_by_event = defaultdict(int)
    sum_by_event = defaultdict(float)
    # accumulators
    count_by_purchase_event = defaultdict(int)
    sum_by_purchase_event = defaultdict(float)
    purchase_total_by_user= defaultdict(float)

    # nunique user_id toàn cục (nhớ user đã gặp)
    seen_users = set()

    # dedupe event_id toàn cục (nhớ event_id đã gặp)
    seen_event_ids = set()

    chunksize = 2  # demo nhỏ, thực tế có thể 100_000
    
    for chunk in pd.read_csv(root_path, chunksize=chunksize):
        # clean nhẹ trong chunk
        chunk["event"]=chunk["event"].astype(str).str.strip().str.lower()
        chunk["amount"]=pd.to_numeric(chunk["amount"], errors="coerce").fillna(0.0)
        # dedupe event_id (giữ dòng đầu tiên toàn cục)
        mask=~chunk["event_id"].isin(seen_event_ids)
        chunk_new=chunk.loc[mask,:]
        seen_event_ids.update(chunk_new["event_id"])
        seen_users.update(chunk_new["user_id"])
        result_df= chunk_new.groupby("event").agg(event_count=("event","count"),
                                                  event_amount=("amount","sum")).reset_index()
        
        for _, row in result_df.iterrows():
            ev= row["event"]
            
            if ev == "purchase":
                """Level 2 (vừa)

                    B2.1 Tính thêm purchase_count và purchase_sum riêng.|"""
                count_by_purchase_event[ev] +=int(row["event_count"])
                sum_by_purchase_event[ev] +=float(row["event_amount"])
            count_by_event[ev] +=int(row["event_count"])
            sum_by_event[ev] +=float(row["event_amount"])
        
        """Level 3 (vừa → khó)

            B3.1 Thêm group theo user_id để tính purchase_total_by_user theo kiểu chunked.
            Gợi ý: dùng defaultdict(float) và cộng dồn."""
        result_user_df= chunk_new.loc[chunk_new["event"]=='purchase',:].groupby("user_id").agg(event_count=("event","count"),
                                                  event_amount=("amount","sum")).reset_index()
        print(result_user_df)
        
        for _,row in result_user_df.iterrows():
            us= row["user_id"]
            purchase_total_by_user[us]+=row["event_amount"]
    print("count_by_event: ", count_by_event)
    print("sum_by_event: ", sum_by_event)
    print("unique_users:", len(seen_users))
    print("count_by_purchase_event", count_by_purchase_event)
    print("sum_by_purchase_event", sum_by_purchase_event)
    print("purchase_total_by_user", purchase_total_by_user)
    """B4.1 Viết output report ra CSV:

        data/processed/day13/kpi_event.csv gồm event,count,sum

        data/processed/day13/kpi_summary.json gồm unique_users"""
    events = sorted(set(count_by_event.keys()) | set(sum_by_event.keys()))
    df_kpi = pd.DataFrame({
    "event": events,
    "count": [int(count_by_event.get(e, 0)) for e in events],
    "sum": [float(sum_by_event.get(e, 0.0)) for e in events],
})
    out_dir = root / "data" / "processed" / "day13"
    out_dir.mkdir(parents=True, exist_ok=True)
    kpi_event_path = out_dir / "kpi_event.csv"
    df_kpi.to_csv(kpi_event_path, index=False)
    summary = {"unique_users": len(seen_users), "users": sorted(seen_users)}
    kpi_summary_path = out_dir / "kpi_summary.json"
    with kpi_summary_path.open("w", encoding="utf-8") as f:
        import json
        json.dump(summary, f, ensure_ascii=False, indent=2)

  
   

if __name__ == "__main__":
    main()