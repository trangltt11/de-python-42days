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

    # nunique user_id toàn cục (nhớ user đã gặp)
    seen_users = set()

    # dedupe event_id toàn cục (nhớ event_id đã gặp)
    seen_event_ids = set()

    chunksize = 2  # demo nhỏ, thực tế có thể 100_000
    
    for chunk in pd.read_csv(root_path, chunksize=chunksize):
        # clean nhẹ trong chunk
        chunk["event_id"]=chunk["event_id"].astype(str).str.strip().str.lower()
        chunk["amount"]=pd.to_numeric(chunk["amount"], errors="coerce").fillna(0.0)
        # dedupe event_id (giữ dòng đầu tiên toàn cục)
        mask=~chunk["event_id"].isin(seen_event_ids)
        chunk_new=chunk.loc[mask,:]
        seen_event_ids.update(chunk_new["event_id"])
        seen_users.update(chunk_new["user_id"])
        print(chunk_new)
        


if __name__ == "__main__":
    main()