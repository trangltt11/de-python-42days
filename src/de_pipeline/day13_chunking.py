from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import pandas as pd


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    path = root / "data" / "raw" / "day13_events.csv"

    # accumulators
    count_by_event = defaultdict(int)
    sum_by_event = defaultdict(float)

    # nunique user_id toàn cục (nhớ user đã gặp)
    seen_users = set()

    # dedupe event_id toàn cục (nhớ event_id đã gặp)
    seen_event_ids = set()

    chunksize = 3  # demo nhỏ, thực tế có thể 100_000

    for chunk in pd.read_csv(path, chunksize=chunksize):
        # clean nhẹ trong chunk
        chunk["event"] = chunk["event"].astype(str).str.strip().str.lower()
        chunk["amount"] = pd.to_numeric(chunk["amount"], errors="coerce").fillna(0.0)

        # dedupe event_id (giữ dòng đầu tiên toàn cục)
        mask_new = ~chunk["event_id"].isin(seen_event_ids)
        chunk = chunk[mask_new].copy()
        seen_event_ids.update(chunk["event_id"].tolist())

        # update seen users
        seen_users.update(chunk["user_id"].dropna().astype(str).tolist())

        # aggregate trong chunk
        grp = chunk.groupby("event")["amount"].agg(["count", "sum"]).reset_index()

        for _, row in grp.iterrows():
            ev = row["event"]
            count_by_event[ev] += int(row["count"])
            sum_by_event[ev] += float(row["sum"])

    print("=== FINAL KPI (chunked) ===")
    print("unique_users:", len(seen_users))
    print("count_by_event:", dict(count_by_event))
    print("sum_by_event:", dict(sum_by_event))


if __name__ == "__main__":
    main()
