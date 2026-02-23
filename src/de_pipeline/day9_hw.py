from __future__ import annotations

from pathlib import Path
import pandas as pd


def main() -> None:
    root = Path(__file__).resolve().parents[2]  # de-python-42days
    events_path = root / "data" / "raw" / "day2_events.jsonl"
    users_path = root / "data" / "raw" / "day9_users.csv"

    # 1) Load events (fact)
    events = pd.read_json(events_path, lines=True)

    # Clean tối thiểu (giống Day 8)
    events["event"] = events["event"].astype(str).str.strip().str.lower()
    events["amount"] = pd.to_numeric(events["amount"], errors="coerce").fillna(0.0)
    events["ts"] = pd.to_datetime(events["ts"], errors="coerce")

    # Dedupe event_id để tránh double count
    events = events.drop_duplicates(subset=["event_id"], keep="first")

    # 2) Load users (dimension)
    users = pd.read_csv(users_path)
    print(users)
    users["user_id"] = users["user_id"].astype(str).str.strip()

    # 3) Data quality check trước khi join
    # users.user_id phải unique (dimension chuẩn)
    dup_users_keep_False = users["user_id"].duplicated(keep=False).sum()
    print("Duplicate user_id in users keep_False:", dup_users_keep_False)

    drop_duplecate_user=users.drop_duplicates(subset=["user_id"], keep=False)
    print("drop_duplecate_user:" ,drop_duplecate_user)
    # 4) Join (left join) + validate cardinality
    # events: many, users: one => many_to_one
    joined = events.merge(drop_duplecate_user, on="user_id", how="inner", validate="many_to_one")

    print(joined)
    """Level 2 (vừa)

    B2.1 Tính purchase_total theo segment (VIP vs Normal).
    Gợi ý:

    joined[joined["event"]=="purchase"].groupby("segment")["amount"].sum()"""
    purchase_total=joined.loc[joined["event"]=="purchase"].groupby("segment")["amount"].sum().reset_index()

    print("-------------------purchase amount total--------------------")
    print(purchase_total)
    """Level 3 (vừa → khó)

    B3.1 Cố tình tạo lỗi join:

    sửa day9_users.csv để user_id u3 xuất hiện 2 dòng

    chạy lại code → xem pandas báo lỗi gì với validate="many_to_one" """
    """Level 4 (khó)

    B4.1 Tạo “users_latest” bằng cách dedupe users (giữ dòng cuối) rồi mới join.
    Gợi ý: drop_duplicates(subset=["user_id"], keep="last")
    B4.2 Ghi report: số user bị trùng trước khi dedupe."""

    dup_users_keep_False = users["user_id"].duplicated(keep="last").sum()
    print("Duplicate user_id in users keep_False:", dup_users_keep_False)
    users_latest=users.drop_duplicates(subset=["user_id"],keep="last")
    print("users_latest",users_latest)
    joined_new = events.merge(users_latest, on="user_id", how="inner", validate="many_to_one")
    print("joined_new",joined_new)

if __name__ == "__main__":
    main()
