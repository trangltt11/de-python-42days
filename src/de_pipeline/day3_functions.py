from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, Any


Record = dict[str, Any]


def read_jsonl(path: Path) -> list[Record]:
    """Đọc JSONL (mỗi dòng 1 JSON object) -> list[dict]."""
    records: list[Record] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records

path = Path("data/raw/day2_events.jsonl")
records = read_jsonl(path)

#Bài 1.1 Viết filter_fn(r) trả True nếu event == "refund".
def filter_fn(r:Record, type_event:str )-> bool:
    return r.get("event")==type_event

check_kq=[i for i in records if filter_fn(i,"view")]
print(check_kq)
#deduped
def deduped_record (r:records)-> list[dict]:
    set_dedupe:set[str]=set()
    list_dedupe:[dict]=[]
    for i in r:
        if i.get("event_id") in set_dedupe:
            continue
        list_dedupe.append(i)
        set_dedupe.add(i.get("event_id") )
    return list_dedupe
print("------------------------") 
print(deduped_record(records))
print("------------------------") 
#Bài 1.2 Dùng filter_records(deduped, filter_fn) để lấy refunds và tính tổng refund amount.
def filter_records (r:records)->float:
    a=deduped_record(records)
    refund_records=[i for i in a if filter_fn(i,"refund")]
    return (sum(i.get("amount") for i in refund_records))
print(filter_records(records))
#Bài 2.1 Viết hàm
"""Ví dụ: đếm số event theo user_id

Nếu có filter_fn, chỉ đếm record thỏa điều kiện"""
def group_count(r:records, group_key:str) -> dict:

    dict_user_event={}
    list_event_purchase:list[dict]=[i for i in r if filter_fn(i,"purchase") ]
    for i in list_event_purchase:
        key=i.get("user_id")
        dict_user_event[key]= dict_user_event.get(key,0)+1
    return dict_user_event
print(group_count(records,"user_id"))

