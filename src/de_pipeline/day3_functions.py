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
def event_amount (r:records,event:str)->float:
    a=deduped_record(records)
    refund_records=[i for i in a if filter_fn(i,event)]
    return (sum(i.get("amount") for i in refund_records))
print("event_amount",event_amount(records,"purchase"))
#Bài 2.1 Viết hàm
"""Ví dụ: đếm số event theo user_id

Nếu có filter_fn, chỉ đếm record thỏa điều kiện"""
def group_count(r:records, group_key:str,event:str,filter_fn_event: Callable[[Record,str], bool]) -> dict:

    dict_user_event={}
    list_event_purchase:list[dict]=[i for i in r if filter_fn_event(i,event) ]
    for i in list_event_purchase:
        key=i.get("user_id")
        dict_user_event[key]= dict_user_event.get(key,0)+1
    return dict_user_event
print(group_count(records,"user_id","purchase",filter_fn))
"""Bài 3.1 Viết hàm normalize_record(r) trả dict mới (không sửa r gốc) với:

amount: luôn là float (dùng safe_float)

event: lower-case (vd "Purchase" → "purchase")

thiếu event_id hoặc user_id thì raise ValueError"""
def safe_float(x:Any)->float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0
    
def normalize_record(r: Record) -> Record:
    if "event_id" not in r or "user_id" not in r:
          raise ValueError("record can co key event_id va user_id")
    return {**r, "amount": float(r.get("amount")),"event": r.get("event").lower()}

new=[normalize_record (i) for i in records ] 
print(new)

def run_pipeline(path: Path) -> dict:
    records = read_jsonl(path)
    return {
      "total_records": len(records),
      "after_dedupe":  len(deduped_record(records)),
      "purchase_total": event_amount(records,"purchase"),
      "purchase_by_user": group_count(records,"user_id","purchase",filter_fn)
    }
def main() -> None:
    path = Path("data/raw/day2_events.jsonl")
    
    print("kq",run_pipeline(path) )

if __name__ == "__main__":
    main()
    