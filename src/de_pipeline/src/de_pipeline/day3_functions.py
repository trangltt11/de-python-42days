from typing import Callable, Any
records=[{"event_id":"e001","user_id":"u1","event":"view","amount":0,"ts":"2026-01-13T09:00:00+07:00"},
{"event_id":"e002","user_id":"u1","event":"purchase","amount":120.5,"ts":"2026-01-13T09:02:00+07:00"},
{"event_id":"e003","user_id":"u2","event":"purchase","amount":75.0,"ts":"2026-01-13T09:05:00+07:00"},
{"event_id":"e004","user_id":"u2","event":"view","amount":0,"ts":"2026-01-13T09:06:00+07:00"},
{"event_id":"e005","user_id":"u1","event":"purchase","amount":30.0,"ts":"2026-01-13T10:01:00+07:00"},
{"event_id":"e006","user_id":"u3","event":"refund","amount":-30.0,"ts":"2026-01-13T10:10:00+07:00"},
{"event_id":"e007","user_id":"u3","event":"purchase","amount":200.0,"ts":"2026-01-13T10:12:00+07:00"},
{"event_id":"e008","user_id":"u3","event":"purchase","amount":200.0,"ts":"2026-01-13T10:12:00+07:00"},
{"event_id":"e008","user_id":"u3","event":"purchase","amount":200.0,"ts":"2026-01-13T10:12:00+07:00"},
{"event_id":"e009","user_id":"u2","event":"purchase","amount":15.0,"ts":"2026-01-13T11:00:00+07:00"}
]
#Bài 1.1 Viết is_refund(r) trả True nếu event == "refund".
Record ={}
def is_refund(r:Record, type_event:str )-> bool:
    return r.get("event")==type_event

check_kq=[i for i in records if is_refund(i,"view")]
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
#Bài 1.2 Dùng filter_records(deduped, is_refund) để lấy refunds và tính tổng refund amount.
def filter_records (r:records)->float:
    a=deduped_record(records)
    refund_records=[i for i in a if is_refund(i,"refund")]
    return (sum(i.get("amount") for i in refund_records))
print(filter_records(records))
