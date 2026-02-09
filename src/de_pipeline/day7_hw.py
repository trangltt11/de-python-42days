
from .file_io import read_jsonl
from datetime import datetime
import json
from pathlib import Path
"""Level 3 (vừa → khó)

B3.1 Thêm field event_minute = YYYY-MM-DD HH:MM theo Bangkok (dùng strftime).
B3.2 Group theo (event_date, event_hour, event_minute) và đếm record."""

PROJECT_ROOT = Path(__file__).resolve().parents[2]   # .../de-python-42days
path = PROJECT_ROOT / "data" / "raw" / "day2_events.jsonl"
records= read_jsonl(path)
records_time=[{**r,  "event_date": datetime.fromisoformat(r["ts"]).strftime("%Y-%m-%d"),  
                     "event_hour": datetime.fromisoformat(r["ts"]).strftime(("%H")),  
                     "event_minute": datetime.fromisoformat(r["ts"]).strftime(("%H:%M"))}  for r in records]

group_records:dict[tuple[str,str,str],list[dict]]={}
for i in records_time:
    key= (i["event_date"],i["event_hour"],i["event_minute"])
    if key in group_records:
        group_records[key].append(i)
    else:
        group_records[key] = [i]
for k, v in group_records.items():
    print(k, len(v))

