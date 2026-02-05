import csv
from datetime import datetime
import json
from pathlib import Path
from typing import Any

from .types_def import Record
from .io import write_jsonl as write_jsonl_io
from .file_io import write_jsonl ,ensure_parent_dir,write_parquet
from .validate import validate_record
from .io import read_jsonl

#---------------B1.1 Dùng write_jsonl(..., append=True) để thêm 2 records mới vào file data/raw/day2_events.jsonl (tự tạo record).
records=[{"event_id":"e010","user_id":"u2","event":"purchase","amount":15.0,"ts":"2026-01-13T11:00:00+07:00"},
{"event_id":"e011","user_id":"uu","event":"purchase","amount":15.0,"ts":"2026-01-13T11:00:00+07:00"}]
write_jsonl(Path("data/raw/day2_events.jsonl"), records, append=True)
#---------------B2.1 Viết hàm split_valid_invalid(records) trả về (valid, invalid) dựa trên validate_records.
def split_valid_invalid(records:list[dict])-> tuple[list[dict], list[dict]]:
    list_valid:list[dict]=[]
    list_invalid:list[dict]=[]
    for r in records:
        try:
            validate_record(r)
            list_valid.append(r)
        except:
            list_invalid.append(r)
    return(list_valid,list_invalid)

PROJECT_ROOT = Path(__file__).resolve().parents[2]   # .../de-python-42days
path = PROJECT_ROOT / "data" / "raw" / "day2_events.jsonl"
print("khoongggggggggggggggg loiiiiiiiiiiiiiiiii")

data_check=read_jsonl(path)
print(data_check)
valid_check,invalid_check=split_valid_invalid(data_check)
print("--------------valid_check------------------------------")
print(valid_check)
print("---------------------invalid_check-----------------------")
print(invalid_check)
#---------------B2.2 Ghi invalid ra data/bad/date=YYYY-MM-DD/bad.jsonl (partition theo date lấy từ ts).
def write_data_date_partition(record:Record, path: Path)-> None:

    date=record["ts"][:10]
    date_ts= datetime.strptime(date, "%Y-%m-%d").date()
    s = date_ts.strftime("%Y-%m-%d")
    ss=  "date="+s
    file_path = path/f"{ss}"/ f"{s}.jsonl"
    print(record)
    ensure_parent_dir(file_path)
    write_jsonl_io(file_path, record)

path_root =  PROJECT_ROOT / "data" / "bad" 

for i in invalid_check:
    write_data_date_partition(i,path_root)
print("---------------------hong loii-----------------------")
#---------------B3.1 Khi ghi Parquet, hãy chỉ ghi valid records và thêm cột mới
def write_data_date_partition(records:list[dict], path: Path)-> None:
    import pandas as pd
    for i in records:
        date=i["ts"][:10]
        date_ts= datetime.strptime(date, "%Y-%m-%d").date()
        i["ingest_date"] = date_ts
        s = date_ts.strftime("%Y-%m-%d")
        ss=  "date="+s
        file_path = path/f"{ss}"/ f"event.parquet"
        print(i)
        ensure_parent_dir(file_path)
        df = pd.DataFrame([i])
        df.to_parquet(file_path, index=False)  # cần pyarrow


path_root =  PROJECT_ROOT / "data" / "processed"     
write_data_date_partition(valid_check,path_root)

print("---------------------hong loii-----------------------")
def extract_date(ts: str) -> str:
    return ts[:10]  # "2026-01-13T..." -> "2026-01-13"
by_date: dict[str, list[Record]] = {}

for r in valid_check:
        ts = r.get("ts")
        if not isinstance(ts, str) or len(ts) < 10:
            # trường hợp cực hiếm nếu ts lỗi (thường không xảy ra vì đã validate)
            date = "unknown"
        else:
            date = extract_date(ts)

        # 5) thêm cột ingest_date (khuyên tạo dict mới, không sửa record gốc)
        r2 = {**r, "ingest_date": date}

        by_date.setdefault(date, []).append(r2)

print(by_date)        