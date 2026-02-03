import csv
import json
from pathlib import Path
from typing import Any

from .types_def import Record
from .file_io import write_jsonl, write_parquet
from .validate import validate_record
from .io import read_jsonl


records=[{"event_id":"e010","user_id":"u2","event":"purchase","amount":15.0,"ts":"2026-01-13T11:00:00+07:00"},
{"event_id":"e011","user_id":"uu","event":"purchase","amount":15.0,"ts":"2026-01-13T11:00:00+07:00"}]
write_jsonl(Path("data/raw/day2_events.jsonl"), records, append=True)

def split_valid_invalid(records:list[dict])-> tuple[list[dict], list[dict]]:
    list_valid:list[dict]=[]
    list_invalid:list[dict]=[]
    for r in records:
        try:
            validate_record(r)
            list_valid.append[r]
        except:
            list_invalid.append[r]
    return(list_valid,list_invalid)

path = Path(r"E:\py file\LEAR PYTHON\de-python-42days\data\raw\day2_events.jsonl")
print("khoongggggggggggggggg loiiiiiiiiiiiiiiiii")

data_check=read_jsonl(path)
print("khoongggggggggggggggg loiiiiiiiiiiiiiiiii22222222222")
valid_check,invalid_check=split_valid_invalid(data_check)
print(valid_check)
print("--------------------------------------------")
print(invalid_check)
