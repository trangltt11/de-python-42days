from pathlib import Path
from typing import Any
import json
Record=dict[str, Any]

def read_jsonl(path: Path) -> list[Record]:
    records:list[Record]=[]
    with path.open("r",encoding="utf-8") as f:
        for line in f:
            record=line.strip()
            if not record:
                continue
            try:
                records.append(json.loads(record))
            except json.JSONDecodeError:
                continue
        return records

a=read_jsonl(Path("d:/python tutorial/de-python-42days/data/raw/day2_events.jsonl"))
print(len(a))

