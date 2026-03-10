from typing import Any
import json
from pathlib import Path

Record = dict[str, Any]
path= Path(r"D:\python tutorial\de-python-42days\data\raw\day13_events.csv")
records: list[Record] = []
with path.open("r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        records.append(json.loads(line))
print(records)