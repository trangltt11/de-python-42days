
import json
from pathlib import Path

path = Path(r"E:\py file\LEAR PYTHON\de-python-42days\data\raw\day2_events.jsonl")
records: list[dict] = []
with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
print(records)