import json
from pathlib import Path
from .types_def import Record


def read_jsonl(path: Path) -> list[Record]:
    """Đọc JSONL (mỗi dòng 1 JSON object) -> list[Record]."""
    records: list[Record] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records
