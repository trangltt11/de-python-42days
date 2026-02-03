from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from .types_def import Record


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


# ---------- JSONL ----------

def read_jsonl(path: Path) -> list[Record]:
    """Đọc JSONL -> list[Record]."""
    records: list[Record] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def write_jsonl(path: Path, records: list[Record], *, append: bool = False) -> None:
    """
    Ghi JSONL:
    - append=False: overwrite
    - append=True : nối thêm cuối file
    """
    ensure_parent_dir(path)
    mode = "a" if append else "w"
    with path.open(mode, encoding="utf-8", newline="\n") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


# ---------- CSV ----------

def read_csv(path: Path) -> list[Record]:
    """Đọc CSV -> list[Record] (toàn string)."""
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


def write_csv(path: Path, records: list[Record], fieldnames: list[str]) -> None:
    """Ghi CSV từ list[Record]."""
    ensure_parent_dir(path)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow({k: r.get(k, "") for k in fieldnames})


# ---------- Parquet (pandas) ----------

def write_parquet(path: Path, records: list[Record]) -> None:
    """Ghi Parquet (cơ bản) dùng pandas + pyarrow."""
    ensure_parent_dir(path)
    import pandas as pd

    df = pd.DataFrame(records)
    df.to_parquet(path, index=False)  # cần pyarrow
