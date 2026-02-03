import csv
import json
from pathlib import Path
from typing import Any

from .types_def import Record
from .file_io import write_jsonl, write_parquet

records=[{"event_id":"e010","user_id":"u2","event":"purchase","amount":15.0,"ts":"2026-01-13T11:00:00+07:00"},
{"event_id":"e011","user_id":"uu","event":"purchase","amount":15.0,"ts":"2026-01-13T11:00:00+07:00"}]
write_jsonl(Path("data/raw/day2_events.jsonl"), records, append=True)