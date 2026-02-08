"""B1.1 Parse ts của record đầu tiên và in:

dt

dt.tzinfo

dt.astimezone(timezone.utc)"""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from .validate import Record
from .file_io import read_jsonl
from pathlib import Path
path: Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]   # .../de-python-42days
path = PROJECT_ROOT / "data" / "raw" / "day2_events.jsonl"
reoords= read_jsonl(path)
dt = datetime.fromisoformat(reoords[0]["ts"])
print(dt)
print(dt.tzinfo)
print(timezone.utc)
print(dt.astimezone(timezone.utc))
"""Level 2 (vừa)

B2.1 Viết hàm extract_date_bkk(ts: str) -> str trả YYYY-MM-DD theo Bangkok time.
B2.2 So sánh date theo Bangkok vs theo UTC (có thể khác nếu gần 00:00)."""
def extract_date_bkk(ts: str) -> str:
    dt=datetime.astimezone(ts)
    BKK = ZoneInfo("Asia/Bangkok")  # timezone của bạn
    if dt.tzinfo is None:
        raise ValueError("ts must include timezone offset, e.g. +07:00")
    return dt.astimezone(BKK)
"""Level 3 (vừa → khó)

B3.1 Thêm field event_minute = YYYY-MM-DD HH:MM theo Bangkok (dùng strftime).
B3.2 Group theo (event_date, event_hour, event_minute) và đếm record."""