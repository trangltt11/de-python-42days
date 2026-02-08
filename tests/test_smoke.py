from zoneinfo import ZoneInfo
from datetime import datetime, timezone


ts="2026-01-13T09:00:00+07:00"
dt=datetime.fromisoformat(ts)
if dt.tzinfo is None:
    raise ValueError("ts must include timezone offset, e.g. +07:00")
BKK = ZoneInfo("Asia/Bangkok")  # timezone của bạn
dtbk=dt.astimezone(BKK)
print(dt, dtbk)