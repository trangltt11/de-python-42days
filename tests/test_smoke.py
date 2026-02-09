<<<<<<< HEAD
a={'event_id': 'e001', 'user_id': 'u1', 'event': 'Purchase', 'amount': 120.5, 'ts': '2026-01-11T09:05:00+07:00', 'event_date': '2026-01-11', 'event_hour': '09', 'event_minute': '05'}
print(a['event_id'])
=======
from zoneinfo import ZoneInfo
from datetime import datetime, timezone


ts="2026-01-13T09:00:00+07:00"
dt=datetime.fromisoformat(ts)
if dt.tzinfo is None:
    raise ValueError("ts must include timezone offset, e.g. +07:00")
BKK = ZoneInfo("Asia/Bangkok")  # timezone của bạn
dtbk=dt.astimezone(BKK)
print(dt, dtbk)
>>>>>>> f59c9b0b812bed756bcaa1160c86abf9e08682d7
