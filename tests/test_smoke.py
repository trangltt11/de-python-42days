from datetime import datetime

class RecordValidationError(ValueError):
    """Lỗi validation cho một record (dữ liệu không hợp lệ)."""

r={"event_id": "aa09", "user_id": "u1", "event": "", "amount": 0, "ts": "2026-01-13"}

try:    
        ts= r.get("ts","")
        print(ts)
        convert_ts=datetime.fromisoformat(ts)
        print(convert_ts)
        print(type(convert_ts))

except:
        raise RecordValidationError("invalid ts format")