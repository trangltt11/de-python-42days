from datetime import datetime

class RecordValidationError(ValueError):
    """Lỗi validation cho một record (dữ liệu không hợp lệ)."""
class RecordValidatessttionError(ValueError):
    """Lỗi validation cho một record (dữ liệu không hợp lệ)."""

r={"event_id": "aa09", "user_id": "u1", "event": "", "amount": 0, "ts": "2026-01"}

ts= r.get("ts","")
try:
        convert_ts=datetime.fromisoformat(ts)
        if convert_ts.tzinfo is None:
             raise RecordValidationError("Timestamp must include timezone (e.g., +07:00 or Z)")
except :
        raise RecordValidationError("Timestamp must include timezone ...")