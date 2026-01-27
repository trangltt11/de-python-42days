class RecordValidationError(ValueError):
    """Lỗi validation cho một record (dữ liệu không hợp lệ)."""
def must_be_positive(x:float)->float:
    if x<0:
        raise RecordValidationError(" must be > 0")
    return x



a={"event_id":"aa09","user_id":"a","event":'4',"amount":0,"ts":"2026-01-13T09:00:00+07:00"}


user_id=str(a.get("user_id","u"))

print(user_id)