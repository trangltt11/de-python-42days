class RecordValidationError(ValueError):
    """Lỗi validation cho một record (dữ liệu không hợp lệ)."""

def require_keys(r):
    missing = ["user_id"]
    if missing:
        raise RecordValidationError(f"Missing required keys: {missing}")


require_keys({})

