class RecordValidationError(ValueError):
    """Lỗi validation cho một record."""
    pass


def validate_record(record):
    if "user_id" not in record:
        raise RecordValidationError("Missing user_id")
validate_record({"name": "Khanh"})