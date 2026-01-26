from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .types_def import Record
from .ops import safe_float
from .io import write_jsonl

import json
from typing import Callable, Any

class RecordValidationError(ValueError):
    """Lỗi validation cho một record (dữ liệu không hợp lệ)."""


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    error_type: str | None = None
    message: str | None = None


REQUIRED_KEYS = ("event_id", "user_id", "event", "ts")


def require_keys(r: Record, keys: tuple[str, ...] = REQUIRED_KEYS) -> None:
    """Bắt buộc phải có các key quan trọng."""
    missing = [k for k in keys if k not in r]
    if missing:
        raise RecordValidationError(f"Missing required keys: {missing}")


def validate_event(r: Record) -> None:
    """event phải là string và thuộc tập cho phép."""
    event = r.get("event")
    if not isinstance(event, str) or not event.strip():
        raise RecordValidationError("event must be a non-empty string")
    
    event_id=r.get("event_id")
    if event_id[0] not in "e"   :
        raise RecordValidationError(f"event '{event_id}' value must start with e and follow with number")
    try:
        number= int(event_id[1:])
    except:
        raise RecordValidationError(f"event '{event_id}' follow with number ater first character")

    allowed = {"view", "purchase", "refund"}
    event_norm = event.strip().lower()
    if event_norm not in allowed:
        raise RecordValidationError(f"event '{event_norm}' not in allowed={sorted(allowed)}")


def validate_amount(r: Record) -> None:
    """
    Rule đơn giản:
    - purchase: amount > 0
    - view: amount == 0 (hoặc thiếu thì coi 0)
    - refund: amount < 0
    """
    event = str(r.get("event", "")).strip().lower()
    amt = safe_float(r.get("amount"), 0.0)

    if event == "purchase" and amt <= 0:
        raise RecordValidationError("purchase must have amount > 0")
    if event == "view" and amt != 0:
        raise RecordValidationError("view must have amount == 0")
    if event == "refund" and amt >= 0:
        raise RecordValidationError("refund must have amount < 0")





def validate_record(r: Record) -> None:
    """Validate đầy đủ 1 record. Nếu sai -> raise RecordValidationError."""
    require_keys(r)
    validate_event(r)
    validate_amount(r)


def validate_records(records: list[Record], path:str) -> tuple[list[Record], list[ValidationResult]]:
    """
    Validate nhiều record:
    - Trả về (valid_records, results)
    - results chứa ok=False với loại lỗi và message, để thống kê.
    """
    valid: list[Record] = []
    results: list[ValidationResult] = []

    for r in records:
        try:
            validate_record(r)
            valid.append(r)
            results.append(ValidationResult(ok=True))
        except RecordValidationError as e:
            results.append(ValidationResult(ok=False, error_type=type(e).__name__, message=str(e) ))
            write_jsonl(path,r)
    return valid, results
