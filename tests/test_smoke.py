
from dataclasses import dataclass
@dataclass(frozen=True)
class ValidationResult(ValueError):
    ok: bool
    error_type: str | None = None
    message: str | None = None

def require_keys(r):
    missing = ["user_id"]
    if "user_id" not in r:
        raise ValidationResult("looix")





a=require_keys({})
