
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





vr = ValidationResult(False, "X", "Y")
results = [vr]

print(results )
print(results[0])        # <class 'ValidationResult'>
print(results[0].ok)           # False
print(results[0].error_type)   # X
print(results[0].message)      # Y
