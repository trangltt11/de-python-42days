from .types_def import Record


def is_purchase(r: Record) -> bool:
    return r.get("event") == "purchase"


def is_refund(r: Record) -> bool:
    return r.get("event") == "refund"
