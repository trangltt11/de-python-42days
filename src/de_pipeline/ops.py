from __future__ import annotations

from typing import Any, Callable
from .types_def import Record


def safe_float(x: Any, default: float = 0.0) -> float:
    """Ép kiểu float an toàn: None/missing/''/'  '/abc -> default."""
    try:
        if x is None:
            return default
        if isinstance(x, str) and x.strip() == "":
            return default
        return float(x)
    except (ValueError, TypeError):
        return default


def filter_records(records: list[Record], predicate: Callable[[Record], bool]) -> list[Record]:
    """Lọc records theo điều kiện predicate(record) -> True/False."""
    return [r for r in records if predicate(r)]


def dedupe_by_key(records: list[Record], key: str) -> list[Record]:
    """Dedupe giữ thứ tự theo 1 field, ví dụ key='event_id'."""
    seen: set[Any] = set()
    out: list[Record] = []
    for r in records:
        k = r.get(key)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def group_sum(
    records: list[Record],
    group_key: str,
    value_key: str,
    *,
    filter_fn: Callable[[Record], bool] | None = None
) -> dict[Any, float]:
    """Group-by 1 key và sum 1 value (có thể filter)."""
    totals: dict[Any, float] = {}
    for r in records:
        if filter_fn is not None and not filter_fn(r):
            continue
        g = r.get(group_key)
        v = safe_float(r.get(value_key), 0.0)
        totals[g] = totals.get(g, 0.0) + v
    return totals


def group_count(
    records: list[Record],
    group_key: str,
    *,
    filter_fn: Callable[[Record], bool] | None = None
) -> dict[Any, int]:
    """Group-by 1 key và count records (có thể filter)."""
    counts: dict[Any, int] = {}
    for r in records:
        if filter_fn is not None and not filter_fn(r):
            continue
        g = r.get(group_key)
        counts[g] = counts.get(g, 0) + 1
    return counts
