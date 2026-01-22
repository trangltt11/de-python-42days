from pathlib import Path

from .io import read_jsonl
from .ops import dedupe_by_key, filter_records, group_sum, safe_float
from .predicates import is_purchase


def run(path: Path) -> dict:
    records = read_jsonl(path)
    deduped = dedupe_by_key(records, "event_id")
    purchases = filter_records(deduped, is_purchase)

    total_purchase = sum(safe_float(r.get("amount")) for r in purchases)
    purchase_by_user = group_sum(deduped, "user_id", "amount", filter_fn=is_purchase)

    return {
        "total_records": len(records),
        "after_dedupe": len(deduped),
        "purchase_records": len(purchases),
        "total_purchase": total_purchase,
        "purchase_by_user": purchase_by_user,
    }


def main() -> None:
    path = Path("data/raw/day2_events.jsonl")
    result = run(path)
    print(result)


if __name__ == "__main__":
    main()
