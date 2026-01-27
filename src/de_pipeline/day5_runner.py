from pathlib import Path
from collections import Counter

from .io import read_jsonl
from .ops import dedupe_by_key, group_sum
from .predicates import is_purchase
from .validate import validate_records


def main() -> None:
    path = Path("data/raw/day2_events.jsonl")
    path_out= Path("data/raw/bad_records.jsonl")
    records = read_jsonl(path)

    # Dedupe trước (tuỳ bạn, có thể validate trước cũng được)
    deduped = dedupe_by_key(records, "event_id")

    valid, results = validate_records(deduped,path_out)

    # Thống kê lỗi
    error_messages = [r.message for r in results if not r.ok]
    print("Total:", len(records))
    print("After dedupe:", len(deduped))
    print("Valid:", len(valid))
    print("Invalid:", len(deduped) - len(valid))

    # Đếm loại lỗi
    error_types = [r for r in results if not r.ok]
    print("Error types:", len(error_types))


    # Chạy aggregate trên valid records
    purchase_by_user = group_sum(valid, "user_id", "amount", filter_fn=is_purchase)
    print("Purchase total by user:", purchase_by_user)



if __name__ == "__main__":
    main()
    
