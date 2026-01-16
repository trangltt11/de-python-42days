import json
from pathlib import Path


def read_jsonl(path: Path) -> list[dict]:
    """
    Đọc file JSONL:
    - Mỗi dòng là 1 JSON object
    - Trả về list các dict
    """
    records: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def main() -> None:
    path = Path("data/raw/day2_events.jsonl")

    # (1) Parse: JSONL -> list of dict
    records = read_jsonl(path)
    print("Total records:", len(records))
    print("First record:", records[0])  # minh hoạ dict

    # (2) Filter (cơ bản): chỉ lấy purchase
    purchases = [r for r in records if r.get("event") == "purchase"]
    print("Purchases:", len(purchases))

    # (3) Aggregate 1: tổng amount của purchase
    total_purchase_amount = sum(float(r.get("amount", 0))  for r in purchases)
    print("Total purchase amount:", total_purchase_amount)

    # (4) Aggregate 2: tổng amount theo user_id (group by)
    #     Dùng dict để gom nhóm: totals[user] += amount
    totals_by_user: dict[str, float] = {}
    for r in purchases:
        user = r.get("user_id")
        amt = float(r.get("amount", 0))
        totals_by_user[user] = totals_by_user.get(user, 0.0) + amt

    print("Totals by user:", totals_by_user)

    # (5) Deduplicate bằng set: loại event_id trùng
    seen_ids: set[str] = set()
    deduped: list[dict] = []
    for r in records:
        eid = r.get("event_id")
        if eid in seen_ids:
            continue
        seen_ids.add(eid)
        deduped.append(r)

    print("After dedupe:", len(deduped))

    # (6) Aggregate 3 (khó hơn): đếm số event theo (user_id, event)
    #     Dùng tuple làm key nhiều cột: (user_id, event)
    count_by_user_event: dict[tuple[str, str], int] = {}
    for r in deduped:
        key = (r.get("user_id"), r.get("event"))
        count_by_user_event[key] = count_by_user_event.get(key, 0) + 1

    print("Count by (user,event):", count_by_user_event)

    #deduped_records
    print("---------------------------------------------------")
    set_deduped_records=set()
    deduped_records=[]
    for i in records:
        k=i.get("event_id")
        print("---------------------------------------------------")
        print(k)
        
        if k in set_deduped_records: 
            print('value trung '+ k)
            continue
        deduped_records.append(i)
        set_deduped_records.add(k)
    print (set_deduped_records)
    print("---------------------------------------------------")
    print (deduped_records)
    #4.2 So sánh
    #deduped_records=[i for i in records if i.get("event")=='purchase']
    print("---------------------------------------------------")
    sum_deduped_records={}
    for i in deduped_records:
        if i.get("event")=='purchase':
            amt=float(i.get("amount",0) )
            sum_deduped_records['purchase']=sum_deduped_records.get("purchase",0)+amt
    print(sum_deduped_records)
    print("---------------------------------------------------")
    sum_records={}
    for i in records:
        if i.get("event")=='purchase':
            amt=float(i.get("amount",0) )
            sum_records['purchase']=sum_records.get("purchase",0)+amt
    print(sum_records)    
    #Bài 4.3 Tìm danh sách các event_id bị trùng (xuất hiện >= 2 lần).
    print("---------------------------------------------------")
    dict_event={}    
    for i in records:
        key=i.get("event_id")
        dict_event[key]=dict_event.get(key,0)+1
    print("---------------------------------------------------")
    a={k: v for k, v in dict_event.items() if v >= 2}
    print(a)
    #Level 5 — Group by nhiều cột bằng tuple key (khó)
    count_by_user_event={}
    amount_by_user_evnet={}
    for i in records:
        ii=(i.get("user_id"),i.get("event"))
        amt=i.get("amount")
        count_by_user_event[ii]=count_by_user_event.get(ii,0)+1
        amount_by_user_evnet[ii]=amount_by_user_evnet.get(ii,0)+amt
    print(count_by_user_event)
    print(amount_by_user_evnet)
       
        


if __name__ == "__main__":
    main()
