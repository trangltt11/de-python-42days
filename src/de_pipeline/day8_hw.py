from __future__ import annotations
from .file_io import read_jsonl
from pathlib import Path
import pandas as pd
"""B1.1 In ra số dòng và số cột của df.
Gợi ý: df.shape

B1.2 In ra 5 dòng cuối.
Gợi ý: df.tail()"""

PROJECT_ROOT = Path(__file__).resolve().parents[2]   # .../de-python-42days
path = PROJECT_ROOT / "data" / "raw" / "day2_events.jsonl"
df=pd.read_json(path, lines=True)
print(df)
print(df.shape)
print(df.tail(5))

"""B2.1 Tạo cột is_purchase (True/False) dựa trên event == "purchase".
B2.2 Lọc df chỉ lấy purchase và tính tổng amount.
"""
df["is_purchase"]=df["event"].astype(str).str.strip().str.lower()=="purchase"
print("============================")
print(df)
print("============================")
print(df.shape[0])
print("============loc chi lay event purchase================")
a = df.loc[df["is_purchase"]==True]

print("============================")
print(a)

print("========dem so dong====================")
print(a.shape[0])
print("==========tinh tong so amoun cua event purchase==================")
sum_amount=df["amount"].sum()
print(sum_amount)
"""Level 3 (vừa → khó)

B3.1 Group by user_id và tính:

purchase_count (số purchase)

purchase_total (tổng amount purchase)

Gợi ý: dùng df[df["event"]=="purchase"].groupby("user_id")..."""
def caculate_df (type_event:str, path: Path )-> None:
    
    df=pd.read_json(path, lines=True)
    mask_event=df["event"].astype(str).str.strip().str.lower()== type_event.lower()

    df_event=df.loc[df["event"==type_event]]