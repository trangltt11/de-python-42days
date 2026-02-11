from __future__ import annotations
from datetime import datetime, timezone
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
def caculate_df (type_event:str, path: Path )-> pd:
    
    df=pd.read_json(path, lines=True)
    mask_event=df["event"].astype(str).str.strip().str.lower()== type_event.lower()
    print("==========mask_event==================")
    print(mask_event)
    print("==========df_event==================")
    df_event=df.loc[mask_event]
    print(df_event)
    print("==========df_event_unique==================")
    df_event_unique=df_event.drop_duplicates(subset=["event_id"], keep="first")
    print(df_event_unique)
    df_user_id_total=df_event_unique.groupby("user_id").agg(
        purchase_count=("event_id", "count"),
        purchase_total=("amount","sum")
            
    ).reset_index()
    return df_user_id_total

df_user_id_total=caculate_df("purchase",path)
print(df_user_id_total)

"""B4.1 Xuất Parquet theo partition event_date (mô phỏng):

tạo 1 file parquet cho mỗi ngày vào folder data/processed/day8/date=YYYY-MM-DD/

Gợi ý: loop theo df["event_date"].unique()."""
def df_to_parquet(path: Path)-> None:
    df=pd.read_json(path, lines=True)
    df_unique=df.drop_duplicates(subset=["event_id"],keep='first').copy()
    df_unique["event_date"]= pd.to_datetime(df_unique["ts"], errors="coerce").dt.strftime("%Y-%m-%d")

    for i in df_unique["event_date"].unique() :
        str_Date=df_unique[df_unique["event_date"]==i]
        print(f"------------{i}------------------")
        print(str_Date)
        PROJECT_ROOT = Path(__file__).resolve().parents[2]   # .../de-python-42days
        out_path = PROJECT_ROOT / "data" / "processed" / "day8"/ f"date={i}"
        print(f"------------------------------------")
        print(out_path)
        str_Date.to_parquet(out_path, index=False)
    

df_to_parquet(path)