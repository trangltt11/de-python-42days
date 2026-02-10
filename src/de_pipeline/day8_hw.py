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
print(df)