from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
records=[{"event_id":"e001","user_id":"u1","event":"Purchase","amount":120.5,"ts":"2026-01-11T09:05:00+07:00"},
{"event_id":"e002","user_id":"u1","event":"Purchase","amount":120.5,"ts":"2026-01-11T09:05:00+07:00"},
{"event_id":"e003","user_id":"u2","event":"purchase","amount":75.0,"ts":"2026-01-11T09:05:00+07:00"},
{"event_id":"e004","user_id":"u2","event":"purchase","amount":75.0,"ts":"2026-01-11T09:05:00+07:00"},
{"event_id":"e005","user_id":"u1","event":"purchase","amount":30.0,"ts":"2026-01-13T10:01:00+07:00"},
{"event_id":"e006","user_id":"u3","event":"refund","amount":-30.0,"ts":"2026-01"},
{"event_id":"e007","user_id":"u3","event":"purchase","amount":200.0,"ts":"2026-01-14T10:12:00+07:00"},
{"event_id":"e008","user_id":"u3","event":"purchase","amount":200.0,"ts":"2026-01-12T10:12:00+07:00"},
{"event_id":"e008","user_id":"u3","event":"purchase","amount":200.0,"ts":"2026-01-11T10:12:00+07:00"},
{"event_id":"e009","user_id":"u2","event":"purchase","amount":15.0,"ts":"2026-01-10T11:00:00+07:00"},
{"event_id":"ae09","user_id":"u2","event":"purchase","amount":15.0,"ts":"2026-01-10T11:00:00+07:00"},
{"event_id": "e010", "user_id": "u2", "event": "purchase", "amount": 15.0, "ts": "2026-01-13T11:00:00+07:00"},
{"event_id": "e011", "user_id": "uu", "event": "purchase", "amount": 15.0, "ts": "2026-01-13T11:00:00+07:00"}]


df = pd.DataFrame(records)

df_unique=df.drop_duplicates(subset=["event_id"],keep='first').copy()
df_unique["event_date"]= pd.to_datetime(df_unique["ts"], errors="coerce").dt.strftime("%Y-%m-%d")
print(df_unique)
parquet_df: dict[str, list[dict]]={}
print("------------------------------")
for i in df_unique["event_date"].unique() :
    str_Date=df_unique[df_unique["event_date"]==i]
    print(str_Date)
    