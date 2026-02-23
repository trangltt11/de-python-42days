from __future__ import annotations

from pathlib import Path
import pandas as pd

root = Path(__file__).resolve().parents[1]  # de-python-42days
print(root)
events_path = root / "data" / "raw" / "day2_events.jsonl"

users_path = root / "data" / "raw" / "day9_users.csv"



import pandas as pd

data = [
    {"user_id": "u1", "user_name": "An", "segment": "VIP"},
    {"user_id": "u2", "user_name": "Binh", "segment": "Normal"},
    {"user_id": "u3", "user_name": "Chi", "segment": "VIP"},
    {"user_id": "u4", "user_name": "Duy", "segment": "Normal"},
]

df = pd.DataFrame(data)
df.to_csv(users_path, index=False)
