import pandas as pd

df = pd.DataFrame([
    {"event_id": "e001", "user_id": "u1", "event": "purchase", "amount": 10, "ts": "2026-01-10"},
    {"event_id": "e002", "user_id": "u2", "event": "refund",   "amount":  5, "ts": "2026-01-10"},
    {"event_id": "e002", "user_id": "u3", "event": "purchase", "amount": 12, "ts": "2026-01-11"},
    {"event_id": "e003", "user_id": "u1", "event": "purchase", "amount": 20, "ts": "2026-01-12"},
    {"event_id": "e003", "user_id": "u1", "event": "purchase", "amount": 20, "ts": "2026-01-12"},
])
dup_mask = df["event_id"].duplicated(keep=False)
print(dup_mask)

print(df.loc[dup_mask, ["event_id", "user_id", "event", "amount", "ts"]])

df.info()

