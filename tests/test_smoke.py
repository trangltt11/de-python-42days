import pandas as pd
r = {"event_id":"e002","user_id":"u1","event":"Purchase","amount":120.5,"ts":"2026-01-10"}
a=list[r]
b=list(r)
print("--------------------------------")
print(a)
print("--------------------------------")
print(type(a))
print("--------------------------------")

df = pd.DataFrame([r])
print(df)