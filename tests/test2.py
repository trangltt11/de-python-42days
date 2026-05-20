from pathlib import Path
import pandas as pd

root = Path(r"E:\py file\LEAR PYTHON\de-python-42days\data\processed\day28")

dfs = []

for parquet_file in root.rglob("*.parquet"):
    df = pd.read_parquet(parquet_file)
    dfs.append(df)

all_df = pd.concat(dfs, ignore_index=True)
print(all_df)