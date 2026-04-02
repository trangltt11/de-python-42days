from __future__ import annotations
from pathlib import Path
import pyarrow.dataset as ds
import pandas as pd

def main() -> None:
    root = Path(__file__).resolve().parents[2]
    path = root / "data" / "processed" / "day27" / "events_clean.parquet"

    dataset = ds.dataset(path, format="parquet")
    # filter purchase
    table = dataset.to_table(filter=(ds.field("event") == "purchase"), columns=["event_id","user_id","amount"])
    print(table.to_pandas())

    df = pd.read_parquet(path)
    print(df.dtypes)

if __name__ == "__main__":
    main()