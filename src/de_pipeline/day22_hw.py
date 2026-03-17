from __future__ import annotations

from pathlib import Path
import pandas as pd
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    String, Float, DateTime, select, func
)

def main()-> None:
    root= Path(__file__).resolve().parents[2]
    # 1) Input JSONL
    jsonl_path=root/"data"/"raw"/"day2_event.jsonl"
    df= pd.read_json(jsonl_path, line=True)
    #2 clean data
    df["event"]= df["event"].astype(str).str.strip().str.lower()
    
