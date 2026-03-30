from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Day25 Data Quality Checks")

    p.add_argument("--processed-root", default="data/processed/day15",
                  help="Processed root containing date=*/events.parquet (relative to project root)")
    p.add_argument("--users-csv", default="data/raw/day9_users.csv",
                  help="Users dimension csv for FK check (relative to project root)")
    p.add_argument("--output", default="data/processed/day25/dq_report.json",
                  help="Output report path (relative to project root)")
    p.add_argument("--max-null-rate", type=float, default=0.0,
                  help="Max allowed null rate per selected column (0.0 means no null allowed)")

    return p.parse_args()
args = parse_args()
print(args)