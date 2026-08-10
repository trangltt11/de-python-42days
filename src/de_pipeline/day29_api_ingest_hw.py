from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import requests

from de_pipeline.file_io import write_jsonl  # bạn đã có write_jsonl(path, records, append=...)


BASE_URL = "https://jsonplaceholder.typicode.com"


def ensure_dir(path: Path) -> Pạth:
    root = Path(__file__).resolve().parents[2]
    out_path=root/path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    print(root)
    print(out_path)
    return out_path

def fetch_posts(page: int, limit: int, timeout_s: float = 10.0) -> list[dict[str, Any]]:
    """
    Demo API: GET /posts?_page=...&_limit=...
    JSONPlaceholder hỗ trợ _page/_limit (đủ cho bài hôm nay).
    """
    url = f"{BASE_URL}/posts"
    params = {"_page": page, "_limit": limit}
    resp = requests.get(url, params=params, timeout=timeout_s)
    resp.raise_for_status()  # fail fast nếu 4xx/5xx
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError("Expected list JSON from API")
    return data


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Day29: ingest API to raw JSONL")

    p.add_argument("--page", type=int, default=1, help="Page number to fetch")
    p.add_argument("--limit", type=int, default=3, help="Items per page")
    p.add_argument("--out", default="data/raw/day29/posts.jsonl", help="Output JSONL path")
    p.add_argument("--overwrite", action="store_true", help="Overwrite output file (default append)")

    return p.parse_args()


def main() -> None:
    args = parse_args()
    path = Path(args.out)
    out_path= ensure_dir(path)
    
    nums = [1, 2, 3]

    for x in nums:
        records = fetch_posts(page=x, limit=10)

        # Viết JSONL:
        # overwrite -> append=False
        # default -> append=True (để bạn có thể fetch nhiều page và nối)
        write_jsonl(out_path, records, append= False)

        print(f"Fetched {len(records)} records from page={args.page}, limit={args.limit}")


    print("Wrote:", out_path)
    import pandas as pd
    df = pd.read_json(out_path, lines=True)
    print(df.shape)


if __name__ == "__main__":
    main()