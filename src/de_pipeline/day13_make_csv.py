from pathlib import Path
import pandas as pd


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    jsonl_path = root / "data" / "raw" / "day2_events.jsonl"
    csv_path = root / "data" / "raw" / "day13_events.csv"

    df = pd.read_json(jsonl_path, lines=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    print("Wrote:", csv_path)


if __name__ == "__main__":
    main()
