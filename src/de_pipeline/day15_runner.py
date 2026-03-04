from pathlib import Path
from de_pipeline.day15_pipeline import PipelinePaths, run


def main() -> None:
    root = Path(__file__).resolve().parents[2]

    paths = PipelinePaths(
        input_jsonl=root / "data" / "raw" / "day2_events.jsonl",
        processed_root=root / "data" / "processed" / "day15",
        bad_root=root / "data" / "bad" / "day15",
    )

    stats = run(paths)
    print("=== DAY15 STATS ===")
    print(stats)
    print("Processed root:", paths.processed_root)
    print("Bad root:", paths.bad_root)


if __name__ == "__main__":
    main()