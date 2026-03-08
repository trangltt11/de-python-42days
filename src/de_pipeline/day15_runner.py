from pathlib import Path
from .day15_pipeline import PipelinePaths, run_def


def main() -> None:
    root = Path(__file__).resolve().parents[2]

    paths = PipelinePaths(
        input_jsonl=root / "data" / "raw" / "day2_events.jsonl",
        processed_root=root / "data" / "processed" / "day15",
        bad_root=root / "data" / "bad" / "day15"
    )

    stats, report_js = run_def(paths)
    print("=== DAY15 STATS ===")
    print(stats)
    print("Processed root:", paths.processed_root)
    print("Bad root:", paths.bad_root)
    print ("report_js: ",report_js)
     


if __name__ == "__main__":
    main()