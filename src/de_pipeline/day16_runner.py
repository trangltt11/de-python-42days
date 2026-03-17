from pathlib import Path

from .config import load_config
from .day15_pipeline import PipelinePaths, run_def


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    cfg = load_config(project_root, project_root / "configs" / "config.yaml")

    paths = PipelinePaths(
        input_jsonl=cfg.input_jsonl,
        processed_root=cfg.processed_root,
        bad_root=cfg.bad_root,
    )

    stats = run_def(paths)
    print("=== DAY16 STATS ===")
    print(stats)
    print("input:", paths.input_jsonl)
    print("processed_root:", paths.processed_root)
    print("bad_root:", paths.bad_root)


if __name__ == "__main__":
    main()