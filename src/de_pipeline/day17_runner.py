from pathlib import Path
import uuid

from .config import load_config
from .day15_pipeline_copy import PipelinePaths, run_def
from .logging_setup import setup_logging


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]

    # setup logger
    run_id = uuid.uuid4().hex[:8]
    logger = setup_logging(project_root / "logs", level="INFO")

    cfg = load_config(project_root, project_root / "configs" / "config.yaml")

    logger.debug("debug message...")

    paths = PipelinePaths(
        input_jsonl=cfg.input_jsonl,
        processed_root=cfg.processed_root,
        bad_root=cfg.bad_root,
    )

    logger.info(f"[run_id={run_id}] start pipeline")
    logger.info(f"[run_id={run_id}] input={paths.input_jsonl}")
    logger.info(f"[run_id={run_id}] processed_root={paths.processed_root}")
    logger.info(f"[run_id={run_id}] bad_root={paths.bad_root}")

    try:
        stats = run_def(paths,logger=logger, run_id=run_id)
        logger.info(f"[run_id={run_id}] stats={stats}")
        logger.info(f"[run_id={run_id}] done")
    except Exception:
        logger.exception(f"[run_id={run_id}] pipeline failed")
        raise


if __name__ == "__main__":
    main()