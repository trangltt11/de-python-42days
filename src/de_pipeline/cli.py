from __future__ import annotations

import argparse
from pathlib import Path
import sys
import uuid

from .config import load_config
from .day15_pipeline_copy import PipelinePaths, run_def
from .logging_setup import setup_logging


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DE Pipeline CLI")

    # Option 1: chạy bằng YAML config
    p.add_argument("--config", default="configs/config.yaml", help="Path to YAML config (relative to project root)")

    # Option 2: override trực tiếp bằng args
    p.add_argument("--input", help="Input JSONL path (relative to project root)")
    p.add_argument("--processed", help="Processed root path (relative to project root)")
    p.add_argument("--bad", help="Bad root path (relative to project root)")

    # logging
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    return p.parse_args()


def main() -> int:
    root = project_root()
    args = parse_args()

    logger = setup_logging(root / "logs", level=args.log_level)
    run_id = uuid.uuid4().hex[:8]

    # load config first
    cfg = load_config(root, root / args.config)

    # override bằng args nếu user truyền
    input_path = root / args.input if args.input else cfg.input_jsonl
    processed_root = root / args.processed if args.processed else cfg.processed_root
    bad_root = root / args.bad if args.bad else cfg.bad_root

    paths = PipelinePaths(
        input_jsonl=input_path,
        processed_root=processed_root,
        bad_root=bad_root,
    )

    logger.info(f"[run_id={run_id}] CLI start")
    logger.info(f"[run_id={run_id}] input={paths.input_jsonl}")
    logger.info(f"[run_id={run_id}] processed_root={paths.processed_root}")
    logger.info(f"[run_id={run_id}] bad_root={paths.bad_root}")

    try:
        stats = run_def(paths, logger=logger, run_id=run_id)  # dùng version run có logger
        logger.info(f"[run_id={run_id}] CLI done stats={stats}")
        return 0
    except Exception:
        logger.exception(f"[run_id={run_id}] CLI failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())