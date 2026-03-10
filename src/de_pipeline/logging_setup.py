from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(log_dir: Path, level: str = "INFO") -> logging.Logger:
    """
    Setup logger:
    - console handler
    - rotating file handler (log file quay vòng)
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "pipeline.log"

    logger = logging.getLogger("de_pipeline")# tao object cua class Logging
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # tránh add handler nhiều lần nếu chạy lại trong notebook/IDE
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # file rotating
    fh = RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger