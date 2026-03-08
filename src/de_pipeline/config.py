from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import os
import yaml


@dataclass(frozen=True)
class PipelineConfig:
    input_jsonl: Path
    processed_root: Path
    bad_root: Path


def _get_env(name: str) -> str | None:
    v = os.getenv(name)
    return v if v and v.strip() else None


def load_config(project_root: Path, yaml_path: Path) -> PipelineConfig:
    """
    Load config từ YAML, rồi override bằng env vars nếu có:
    - INPUT_JSONL
    - PROCESSED_ROOT
    - BAD_ROOT
    """
    data: dict[str, Any] = {}
    with yaml_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    p = (data.get("pipeline") or {})
    input_jsonl = p.get("input_jsonl")
    processed_root = p.get("processed_root")
    bad_root = p.get("bad_root")

    # --- override bằng env ---
    input_jsonl = _get_env("INPUT_JSONL") or input_jsonl
    processed_root = _get_env("PROCESSED_ROOT") or processed_root
    bad_root = _get_env("BAD_ROOT") or bad_root

    # --- validate tối thiểu ---
    if not input_jsonl or not processed_root or not bad_root:
        raise ValueError("Missing config values. Need input_jsonl/processed_root/bad_root")

    # convert sang Path tuyệt đối
    return PipelineConfig(
        input_jsonl=project_root / Path(input_jsonl),
        processed_root=project_root / Path(processed_root),
        bad_root=project_root / Path(bad_root),
    )