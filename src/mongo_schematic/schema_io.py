from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml


def load_schema(path: Path) -> Dict[str, Any]:
    data = yaml.safe_load(path.read_text())
    if not data:
        return {}
    return data


def write_schema(path: Path, payload: Dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False))
    return path


def get_schema_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not payload:
        return {}
    if "schema" in payload and isinstance(payload["schema"], dict):
        return payload["schema"]
    return payload
