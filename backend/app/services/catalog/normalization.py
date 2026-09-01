"""Shared catalog value normalization and hashing primitives."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
from typing import Any


def json_loads(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return default


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def preview_base_kind(kind: str) -> str:
    return kind.split(":unit", 1)[0]


def preview_unit(kind: str) -> int:
    match = re.search(r":unit(\d+)$", kind)
    return max(1, int(match.group(1))) if match else 1


def preview_kind(kind: str, unit: int) -> str:
    return kind if unit <= 1 else f"{kind}:unit{unit}"


def preview_unit_label(kind: str) -> str:
    unit = preview_unit(kind)
    if unit <= 26:
        return f"Unit {chr(64 + unit)}"
    return f"Unit {unit}"


__all__ = [
    "json_loads",
    "canonical_json",
    "sha256_text",
    "sha256_bytes",
    "sha256_file",
    "preview_base_kind",
    "preview_unit",
    "preview_kind",
    "preview_unit_label",
]
