"""Shared exceptions and durable JSON helpers for the calibration coordinator."""

from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
from typing import Any


class CalCoordError(Exception):
    """Base class for expected domain/request errors."""


class ValidationError(CalCoordError):
    """A request or trace event is structurally invalid."""


class PermissionDenied(CalCoordError):
    """An operation is not valid for the presented permit/worker/time."""

    def __init__(self, code: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details

    def to_dict(self) -> dict[str, Any]:
        return {"allowed": False, "code": self.code, "reason": self.message, **self.details}


def load_json(path: str | Path, default: Any) -> Any:
    path = Path(path)
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as source:
        return json.load(source)


def atomic_write_json(path: str | Path, value: Any) -> None:
    """Write JSON atomically so a killed process never leaves a half file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as target:
            json.dump(value, target, ensure_ascii=False, indent=2, sort_keys=False)
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise
