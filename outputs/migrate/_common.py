"""Shared helpers for canonical-schema migrations (see outputs/migrate/README.md)."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

# Deterministic namespace for uuid5 (not RFC 4122 DNS; arbitrary fixed UUID)
NS_MIGRATION = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def repo_root() -> Path:
    """Repository root: .../data-contract-enforcer (parent of outputs/)."""
    return Path(__file__).resolve().parent.parent.parent


def uuid5_for(*parts: str) -> str:
    """Stable UUID string from joined parts."""
    s = "\x1f".join(str(p) for p in parts)
    return str(uuid.uuid5(NS_MIGRATION, s))


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_iso(value: Any) -> str:
    """Normalize to ISO 8601 Z; fall back to now."""
    if value is None:
        return iso_now()
    if isinstance(value, (int, float)):
        return iso_now()
    s = str(value).strip()
    if not s:
        return iso_now()
    # Already has Z or offset
    if s.endswith("Z") or "+" in s[10:] or (len(s) > 10 and s[10] in "+-"):
        if s.endswith("Z"):
            return s
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            return iso_now()
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return iso_now()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def sha256_file(path: Path) -> str | None:
    if not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def synthetic_rubric_id(rubric_path: str) -> str:
    """64 hex chars for rubric_id when file hash unavailable."""
    h = hashlib.sha256(rubric_path.encode("utf-8")).hexdigest()
    return h


def iter_json_objects_from_line(line: str) -> list[dict[str, Any]]:
    """Parse one or more JSON objects concatenated on a single line."""
    line = line.strip()
    out: list[dict[str, Any]] = []
    if not line:
        return out
    i = 0
    n = len(line)
    while i < n:
        while i < n and line[i] != "{":
            i += 1
        if i >= n:
            break
        depth = 0
        start = i
        for j in range(i, n):
            c = line[j]
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    chunk = line[start : j + 1]
                    try:
                        out.append(json.loads(chunk))
                    except json.JSONDecodeError:
                        pass
                    i = j + 1
                    break
        else:
            break
    return out


def env_repo_root() -> str:
    return os.environ.get("REPO_ROOT", "unknown")
