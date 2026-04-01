"""
Migrate outputs/traces/run.jsonl to canonical trace_record JSONL (LangSmith export shape).

Emits one row with top-level inputs/outputs copied from the workflow trace.

Output: outputs/migrate/traces/runs.jsonl
Run: python outputs/migrate/migrate_langsmith_trace.py
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_MIGRATE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_MIGRATE_DIR))

from _common import repo_root, uuid5_for, write_jsonl


def _trace_record(data: dict) -> dict:
    inputs = data.get("inputs")
    outputs = data.get("outputs")
    if not isinstance(inputs, dict):
        inputs = {}
    if not isinstance(outputs, dict):
        outputs = {}

    meta = data.get("metadata") or {}
    rev = str(meta.get("revision_id") or "")
    ls = data.get("langsmith") or {}
    proj = ls.get("tracing_project") or {}
    ws = ls.get("workspace") or {}
    proj_id = str(proj.get("id") or "unknown-project")
    proj_name = str(proj.get("name") or "automaton-auditor")

    run_uuid = uuid5_for("langsmith-trace", proj_id, rev)

    # Deterministic window from revision_id so re-runs match
    h = int(hashlib.sha256(rev.encode("utf-8") if rev else b"none").hexdigest()[:12], 16)
    base = datetime(2020, 1, 1, tzinfo=timezone.utc)
    start = base + timedelta(seconds=h % 10_000_000)
    end = start + timedelta(seconds=1)

    def fmt(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    session_id = str(ws.get("id") or uuid5_for("session", proj_id))

    return {
        "id": run_uuid,
        "name": proj_name,
        "run_type": "chain",
        "inputs": inputs,
        "outputs": outputs,
        "error": None,
        "start_time": fmt(start),
        "end_time": fmt(end),
        "total_tokens": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_cost": 0.0,
        "tags": ["migrated", "week2-digital-courtroom", proj_name],
        "parent_run_id": None,
        "session_id": session_id,
    }


def migrate(src: Path, dst: Path) -> int:
    data = json.loads(src.read_text(encoding="utf-8").strip())
    if not isinstance(data, dict):
        print("error: root must be object", file=sys.stderr)
        return 1
    row = _trace_record(data)
    write_jsonl(dst, [row])
    print(f"wrote 1 trace_record -> {dst}")
    return 0


def main() -> int:
    root = repo_root()
    ap = argparse.ArgumentParser(description="Migrate workflow trace to canonical trace_record JSONL.")
    ap.add_argument(
        "--src",
        type=Path,
        default=root / "outputs" / "traces" / "run.jsonl",
    )
    ap.add_argument(
        "--dst",
        type=Path,
        default=root / "outputs" / "migrate" / "traces" / "runs.jsonl",
    )
    args = ap.parse_args()
    if not args.src.is_file():
        print(f"error: missing source {args.src}", file=sys.stderr)
        return 1
    return migrate(args.src, args.dst)


if __name__ == "__main__":
    raise SystemExit(main())
