"""
Migrate outputs/week5/events.jsonl to canonical event_record (canonical_schema.md).

Output: outputs/migrate/week5/events.jsonl
Run: python outputs/migrate/migrate_week5.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_MIGRATE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_MIGRATE_DIR))

from _common import repo_root, to_iso, uuid5_for, write_jsonl


def _aggregate_type(stream_id: str) -> str:
    s = stream_id.lower()
    if s.startswith("loan-"):
        return "Loan"
    if s.startswith("docpkg-"):
        return "DocumentPackage"
    # PascalCase slug
    parts = stream_id.replace("-", "_").split("_")
    return "".join(p[:1].upper() + p[1:].lower() if p else "" for p in parts) or "Aggregate"


def migrate(src: Path, dst: Path) -> int:
    raw_rows: list[dict] = []
    for line in src.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if isinstance(obj, dict):
            raw_rows.append(obj)

    # sort by stream_id then recorded_at for stable sequence
    def sort_key(r: dict) -> tuple:
        return (str(r.get("stream_id") or ""), str(r.get("recorded_at") or ""))

    raw_rows.sort(key=sort_key)

    seq_by_stream: dict[str, int] = {}
    out: list[dict] = []
    for r in raw_rows:
        stream_id = str(r.get("stream_id") or "unknown")
        seq_by_stream[stream_id] = seq_by_stream.get(stream_id, 0) + 1
        seq = seq_by_stream[stream_id]
        agg_id = uuid5_for("aggregate", stream_id)
        et = str(r.get("event_type") or "unknown")
        ev = r.get("event_version", 1)
        recorded = (
            r.get("recorded_at") or r.get("occurred_at") or ""
        )
        event_id = uuid5_for(stream_id, str(recorded), et, str(ev))

        occurred = to_iso(recorded) if recorded else to_iso(None)

        out.append(
            {
                "event_id": event_id,
                "event_type": et,
                "aggregate_id": agg_id,
                "aggregate_type": _aggregate_type(stream_id),
                "sequence_number": seq,
                "payload": r.get("payload") if isinstance(r.get("payload"), dict) else {},
                "metadata": {
                    "causation_id": None,
                    "correlation_id": agg_id,
                    "user_id": "system",
                    "source_service": "migrated",
                },
                "schema_version": "1.0",
                "occurred_at": occurred,
                "recorded_at": to_iso(recorded) if recorded else occurred,
            }
        )

    write_jsonl(dst, out)
    print(f"wrote {len(out)} event_record(s) -> {dst}")
    return 0


def main() -> int:
    root = repo_root()
    ap = argparse.ArgumentParser(description="Migrate Week 5 events to canonical event_record JSONL.")
    ap.add_argument(
        "--src",
        type=Path,
        default=root / "outputs" / "week5" / "events.jsonl",
    )
    ap.add_argument(
        "--dst",
        type=Path,
        default=root / "outputs" / "migrate" / "week5" / "events.jsonl",
    )
    args = ap.parse_args()
    if not args.src.is_file():
        print(f"error: missing source {args.src}", file=sys.stderr)
        return 1
    return migrate(args.src, args.dst)


if __name__ == "__main__":
    raise SystemExit(main())
