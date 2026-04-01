"""
Migrate outputs/week1/intent_records.jsonl to canonical intent_record (canonical_schema.md).

Output: outputs/migrate/week1/intent_records.jsonl
Run: python outputs/migrate/migrate_week1.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_MIGRATE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_MIGRATE_DIR))

from _common import iso_now, iter_json_objects_from_line, repo_root, to_iso, uuid5_for, write_jsonl


def _code_refs_from_row(row: dict) -> list[dict]:
    refs: list[dict] = []
    files = row.get("files") or []
    for f in files:
        if not isinstance(f, dict):
            continue
        rel = f.get("relative_path") or f.get("path") or "unknown"
        convs = f.get("conversations") or []
        for conv in convs:
            if not isinstance(conv, dict):
                continue
            for rng in conv.get("ranges") or []:
                if not isinstance(rng, dict):
                    continue
                ls = int(rng.get("start_line") or 1)
                le = int(rng.get("end_line") or ls)
                refs.append(
                    {
                        "file": str(rel).replace("\\", "/"),
                        "line_start": ls,
                        "line_end": max(le, ls),
                        "symbol": "unknown",
                        "confidence": 0.5,
                    }
                )
        if not refs and f.get("relative_path"):
            refs.append(
                {
                    "file": str(f["relative_path"]).replace("\\", "/"),
                    "line_start": 1,
                    "line_end": 1,
                    "symbol": "unknown",
                    "confidence": 0.5,
                }
            )
    if not refs:
        refs.append(
            {
                "file": "unknown",
                "line_start": 1,
                "line_end": 1,
                "symbol": "unknown",
                "confidence": 0.5,
            }
        )
    return refs


def _description(row: dict) -> str:
    parts = []
    if row.get("mutation_class"):
        parts.append(str(row["mutation_class"]))
    if row.get("tool"):
        parts.append(str(row["tool"]))
    if row.get("intent_id"):
        parts.append(f"intent={row['intent_id']}")
    f = row.get("files")
    if isinstance(f, list) and f and isinstance(f[0], dict) and f[0].get("relative_path"):
        parts.append(str(f[0]["relative_path"]))
    return " | ".join(parts) if parts else "migrated-from-trace"


def row_to_intent_record(row: dict) -> dict:
    rid = row.get("intent_id") or row.get("id") or "unknown"
    intent_uuid = uuid5_for("intent", str(rid), str(row.get("id") or ""))
    created = row.get("timestamp") or row.get("created_at")
    return {
        "intent_id": intent_uuid,
        "description": _description(row),
        "code_refs": _code_refs_from_row(row),
        "governance_tags": [],
        "created_at": to_iso(created) if created else iso_now(),
    }


def _iter_raw_decode_objects(line: str) -> list[dict]:
    """Extract successive JSON objects using JSONDecoder.raw_decode (tolerates trailing junk)."""
    dec = json.JSONDecoder()
    i, n = 0, len(line)
    out: list[dict] = []
    while i < n:
        while i < n and line[i].isspace():
            i += 1
        if i >= n or line[i] != "{":
            break
        try:
            obj, j = dec.raw_decode(line, i)
            if isinstance(obj, dict):
                out.append(obj)
            i = j
        except json.JSONDecodeError:
            break
    return out


def migrate(src: Path, dst: Path) -> int:
    text = src.read_text(encoding="utf-8")
    rows_out: list[dict] = []
    for line_no, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        objs = iter_json_objects_from_line(line)
        if not objs:
            objs = _iter_raw_decode_objects(line)
        if not objs:
            try:
                objs = [json.loads(line)]
            except json.JSONDecodeError as e:
                print(f"warning: line {line_no}: skip ({e})", file=sys.stderr)
                continue
        for obj in objs:
            if not isinstance(obj, dict):
                continue
            rows_out.append(row_to_intent_record(obj))
    write_jsonl(dst, rows_out)
    print(f"wrote {len(rows_out)} intent_record(s) -> {dst}")
    return 0


def main() -> int:
    root = repo_root()
    ap = argparse.ArgumentParser(description="Migrate Week 1 intent traces to canonical intent_record JSONL.")
    ap.add_argument(
        "--src",
        type=Path,
        default=root / "outputs" / "week1" / "intent_records.jsonl",
    )
    ap.add_argument(
        "--dst",
        type=Path,
        default=root / "outputs" / "migrate" / "week1" / "intent_records.jsonl",
    )
    args = ap.parse_args()
    if not args.src.is_file():
        print(f"error: missing source {args.src}", file=sys.stderr)
        return 1
    return migrate(args.src, args.dst)


if __name__ == "__main__":
    raise SystemExit(main())
