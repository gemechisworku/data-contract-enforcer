"""
Migrate outputs/week3/extractions.jsonl to canonical extraction_record (canonical_schema.md).

Output: outputs/migrate/week3/extractions.jsonl
Run: python outputs/migrate/migrate_week3.py
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

_MIGRATE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_MIGRATE_DIR))

from _common import repo_root, to_iso, uuid5_for, write_jsonl


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def row_to_extraction(row: dict) -> dict:
    doc_slug = str(row.get("doc_id") or "unknown")
    doc_uuid = uuid5_for("doc", doc_slug)
    conf = float(row.get("confidence_score") or 0.0)
    conf = max(0.0, min(1.0, conf))
    fact_id = uuid5_for("fact", doc_slug, "1")
    proc_sec = float(row.get("processing_time") or 0.0)
    proc_ms = max(1, int(round(proc_sec * 1000)))

    return {
        "doc_id": doc_uuid,
        "source_path": "unknown",
        "source_hash": _sha256(doc_slug),
        "extracted_facts": [
            {
                "fact_id": fact_id,
                "text": f"Document processing summary for {doc_slug}",
                "entity_refs": [],
                "confidence": conf,
                "page_ref": None,
                "source_excerpt": "",
            }
        ],
        "entities": [],
        "extraction_model": "unknown",
        "processing_time_ms": proc_ms,
        "token_count": {"input": 0, "output": 0},
        "extracted_at": to_iso(row.get("timestamp_utc")),
    }


def migrate(src: Path, dst: Path) -> int:
    rows_out: list[dict] = []
    for line in src.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        if isinstance(row, dict):
            rows_out.append(row_to_extraction(row))
    write_jsonl(dst, rows_out)
    print(f"wrote {len(rows_out)} extraction_record(s) -> {dst}")
    return 0


def main() -> int:
    root = repo_root()
    ap = argparse.ArgumentParser(description="Migrate Week 3 extractions to canonical extraction_record JSONL.")
    ap.add_argument(
        "--src",
        type=Path,
        default=root / "outputs" / "week3" / "extractions.jsonl",
    )
    ap.add_argument(
        "--dst",
        type=Path,
        default=root / "outputs" / "migrate" / "week3" / "extractions.jsonl",
    )
    args = ap.parse_args()
    if not args.src.is_file():
        print(f"error: missing source {args.src}", file=sys.stderr)
        return 1
    return migrate(args.src, args.dst)


if __name__ == "__main__":
    raise SystemExit(main())
