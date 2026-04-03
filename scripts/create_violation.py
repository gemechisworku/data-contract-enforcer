"""
Inject the canonical scale-change violation (confidence 0.0–1.0 → 0–100) for Week 3
extraction records with nested extracted_facts.

Run from repo root:
  python scripts/create_violation.py

Requires baselines from a clean run first (schema_snapshots/baselines.json) so drift
detection can compare means; the ValidationRunner range check fires regardless.

Writes:
  - outputs/week3/extractions_violated.jsonl
  - violation_log/violations.jsonl (metadata line first)
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_DEFAULT_IN = _REPO / "outputs/migrate/week3/extractions.jsonl"
_DEFAULT_OUT = _REPO / "outputs/week3/extractions_violated.jsonl"
_DEFAULT_LOG = _REPO / "violation_log/violations.jsonl"


def _rel_to_repo(path: Path) -> str:
    try:
        return path.resolve().relative_to(_REPO.resolve()).as_posix()
    except ValueError:
        return str(path)


def inject_scale_change(records: list[dict]) -> list[dict]:
    for r in records:
        for fact in r.get("extracted_facts", []) or []:
            c = fact.get("confidence")
            if isinstance(c, (int, float)):
                fact["confidence"] = round(float(c) * 100, 1)
    return records


def main() -> int:
    p = argparse.ArgumentParser(description="Inject scale-change violation into Week 3 JSONL.")
    p.add_argument(
        "--input",
        type=Path,
        default=_DEFAULT_IN,
        help="Source JSONL (nested extracted_facts, 0–1 confidence)",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=_DEFAULT_OUT,
        help="Violated JSONL output path",
    )
    p.add_argument(
        "--log",
        type=Path,
        default=_DEFAULT_LOG,
        help="violation_log JSONL (metadata written as first line)",
    )
    args = p.parse_args()

    inp = args.input
    if not inp.is_file():
        print(f"error: input not found: {inp}", flush=True)
        return 1

    records: list[dict] = []
    with open(inp, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))

    violated = inject_scale_change(records)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        for r in violated:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    meta = {
        "injection_note": True,
        "injected_at": now,
        "type": "scale_change",
        "description": "extracted_facts[].confidence multiplied by 100 (0.87 → 87.0)",
        "source_file": _rel_to_repo(inp),
        "output_file": _rel_to_repo(args.output),
    }

    args.log.parent.mkdir(parents=True, exist_ok=True)
    # Prepend metadata at top; keep any existing violation lines after
    rest = ""
    if args.log.is_file():
        text = args.log.read_text(encoding="utf-8")
        lines = text.splitlines()
        # drop previous injection header if present
        if lines and lines[0].strip().startswith("{"):
            try:
                first = json.loads(lines[0])
                if first.get("injection_note") is True and first.get("type") == "scale_change":
                    lines = lines[1:]
            except json.JSONDecodeError:
                pass
        rest = "\n".join(lines)
        if rest:
            rest = rest + "\n"

    with open(args.log, "w", encoding="utf-8") as f:
        f.write(json.dumps(meta, ensure_ascii=False) + "\n")
        if rest:
            f.write(rest)

    print(
        json.dumps(
            {
                "ok": True,
                "rows": len(violated),
                "written": str(args.output),
                "violation_log": str(args.log),
            },
            indent=2,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
