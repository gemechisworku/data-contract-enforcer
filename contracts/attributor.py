# contracts/attributor.py — ViolationAttributor entry point
"""Maps validation failures to lineage nodes (Phase 2). See canonical course layout."""

from __future__ import annotations

import argparse


def main() -> int:
    p = argparse.ArgumentParser(description="ViolationAttributor: blame chains from lineage.")
    p.add_argument("--violation-log", type=str, default="violation_log/violations.jsonl")
    p.add_argument("--lineage", type=str, default="outputs/week4/lineage_snapshots.jsonl")
    args = p.parse_args()
    print(
        "ViolationAttributor: wire validation output + lineage snapshot.\n"
        f"  violation_log={args.violation_log!r} lineage={args.lineage!r}\n"
        "  Implement graph reverse-walk per DOMAIN_NOTES / canonical_schema Week 4."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
