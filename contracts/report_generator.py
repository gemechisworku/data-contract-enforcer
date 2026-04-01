# contracts/report_generator.py — EnforcerReport entry point
"""Stakeholder PDF / summary from validation_reports + violation_log."""

from __future__ import annotations

import argparse


def main() -> int:
    p = argparse.ArgumentParser(description="EnforcerReport: assemble stakeholder report.")
    p.add_argument("--validation-reports", type=str, default="validation_reports")
    p.add_argument("--out-dir", type=str, default="enforcer_report")
    args = p.parse_args()
    print(
        "EnforcerReport: aggregate JSON from "
        f"{args.validation_reports!r} → {args.out_dir!r}. Implement PDF/HTML export."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
