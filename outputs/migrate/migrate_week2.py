"""
Migrate outputs/traces/run.jsonl (single JSON workflow) to canonical verdict_record.

Output: outputs/migrate/week2/verdicts.jsonl
Run: python outputs/migrate/migrate_week2.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_MIGRATE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_MIGRATE_DIR))

from _common import iso_now, repo_root, sha256_file, synthetic_rubric_id, to_iso, uuid5_for, write_jsonl


def _verdict_from_run(data: dict) -> dict:
    inputs = data.get("inputs") or {}
    outputs = data.get("outputs") or {}
    meta = data.get("metadata") or {}
    repo_url = str(inputs.get("repo_url") or "")
    pdf_path = str(inputs.get("pdf_path") or "")
    rubric_path = str(inputs.get("rubric_path") or "")
    rev = str(meta.get("revision_id") or meta.get("revision") or "")

    verdict_id = uuid5_for("verdict", repo_url, pdf_path, rubric_path, rev)

    rubric_file = Path(rubric_path)
    rh = sha256_file(rubric_file)
    rubric_id = rh if rh else synthetic_rubric_id(rubric_path)

    rubric_version = "1.0.0"

    scores: dict = {}
    final_report = outputs.get("final_report") or {}
    overall_score = float(final_report.get("overall_score") or 0.0)
    criteria = final_report.get("criteria") or []

    for c in criteria:
        if not isinstance(c, dict):
            continue
        dim_id = str(c.get("dimension_id") or "unknown")
        fs = c.get("final_score")
        if fs is None:
            continue
        try:
            score_int = int(round(float(fs)))
        except (TypeError, ValueError):
            continue
        score_int = max(1, min(5, score_int))
        notes = str(c.get("remediation") or c.get("dimension_name") or "")[:2000]
        ev = c.get("dissent_summary")
        evidence_list = [str(ev)] if ev else [notes[:500] if notes else "migrated"]
        scores[dim_id] = {
            "score": score_int,
            "evidence": evidence_list,
            "notes": notes,
        }

    if not scores and criteria:
        # fallback: empty criteria shape
        pass

    if overall_score <= 0 and scores:
        overall_score = sum(v["score"] for v in scores.values()) / max(len(scores), 1)

    if overall_score >= 4.0:
        overall_verdict = "PASS"
    elif overall_score >= 3.0:
        overall_verdict = "WARN"
    else:
        overall_verdict = "FAIL"

    # confidence: average of detective confidences if present
    confidences: list[float] = []
    evidences = outputs.get("evidences") or {}
    if isinstance(evidences, dict):
        for _k, arr in evidences.items():
            if not isinstance(arr, list):
                continue
            for item in arr:
                if isinstance(item, dict) and "confidence" in item:
                    try:
                        confidences.append(float(item["confidence"]))
                    except (TypeError, ValueError):
                        pass
    confidence = sum(confidences) / len(confidences) if confidences else 0.85

    evaluated_at = to_iso(meta.get("evaluated_at")) if meta.get("evaluated_at") else iso_now()

    target_ref = pdf_path or repo_url or "unknown"

    return {
        "verdict_id": verdict_id,
        "target_ref": target_ref,
        "rubric_id": rubric_id,
        "rubric_version": rubric_version,
        "scores": scores,
        "overall_verdict": overall_verdict,
        "overall_score": float(overall_score),
        "confidence": float(confidence),
        "evaluated_at": evaluated_at,
    }


def migrate(src: Path, dst: Path) -> int:
    raw = src.read_text(encoding="utf-8").strip()
    data = json.loads(raw)
    if not isinstance(data, dict):
        print("error: root must be a JSON object", file=sys.stderr)
        return 1
    row = _verdict_from_run(data)
    write_jsonl(dst, [row])
    print(f"wrote 1 verdict_record -> {dst}")
    return 0


def main() -> int:
    root = repo_root()
    ap = argparse.ArgumentParser(description="Migrate Week 2 trace to canonical verdict_record JSONL.")
    ap.add_argument(
        "--src",
        type=Path,
        default=root / "outputs" / "traces" / "run.jsonl",
    )
    ap.add_argument(
        "--dst",
        type=Path,
        default=root / "outputs" / "migrate" / "week2" / "verdicts.jsonl",
    )
    args = ap.parse_args()
    if not args.src.is_file():
        print(f"error: missing source {args.src}", file=sys.stderr)
        return 1
    return migrate(args.src, args.dst)


if __name__ == "__main__":
    raise SystemExit(main())
