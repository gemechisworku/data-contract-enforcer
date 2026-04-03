# contracts/attributor.py — ViolationAttributor (registry-first)
"""
Maps validation failures to subscribers (registry) and lineage depth, with optional git blame.

At Tier 2, registry_blast_radius becomes GET /api/registry/subscriptions?contract_id=&breaking_field=
with the same signature and return shape.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

_DATAFLOW_REL = ("PRODUCES", "WRITES", "CONSUMES")


def registry_blast_radius(
    contract_id: str,
    failing_field: str,
    registry_path: str | Path,
) -> list[dict[str, Any]]:
    """Step 1: Registry blast radius query (primary source)."""
    path = Path(registry_path)
    with open(path, encoding="utf-8") as f:
        registry = yaml.safe_load(f)
    affected: list[dict[str, Any]] = []
    for sub in registry.get("subscriptions", []) or []:
        if sub.get("contract_id") != contract_id:
            continue
        for bf in sub.get("breaking_fields", []) or []:
            rf = str(bf.get("field", ""))
            if _registry_field_matches(rf, failing_field):
                affected.append(
                    {
                        "subscriber_id": sub.get("subscriber_id"),
                        "contact": sub.get("contact", "unknown"),
                        "validation_mode": sub.get("validation_mode", "AUDIT"),
                        "reason": bf.get("reason", ""),
                    }
                )
                break
    return affected


def _registry_field_matches(registry_field: str, failing_field: str) -> bool:
    """Match per subscription registry: exact or failing_field is under registry prefix path."""
    if registry_field == failing_field:
        return True
    return bool(registry_field and failing_field.startswith(registry_field))


def compute_transitive_depth(
    producer_node_id: str,
    lineage_path: str | Path,
    max_depth: int = 2,
) -> dict[str, Any]:
    """Step 2: Lineage transitive depth (enrichment). Uses last JSONL record as snapshot."""
    path = Path(lineage_path)
    with open(path, encoding="utf-8") as f:
        lines = [ln for ln in f if ln.strip()]
    if not lines:
        return {"direct": [], "transitive": [], "max_depth": 0}
    snapshot = json.loads(lines[-1])
    edges_raw = snapshot.get("edges") or []
    visited: set[str] = set()
    frontier: set[str] = {producer_node_id}
    depth_map: dict[str, int] = {}

    for depth in range(1, max_depth + 1):
        next_frontier: set[str] = set()
        for node in frontier:
            for edge in edges_raw:
                src = edge.get("source")
                tgt = edge.get("target")
                rel_raw = edge.get("relationship") or edge.get("edge_type") or ""
                rel = str(rel_raw).upper()
                if rel not in _DATAFLOW_REL:
                    continue
                if src == node and tgt is not None:
                    if tgt not in visited:
                        depth_map[str(tgt)] = depth
                        next_frontier.add(str(tgt))
                        visited.add(str(tgt))
        frontier = next_frontier
        if not frontier:
            break

    return {
        "direct": [n for n, d in depth_map.items() if d == 1],
        "transitive": [n for n, d in depth_map.items() if d > 1],
        "max_depth": max(depth_map.values()) if depth_map else 0,
    }


def get_recent_commits(file_path: str, repo_root: str | Path, days: int = 14) -> list[dict[str, str]]:
    """Step 3a: Git commits touching file_path (best-effort; empty if not a git repo)."""
    root = Path(repo_root)
    cmd = [
        "git",
        "log",
        "--follow",
        f"--since={days} days ago",
        "--format=%H|%ae|%cI|%s",
        "--",
        file_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=root)
    if result.returncode != 0:
        return []
    commits: list[dict[str, str]] = []
    for line in result.stdout.strip().split("\n"):
        if "|" not in line:
            continue
        parts = line.split("|", 3)
        if len(parts) < 4:
            continue
        h, ae, ci, s = parts[0], parts[1], parts[2], parts[3]
        commits.append(
            {
                "commit_hash": h,
                "author": ae,
                "commit_timestamp": ci.strip(),
                "commit_message": s,
            }
        )
    return commits


def score_candidates(
    commits: list[dict[str, str]],
    violation_ts: str,
    lineage_distance: float,
) -> list[dict[str, Any]]:
    """Step 3b: Rank recent commits by recency vs violation time and graph distance."""
    scored: list[dict[str, Any]] = []
    vt = _parse_iso_ts(violation_ts)
    for rank, c in enumerate(commits[:5], 1):
        ct = _parse_iso_ts(c["commit_timestamp"])
        days = abs((vt - ct).days)
        score = max(0.0, round(1.0 - (days * 0.1) - (lineage_distance * 0.2), 3))
        scored.append({**c, "rank": rank, "confidence_score": score})
    return sorted(scored, key=lambda x: x["confidence_score"], reverse=True)


def _parse_iso_ts(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if "T" not in s and " " in s[:11]:
        s = s.replace(" ", "T", 1)
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def write_violation(
    check_result: dict[str, Any],
    registry_blast: list[dict[str, Any]],
    lineage_enrichment: dict[str, Any],
    blame_chain: list[dict[str, Any]],
    out_path: str | Path,
) -> None:
    """Step 4: Append structured violation entry to JSONL log."""
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "violation_id": str(uuid.uuid4()),
        "check_id": check_result.get("check_id", "unknown"),
        "detected_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "blast_radius": {
            "source": "registry",
            "direct_subscribers": registry_blast,
            "transitive_nodes": lineage_enrichment.get("transitive", []),
            "contamination_depth": lineage_enrichment.get("max_depth", 0),
            "note": "direct_subscribers from registry; transitive_nodes from lineage graph enrichment",
        },
        "blame_chain": blame_chain,
        "records_failing": check_result.get("records_failing", 0),
    }
    with open(out, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def build_check_result(
    finding: dict[str, Any],
    report_timestamp: str | None = None,
    records_failing: int = 0,
) -> dict[str, Any]:
    """Normalize a ValidationRunner statistical/structural finding into check_result shape."""
    chk = finding.get("check", "unknown")
    field = finding.get("field", "")
    return {
        "check_id": f"{chk}:{field}" if field else str(chk),
        "check": chk,
        "field": field,
        "severity": finding.get("severity") or finding.get("status"),
        "detail": finding.get("detail") or finding.get("message"),
        "report_timestamp_utc": report_timestamp,
        "records_failing": records_failing,
    }


def attribute_finding(
    finding: dict[str, Any],
    *,
    contract_id: str,
    registry_path: Path,
    lineage_path: Path,
    producer_node_id: str,
    violation_out: Path,
    repo_root: Path | None = None,
    data_file_for_blame: str | None = None,
    violation_ts: str | None = None,
    max_depth: int = 2,
) -> dict[str, Any]:
    """
    Run steps 1–4 for one runner finding (e.g. statistical range/drift row).
    Returns the blast list, lineage enrichment, and blame candidates (not written if dry_run).
    """
    field = str(finding.get("field", ""))
    blast = registry_blast_radius(contract_id, field, registry_path)
    lineage = compute_transitive_depth(producer_node_id, lineage_path, max_depth=max_depth)
    dist = float(lineage.get("max_depth") or 0)
    ts = violation_ts or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    blame: list[dict[str, Any]] = []
    if repo_root and data_file_for_blame:
        commits = get_recent_commits(data_file_for_blame, repo_root)
        blame = score_candidates(commits, ts, dist)
    check_result = build_check_result(finding, records_failing=finding.get("records_failing", 0))
    write_violation(check_result, blast, lineage, blame, violation_out)
    return {
        "registry_blast_radius": blast,
        "lineage_enrichment": lineage,
        "blame_chain": blame,
        "written_to": str(violation_out),
    }


def _pick_failures(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Statistical/structural rows that represent failures (not PASS drift noise)."""
    out: list[dict[str, Any]] = []
    for section in ("structural", "statistical"):
        for row in report.get(section, []) or []:
            sev = (row.get("severity") or "").upper()
            st = (row.get("status") or "").upper()
            if row.get("check") == "statistical_drift" and st == "PASS":
                continue
            if sev in ("CRITICAL", "FAIL") or st == "FAIL":
                out.append(row)
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="ViolationAttributor: registry-first blast radius + lineage + blame.")
    p.add_argument("--report", type=Path, default=Path("validation_reports/violated_run.json"), help="ValidationRunner JSON")
    p.add_argument("--contract-id", type=str, default="week3-document-refinery-extractions", help="Producer contract id")
    p.add_argument("--registry", type=Path, default=Path("contract_registry/subscriptions.yaml"))
    p.add_argument("--lineage", type=Path, default=Path("outputs/migrate/week4/lineage_snapshots.jsonl"))
    p.add_argument(
        "--producer-node",
        type=str,
        default="FILE::src/analyzers/dag_config_parser.py",
        help="Lineage node id that appears as edge.source for forward walk",
    )
    p.add_argument("--violations-out", type=Path, default=Path("violation_log/attributed_violations.jsonl"))
    p.add_argument("--repo-root", type=Path, default=None, help="Git repo root for blame (optional)")
    p.add_argument("--blame-file", type=str, default=None, help="Path relative to repo for git log --follow")
    args = p.parse_args()

    if not args.report.is_file():
        print(f"error: report not found: {args.report}", flush=True)
        return 1
    report = json.loads(args.report.read_text(encoding="utf-8"))
    failures = _pick_failures(report)
    if not failures:
        print(json.dumps({"ok": True, "message": "no CRITICAL/FAIL findings to attribute", "report": str(args.report)}, indent=2))
        return 0

    repo_root = args.repo_root
    blame_file = args.blame_file
    if repo_root is None:
        here = Path(__file__).resolve().parents[1]
        if (here / ".git").is_dir():
            repo_root = here
            blame_file = blame_file or "outputs/week3/extractions_violated.jsonl"

    ts = report.get("timestamp_utc") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    results = []
    for row in failures:
        row["records_failing"] = row.get("records_failing", 1)
        r = attribute_finding(
            row,
            contract_id=args.contract_id,
            registry_path=args.registry,
            lineage_path=args.lineage,
            producer_node_id=args.producer_node,
            violation_out=args.violations_out,
            repo_root=repo_root,
            data_file_for_blame=blame_file,
            violation_ts=ts,
        )
        results.append(r)

    print(json.dumps({"ok": True, "attributed": len(results), "output": str(args.violations_out)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
