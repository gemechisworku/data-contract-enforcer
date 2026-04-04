# contracts/schema_analyzer.py — SchemaEvolutionAnalyzer (BACKWARD default)
"""
Diffs timestamped schema snapshots from schema_snapshots/{contract_id}/ and classifies changes.

Usage:
  python contracts/schema_analyzer.py \\
    --contract-id week3-document-refinery-extractions \\
    --since "7 days ago" \\
    --output validation_reports/schema_evolution_week3.json

Requires at least two snapshot YAML files from ContractGenerator (see write_schema_snapshot in generator.py).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from attributor import compute_transitive_depth, registry_blast_radius

_COMPATIBILITY_DEFAULT = "BACKWARD"


def parse_since(s: str | None) -> datetime | None:
    """Parse filters like '7 days ago'. None / 'all' = no time filter."""
    if not s:
        return None
    t = s.strip().lower()
    if t in ("all", "any", "*", "forever"):
        return None
    m = re.match(r"^(\d+)\s+days?\s+ago$", t)
    if m:
        return datetime.now(timezone.utc) - timedelta(days=int(m.group(1)))
    m = re.match(r"^(\d+)\s+hours?\s+ago$", t)
    if m:
        return datetime.now(timezone.utc) - timedelta(hours=int(m.group(1)))
    try:
        dt = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _snapshot_time(path: Path) -> datetime:
    try:
        with open(path, encoding="utf-8") as f:
            doc = yaml.safe_load(f)
        if isinstance(doc, dict) and doc.get("snapshot_at"):
            sa = str(doc["snapshot_at"])
            if sa.endswith("Z"):
                sa = sa[:-1] + "+00:00"
            return datetime.fromisoformat(sa.replace("Z", "+00:00"))
    except (OSError, yaml.YAMLError, TypeError, ValueError):
        pass
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def list_snapshots_in_window(
    contract_id: str,
    root: Path,
    since: datetime | None,
) -> list[tuple[Path, datetime]]:
    """Sorted ascending by snapshot time."""
    d = root / contract_id
    if not d.is_dir():
        return []
    out: list[tuple[Path, datetime]] = []
    for p in sorted(d.glob("*.yaml")):
        t = _snapshot_time(p)
        if since is not None and t < since:
            continue
        out.append((p, t))
    out.sort(key=lambda x: x[1])
    return out


def _is_type_widen(old_t: str | None, new_t: str | None) -> bool:
    """BACKWARD-friendly: integer -> number is widening."""
    if old_t is None or new_t is None:
        return False
    if old_t == new_t:
        return False
    if old_t == "integer" and new_t == "number":
        return True
    return False


def _num_range_relaxed(old_clause: dict, new_clause: dict) -> bool | None:
    """True if bounds relaxed (more values allowed), False if narrowed, None if N/A."""
    o_lo = old_clause.get("minimum")
    o_hi = old_clause.get("maximum")
    n_lo = new_clause.get("minimum")
    n_hi = new_clause.get("maximum")
    if o_lo is None and o_hi is None and n_lo is None and n_hi is None:
        return None
    try:
        if n_lo is not None and o_lo is not None and float(n_lo) > float(o_lo):
            return False
        if n_hi is not None and o_hi is not None and float(n_hi) < float(o_hi):
            return False
        if n_lo is not None and o_lo is not None and float(n_lo) < float(o_lo):
            return True
        if n_hi is not None and o_hi is not None and float(n_hi) > float(o_hi):
            return True
    except (TypeError, ValueError):
        return None
    return None


def classify_change(field: str, old_clause: dict[str, Any] | None, new_clause: dict[str, Any] | None) -> tuple[str, str]:
    """
    BACKWARD compatibility as default (Confluent BACKWARD semantics).
    Returns (verdict, human_readable_reason).
    """
    if old_clause is None and new_clause is None:
        return ("COMPATIBLE", f"No clauses for {field}")

    if old_clause is None:
        req = bool(new_clause.get("required", False)) if new_clause else False
        if req:
            return (
                "BREAKING",
                f"New required field {field}. BACKWARD: existing producers do not emit this column; block deploy or dual-publish.",
            )
        return ("COMPATIBLE", f"New optional field {field}; consumers may ignore (Confluent BACKWARD: allowed).")

    if new_clause is None:
        return (
            "BREAKING",
            f"Field removed: {field}. Deprecation period mandatory; notify all registry subscribers.",
        )

    ot = old_clause.get("type")
    nt = new_clause.get("type")
    if ot != nt:
        # Rubric: explicit CRITICAL breaking — float 0.0–1.0 confidence scale → int 0–100
        if ot == "number" and nt == "integer":
            o_lo, o_hi = old_clause.get("minimum"), old_clause.get("maximum")
            n_lo, n_hi = new_clause.get("minimum"), new_clause.get("maximum")
            try:
                if (
                    o_lo is not None
                    and o_hi is not None
                    and n_lo is not None
                    and n_hi is not None
                    and float(o_lo) == 0.0
                    and float(o_hi) == 1.0
                    and int(float(n_lo)) == 0
                    and int(float(n_hi)) == 100
                ):
                    return (
                        "BREAKING",
                        f"CRITICAL: narrow type/scale for {field}: unit-interval float [0.0,1.0] -> integer [0,100] "
                        f"(breaks confidence thresholds, range checks, and statistical baselines).",
                    )
            except (TypeError, ValueError):
                pass
        if _is_type_widen(ot, nt):
            return ("COMPATIBLE", f"Type widened {ot} -> {nt} for {field} (no precision loss for integers).")
        return ("BREAKING", f"Type changed {ot} -> {nt} for {field} (narrowing or incompatible).")

    rr = _num_range_relaxed(old_clause, new_clause)
    if rr is False:
        o_lo, o_hi = old_clause.get("minimum"), old_clause.get("maximum")
        n_lo, n_hi = new_clause.get("minimum"), new_clause.get("maximum")
        return (
            "BREAKING",
            f"Range narrowed for {field}: min {o_lo} -> {n_lo}, max {o_hi} -> {n_hi} (distribution / validation may fail).",
        )
    if old_clause.get("maximum") != new_clause.get("maximum") or old_clause.get("minimum") != new_clause.get("minimum"):
        if rr is True:
            return ("COMPATIBLE", f"Range relaxed for {field} (min/max); re-run statistical checks.")
        return (
            "BREAKING",
            f"Range changed: min {old_clause.get('minimum')} -> {new_clause.get('minimum')}, "
            f"max {old_clause.get('maximum')} -> {new_clause.get('maximum')} for {field}",
        )

    old_enum = set(old_clause.get("enum") or [])
    new_enum = set(new_clause.get("enum") or [])
    if old_enum or new_enum:
        removed = old_enum - new_enum
        added = new_enum - old_enum
        if removed:
            return ("BREAKING", f"Enum values removed from {field}: {sorted(removed)}. BACKWARD: old records may use removed values.")
        if added:
            return ("COMPATIBLE", f"Enum values added to {field}: {sorted(added)} (additive; notify subscribers).")

    if old_clause.get("pattern") != new_clause.get("pattern") or old_clause.get("format") != new_clause.get("format"):
        return ("BREAKING", f"Pattern/format changed for {field}; validation and parsers may diverge.")

    if old_clause.get("required") is True and new_clause.get("required") is False:
        return ("COMPATIBLE", f"Field {field} no longer required (relaxation).")
    if old_clause.get("required") is False and new_clause.get("required") is True:
        return ("BREAKING", f"Field {field} became required; BACKWARD blocks until all producers emit it.")

    return ("COMPATIBLE", f"No material schema change detected for {field}.")


def diff_schemas(
    old_schema: dict[str, Any],
    new_schema: dict[str, Any],
) -> list[dict[str, Any]]:
    fields = set(old_schema.keys()) | set(new_schema.keys())
    rows: list[dict[str, Any]] = []
    for field in sorted(fields):
        o = old_schema.get(field)
        n = new_schema.get(field)
        verdict, reason = classify_change(field, o, n)
        rows.append(
            {
                "field": field,
                "verdict": verdict,
                "reason": reason,
                "old_clause": o,
                "new_clause": n,
            }
        )
    return rows


def _failure_mode_for_change(row: dict[str, Any], subscriber_id: str) -> str:
    v = row["verdict"]
    f = row["field"]
    if v == "COMPATIBLE":
        return f"{subscriber_id}: no code change required if ignoring unknown columns."
    if "removed" in row["reason"].lower() or "Field removed" in row["reason"]:
        return f"{subscriber_id}: reads of `{f}` may KeyError / null column; pin schema or add default."
    if "required" in row["reason"].lower():
        return f"{subscriber_id}: ingestion must accept new required `{f}` or fail validation."
    if "Type" in row["reason"] or "type" in row["reason"]:
        return f"{subscriber_id}: coercion / comparison logic for `{f}` may break (e.g. float vs int scale)."
    if "Enum" in row["reason"]:
        return f"{subscriber_id}: switch/case on `{f}` may reject new or legacy values."
    if "Range" in row["reason"] or "range" in row["reason"]:
        return f"{subscriber_id}: thresholds and statistical checks on `{f}` may fire; rebaseline."
    return f"{subscriber_id}: review downstream validators and transforms for `{f}`."


def build_migration_impact_report(
    *,
    contract_id: str,
    old_path: Path,
    new_path: Path,
    old_ts: datetime,
    new_ts: datetime,
    changes: list[dict[str, Any]],
    breaking: list[dict[str, Any]],
    registry_path: Path,
    lineage_path: Path,
    producer_node_id: str,
) -> dict[str, Any]:
    """Full impact doc when any BREAKING change exists."""
    lineage_info = compute_transitive_depth(producer_node_id, lineage_path, max_depth=3)
    blast_by_field: dict[str, list[dict[str, Any]]] = {}
    failure_modes: list[dict[str, Any]] = []
    for row in breaking:
        f = row["field"]
        blast = registry_blast_radius(contract_id, f, registry_path)
        blast_by_field[f] = blast
        for b in blast:
            failure_modes.append(
                {
                    "field": f,
                    "subscriber_id": b.get("subscriber_id"),
                    "failure_mode": _failure_mode_for_change(row, str(b.get("subscriber_id"))),
                }
            )

    stamp = new_ts.strftime("%Y%m%dT%H%M%SZ")
    return {
        "contract_id": contract_id,
        "compatibility_mode": _COMPATIBILITY_DEFAULT,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "diff": {
            "from_snapshot": str(old_path),
            "to_snapshot": str(new_path),
            "from_time": old_ts.isoformat().replace("+00:00", "Z"),
            "to_time": new_ts.isoformat().replace("+00:00", "Z"),
            "human_readable_summary": [
                f"{c['field']}: {c['verdict']} — {c['reason']}" for c in changes
            ],
        },
        "compatibility_verdict": "BREAKING" if breaking else "COMPATIBLE",
        "lineage_graph_blast": {
            "producer_node": producer_node_id,
            "direct_downstream": lineage_info.get("direct", []),
            "transitive_downstream": lineage_info.get("transitive", []),
            "max_depth": lineage_info.get("max_depth", 0),
            "note": "Graph traversal enrichment; registry subscribers remain primary for contract blast radius.",
        },
        "registry_blast_radius_by_field": blast_by_field,
        "per_consumer_failure_modes": failure_modes,
        "migration_checklist": [
            "Announce breaking fields to all registry subscribers (contact in subscriptions.yaml).",
            "Pin or rollback producer contract version until dual-publish window is agreed.",
            "Update ValidationRunner / Soda checks and re-establish statistical baselines for affected numeric fields.",
            "Run SchemaEvolutionAnalyzer again after deploy to confirm only COMPATIBLE deltas remain.",
            "For enum removals: remove value from producer last, after consumers stop emitting it.",
        ],
        "rollback_plan": [
            "Revert ContractGenerator output and re-emit previous schema snapshot (restore prior YAML).",
            "Restore prior baselines.json from version control for drift checks.",
            "If data already migrated: run compensating job to map new columns back to old shape (feature flag).",
        ],
        "breaking_changes": breaking,
    }


def run_analyzer(
    *,
    contract_id: str,
    since: str | None,
    snapshots_root: Path,
    registry_path: Path,
    lineage_path: Path,
    producer_node_id: str,
    output_report: Path,
    migration_dir: Path,
) -> dict[str, Any]:
    cutoff = parse_since(since)
    snaps = list_snapshots_in_window(contract_id, snapshots_root, cutoff)
    if len(snaps) < 2:
        return {
            "ok": False,
            "error": "need_at_least_two_snapshots",
            "message": (
                f"Found {len(snaps)} snapshot(s) under {snapshots_root / contract_id}. "
                "Run ContractGenerator twice (e.g. clean data, then violated data) to create consecutive snapshots."
            ),
            "snapshots": [str(p) for p, _ in snaps],
        }

    (old_path, old_ts) = snaps[-2]
    (new_path, new_ts) = snaps[-1]

    with open(old_path, encoding="utf-8") as f:
        old_doc = yaml.safe_load(f)
    with open(new_path, encoding="utf-8") as f:
        new_doc = yaml.safe_load(f)
    old_schema = (old_doc or {}).get("schema") or {}
    new_schema = (new_doc or {}).get("schema") or {}

    changes = diff_schemas(old_schema, new_schema)
    breaking = [c for c in changes if c["verdict"] == "BREAKING"]

    report: dict[str, Any] = {
        "ok": True,
        "contract_id": contract_id,
        "compatibility_mode": _COMPATIBILITY_DEFAULT,
        "compared": {"older": str(old_path), "newer": str(new_path), "older_at": old_ts.isoformat(), "newer_at": new_ts.isoformat()},
        "changes": changes,
        "summary": {
            "total": len(changes),
            "breaking": len(breaking),
            "compatible": len(changes) - len(breaking),
        },
    }

    if breaking:
        mig = build_migration_impact_report(
            contract_id=contract_id,
            old_path=old_path,
            new_path=new_path,
            old_ts=old_ts,
            new_ts=new_ts,
            changes=changes,
            breaking=breaking,
            registry_path=registry_path,
            lineage_path=lineage_path,
            producer_node_id=producer_node_id,
        )
        report["migration_impact"] = mig
        mig_name = f"migration_impact_{contract_id}_{new_ts.strftime('%Y%m%dT%H%M%SZ')}.json"
        mig_path = migration_dir / mig_name
        migration_dir.mkdir(parents=True, exist_ok=True)
        with open(mig_path, "w", encoding="utf-8") as f:
            json.dump(mig, f, indent=2, default=str)
        report["migration_impact_path"] = str(mig_path)

    output_report.parent.mkdir(parents=True, exist_ok=True)
    with open(output_report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    return report


def main() -> int:
    p = argparse.ArgumentParser(description="SchemaEvolutionAnalyzer: diff schema snapshots and classify changes.")
    p.add_argument("--contract-id", required=True, help="e.g. week3-document-refinery-extractions")
    p.add_argument("--since", default="30 days ago", help='Time filter for snapshots (e.g. "7 days ago", or ISO date). Use "all" for no filter.')
    p.add_argument("--output", type=Path, default=Path("validation_reports/schema_evolution.json"), help="Evolution JSON report")
    p.add_argument("--snapshots-root", type=Path, default=Path("schema_snapshots"), help="Root containing {contract_id}/*.yaml")
    p.add_argument("--registry", type=Path, default=Path("contract_registry/subscriptions.yaml"))
    p.add_argument("--lineage", type=Path, default=Path("outputs/migrate/week4/lineage_snapshots.jsonl"))
    p.add_argument(
        "--producer-node",
        default="FILE::src/analyzers/dag_config_parser.py",
        help="Lineage node for downstream blast in migration report",
    )
    p.add_argument("--migration-dir", type=Path, default=Path("validation_reports"), help="Where to write migration_impact_*.json")
    args = p.parse_args()

    since = None if args.since and args.since.strip().lower() == "all" else args.since
    rep = run_analyzer(
        contract_id=args.contract_id,
        since=since,
        snapshots_root=args.snapshots_root,
        registry_path=args.registry,
        lineage_path=args.lineage,
        producer_node_id=args.producer_node,
        output_report=args.output,
        migration_dir=args.migration_dir,
    )
    print(json.dumps({"ok": rep.get("ok", True), "summary": rep.get("summary"), "migration_impact_path": rep.get("migration_impact_path"), "error": rep.get("error")}, indent=2))
    return 0 if rep.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
