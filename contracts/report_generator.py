# contracts/report_generator.py — Enforcer Report (JSON, Markdown, PDF)
"""
Aggregates ValidationRunner JSON, schema evolution, AI extension outputs, and registry data
into a stakeholder-facing report under enforcer_report/.

Usage (one line — works in PowerShell, cmd, and bash):

  uv run python contracts/report_generator.py --validation-dir validation_reports --registry contract_registry/subscriptions.yaml --contract-id week3-document-refinery-extractions --contract-yaml generated_contracts/week3_extractions.yaml --data-jsonl outputs/migrate/week3/extractions.jsonl --baselines schema_snapshots/baselines.json --schema-evolution validation_reports/schema_evolution_week3.json

PowerShell only: line continuation is the backtick `, not \\. Example:

  uv run python contracts/report_generator.py `
    --contract-id week3-document-refinery-extractions `
    --contract-yaml generated_contracts/week3_extractions.yaml `
    --data-jsonl outputs/migrate/week3/extractions.jsonl

Bash: use \\ at end of line for continuation.
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

# Reuse registry blast for narratives
from attributor import registry_blast_radius

_SEVERITY_RANK = {"CRITICAL": 0, "FAIL": 1, "HIGH": 1, "WARN": 2, "MEDIUM": 3, "LOW": 4}

_REPO_ROOT = Path(__file__).resolve().parents[1]

# Defaults when --contract-yaml / --data-jsonl are omitted (repo-relative to project root)
_DEFAULT_CONTRACT_YAML_BY_CONTRACT_ID: dict[str, Path] = {
    "week3-document-refinery-extractions": Path("generated_contracts/week3_extractions.yaml"),
    "week5-event-sourcing-events": Path("generated_contracts/week5_events.yaml"),
    "week4-lineage-snapshot": Path("generated_contracts/week4_lineage.yaml"),
    "langsmith-trace-export": Path("generated_contracts/langsmith_traces.yaml"),
}
_DEFAULT_DATA_JSONL_BY_CONTRACT_ID: dict[str, Path] = {
    "week3-document-refinery-extractions": Path("outputs/migrate/week3/extractions.jsonl"),
    "week5-event-sourcing-events": Path("outputs/migrate/week5/events.jsonl"),
    "week4-lineage-snapshot": Path("outputs/migrate/week4/lineage_snapshots.jsonl"),
    "langsmith-trace-export": Path("outputs/migrate/traces/runs.jsonl"),
}


def _display_path(path: Path) -> str:
    """Stable repo-relative path string for report text."""
    try:
        rp = path.resolve()
        return rp.relative_to(_REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def resolve_report_artifacts(
    contract_id: str,
    *,
    contract_yaml: Path | None,
    data_jsonl: Path | None,
    baselines: Path | None,
    schema_evolution: Path | None,
) -> dict[str, Path | None]:
    """Resolve concrete paths so recommendations need no placeholders."""

    def _abs(p: Path) -> Path:
        return p.resolve() if p.is_absolute() else (_REPO_ROOT / p).resolve()

    cy = contract_yaml or _DEFAULT_CONTRACT_YAML_BY_CONTRACT_ID.get(
        contract_id, Path("generated_contracts") / f"{contract_id}.yaml"
    )
    dj = data_jsonl or _DEFAULT_DATA_JSONL_BY_CONTRACT_ID.get(
        contract_id, Path("outputs/migrate/week3/extractions.jsonl")
    )
    base = baselines or Path("schema_snapshots/baselines.json")
    return {
        "contract_yaml": _abs(cy),
        "data_jsonl": _abs(dj),
        "baselines": _abs(base),
        "schema_evolution": _abs(schema_evolution) if schema_evolution else None,
    }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def load_runner_reports(validation_dir: Path) -> list[tuple[Path, dict[str, Any]]]:
    """Load all JSON files that look like ValidationRunner output."""
    out: list[tuple[Path, dict[str, Any]]] = []
    if not validation_dir.is_dir():
        return out
    for p in sorted(validation_dir.glob("*.json")):
        try:
            doc = json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if doc.get("runner") == "ValidationRunner" or doc.get("report_id"):
            out.append((p, doc))
    return out


def flatten_findings(
    reports: list[tuple[Path, dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Each structural/statistical row becomes one check, tagged with source file."""
    rows: list[dict[str, Any]] = []
    for path, rep in reports:
        src = path.name
        if rep.get("results"):
            for row in rep["results"]:
                chk = str(row.get("check", ""))
                section = "statistical" if chk in ("range", "statistical_drift") else "structural"
                rows.append({**row, "_source_report": src, "_section": section})
            continue
        for row in rep.get("structural", []) or []:
            rows.append({**row, "_source_report": src, "_section": "structural"})
        for row in rep.get("statistical", []) or []:
            rows.append({**row, "_source_report": src, "_section": "statistical"})
    return rows


def is_check_passed(row: dict[str, Any]) -> bool:
    """A single check passes if it is not a failing structural or statistical finding."""
    sev = (row.get("severity") or "").upper()
    st = (row.get("status") or "").upper()
    if st == "PASS":
        return True
    if st in ("FAIL", "ERROR"):
        return False
    if row.get("_section") == "structural":
        return sev != "CRITICAL"
    if row.get("check") == "statistical_drift" and st == "PASS":
        return True
    if st == "FAIL" or sev in ("CRITICAL", "FAIL"):
        return False
    return True


def count_critical(row: dict[str, Any]) -> bool:
    return (row.get("severity") or "").upper() == "CRITICAL"


def compute_data_health_score(findings: list[dict[str, Any]]) -> tuple[float, int, int, int]:
    """
    Formula: (checks_passed / total_checks) * 100, minus 20 per CRITICAL.
    Returns (score_0_100, total_checks, checks_passed, critical_count).
    """
    if not findings:
        return 100.0, 0, 0, 0
    total = len(findings)
    passed = sum(1 for f in findings if is_check_passed(f))
    critical_ct = sum(1 for f in findings if count_critical(f))
    base = (passed / total) * 100.0
    score = base - 20.0 * critical_ct
    score = max(0.0, min(100.0, score))
    return round(score, 1), total, passed, critical_ct


def health_narrative(score: float, critical_ct: int, total: int) -> str:
    if total == 0:
        return (
            "No ValidationRunner checks were found in the configured reports directory; "
            "run the runner and re-generate this report."
        )
    if score >= 90 and critical_ct == 0:
        return (
            f"Data health score is {score:.0f}/100: most contract checks passed; "
            "no CRITICAL structural violations were recorded in the aggregated runs."
        )
    if critical_ct > 0:
        return (
            f"Data health score is {score:.0f}/100: {critical_ct} CRITICAL check(s) detected "
            f"across {total} checks; each CRITICAL deducts 20 points from the base pass-rate score. "
            "Address these before downstream consumers rely on the affected fields."
        )
    return (
        f"Data health score is {score:.0f}/100 over {total} checks; "
        "review FAIL-level statistical or range findings below."
    )


def violations_by_severity(findings: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {"CRITICAL": 0, "FAIL": 0, "WARN": 0, "ERROR": 0, "OTHER": 0}
    for f in findings:
        if not is_check_passed(f):
            sev = (f.get("severity") or "").upper()
            st = (f.get("status") or "").upper()
            if st == "ERROR":
                counts["ERROR"] += 1
            elif sev == "CRITICAL":
                counts["CRITICAL"] += 1
            elif sev == "FAIL" or st == "FAIL":
                counts["FAIL"] += 1
            elif sev == "WARN" or st == "WARN":
                counts["WARN"] += 1
            else:
                counts["OTHER"] += 1
    return counts


def _sort_key_violation(f: dict[str, Any]) -> tuple[int, str]:
    sev = (f.get("severity") or "").upper()
    st = (f.get("status") or "").upper()
    rank = _SEVERITY_RANK.get(sev, 5)
    if st == "FAIL":
        rank = min(rank, 1)
    return (rank, str(f.get("field", "")))


def top_failures(findings: list[dict[str, Any]], limit: int = 3) -> list[dict[str, Any]]:
    bad = [f for f in findings if not is_check_passed(f)]
    bad.sort(key=_sort_key_violation)
    return bad[:limit]


def plain_language_violation(
    row: dict[str, Any],
    *,
    contract_id: str,
    system_name: str,
    registry_path: Path,
) -> str:
    """Readable paragraph: system, field, impact, subscribers."""
    field = str(row.get("field", "unknown_field"))
    chk = str(row.get("check", "unknown_check"))
    source = row.get("_source_report", "unknown_report")
    blast = registry_blast_radius(contract_id, field, registry_path)
    subs = ", ".join(b.get("subscriber_id", "") for b in blast) or "no registry match for this field"
    detail = row.get("detail") or row.get("message") or ""
    data_max = row.get("data_max")
    data_min = row.get("data_min")
    cmax = row.get("contract_maximum")
    cmin = row.get("contract_minimum")
    z = row.get("z_score")

    if chk == "range" and field:
        body = (
            f"Range validation failed: observed data span [{data_min}, {data_max}] "
            f"but the contract requires [{cmin}, {cmax}]."
        )
    elif chk == "statistical_drift":
        body = (
            f"Statistical drift: mean moved about {z} standard deviations from the saved baseline "
            f"({detail})."
        )
    else:
        body = str(detail)[:400]

    return (
        f"[{system_name}] ({contract_id}) in report `{source}`: field **`{field}`** — {body} "
        f"Registry subscribers at risk: {subs}. "
        f"Fix the producer or migrate data before consumers trust this column."
    )


def load_schema_evolution(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def schema_changes_plain_language(
    ev: dict[str, Any] | None,
    *,
    days: int = 7,
) -> list[dict[str, Any]]:
    """Summarize schema diff report; flag if the newer snapshot is outside the rolling window."""
    if not ev or not ev.get("ok"):
        return [
            {
                "summary": "No schema evolution report found or analyzer returned ok=false.",
                "verdict": "INFO",
                "action_required": "Run schema_analyzer after at least two ContractGenerator snapshots.",
            }
        ]
    cutoff = _utc_now() - timedelta(days=days)
    compared = ev.get("compared") or {}
    newer_at = compared.get("newer_at") or compared.get("newer")
    try:
        if isinstance(newer_at, str):
            nt = datetime.fromisoformat(newer_at.replace("Z", "+00:00"))
            if nt.tzinfo is None:
                nt = nt.replace(tzinfo=timezone.utc)
        else:
            nt = _utc_now()
    except ValueError:
        nt = _utc_now()

    summaries: list[dict[str, Any]] = []
    if nt < cutoff:
        summaries.append(
            {
                "field": "(meta)",
                "verdict": "STALE",
                "summary": f"Newer snapshot time {newer_at} is before rolling {days}-day window ({cutoff.date()}).",
                "action_required": "Re-run ContractGenerator and schema_analyzer so the diff reflects recent work.",
            }
        )
    for ch in ev.get("changes", []) or []:
        verdict = ch.get("verdict", "")
        field = ch.get("field", "")
        reason = ch.get("reason", "")
        action = (
            "Coordinate downstream: breaking changes need subscriber sign-off and migration."
            if verdict == "BREAKING"
            else "No mandatory downstream code change; notify subscribers if they parse this field."
        )
        summaries.append(
            {
                "field": field,
                "verdict": verdict,
                "summary": reason[:500],
                "action_required": action,
            }
        )
    return summaries


def load_ai_bundle(path: Path | None) -> dict[str, Any]:
    if path is None or not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    pv = raw.get("prompt_validation")
    if isinstance(pv, dict) and "records" in pv:
        raw = {
            **raw,
            "prompt_validation": {k: v for k, v in pv.items() if k != "records"},
        }
    return raw


def ai_risk_assessment(ai: dict[str, Any]) -> dict[str, Any]:
    emb = ai.get("embedding_drift") or {}
    pv = ai.get("prompt_validation") or {}
    out = ai.get("output_violation_rate") or {}

    drift_score = emb.get("drift_score")
    emb_status = emb.get("status", "not run")
    drift_pass = emb_status == "PASS"
    baseline_just_set = emb_status == "BASELINE_SET"
    prompt_ok = isinstance(pv, dict) and pv.get("quarantined", 0) == 0
    out_status = out.get("status", "UNKNOWN")
    out_rate = out.get("violation_rate")

    out_ok = out_status in ("PASS", "UNKNOWN") and (out_rate is None or out_rate <= 0.02)
    reliable = bool(drift_pass and prompt_ok and out_ok)

    narrative = (
        f"Embedding drift: status={emb_status}, score={drift_score}. "
        f"Prompt inputs: {pv.get('valid', 'n/a')} valid / {pv.get('quarantined', 'n/a')} quarantined. "
        f"LLM output schema violation rate: {out_rate} (status {out_status}). "
    )
    if baseline_just_set:
        narrative += "Embedding baseline was just established—run embedding drift again on a second schedule for a comparable score. "
    if reliable:
        narrative += "Overall: AI-facing paths look within configured bounds."
    elif baseline_just_set and prompt_ok and out_ok:
        narrative += "Overall: prompt and output checks are clean; complete a second embedding run to confirm drift stability."
    else:
        narrative += "Overall: review embedding drift, quarantined prompt rows, or rising output violations."

    return {
        "reliable_data_for_ai": reliable,
        "embedding_drift_within_bounds": drift_pass,
        "embedding_baseline_pending": baseline_just_set,
        "prompt_quarantine_clean": prompt_ok,
        "output_violation_stable": out_status == "PASS" or out_rate is None,
        "narrative": narrative.strip(),
        "raw": {"embedding_drift": emb, "prompt_validation": pv, "output_violation_rate": out},
    }


def build_recommendations(
    top: list[dict[str, Any]],
    contract_id: str,
    critical_ct: int,
    *,
    validation_dir: Path,
    artifacts: dict[str, Path | None],
) -> list[str]:
    """Data-driven actions using resolved contract, data, and baseline paths (no placeholders)."""
    recs: list[str] = []
    seen: set[str] = set()

    cy = artifacts.get("contract_yaml")
    dj = artifacts.get("data_jsonl")
    bl = artifacts.get("baselines")
    sev_path = artifacts.get("schema_evolution")

    cy_s = _display_path(cy) if cy else "generated_contracts/<contract>.yaml"
    dj_s = _display_path(dj) if dj else "outputs/<data>.jsonl"
    bl_s = _display_path(bl) if bl else "schema_snapshots/baselines.json"
    sev_s = _display_path(sev_path) if sev_path else f"validation_reports/schema_evolution_{contract_id.split('-')[0]}.json"

    def add(text: str) -> None:
        if text not in seen and len(recs) < 5:
            seen.add(text)
            recs.append(text)

    for row in top:
        fld = str(row.get("field", "") or "unknown_field")
        chk = str(row.get("check", "") or "unknown_check")
        chk_id = str(row.get("check_id", "") or f"{chk}:{fld}")
        src = str(row.get("_source_report", "") or "validation_report.json")
        sev = str(row.get("severity", "") or "")
        st = str(row.get("status", "") or "").strip()
        if not st and sev:
            st = sev
        report_fp = _display_path((validation_dir / src).resolve()) if src else _display_path(validation_dir / "validation_report.json")
        add(
            f"Fix `{chk_id}` on field `{fld}` (status={st}, severity={sev}): edit schema key `{fld}` in `{cy_s}` "
            f"to match the intended contract; align source data in `{dj_s}`; evidence in `{report_fp}` "
            f"(`actual_value` vs `expected`)."
        )
    if critical_ct > 0:
        add(
            f"Gate CI: `uv run python contracts/runner.py --contract {cy_s} --data {dj_s} "
            f"--report validation_reports/validation_report.json --mode ENFORCE` "
            f"(blocks CRITICAL/HIGH/ERROR for `{contract_id}`)."
        )
    add(
        f"Refresh statistical baselines for drift: run the runner on known-good data so `{bl_s}` updates means/stddev "
        f"for numeric columns, or delete baselines to re-establish on next pass."
    )
    add(
        f"After schema or data fixes: `uv run python contracts/generator.py` for this producer, then "
        f"`uv run python contracts/schema_analyzer.py --contract-id {contract_id} --since \"7 days ago\" "
        f"--output {sev_s}` and commit new files under `schema_snapshots/{contract_id}/`."
    )
    return recs[:5]


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    lines: list[str] = []
    lines.append(f"# Enforcer Report\n")
    lines.append(f"**Generated:** {payload['generated_at']}  \n")
    lines.append(f"**Period:** {payload['period']}\n")
    lines.append("\n## Data health score\n")
    s = payload["data_health"]
    lines.append(f"**Score:** {s['score']}/100  \n")
    lines.append(f"{s['narrative']}\n")
    lines.append(f"\n*(checks passed: {s['checks_passed']} / {s['total_checks']}; CRITICAL count: {s['critical_count']})*\n")

    lines.append("\n## Violations this week\n")
    vb = payload["violations_by_severity"]
    lines.append(
        f"- CRITICAL: {vb.get('CRITICAL', 0)}\n- FAIL: {vb.get('FAIL', 0)}\n"
        f"- WARN: {vb.get('WARN', 0)}\n- ERROR: {vb.get('ERROR', 0)}\n"
    )
    lines.append("\n### Most significant (plain language)\n")
    for i, para in enumerate(payload.get("top_violations_plain", []), 1):
        lines.append(f"{i}. {para}\n")

    lines.append("\n## Schema changes detected (rolling context)\n")
    for sc in payload.get("schema_changes", []):
        if isinstance(sc, dict) and "field" in sc:
            lines.append(
                f"- **{sc.get('field')}** — {sc.get('verdict')}: {sc.get('summary', '')[:300]} "
                f"*Action:* {sc.get('action_required', '')}\n"
            )
        else:
            lines.append(f"- {sc.get('summary', sc)}\n")

    lines.append("\n## AI system risk assessment\n")
    ar = payload["ai_risk"]
    lines.append(f"{ar['narrative']}\n")

    lines.append("\n## Recommended actions (priority order)\n")
    for i, act in enumerate(payload.get("recommended_actions", []), 1):
        lines.append(f"{i}. {act}\n")

    path.write_text("\n".join(lines), encoding="utf-8")


def write_pdf(path: Path, payload: dict[str, Any]) -> None:
    try:
        from fpdf import FPDF
        from fpdf.enums import XPos, YPos
    except ImportError as e:
        raise ImportError("PDF export requires fpdf2: uv sync --extra report") from e

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, "Enforcer Report", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Helvetica", size=10)
    pdf.multi_cell(0, 5, _ascii_safe(payload["generated_at"] + " | " + payload["period"]))
    pdf.ln(2)

    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 6, "Data health score", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Helvetica", size=10)
    s = payload["data_health"]
    pdf.multi_cell(0, 5, _ascii_safe(f"Score: {s['score']}/100. {s['narrative']}"))
    pdf.ln(1)

    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 6, "Violations (severity counts)", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Helvetica", size=10)
    vb = payload["violations_by_severity"]
    pdf.multi_cell(
        0,
        5,
        _ascii_safe(
            f"CRITICAL={vb.get('CRITICAL', 0)}, FAIL={vb.get('FAIL', 0)}, WARN={vb.get('WARN', 0)}"
        ),
    )
    pdf.ln(1)

    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 6, "Top violations", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Helvetica", size=9)
    for para in payload.get("top_violations_plain", [])[:3]:
        pdf.multi_cell(0, 4, _ascii_safe(para))
        pdf.ln(1)

    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 6, "Schema changes", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Helvetica", size=9)
    for sc in payload.get("schema_changes", [])[:12]:
        if isinstance(sc, dict):
            pdf.multi_cell(0, 4, _ascii_safe(f"{sc.get('field')} [{sc.get('verdict')}]: {sc.get('summary', '')[:200]}"))
        else:
            pdf.multi_cell(0, 4, _ascii_safe(str(sc)))
        pdf.ln(0.5)

    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 6, "AI risk", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Helvetica", size=9)
    pdf.multi_cell(0, 4, _ascii_safe(payload["ai_risk"]["narrative"][:1200]))
    pdf.ln(1)

    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 6, "Recommended actions", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Helvetica", size=9)
    for i, act in enumerate(payload.get("recommended_actions", []), 1):
        pdf.multi_cell(0, 4, _ascii_safe(f"{i}. {act}"))
        pdf.ln(1)

    pdf.output(str(path))


def _ascii_safe(s: str) -> str:
    return s.encode("ascii", "replace").decode("ascii")


def generate_report(
    *,
    validation_dir: Path,
    registry_path: Path,
    contract_id: str,
    system_name: str,
    schema_evolution_path: Path | None,
    ai_bundle_path: Path | None,
    out_dir: Path,
    date_str: str | None = None,
    contract_yaml: Path | None = None,
    data_jsonl: Path | None = None,
    baselines_path: Path | None = None,
) -> dict[str, Any]:
    date_str = date_str or _utc_now().strftime("%Y-%m-%d")
    artifacts = resolve_report_artifacts(
        contract_id,
        contract_yaml=contract_yaml,
        data_jsonl=data_jsonl,
        baselines=baselines_path,
        schema_evolution=schema_evolution_path,
    )
    reports = load_runner_reports(validation_dir)
    findings = flatten_findings(reports)
    score, total, passed, crit_ct = compute_data_health_score(findings)
    narrative = health_narrative(score, crit_ct, total)
    vb = violations_by_severity(findings)
    top = top_failures(findings, 3)
    top_plain = [
        plain_language_violation(
            r,
            contract_id=contract_id,
            system_name=system_name,
            registry_path=registry_path,
        )
        for r in top
    ]

    ev = load_schema_evolution(schema_evolution_path)
    schema_summaries = schema_changes_plain_language(ev, days=7)

    ai = load_ai_bundle(ai_bundle_path)
    ai_risk = ai_risk_assessment(ai)

    recs = build_recommendations(
        top,
        contract_id,
        crit_ct,
        validation_dir=validation_dir,
        artifacts=artifacts,
    )

    now = _utc_now().isoformat().replace("+00:00", "Z")
    period_end = _utc_now().date()
    period_start = (_utc_now() - timedelta(days=7)).date()

    payload: dict[str, Any] = {
        "generated_at": now,
        "period": f"{period_start} to {period_end} (schema context: last 7 days)",
        "data_health": {
            "score": score,
            "narrative": narrative,
            "total_checks": total,
            "checks_passed": passed,
            "critical_count": crit_ct,
            "formula": "(checks_passed / total_checks) * 100 - 20 * critical_violations, clamped 0-100",
        },
        "violations_by_severity": vb,
        "top_violations_plain": top_plain,
        "schema_changes": schema_summaries,
        "ai_risk": ai_risk,
        "recommended_actions": recs,
        "sources": {
            "runner_reports": [_display_path(p) for p, _ in reports],
            "schema_evolution": _display_path(schema_evolution_path) if schema_evolution_path else None,
            "ai_bundle": _display_path(ai_bundle_path) if ai_bundle_path else None,
            "contract_yaml": _display_path(artifacts["contract_yaml"]) if artifacts.get("contract_yaml") else None,
            "data_jsonl": _display_path(artifacts["data_jsonl"]) if artifacts.get("data_jsonl") else None,
            "baselines": _display_path(artifacts["baselines"]) if artifacts.get("baselines") else None,
            "contract_id": contract_id,
        },
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    base = f"report_{date_str}"
    json_path = out_dir / f"{base}.json"
    md_path = out_dir / f"{base}.md"
    pdf_path = out_dir / f"{base}.pdf"

    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    write_markdown(md_path, payload)
    try:
        write_pdf(pdf_path, payload)
        pdf_written = str(pdf_path)
    except ImportError:
        pdf_written = ""

    payload["_output_files"] = {"json": str(json_path), "markdown": str(md_path), "pdf": pdf_written or None}
    return payload


def main() -> int:
    p = argparse.ArgumentParser(description="Generate Enforcer Report (JSON, Markdown, PDF).")
    p.add_argument("--validation-dir", type=Path, default=Path("validation_reports"))
    p.add_argument("--registry", type=Path, default=Path("contract_registry/subscriptions.yaml"))
    p.add_argument("--contract-id", default="week3-document-refinery-extractions")
    p.add_argument("--system-name", default="Week 3 Document Refinery")
    p.add_argument("--schema-evolution", type=Path, default=Path("validation_reports/schema_evolution_week3.json"))
    p.add_argument("--ai-bundle", type=Path, default=Path("validation_reports/ai_extensions.json"))
    p.add_argument("--out-dir", type=Path, default=Path("enforcer_report"))
    p.add_argument("--date", default=None, help="YYYY-MM-DD suffix for filenames (default: today UTC)")
    p.add_argument(
        "--contract-yaml",
        type=Path,
        default=None,
        help="Bitol YAML used by ValidationRunner (default: by --contract-id)",
    )
    p.add_argument(
        "--data-jsonl",
        type=Path,
        default=None,
        help="JSONL validated by the runner (default: by --contract-id)",
    )
    p.add_argument(
        "--baselines",
        type=Path,
        default=None,
        help="schema_snapshots/baselines.json path for drift (default: schema_snapshots/baselines.json)",
    )
    args = p.parse_args()

    payload = generate_report(
        validation_dir=args.validation_dir,
        registry_path=args.registry,
        contract_id=args.contract_id,
        system_name=args.system_name,
        schema_evolution_path=args.schema_evolution if args.schema_evolution.is_file() else None,
        ai_bundle_path=args.ai_bundle if args.ai_bundle.is_file() else None,
        out_dir=args.out_dir,
        date_str=args.date,
        contract_yaml=args.contract_yaml,
        data_jsonl=args.data_jsonl,
        baselines_path=args.baselines,
    )
    print(json.dumps({"ok": True, "data_health_score": payload["data_health"]["score"], "outputs": payload.get("_output_files")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
