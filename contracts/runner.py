# contracts/runner.py — ValidationRunner (structural first, statistical second; never crash)
"""
Usage:
  python contracts/runner.py --source outputs/migrate/week3/extractions.jsonl \\
    --contract generated_contracts/week3_extractions.yaml \\
    --report validation_reports/validation_report.json

Aliases (rubric-compatible): --data for --source, --output for --report.
AUDIT: exit 0 (log only). WARN: exit 1 on CRITICAL or ERROR. ENFORCE (default): exit 1 on CRITICAL, HIGH, or ERROR.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

_CONTRACTS_DIR = Path(__file__).resolve().parent
if str(_CONTRACTS_DIR) not in sys.path:
    sys.path.insert(0, str(_CONTRACTS_DIR))

import generator as _contract_generator  # noqa: E402
from baseline_store import write_baselines  # noqa: E402


def load_contract(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def normalize_schema_to_clauses(schema: Any) -> list[dict[str, Any]]:
    """Bitol v3 uses schema as a dict of field -> spec; legacy uses a list of {name, ...}."""
    if isinstance(schema, list):
        return schema
    if isinstance(schema, dict):
        clauses: list[dict[str, Any]] = []
        for name, spec in schema.items():
            if not isinstance(spec, dict):
                continue
            clauses.append({"name": name, **spec})
        return clauses
    return []


def _flatten_dataframe_for_contract(contract: dict[str, Any], rows: list[dict]) -> pd.DataFrame:
    """Match ContractGenerator: exploded facts vs one row per event."""
    if not rows:
        return _contract_generator.flatten_for_profile(rows)
    cid = (contract.get("id") or "").lower()
    first = rows[0]
    if "extracted_facts" in first:
        return _contract_generator.flatten_for_profile(rows)
    if "event_id" in first and isinstance(first.get("event_id"), str):
        return _contract_generator.flatten_for_events(rows)
    if "week5" in cid or "event-sourcing" in cid:
        return _contract_generator.flatten_for_events(rows)
    return _contract_generator.flatten_for_profile(rows)


def load_jsonl_safe(path: Path) -> tuple[list[dict] | None, str | None]:
    """Load JSONL; return (records, error_message). Never raises."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        return None, f"read_error: {e}"
    rows: list[dict] = []
    for i, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as e:
            return None, f"json_error_line_{i}: {e}"
    return rows, None


def check_statistical_drift(column: str, current_mean: float, current_std: float, baselines: dict) -> dict | None:
    """Implement the statistical drift check exactly as specified."""
    if column not in baselines:
        return None  # no baseline yet; will be written after this run
    b = baselines[column]
    z_score = abs(current_mean - b["mean"]) / max(b["stddev"], 1e-9)
    if z_score > 3:
        return {
            "status": "FAIL",
            "z_score": round(z_score, 2),
            "message": f"{column} mean drifted {z_score:.1f} stddev from baseline",
        }
    if z_score > 2:
        return {
            "status": "WARN",
            "z_score": round(z_score, 2),
            "message": f"{column} mean within warning range ({z_score:.1f} stddev)",
        }
    return {"status": "PASS", "z_score": round(z_score, 2)}


def _numeric_like_for_contract(series: pd.Series, contract_type: str) -> bool:
    """Contract type 'number' requires float64 or int64 per spec."""
    if contract_type != "number":
        return True
    dt = series.dtype
    return dt == "float64" or dt == "int64"


def run_structural(schema: list[dict], df: pd.DataFrame) -> list[dict]:
    findings: list[dict] = []
    n = len(df)

    for clause in schema:
        name = clause.get("name")
        if not name:
            findings.append(
                {"check": "schema_clause", "severity": "CRITICAL", "detail": "clause_missing_name", "clause": clause}
            )
            continue
        if name not in df.columns:
            findings.append(
                {
                    "check": "column_present",
                    "field": name,
                    "severity": "CRITICAL",
                    "detail": f"column '{name}' not in dataframe",
                    "status": "ERROR",
                }
            )
            continue

        s = df[name]

        # required → null_fraction == 0.0
        if clause.get("required") is True:
            nf = float(s.isna().mean())
            if nf > 0.0:
                findings.append(
                    {
                        "check": "required_field",
                        "field": name,
                        "severity": "CRITICAL",
                        "detail": "nulls_found_for_required_field",
                        "null_fraction": nf,
                        "null_count": int(s.isna().sum()),
                    }
                )

        # type number → pandas float64 or int64
        if clause.get("type") == "number":
            if not _numeric_like_for_contract(s, "number"):
                findings.append(
                    {
                        "check": "type_match",
                        "field": name,
                        "severity": "CRITICAL",
                        "detail": "expected_number_column_float64_or_int64",
                        "pandas_dtype": str(s.dtype),
                    }
                )

        # enum conformance
        if "enum" in clause and clause["enum"] is not None:
            allowed = clause["enum"]
            allowed_set = set(allowed)

            def _enum_repr(v: Any) -> str:
                if isinstance(v, (list, dict)):
                    return json.dumps(v, sort_keys=True)
                return str(v)

            def _value_in_enum(v: Any) -> bool:
                if isinstance(v, (list, dict)):
                    return _enum_repr(v) in allowed_set
                try:
                    if v in allowed_set:
                        return True
                except TypeError:
                    pass
                return str(v) in allowed_set

            non_conforming: list[Any] = []
            for v in s.dropna():
                if not _value_in_enum(v):
                    non_conforming.append(v)
            if non_conforming:
                findings.append(
                    {
                        "check": "enum_conformance",
                        "field": name,
                        "severity": "CRITICAL",
                        "non_conforming_count": len(non_conforming),
                        "sample_non_conforming": non_conforming[:20],
                    }
                )

        # UUID pattern
        if clause.get("format") == "uuid":
            pattern_str = clause.get("pattern") or r"^[0-9a-f-]{36}$"
            try:
                pat = re.compile(pattern_str)
            except re.error:
                pat = re.compile(r"^[0-9a-fA-F-]{36}$")
            sn = s.dropna()
            if len(sn) > 10_000:
                sn = sn.sample(n=100, random_state=0)
            mism = []
            for v in sn:
                if not pat.match(str(v)):
                    mism.append(str(v)[:80])
            if mism:
                findings.append(
                    {
                        "check": "uuid_pattern",
                        "field": name,
                        "severity": "CRITICAL",
                        "pattern": pattern_str,
                        "mismatch_count": len(mism),
                        "sample_mismatches": mism[:10],
                    }
                )

        # date-time parse
        if clause.get("format") == "date-time":
            unparseable = 0
            for v in s.dropna():
                t = str(v).strip()
                if t.endswith("Z"):
                    t = t[:-1] + "+00:00"
                try:
                    datetime.fromisoformat(t)
                except (ValueError, TypeError):
                    unparseable += 1
            if unparseable:
                findings.append(
                    {
                        "check": "date_time_format",
                        "field": name,
                        "severity": "CRITICAL",
                        "unparseable_count": unparseable,
                    }
                )

    return findings


def run_statistical_range(schema: list[dict], df: pd.DataFrame) -> list[dict]:
    out: list[dict] = []
    for clause in schema:
        name = clause.get("name")
        if not name or name not in df.columns:
            continue
        s = df[name]
        if not pd.api.types.is_numeric_dtype(s):
            continue
        lo = clause.get("minimum")
        hi = clause.get("maximum")
        if lo is None and hi is None:
            continue
        sn = pd.to_numeric(s, errors="coerce").dropna()
        if sn.empty:
            continue
        dmin, dmax = float(sn.min()), float(sn.max())
        ok_lo = lo is None or dmin >= float(lo)
        ok_hi = hi is None or dmax <= float(hi)
        if not (ok_lo and ok_hi):
            out.append(
                {
                    "check": "range",
                    "field": name,
                    "severity": "CRITICAL",
                    "data_min": dmin,
                    "data_max": dmax,
                    "contract_minimum": lo,
                    "contract_maximum": hi,
                    "detail": "data_range_outside_contract_bounds",
                }
            )
    return out


def run_statistical_drift_section(df: pd.DataFrame, baselines_path: Path) -> list[dict]:
    """Drift vs baselines; only runs when baselines file exists."""
    findings: list[dict] = []
    if not baselines_path.is_file():
        return findings
    try:
        raw = json.loads(baselines_path.read_text(encoding="utf-8"))
        baselines: dict = raw.get("columns") or {}
    except (json.JSONDecodeError, OSError):
        return findings

    for col in df.select_dtypes(include="number").columns:
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if s.empty:
            continue
        cur_mean = float(s.mean())
        cur_std = float(s.std()) if len(s) > 1 else 0.0
        r = check_statistical_drift(col, cur_mean, cur_std, baselines)
        if r is None:
            continue
        if r["status"] == "PASS":
            findings.append({"check": "statistical_drift", "field": col, **r})
        elif r["status"] == "WARN":
            findings.append({"check": "statistical_drift", "field": col, "severity": "WARN", **r})
        else:
            findings.append({"check": "statistical_drift", "field": col, "severity": "FAIL", **r})

    return findings


def _check_id(chk: str, field: str) -> str:
    return f"{chk}:{field}" if field else str(chk)


def normalize_structural_finding(f: dict[str, Any]) -> dict[str, Any]:
    out = dict(f)
    chk = str(f.get("check", ""))
    field = str(f.get("field", ""))
    out["check_id"] = _check_id(chk, field)

    if chk == "column_present":
        out["status"] = "ERROR"
        out["severity"] = "CRITICAL"
        out["actual_value"] = None
        out["expected"] = "column present in flattened JSONL/DataFrame"
        out["message"] = str(f.get("detail", ""))
        return out

    if chk == "schema_clause":
        out["status"] = "ERROR"
        out["severity"] = "CRITICAL"
        out["actual_value"] = f.get("clause")
        out["expected"] = "schema clause with name"
        out["message"] = str(f.get("detail", ""))
        return out

    out["status"] = "FAIL"
    sev = str(f.get("severity", "CRITICAL"))
    out["severity"] = sev if sev in ("CRITICAL", "HIGH", "MEDIUM", "LOW", "WARNING") else "CRITICAL"
    detail_keys = (
        "null_fraction",
        "null_count",
        "pandas_dtype",
        "non_conforming_count",
        "sample_non_conforming",
        "mismatch_count",
        "sample_mismatches",
        "unparseable_count",
        "pattern",
    )
    out["actual_value"] = {k: f[k] for k in detail_keys if k in f}
    out["expected"] = "contract clause satisfied"
    out["message"] = str(f.get("detail", chk))
    return out


def normalize_statistical_finding(f: dict[str, Any]) -> dict[str, Any]:
    out = dict(f)
    chk = str(f.get("check", ""))
    field = str(f.get("field", ""))
    out["check_id"] = _check_id(chk, field)

    if chk == "range":
        out["status"] = "FAIL"
        out["severity"] = "CRITICAL"
        out["actual_value"] = {"min": f.get("data_min"), "max": f.get("data_max")}
        out["expected"] = {"min": f.get("contract_minimum"), "max": f.get("contract_maximum")}
        out["message"] = (
            "Numeric min/max outside contract bounds (independent check; still runs if drift passes). "
            + str(f.get("detail", ""))
        )
        return out

    if chk == "statistical_drift":
        st = str(f.get("status", "")).upper()
        if st == "PASS":
            out["status"] = "PASS"
            out["severity"] = "LOW"
            out["actual_value"] = {"z_score": f.get("z_score")}
            out["expected"] = "mean within baseline band (warn >2 stddev, fail >3 stddev)"
            out["message"] = str(f.get("message", ""))
        elif st == "WARN" or str(f.get("severity", "")).upper() == "WARN":
            out["status"] = "WARN"
            out["severity"] = "WARNING"
            out["actual_value"] = {"z_score": f.get("z_score")}
            out["expected"] = "mean within 3 stddev of stored baseline"
            out["message"] = str(f.get("message", ""))
        else:
            out["status"] = "FAIL"
            out["severity"] = "HIGH"
            out["actual_value"] = {"z_score": f.get("z_score")}
            out["expected"] = "mean within 3 stddev of stored baseline"
            out["message"] = str(f.get("message", ""))
        return out

    out.setdefault("status", "FAIL")
    out.setdefault("severity", "MEDIUM")
    out.setdefault("message", str(f.get("detail", "")))
    return out


def overall_from_normalized_results(results: list[dict[str, Any]]) -> str:
    if any(r.get("status") == "ERROR" for r in results):
        return "ERROR"
    if any(r.get("status") == "FAIL" and r.get("severity") == "CRITICAL" for r in results):
        return "CRITICAL"
    if any(r.get("status") == "FAIL" and r.get("severity") == "HIGH" for r in results):
        return "FAIL"
    if any(r.get("status") == "WARN" for r in results):
        return "WARN"
    return "PASS"


def exit_code_for_mode(mode: str, results: list[dict[str, Any]]) -> int:
    if mode == "AUDIT":
        return 0
    for r in results:
        sev = str(r.get("severity", "")).upper()
        st = str(r.get("status", "")).upper()
        if mode == "WARN":
            if sev == "CRITICAL" or st == "ERROR":
                return 1
        elif mode == "ENFORCE":
            if sev in ("CRITICAL", "HIGH") or st == "ERROR":
                return 1
    return 0


def default_snapshot_id(source: Path) -> str:
    try:
        st = source.stat()
        raw = f"{source.resolve()}|{st.st_mtime_ns}|{st.st_size}"
    except OSError:
        raw = str(source)
    return "snap_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ValidationRunner: structural then statistical checks.")
    p.add_argument(
        "--source",
        "--data",
        type=Path,
        required=True,
        dest="source",
        help="JSONL data file",
    )
    p.add_argument("--contract", type=Path, required=True, help="YAML contract from ContractGenerator")
    p.add_argument(
        "--report",
        "--output",
        type=Path,
        default=Path("validation_reports/validation_report.json"),
        dest="report",
        help="Output JSON report path",
    )
    p.add_argument(
        "--baselines",
        type=Path,
        default=Path("schema_snapshots/baselines.json"),
        help="Baselines file for drift (created on first run if missing)",
    )
    p.add_argument(
        "--mode",
        choices=("ENFORCE", "AUDIT", "WARN"),
        default="ENFORCE",
        help="AUDIT: always exit 0. WARN: exit 1 on CRITICAL/ERROR. ENFORCE: exit 1 on CRITICAL, HIGH, or ERROR.",
    )
    p.add_argument(
        "--snapshot-id",
        default=None,
        help="Stable id for this validation run (default: hash of --source file metadata)",
    )
    return p.parse_args()


def main() -> int:
    run_ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    report: dict[str, Any] = {
        "runner": "ValidationRunner",
        "timestamp_utc": run_ts,
        "structural": [],
        "statistical": [],
        "errors": [],
        "overall": "UNKNOWN",
    }

    args = parse_args()
    try:
        contract = load_contract(args.contract)
    except Exception as e:
        report["errors"].append({"phase": "load_contract", "error": str(e), "traceback": traceback.format_exc()})
        report["overall"] = "ERROR"
        _finalize_rubric_report(report, contract_id="unknown", snapshot_id="unknown", results=[], run_ts=run_ts)
        _write_report(args.report, report)
        return 1

    contract_id = str(contract.get("id") or contract.get("info", {}).get("title") or "unknown")

    schema = normalize_schema_to_clauses(contract.get("schema"))
    if not schema:
        report["errors"].append({"phase": "contract_schema", "error": "contract has empty or missing schema"})
        report["overall"] = "ERROR"
        _finalize_rubric_report(report, contract_id=contract_id, snapshot_id="unknown", results=[], run_ts=run_ts)
        _write_report(args.report, report)
        return 1

    rows, err = load_jsonl_safe(args.source)
    if err:
        report["errors"].append({"phase": "load_data", "error": err})
        report["overall"] = "ERROR"
        _finalize_rubric_report(report, contract_id=contract_id, snapshot_id="unknown", results=[], run_ts=run_ts)
        _write_report(args.report, report)
        return 1

    try:
        df = _flatten_dataframe_for_contract(contract, rows or [])
    except Exception as e:
        report["errors"].append({"phase": "flatten", "error": str(e), "traceback": traceback.format_exc()})
        report["overall"] = "ERROR"
        _finalize_rubric_report(report, contract_id=contract_id, snapshot_id="unknown", results=[], run_ts=run_ts)
        _write_report(args.report, report)
        return 1

    snapshot_id = args.snapshot_id or default_snapshot_id(args.source)

    # Structural (first)
    try:
        raw_structural = run_structural(schema, df)
    except Exception as e:
        report["errors"].append({"phase": "structural", "error": str(e), "traceback": traceback.format_exc()})
        raw_structural = [{"severity": "CRITICAL", "check": "structural_runner", "detail": str(e), "field": ""}]

    report["structural"] = [normalize_structural_finding(f) for f in raw_structural]

    # Statistical: range (confidence bounds), then drift — independent code paths
    stat_list: list[dict[str, Any]] = []
    try:
        stat_list.extend(run_statistical_range(schema, df))
    except Exception as e:
        report["errors"].append({"phase": "statistical_range", "error": str(e), "traceback": traceback.format_exc()})

    try:
        stat_list.extend(run_statistical_drift_section(df, args.baselines))
    except Exception as e:
        report["errors"].append({"phase": "statistical_drift", "error": str(e), "traceback": traceback.format_exc()})

    report["statistical"] = [normalize_statistical_finding(f) for f in stat_list]

    results: list[dict[str, Any]] = list(report["structural"]) + list(report["statistical"])
    report["overall"] = overall_from_normalized_results(results)

    # Write baselines after run if file missing (first successful pipeline through data)
    if not args.baselines.is_file() and len(df) > 0:
        try:
            write_baselines(df, args.baselines)
            report["baselines_written"] = str(args.baselines)
        except Exception as e:
            report["errors"].append({"phase": "write_baselines", "error": str(e)})

    _finalize_rubric_report(report, contract_id=contract_id, snapshot_id=snapshot_id, results=results, run_ts=run_ts)

    _write_report(args.report, report)
    print(json.dumps({"overall": report["overall"], "report": str(args.report), "report_id": report.get("report_id")}, indent=2))
    return exit_code_for_mode(args.mode, results)


def _finalize_rubric_report(
    report: dict[str, Any],
    *,
    contract_id: str,
    snapshot_id: str,
    results: list[dict[str, Any]],
    run_ts: str,
) -> None:
    """Attach rubric top-level fields; keep legacy keys."""
    total = len(results)
    passed = sum(1 for r in results if str(r.get("status", "")).upper() == "PASS")
    failed = sum(1 for r in results if str(r.get("status", "")).upper() == "FAIL")
    warned = sum(
        1
        for r in results
        if str(r.get("status", "")).upper() == "WARN" or str(r.get("severity", "")).upper() == "WARNING"
    )
    errored = sum(1 for r in results if str(r.get("status", "")).upper() == "ERROR")

    report["report_id"] = str(uuid.uuid4())
    report["contract_id"] = contract_id
    report["snapshot_id"] = snapshot_id
    report["run_timestamp"] = run_ts
    report["total_checks"] = total
    report["passed"] = passed
    report["failed"] = failed
    report["warned"] = warned
    report["errored"] = errored
    report["results"] = results


def _write_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)


if __name__ == "__main__":
    raise SystemExit(main())
