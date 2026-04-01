# contracts/runner.py — ValidationRunner (structural first, statistical second; never crash)
"""
Usage:
  python contracts/runner.py --source outputs/migrate/week3/extractions.jsonl \\
    --contract generated_contracts/week3-document-refinery-extractions.yaml \\
    --report reports/validation_report.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

_CONTRACTS_DIR = Path(__file__).resolve().parent
if str(_CONTRACTS_DIR) not in sys.path:
    sys.path.insert(0, str(_CONTRACTS_DIR))

import generator as _contract_generator  # noqa: E402


def load_contract(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


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


def write_baselines(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    baselines: dict[str, dict[str, float]] = {}
    for col in df.select_dtypes(include="number").columns:
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if s.empty:
            continue
        std = float(s.std()) if len(s) > 1 else 0.0
        if std != std:  # NaN
            std = 0.0
        baselines[col] = {"mean": float(s.mean()), "stddev": std}
    payload = {
        "written_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "columns": baselines,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def overall_severity(structural: list, statistical: list) -> str:
    def sev(x: dict) -> str:
        return x.get("severity", "")

    if any(sev(x) == "CRITICAL" for x in structural):
        return "CRITICAL"
    if any(sev(x) == "FAIL" for x in statistical):
        return "FAIL"
    if any(sev(x) == "WARN" for x in statistical):
        return "WARN"
    if any(sev(x) == "CRITICAL" for x in statistical):
        return "CRITICAL"
    return "PASS"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ValidationRunner: structural then statistical checks.")
    p.add_argument("--source", type=Path, required=True, help="JSONL data file")
    p.add_argument("--contract", type=Path, required=True, help="YAML contract from ContractGenerator")
    p.add_argument(
        "--report",
        type=Path,
        default=Path("reports/validation_report.json"),
        help="Output JSON report path",
    )
    p.add_argument(
        "--baselines",
        type=Path,
        default=Path("schema_snapshots/baselines.json"),
        help="Baselines file for drift (created on first run if missing)",
    )
    return p.parse_args()


def main() -> int:
    report: dict[str, Any] = {
        "runner": "ValidationRunner",
        "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
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
        _write_report(args.report, report)
        return 1

    schema = contract.get("schema")
    if not isinstance(schema, list):
        report["errors"].append({"phase": "contract_schema", "error": "contract has no schema list"})
        report["overall"] = "ERROR"
        _write_report(args.report, report)
        return 1

    rows, err = load_jsonl_safe(args.source)
    if err:
        report["errors"].append({"phase": "load_data", "error": err})
        report["overall"] = "ERROR"
        _write_report(args.report, report)
        return 1

    try:
        df = _contract_generator.flatten_for_profile(rows or [])
    except Exception as e:
        report["errors"].append({"phase": "flatten", "error": str(e), "traceback": traceback.format_exc()})
        report["overall"] = "ERROR"
        _write_report(args.report, report)
        return 1

    # Structural (first)
    try:
        report["structural"] = run_structural(schema, df)
    except Exception as e:
        report["errors"].append({"phase": "structural", "error": str(e), "traceback": traceback.format_exc()})
        report["structural"] = [{"severity": "CRITICAL", "check": "structural_runner", "detail": str(e)}]

    # Statistical: range, then drift
    try:
        report["statistical"] = run_statistical_range(schema, df)
    except Exception as e:
        report["errors"].append({"phase": "statistical_range", "error": str(e), "traceback": traceback.format_exc()})

    try:
        report["statistical"].extend(run_statistical_drift_section(df, args.baselines))
    except Exception as e:
        report["errors"].append({"phase": "statistical_drift", "error": str(e), "traceback": traceback.format_exc()})

    report["overall"] = overall_severity(report["structural"], report["statistical"])

    # Write baselines after run if file missing (first successful pipeline through data)
    if not args.baselines.is_file() and len(df) > 0:
        try:
            write_baselines(df, args.baselines)
            report["baselines_written"] = str(args.baselines)
        except Exception as e:
            report["errors"].append({"phase": "write_baselines", "error": str(e)})

    _write_report(args.report, report)
    print(json.dumps({"overall": report["overall"], "report": str(args.report)}, indent=2))
    return 0 if report["overall"] in ("PASS", "WARN") else 1


def _write_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)


if __name__ == "__main__":
    raise SystemExit(main())
