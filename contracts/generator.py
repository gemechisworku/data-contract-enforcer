# contracts/generator.py — ContractGenerator (course pipeline Steps 1–5)
"""
Reads outputs/ JSONL (+ Week 4 lineage), emits Bitol DataContract YAML + parallel dbt schema fragment.

Usage:
  python contracts/generator.py --preset week3
  python contracts/generator.py --preset week5
  python contracts/generator.py --source outputs/week3/extractions.jsonl --file-stem week3_extractions \\
      --contract-id week3-document-refinery-extractions --lineage outputs/week4/lineage_snapshots.jsonl

Optional: uv sync --extra profiling  (Step 1 extended: HTML profile to schema_snapshots/)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from dbt_emit import emit_dbt_schema_yml

# --- Presets (evaluation layout: generated_contracts/week3_extractions.yaml, week5_events.yaml) ---

PRESETS: dict[str, dict[str, Any]] = {
    "week3": {
        # Canonical nested extraction_record shape (matches ValidationRunner migrate path).
        "source": Path("outputs/migrate/week3/extractions.jsonl"),
        "contract_id": "week3-document-refinery-extractions",
        "file_stem": "week3_extractions",
        "title": "Week 3 Document Refinery — Extraction Records",
        "owner": "week3-team",
        "dbt_model": "stg_week3_extractions",
        "flatten": "extraction",
    },
    "week5": {
        # Migrated canonical event_record shape (richer than legacy outputs/week5/events.jsonl).
        "source": Path("outputs/migrate/week5/events.jsonl"),
        "contract_id": "week5-event-sourcing-events",
        "file_stem": "week5_events",
        "title": "Week 5 Event Sourcing — Event Records",
        "owner": "week5-team",
        "dbt_model": "stg_week5_events",
        "flatten": "events",
    },
}


def load_jsonl(path: str | Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def load_lineage_snapshot(path: Path) -> dict:
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        lines = [ln for ln in raw.splitlines() if ln.strip()]
        return json.loads(lines[-1])


def flatten_for_profile(records: list[dict]) -> pd.DataFrame:
    """Explode extracted_facts to one row per fact (flattened fact_* columns)."""
    rows = []
    for r in records:
        base = {k: v for k, v in r.items() if not isinstance(v, (list, dict))}
        for fact in r.get("extracted_facts", [{}]):
            rows.append({**base, **{f"fact_{k}": v for k, v in fact.items()}})
    return pd.DataFrame(rows)


def flatten_for_events(records: list[dict]) -> pd.DataFrame:
    """One row per event; nested dict/list columns JSON-serialized for profiling."""
    rows = []
    for r in records:
        row: dict[str, Any] = {}
        for k, v in r.items():
            if isinstance(v, (dict, list)):
                row[k] = json.dumps(v, sort_keys=True)
            else:
                row[k] = v
        rows.append(row)
    return pd.DataFrame(rows)


def _series_for_uniques(series: pd.Series) -> pd.Series:
    return series.map(
        lambda x: json.dumps(x, sort_keys=True) if isinstance(x, (list, dict)) else x
    )


def profile_column(series: pd.Series, col_name: str) -> dict:
    try:
        nuniq = int(series.nunique())
        uniq_head = series.dropna().unique()[:5]
        work = series
    except TypeError:
        work = _series_for_uniques(series)
        nuniq = int(work.nunique())
        uniq_head = work.dropna().unique()[:5]

    result: dict[str, Any] = {
        "name": col_name,
        "dtype": str(series.dtype),
        "null_fraction": float(series.isna().mean()),
        "cardinality_estimate": nuniq,
        "sample_values": [str(v) for v in uniq_head],
    }

    if str(series.dtype) == "object" and nuniq <= 10:
        work = _series_for_uniques(series)
        all_vals = sorted({str(v) for v in work.dropna().unique()})
        if len(all_vals) == nuniq:
            result["unique_values_full"] = all_vals

    if pd.api.types.is_numeric_dtype(series):
        result["stats"] = {
            "min": float(series.min()),
            "max": float(series.max()),
            "mean": float(series.mean()),
            "p25": float(series.quantile(0.25)),
            "p50": float(series.quantile(0.50)),
            "p75": float(series.quantile(0.75)),
            "p95": float(series.quantile(0.95)),
            "p99": float(series.quantile(0.99)),
            "stddev": float(series.std()),
        }
    return result


def infer_type(dtype_str: str) -> str:
    mapping = {
        "float64": "number",
        "float32": "number",
        "int64": "integer",
        "Int64": "integer",
        "bool": "boolean",
        "boolean": "boolean",
        "object": "string",
    }
    return mapping.get(dtype_str, "string")


def dominant_string_pattern(sample_vals: list[str]) -> str | None:
    if not sample_vals:
        return None
    first = str(sample_vals[0] or "")
    joined = " ".join(str(v) for v in sample_vals[:20])
    if re.match(r"^[0-9a-fA-F-]{36}$", first):
        return "uuid-like"
    if re.search(r"^\d{4}-\d{2}-\d{2}", joined):
        return "iso8601-like"
    return None


def profile_column_ydata(df: pd.DataFrame, out_html: Path | None) -> None:
    if out_html is None:
        return
    try:
        from ydata_profiling import ProfileReport  # type: ignore
    except ImportError:
        print("Optional: uv sync --extra profiling for HTML structural profile.", file=sys.stderr)
        return
    report = ProfileReport(df, title="ContractGenerator Profile", minimal=True)
    out_html.parent.mkdir(parents=True, exist_ok=True)
    report.to_file(out_html)
    print(f"Wrote ydata-profiling report {out_html}")


def check_confidence_distribution(profile: dict, col_name: str) -> None:
    if "confidence" not in col_name.lower() or "stats" not in profile:
        return
    m = profile["stats"]["mean"]
    if m > 0.99 or m < 0.01:
        print(
            f"STATISTICAL FLAG: `{col_name}` mean={m:.4f} — possible clamp/broken distribution.",
            file=sys.stderr,
        )


def column_to_field_spec(
    profile: dict,
    *,
    df: pd.DataFrame,
    col_name: str,
) -> dict[str, Any]:
    dtype_str = profile["dtype"]
    spec: dict[str, Any] = {
        "type": infer_type(dtype_str),
        "required": profile["null_fraction"] == 0.0,
        "description": (
            f"Profiled `{col_name}`: pandas {dtype_str}, "
            f"~{profile['null_fraction']:.2%} nulls, cardinality ~{profile['cardinality_estimate']}."
        ),
    }

    if "confidence" in col_name.lower() and spec["type"] == "number":
        spec["minimum"] = 0.0
        spec["maximum"] = 1.0
        spec["description"] = (
            "Confidence score. MUST stay 0.0–1.0 float. "
            "BREAKING if changed to integer 0–100 or different scale."
        )

    if (
        dtype_str == "object"
        and profile.get("unique_values_full")
        and profile["cardinality_estimate"] <= 10
        and len(profile["unique_values_full"]) == profile["cardinality_estimate"]
    ):
        spec["enum"] = profile["unique_values_full"]

    if col_name.endswith("_id") and spec["type"] == "string":
        spec["format"] = "uuid"
        spec["pattern"] = r"^[0-9a-fA-F-]{36}$"

    if col_name.endswith("_at") and spec["type"] == "string":
        spec["format"] = "date-time"

    if spec["type"] == "string" and col_name in ("source_hash",):
        spec["pattern"] = r"^[a-f0-9]{64}$"
        spec["description"] = "SHA-256 of source file (hex)."

    # uniqueness heuristic for primary keys (skip unhashable / list cells)
    s = df[col_name]
    try:
        nu = int(s.nunique())
    except TypeError:
        nu = int(_series_for_uniques(s).nunique())
    if not s.isna().any() and nu == len(df) and col_name in (
        "doc_id",
        "event_id",
        "fact_fact_id",
    ):
        spec["unique"] = True

    pat = dominant_string_pattern(profile.get("sample_values") or [])
    if pat and "format" not in spec:
        spec["x_dominant_pattern"] = pat

    return spec


def write_schema_snapshot(
    contract_id: str,
    schema_fields: dict[str, Any],
    source_path: Path,
    *,
    root: Path | None = None,
) -> Path:
    """
    Timestamped inferred schema for SchemaEvolutionAnalyzer (diff consecutive runs).
    Path: schema_snapshots/{contract_id}/{timestamp}.yaml
    """
    root = root or Path("schema_snapshots")
    ts_name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snap_dir = root / contract_id
    snap_dir.mkdir(parents=True, exist_ok=True)
    out_path = snap_dir / f"{ts_name}.yaml"
    payload = {
        "contract_id": contract_id,
        "snapshot_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source_data": str(source_path).replace("\\", "/"),
        "schema": schema_fields,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.dump(payload, f, default_flow_style=False, sort_keys=True, allow_unicode=True)
    print(f"Wrote schema snapshot {out_path}")
    return out_path


def build_schema_dict(column_profiles: dict[str, dict], df: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for col, prof in column_profiles.items():
        check_confidence_distribution(prof, col)
        out[col] = column_to_field_spec(prof, df=df, col_name=col)
    return out


def soda_checks_for_table(table_key: str, id_col: str) -> dict[str, Any]:
    return {
        "type": "SodaChecks",
        "specification": {
            f"checks for {table_key}": [
                f"missing_count({id_col}) = 0",
                f"duplicate_count({id_col}) = 0",
                "row_count >= 1",
            ]
        },
    }


def inject_lineage(
    contract: dict[str, Any],
    lineage_path: Path,
    *,
    fields_consumed: list[str],
) -> dict[str, Any]:
    try:
        snapshot = load_lineage_snapshot(lineage_path)
    except (OSError, json.JSONDecodeError) as e:
        contract.setdefault("lineage", {"upstream": [], "downstream": []})
        contract["lineage"]["_error"] = str(e)
        return contract

    edges = snapshot.get("edges") or []
    consumers: list[str] = []
    for e in edges:
        src = str(e.get("source", ""))
        if "week3" in src.lower() or "extraction" in src.lower():
            consumers.append(str(e.get("target", "")))

    downstream = [
        {
            "id": c,
            "description": "Downstream node from Week 4 lineage snapshot",
            "fields_consumed": fields_consumed,
            "breaking_if_changed": [f for f in fields_consumed if "confidence" in f or f in ("doc_id", "event_id")],
        }
        for c in consumers
        if c
    ]
    contract["lineage"] = {
        "upstream": [],
        "downstream": downstream,
        "downstream_consumers": consumers,
    }
    return contract


def build_bitol_contract(
    *,
    contract_id: str,
    title: str,
    owner: str,
    source_path: Path,
    schema_fields: dict[str, Any],
    quality: dict[str, Any],
    lineage: dict[str, Any],
    extra_terms: str | None = None,
) -> dict[str, Any]:
    return {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": title,
            "version": "1.0.0",
            "owner": owner,
            "description": (
                "Auto-generated by ContractGenerator. "
                "Flattened columns for JSONL profiling; nested logical arrays appear as JSON strings or fact_* columns."
            ),
        },
        "servers": {
            "local": {
                "type": "local",
                "path": str(source_path).replace("\\", "/"),
                "format": "jsonl",
            }
        },
        "terms": {
            "usage": "Internal inter-system data contract. Do not publish without review.",
            "limitations": extra_terms or "See schema field descriptions for breaking-change risks.",
        },
        "schema": schema_fields,
        "quality": quality,
        "lineage": lineage,
        "generation": {
            "pipeline": "structural+statistical profiling, lineage injection, optional ydata-profiling",
            "llm_annotations": [],
        },
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ContractGenerator: Bitol YAML + dbt fragment.")
    p.add_argument("--preset", choices=["week3", "week5"], default=None, help="Use course default paths and stems")
    p.add_argument("--source", type=Path, default=None)
    p.add_argument("--contract-id", default=None)
    p.add_argument("--file-stem", default=None, help="Output week3_extractions / week5_events (no .yaml)")
    p.add_argument("--lineage", type=Path, default=Path("outputs/week4/lineage_snapshots.jsonl"))
    p.add_argument("--output", type=Path, default=Path("generated_contracts"))
    p.add_argument(
        "--ydata-profile",
        action="store_true",
        help="Write ydata-profiling HTML under schema_snapshots/profiles/",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.preset:
        pr = PRESETS[args.preset]
        source = Path(pr["source"])
        contract_id = pr["contract_id"]
        file_stem = pr["file_stem"]
        title = pr["title"]
        owner = pr["owner"]
        dbt_model = pr["dbt_model"]
        flatten_mode = pr["flatten"]
    else:
        if not args.source or not args.contract_id or not args.file_stem:
            print("Provide --preset week3|week5 or all of --source --contract-id --file-stem", file=sys.stderr)
            return 2
        source = args.source
        contract_id = args.contract_id
        file_stem = args.file_stem
        title = file_stem.replace("_", " ").title()
        owner = "fde-training"
        dbt_model = f"stg_{file_stem}"
        flatten_mode = "events" if "week5" in file_stem or "event" in file_stem else "extraction"

    records = load_jsonl(source)
    if flatten_mode == "events":
        df = flatten_for_events(records)
    else:
        df = flatten_for_profile(records)

    print(df.describe(include="all"))
    print(df.dtypes)

    if "fact_confidence" in df.columns and df["fact_confidence"].dtype == object:
        print(
            "CONTRACT VIOLATION (data quality): fact_confidence is object dtype — mixed types.",
            file=sys.stderr,
        )

    ydata_path = None
    if args.ydata_profile:
        ydata_path = Path("schema_snapshots/profiles") / f"{file_stem}_ydata.html"
    profile_column_ydata(df, ydata_path)

    column_profiles = {col: profile_column(df[col], col) for col in df.columns}
    schema_fields = build_schema_dict(column_profiles, df)

    id_col = "doc_id" if "doc_id" in df.columns else "event_id" if "event_id" in df.columns else list(df.columns)[0]
    quality = soda_checks_for_table(file_stem, id_col)
    qlist = quality["specification"][f"checks for {file_stem}"]
    for c in df.columns:
        if "confidence" in c.lower() and pd.api.types.is_numeric_dtype(df[c]):
            qlist.append(f"min({c}) >= 0.0")
            qlist.append(f"max({c}) <= 1.0")

    fields_for_lineage = [c for c in ("doc_id", "extracted_facts", "extraction_model", "event_id", "aggregate_id") if c in df.columns]
    contract: dict[str, Any] = build_bitol_contract(
        contract_id=contract_id,
        title=title,
        owner=owner,
        source_path=source,
        schema_fields=schema_fields,
        quality=quality,
        lineage={"upstream": [], "downstream": []},
    )
    contract = inject_lineage(contract, args.lineage, fields_consumed=fields_for_lineage or list(schema_fields.keys())[:5])

    args.output.mkdir(parents=True, exist_ok=True)
    yaml_path = args.output / f"{file_stem}.yaml"
    with open(yaml_path, "w", encoding="utf-8") as f:
        yaml.dump(contract, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    print(f"Wrote {yaml_path}")

    write_schema_snapshot(contract_id, schema_fields, source)

    dbt_path = args.output / f"{file_stem}_dbt.yml"
    emit_dbt_schema_yml(
        model_name=dbt_model,
        schema_fields=schema_fields,
        description=f"dbt tests aligned with {file_stem}.yaml — regenerate, do not hand-edit.",
        out_path=dbt_path,
    )
    print(f"Wrote {dbt_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
