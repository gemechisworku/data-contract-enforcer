# contracts/generator.py — ContractGenerator (Stages 1–4)
"""
Usage:
  python contracts/generator.py \\
    --source outputs/week3/extractions.jsonl \\
    --contract-id week3-document-refinery-extractions \\
    --lineage outputs/week4/lineage_snapshots.jsonl \\
    --output generated_contracts/
"""

from __future__ import annotations

import argparse
import json
import sys

import pandas as pd
import yaml
from pathlib import Path


def load_jsonl(path: str | Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def flatten_for_profile(records: list[dict]) -> pd.DataFrame:
    """Flatten nested JSONL to a flat DataFrame for profiling.
    For arrays like extracted_facts[], explode to one row per item."""
    rows = []
    for r in records:
        base = {k: v for k, v in r.items() if not isinstance(v, (list, dict))}
        for fact in r.get("extracted_facts", [{}]):
            rows.append({**base, **{f"fact_{k}": v for k, v in fact.items()}})
    return pd.DataFrame(rows)


def _series_for_uniques(series: pd.Series) -> pd.Series:
    """Map list/dict cells to JSON strings so nunique/unique work."""
    return series.map(
        lambda x: json.dumps(x, sort_keys=True) if isinstance(x, (list, dict)) else x
    )


def profile_column(series: pd.Series, col_name: str) -> dict:
    """Structural profiling per column (Stage 2). Handles unhashable list cells."""
    try:
        nuniq = int(series.nunique())
        uniq_head = series.dropna().unique()[:5]
        work = series
    except TypeError:
        work = _series_for_uniques(series)
        nuniq = int(work.nunique())
        uniq_head = work.dropna().unique()[:5]

    result = {
        "name": col_name,
        "dtype": str(series.dtype),
        "null_fraction": float(series.isna().mean()),
        "cardinality_estimate": nuniq,
        "sample_values": [str(v) for v in uniq_head],
    }

    # Enum eligibility: cardinality <= 10, object dtype, full domain enumerated
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


def column_to_clause(profile: dict) -> dict:
    """Translate one column profile to a Bitol-style JSON-schema clause (Stage 3)."""
    dtype_str = profile["dtype"]
    clause: dict = {
        "name": profile["name"],
        "description": f"Profiled column `{profile['name']}`: pandas dtype {dtype_str}, "
        f"~{profile['null_fraction']:.2%} nulls, cardinality ~{profile['cardinality_estimate']}.",
        "type": infer_type(dtype_str),
        "required": profile["null_fraction"] == 0.0,
    }

    # Confidence: float range 0–1 (spec: dtype float64 AND name contains 'confidence')
    if "confidence" in profile["name"] and clause["type"] == "number":
        clause["minimum"] = 0.0
        clause["maximum"] = 1.0
        clause["description"] = (
            "Confidence score. Must remain 0.0–1.0 float. "
            "BREAKING if changed to integer 0–100 or different scale."
        )

    # Low-cardinality strings → enum when full domain is known (<=10 values)
    if (
        dtype_str == "object"
        and profile["cardinality_estimate"] <= 10
        and profile.get("unique_values_full")
        and len(profile["unique_values_full"]) == profile["cardinality_estimate"]
    ):
        clause["enum"] = profile["unique_values_full"]

    if profile["name"].endswith("_id"):
        clause["format"] = "uuid"
        clause["pattern"] = r"^[0-9a-fA-F-]{36}$"

    if profile["name"].endswith("_at"):
        clause["format"] = "date-time"

    return clause


def build_contract(column_profiles: dict[str, dict], contract_id: str) -> dict:
    clauses = [column_to_clause(p) for p in column_profiles.values()]
    return {
        "id": contract_id,
        "version": "1.0.0",
        "domain": "fde-training",
        "schema": clauses,
    }


def inject_lineage(contract: dict, lineage_path: Path) -> dict:
    with open(lineage_path, encoding="utf-8") as f:
        snapshot = json.loads(f.readlines()[-1])
    consumers = [
        e["target"]
        for e in snapshot["edges"]
        if "week3" in str(e.get("source", "")) or "extraction" in str(e.get("source", ""))
    ]
    contract["lineage"] = {
        "upstream": [],
        "downstream": [
            {"id": c, "fields_consumed": ["doc_id", "extracted_facts"]} for c in consumers
        ],
    }
    return contract


def check_fact_confidence_violation(df: pd.DataFrame) -> None:
    """Stage 1: if fact_confidence is object (mixed types), document contract violation."""
    if "fact_confidence" not in df.columns:
        return
    dt = df["fact_confidence"].dtype
    if dt == object:
        print(
            "CONTRACT VIOLATION (data quality): column `fact_confidence` has dtype object, "
            "not float64 — mixed or non-numeric types. Fix source data before relying on "
            "confidence range enforcement.",
            file=sys.stderr,
        )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ContractGenerator: profile JSONL and emit Bitol-style YAML (Stages 1–4)."
    )
    p.add_argument(
        "--source",
        type=Path,
        default=Path("outputs/week3/extractions.jsonl"),
        help="Input JSONL to profile",
    )
    p.add_argument(
        "--contract-id",
        default="week3-document-refinery-extractions",
        help="Contract id and output filename stem",
    )
    p.add_argument(
        "--lineage",
        type=Path,
        default=Path("outputs/week4/lineage_snapshots.jsonl"),
        help="Lineage snapshot JSONL (last line = one JSON object)",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=Path("generated_contracts"),
        help="Output directory for generated YAML",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    records = load_jsonl(args.source)
    df = flatten_for_profile(records)

    # Stage 1
    print(df.describe())
    print(df.dtypes)
    check_fact_confidence_violation(df)

    # Stage 2
    column_profiles = {col: profile_column(df[col], col) for col in df.columns}

    # Stages 3–4
    contract = build_contract(column_profiles, args.contract_id)
    contract = inject_lineage(contract, args.lineage)

    args.output.mkdir(parents=True, exist_ok=True)
    output_path = args.output / f"{args.contract_id}.yaml"
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.dump(contract, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
