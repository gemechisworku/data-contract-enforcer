# contracts/ai_extensions.py — AI-specific contract clauses (three independent checks)
"""
Extension 1: Embedding drift via OpenRouter (default) or OpenAI — centroid vs baseline NPZ.
Extension 2: Prompt input JSON Schema validation + quarantine.
Extension 3: LLM output schema violation rate.

Credentials (repo root `.env`, gitignored):
  OPENROUTER_API_KEY — preferred; uses https://openrouter.ai/api/v1
  OPENAI_API_KEY — fallback to api.openai.com

  uv sync --extra ai
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import sys
from pathlib import Path
from typing import Any

# --- env loading (repo-root .env) ---


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_env() -> None:
    """Load `.env` from repository root if python-dotenv is installed."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    env_path = _repo_root() / ".env"
    if env_path.is_file():
        load_dotenv(env_path)


# --- Extension 2: schema ---

WEEK3_PROMPT_SCHEMA: dict[str, Any] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["doc_id", "source_path", "content_preview"],
    "properties": {
        "doc_id": {"type": "string", "minLength": 36, "maxLength": 36},
        "source_path": {"type": "string", "minLength": 1},
        "content_preview": {"type": "string", "maxLength": 8000},
    },
    "additionalProperties": False,
}


def _require_jsonschema():
    try:
        from jsonschema import ValidationError, validate
    except ImportError as e:
        raise ImportError("Extension 2 requires jsonschema: uv sync --extra ai") from e
    return validate, ValidationError


def validate_prompt_inputs(
    records: list[dict[str, Any]],
    schema: dict[str, Any],
    quarantine_path: str | Path = "outputs/quarantine",
) -> dict[str, Any]:
    """
    Validate structured objects intended for prompt interpolation.
    Non-conforming records are appended to quarantine.jsonl under quarantine_path.
    """
    validate, ValidationError = _require_jsonschema()
    valid: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    for r in records:
        try:
            validate(instance=r, schema=schema)
            valid.append(r)
        except ValidationError as e:
            quarantined.append({"record": r, "error": e.message, "path": list(e.path)})

    qdir = Path(quarantine_path)
    if quarantined:
        qdir.mkdir(parents=True, exist_ok=True)
        qfile = qdir / "quarantine.jsonl"
        with open(qfile, "a", encoding="utf-8") as f:
            for q in quarantined:
                f.write(json.dumps(q, default=str) + "\n")

    return {"valid": len(valid), "quarantined": len(quarantined), "records": valid}


# --- Extension 1: embedding client (OpenRouter-first) ---

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
# OpenRouter model IDs use provider prefixes; see https://openrouter.ai/docs
DEFAULT_OPENROUTER_EMBEDDING_MODEL = "openai/text-embedding-3-small"
DEFAULT_OPENAI_EMBEDDING_MODEL = "text-embedding-3-small"


def _require_numpy_openai():
    try:
        import numpy as np
    except ImportError as e:
        raise ImportError("Extension 1 requires numpy: uv sync --extra ai") from e
    try:
        from openai import OpenAI
    except ImportError as e:
        raise ImportError("Extension 1 requires openai: uv sync --extra ai") from e
    return np, OpenAI


def make_embedding_client() -> tuple[Any, str, str]:
    """
    Returns (OpenAI client, provider_name, default_embedding_model).
    Prefers OPENROUTER_API_KEY; falls back to OPENAI_API_KEY.
    """
    load_env()
    _np, OpenAI = _require_numpy_openai()

    or_key = (os.environ.get("OPENROUTER_API_KEY") or "").strip()
    if or_key:
        referer = (os.environ.get("OPENROUTER_HTTP_REFERER") or "https://localhost").strip()
        title = (os.environ.get("OPENROUTER_APP_TITLE") or "data-contract-enforcer").strip()
        client = OpenAI(
            base_url=OPENROUTER_BASE_URL,
            api_key=or_key,
            default_headers={
                "HTTP-Referer": referer,
                "X-Title": title,
            },
        )
        return client, "openrouter", DEFAULT_OPENROUTER_EMBEDDING_MODEL

    oa_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if oa_key:
        return OpenAI(api_key=oa_key), "openai", DEFAULT_OPENAI_EMBEDDING_MODEL

    raise RuntimeError(
        "No embedding API key set. Add OPENROUTER_API_KEY (preferred) or OPENAI_API_KEY to .env "
        "at the repository root."
    )


def embed_sample(
    texts: list[str],
    n: int = 200,
    model: str | None = None,
    *,
    batch_size: int = 100,
    client: Any | None = None,
) -> Any:
    np, _ = _require_numpy_openai()
    if not texts:
        return np.zeros((0, 1))

    clean = [t if isinstance(t, str) else "" for t in texts]
    if len(clean) > n:
        clean = random.sample(clean, n)
    else:
        clean = clean[:n]

    if client is None:
        client, _provider, default_model = make_embedding_client()
        model = model or default_model
    elif model is None:
        _c, _provider, default_model = make_embedding_client()
        model = default_model

    assert model is not None

    all_rows: list[list[float]] = []
    for i in range(0, len(clean), batch_size):
        batch = clean[i : i + batch_size]
        resp = client.embeddings.create(input=batch, model=model)
        for e in resp.data:
            all_rows.append(e.embedding)
    return np.array(all_rows, dtype=np.float64)


def check_embedding_drift(
    texts: list[str],
    baseline_path: str | Path = "schema_snapshots/embedding_baselines.npz",
    threshold: float = 0.15,
    *,
    n: int = 200,
    model: str | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    """
    Compare centroid of embedded sample to stored baseline centroid (cosine-based drift = 1 - sim).
    First run writes baseline (centroid + model id + provider) and returns BASELINE_SET.
    """
    np, _ = _require_numpy_openai()
    path = Path(baseline_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if client is None:
        client, provider, default_model = make_embedding_client()
        model = model or default_model
    else:
        if model is None:
            _c, provider, default_model = make_embedding_client()
            model = default_model
        else:
            provider = "custom"

    if not texts:
        return {
            "status": "ERROR",
            "drift_score": 0.0,
            "message": "No text values to embed.",
            "provider": provider,
            "model": model,
        }

    vecs = embed_sample(texts, n=n, model=model, client=client)
    if vecs.size == 0:
        return {
            "status": "ERROR",
            "drift_score": 0.0,
            "message": "Empty embedding matrix.",
            "provider": provider,
            "model": model,
        }

    centroid = np.mean(vecs, axis=0)

    if not path.exists():
        np.savez(
            path,
            centroid=centroid,
            model=np.array(model, dtype=object),
            provider=np.array(provider, dtype=object),
        )
        return {
            "status": "BASELINE_SET",
            "drift_score": 0.0,
            "message": "Baseline established. Run again to detect drift.",
            "provider": provider,
            "model": model,
        }

    loaded = np.load(path, allow_pickle=True)
    baseline = loaded["centroid"]
    prev_model: str | None = None
    if "model" in loaded.files:
        m = loaded["model"]
        prev_model = str(m.item() if hasattr(m, "item") else m[()])
    if prev_model and prev_model != model:
        return {
            "status": "ERROR",
            "drift_score": 0.0,
            "message": f"Baseline model {prev_model!r} != current {model!r}. Delete baseline or use same model.",
            "provider": provider,
            "model": model,
        }

    if baseline.shape != centroid.shape:
        return {
            "status": "ERROR",
            "drift_score": 0.0,
            "message": f"Baseline dim {baseline.shape} != current {centroid.shape}.",
            "provider": provider,
            "model": model,
        }

    sim = float(
        np.dot(centroid, baseline)
        / (np.linalg.norm(centroid) * np.linalg.norm(baseline) + 1e-9)
    )
    drift = float(1.0 - sim)
    return {
        "status": "FAIL" if drift > threshold else "PASS",
        "drift_score": round(drift, 4),
        "threshold": threshold,
        "interpretation": "semantic content shifted" if drift > threshold else "stable",
        "provider": provider,
        "model": model,
    }


# --- Extension 3 ---


def check_output_violation_rate(
    outputs: list[dict[str, Any]],
    expected_enum_field: str,
    expected_values: tuple[str, ...] | list[str],
    baseline_rate: float | None = None,
    warn_threshold: float = 0.02,
) -> dict[str, Any]:
    allowed = set(expected_values)
    total = len(outputs)
    violations = sum(1 for o in outputs if o.get(expected_enum_field) not in allowed)
    rate = violations / max(total, 1)
    trend = "unknown"
    if baseline_rate is not None:
        if rate > baseline_rate * 1.5:
            trend = "rising"
        elif rate < baseline_rate * 0.5:
            trend = "falling"
        else:
            trend = "stable"
    status = "WARN" if (trend == "rising" or rate > warn_threshold) else "PASS"
    return {
        "total_outputs": total,
        "schema_violations": violations,
        "violation_rate": round(rate, 4),
        "trend": trend,
        "status": status,
        "baseline_rate": baseline_rate,
    }


# --- Helpers ---


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def extract_fact_texts_from_week3(records: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for r in records:
        facts = r.get("extracted_facts")
        if isinstance(facts, list):
            for fact in facts:
                if isinstance(fact, dict) and fact.get("text"):
                    out.append(str(fact["text"]))
        elif r.get("fact_text"):
            out.append(str(r["fact_text"]))
    return out


def _doc_id_prompt_36(raw: str) -> str:
    """Stable 36-char string for JSON Schema minLength/maxLength 36 (UUID-shaped)."""
    s = str(raw)
    if len(s) == 36:
        return s
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:36]


def records_to_prompt_inputs(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Map Week 3 extraction JSONL rows into prompt-input objects for WEEK3_PROMPT_SCHEMA.
    Uses doc_id, source_path, and a bounded text preview from extracted_facts or fact_text.
    """
    out: list[dict[str, Any]] = []
    for r in records:
        doc_id = _doc_id_prompt_36(r.get("doc_id", ""))
        source_path = str(r.get("source_path", "") or "unknown")
        preview_parts: list[str] = []
        facts = r.get("extracted_facts")
        if isinstance(facts, list):
            for fact in facts[:3]:
                if isinstance(fact, dict) and fact.get("text"):
                    preview_parts.append(str(fact["text"]))
        elif r.get("fact_text"):
            preview_parts.append(str(r["fact_text"]))
        content_preview = "\n".join(preview_parts)[:8000]
        out.append(
            {
                "doc_id": doc_id,
                "source_path": source_path,
                "content_preview": content_preview or "(empty)",
            }
        )
    return out


def main() -> int:
    load_env()
    p = argparse.ArgumentParser(description="AI contract extensions (embedding drift, prompt schema, output rate).")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_emb = sub.add_parser("embedding-drift", help="Extension 1: centroid drift vs NPZ baseline (OpenRouter)")
    p_emb.add_argument("--jsonl", type=Path, required=True, help="Week 3 JSONL (nested or flat)")
    p_emb.add_argument(
        "--baseline",
        type=Path,
        default=Path("schema_snapshots/embedding_baselines.npz"),
        help="NPZ path storing centroid vector",
    )
    p_emb.add_argument("--threshold", type=float, default=0.15)
    p_emb.add_argument("--n", type=int, default=200, help="Sample size for embedding")
    p_emb.add_argument(
        "--model",
        default=None,
        help="Override embedding model (default: openai/text-embedding-3-small on OpenRouter)",
    )

    p_pr = sub.add_parser("prompt-validate", help="Extension 2: JSON Schema + quarantine")
    p_pr.add_argument("--jsonl", type=Path, required=True, help="Week 3 JSONL; mapped to prompt input schema")
    p_pr.add_argument("--quarantine", type=Path, default=Path("outputs/quarantine"))
    p_pr.add_argument(
        "--raw",
        action="store_true",
        help="Validate JSONL rows as-is (must already match WEEK3_PROMPT_SCHEMA)",
    )

    p_out = sub.add_parser("output-violation-rate", help="Extension 3: enum field violation rate")
    p_out.add_argument("--jsonl", type=Path, required=True, help="Verdict / structured LLM outputs JSONL")
    p_out.add_argument("--field", default="overall_verdict")
    p_out.add_argument(
        "--values",
        default="PASS,FAIL,WARN",
        help="Comma-separated allowed values",
    )
    p_out.add_argument("--baseline-rate", type=float, default=None)
    p_out.add_argument("--warn-threshold", type=float, default=0.02)

    p_all = sub.add_parser("run-all", help="Run extension 2 + 3 on paths; extension 1 if --extractions set")
    p_all.add_argument(
        "--week3-jsonl",
        "--extractions",
        type=Path,
        dest="week3_jsonl",
        default=None,
        help="Week 3 JSONL for embedding drift + prompt validation",
    )
    p_all.add_argument(
        "--verdicts-jsonl",
        "--verdicts",
        type=Path,
        dest="verdicts_jsonl",
        default=None,
        help="Structured LLM outputs (e.g. verdicts) for output violation rate",
    )
    p_all.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Write combined AI bundle JSON (e.g. validation_reports/ai_extensions.json)",
    )
    p_all.add_argument("--baseline", type=Path, default=Path("schema_snapshots/embedding_baselines.npz"))
    p_all.add_argument("--quarantine", type=Path, default=Path("outputs/quarantine"))
    p_all.add_argument("--embedding-model", default=None)

    args = p.parse_args()
    try:
        if args.cmd == "embedding-drift":
            recs = load_jsonl(args.jsonl)
            texts = extract_fact_texts_from_week3(recs)
            r = check_embedding_drift(
                texts,
                baseline_path=args.baseline,
                threshold=args.threshold,
                n=args.n,
                model=args.model,
            )
            print(json.dumps(r, indent=2))
            return 0 if r.get("status") in ("PASS", "BASELINE_SET") else 1

        if args.cmd == "prompt-validate":
            recs = load_jsonl(args.jsonl)
            to_validate = recs if args.raw else records_to_prompt_inputs(recs)
            r = validate_prompt_inputs(to_validate, WEEK3_PROMPT_SCHEMA, quarantine_path=args.quarantine)
            print(json.dumps({"valid": r["valid"], "quarantined": r["quarantined"]}, indent=2))
            return 1 if r["quarantined"] else 0

        if args.cmd == "output-violation-rate":
            recs = load_jsonl(args.jsonl)
            vals = tuple(x.strip() for x in args.values.split(",") if x.strip())
            r = check_output_violation_rate(
                recs,
                args.field,
                vals,
                baseline_rate=args.baseline_rate,
                warn_threshold=args.warn_threshold,
            )
            print(json.dumps(r, indent=2))
            return 0 if r.get("status") == "PASS" else 1

        if args.cmd == "run-all":
            report: dict[str, Any] = {"embedding_drift": None, "prompt_validation": None, "output_violation_rate": None}
            if args.week3_jsonl and args.week3_jsonl.is_file():
                recs = load_jsonl(args.week3_jsonl)
                texts = extract_fact_texts_from_week3(recs)
                report["embedding_drift"] = check_embedding_drift(
                    texts,
                    baseline_path=args.baseline,
                    model=args.embedding_model,
                )
                prompt_records = records_to_prompt_inputs(recs)
                report["prompt_validation"] = validate_prompt_inputs(
                    prompt_records,
                    WEEK3_PROMPT_SCHEMA,
                    quarantine_path=args.quarantine,
                )
            if args.verdicts_jsonl and args.verdicts_jsonl.is_file():
                verdicts = load_jsonl(args.verdicts_jsonl)
                report["output_violation_rate"] = check_output_violation_rate(
                    verdicts,
                    "overall_verdict",
                    ("PASS", "FAIL", "WARN"),
                )
            out_report = dict(report)
            pv = out_report.get("prompt_validation")
            if isinstance(pv, dict) and "records" in pv:
                out_report["prompt_validation"] = {k: v for k, v in pv.items() if k != "records"}
            text = json.dumps(out_report, indent=2, default=str)
            if args.output:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(text, encoding="utf-8")
            print(json.dumps(out_report, indent=2, default=str))
            return 0
    except ImportError as e:
        print(str(e), file=sys.stderr)
        return 2
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
