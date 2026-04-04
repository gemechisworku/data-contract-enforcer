# Enforcer Report

**Generated:** 2026-04-04T20:15:17.598762Z  

**Period:** 2026-03-28 to 2026-04-04 (schema context: last 7 days)


## Data health score

**Score:** 57.8/100  

Data health score is 58/100: 1 CRITICAL check(s) detected across 9 checks; each CRITICAL deducts 20 points from the base pass-rate score. Address these before downstream consumers rely on the affected fields.


*(checks passed: 7 / 9; CRITICAL count: 1)*


## Violations this week

- CRITICAL: 1
- FAIL: 1
- WARN: 0
- ERROR: 0


### Most significant (plain language)

1. [Week 3 Document Refinery] (week3-document-refinery-extractions) in report `violated_run.json`: field **`fact_confidence`** — Range validation failed: observed data span [0.0, 89.6] but the contract requires [0.0, 1.0]. Registry subscribers at risk: week4-cartographer, week7-contract-enforcer. Fix the producer or migrate data before consumers trust this column.

2. [Week 3 Document Refinery] (week3-document-refinery-extractions) in report `violated_run.json`: field **`fact_confidence`** — Statistical drift: mean moved about 90.01 standard deviations from the saved baseline (fact_confidence mean drifted 90.0 stddev from baseline). Registry subscribers at risk: week4-cartographer, week7-contract-enforcer. Fix the producer or migrate data before consumers trust this column.


## Schema changes detected (rolling context)

- **doc_id** — COMPATIBLE: No material schema change detected for doc_id. *Action:* No mandatory downstream code change; notify subscribers if they parse this field.

- **extracted_at** — COMPATIBLE: No material schema change detected for extracted_at. *Action:* No mandatory downstream code change; notify subscribers if they parse this field.

- **extraction_model** — COMPATIBLE: No material schema change detected for extraction_model. *Action:* No mandatory downstream code change; notify subscribers if they parse this field.

- **fact_confidence** — COMPATIBLE: No material schema change detected for fact_confidence. *Action:* No mandatory downstream code change; notify subscribers if they parse this field.

- **fact_entity_refs** — COMPATIBLE: No material schema change detected for fact_entity_refs. *Action:* No mandatory downstream code change; notify subscribers if they parse this field.

- **fact_fact_id** — COMPATIBLE: No material schema change detected for fact_fact_id. *Action:* No mandatory downstream code change; notify subscribers if they parse this field.

- **fact_page_ref** — COMPATIBLE: No material schema change detected for fact_page_ref. *Action:* No mandatory downstream code change; notify subscribers if they parse this field.

- **fact_source_excerpt** — COMPATIBLE: No material schema change detected for fact_source_excerpt. *Action:* No mandatory downstream code change; notify subscribers if they parse this field.

- **fact_text** — COMPATIBLE: No material schema change detected for fact_text. *Action:* No mandatory downstream code change; notify subscribers if they parse this field.

- **processing_time_ms** — COMPATIBLE: No material schema change detected for processing_time_ms. *Action:* No mandatory downstream code change; notify subscribers if they parse this field.

- **source_hash** — COMPATIBLE: No material schema change detected for source_hash. *Action:* No mandatory downstream code change; notify subscribers if they parse this field.

- **source_path** — COMPATIBLE: No material schema change detected for source_path. *Action:* No mandatory downstream code change; notify subscribers if they parse this field.


## AI system risk assessment

Embedding drift: status=PASS, score=0.0. Prompt inputs: 48 valid / 0 quarantined. LLM output schema violation rate: None (status UNKNOWN). Overall: AI-facing paths look within configured bounds.


## Recommended actions (priority order)

1. Fix `range:fact_confidence` on field `fact_confidence` (status=CRITICAL, severity=CRITICAL): edit schema key `fact_confidence` in `generated_contracts/week3_extractions.yaml` to match the intended contract; align source data in `outputs/migrate/week3/extractions.jsonl`; evidence in `validation_reports/violated_run.json` (`actual_value` vs `expected`).

2. Fix `statistical_drift:fact_confidence` on field `fact_confidence` (status=FAIL, severity=FAIL): edit schema key `fact_confidence` in `generated_contracts/week3_extractions.yaml` to match the intended contract; align source data in `outputs/migrate/week3/extractions.jsonl`; evidence in `validation_reports/violated_run.json` (`actual_value` vs `expected`).

3. Gate CI: `uv run python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/migrate/week3/extractions.jsonl --report validation_reports/validation_report.json --mode ENFORCE` (blocks CRITICAL/HIGH/ERROR for `week3-document-refinery-extractions`).

4. Refresh statistical baselines for drift: run the runner on known-good data so `schema_snapshots/baselines.json` updates means/stddev for numeric columns, or delete baselines to re-establish on next pass.

5. After schema or data fixes: `uv run python contracts/generator.py` for this producer, then `uv run python contracts/schema_analyzer.py --contract-id week3-document-refinery-extractions --since "7 days ago" --output validation_reports/schema_evolution_week3.json` and commit new files under `schema_snapshots/week3-document-refinery-extractions/`.
