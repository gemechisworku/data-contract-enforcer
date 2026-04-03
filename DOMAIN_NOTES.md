# Domain notes: outputs vs canonical schema

This document compares artifacts under `outputs/` to `canonical_schema.md`, records every deviation with file evidence, answers the five required domain questions using those same artifacts (and the specified Week 2 trace source), and summarizes **registered producer→consumer subscriptions** and breaking-field findings ([Part C](#part-c--contract-registry-subscriptions-minimum-interfaces); YAML in `contract_registry/subscriptions.yaml`).

**References:** [Open Data Contract Standard — References (ODCS)](https://github.com/bitol-io/open-data-contract-standard/blob/main/docs/references.md) for relationship notation and contract structure patterns.

---

## Part A — Canonical vs actual (deviations)

### Week 1 — `intent_record` → `outputs/week1/intent_records.jsonl`

| Canonical expectation | Actual | Evidence |
|----------------------|--------|----------|
| Top-level `intent_id` (uuid), `description`, `code_refs[]` with `file`, `line_start`, `line_end`, `symbol`, `confidence` float 0–1, `governance_tags[]`, `created_at` | Records use `id`, `intent_id` (string `"INT-001"`, not uuid), `mutation_class`, `tool`, `files[]` with `relative_path` and nested `conversations`/`ranges`; no `description`, no `governance_tags`, `timestamp` instead of `created_at` | Sample lines 2–3 in `outputs/week1/intent_records.jsonl` |
| One JSON object per line (valid JSONL) | Line 2 concatenates two JSON objects without a newline separator (invalid JSONL for that line) | `outputs/week1/intent_records.jsonl` line 2 ends with `}]}{"id":"6f525284-...` |
| Two `code_refs` shape variants | First lines use a different trace shape (`trace-001`, `timestamp`, `files[].conversations`) than later lines (`tool`, `mutation_class`, `contributor`) | Lines 1 vs 2–3 in same file |

### Week 2 — `verdict_record` → `outputs/week2/verdicts.jsonl` (missing)

| Canonical expectation | Actual | Evidence |
|----------------------|--------|----------|
| File `outputs/week2/verdicts.jsonl` with `verdict_id`, `scores`, `overall_verdict`, etc. | **No such file** in `outputs/` | `glob` over `outputs/` shows only `week1`, `week3`, `week4`, `week5`, `traces`, `extracted_keys.json` |
| Week 2 trace | Single JSON document with `inputs`, `outputs`, `metadata`, `langsmith` (Automaton Auditor run), not `verdict_record` rows | `outputs/extracted_keys.json` lists keys for `outputs/traces/run.jsonl`; file is one JSON object spanning ~1110 lines, not JSONL `verdict_record` entries |

### Week 3 — `extraction_record` → `outputs/week3/extractions.jsonl`

| Canonical expectation | Actual | Evidence |
|----------------------|--------|----------|
| `uuid` `doc_id`, `source_path`, `source_hash`, `extracted_facts[]` (with per-fact `confidence` float 0–1), `entities[]`, `extraction_model`, `token_count`, `extracted_at` | Flat per-run rows: `doc_id` (string slug, not uuid), `strategy_used`, `confidence_score`, `cost_estimate`, `processing_time`, `timestamp_utc`, `escalated_from` — no `extracted_facts`, `entities`, or `extracted_at` | Line 11 of `outputs/week3/extractions.jsonl`: `{"doc_id":"Annual_Report_JUNE-2017","strategy_used":"layout","confidence_score":0.85,...}` |
| Per-fact `confidence` float 0.0–1.0 | Document-level `confidence_score` (float, still 0–1 in sample) — different field name and granularity | Same file, e.g. `confidence_score":0.8960386999567669` line 15 |

### Week 4 — `lineage_snapshot` → `outputs/week4/lineage_snapshotsjsonl`

| Canonical expectation | Actual | Evidence |
|----------------------|--------|----------|
| Filename `lineage_snapshots.jsonl` | Filename `lineage_snapshotsjsonl` (missing `.` before `jsonl`) | `outputs/` listing |
| `snapshot_id`, `codebase_root`, `git_commit`, `nodes[]` with `node_id` `type::path`, `type` enum FILE\|TABLE\|…, `edges[]` with `relationship` IMPORTS\|CALLS\|…, `captured_at` | Single JSON **NetworkX-style** graph: `directed`, `multigraph`, `graph`, `nodes`, `edges` with `edge_type` (e.g. `CONFIGURES`), node `id` as bare path or task name — not the canonical snapshot envelope | Full `outputs/week4/lineage_snapshotsjsonl` (short file) |
| Edge `relationship` in {IMPORTS, CALLS, READS, WRITES, PRODUCES, CONSUMES} | Edge `edge_type` / `transformation_type` (e.g. `CONFIGURES`, `config`) | Same file, `edges[0]` |

### Week 5 — `event_record` → `outputs/week5/events.jsonl`

| Canonical expectation | Actual | Evidence |
|----------------------|--------|----------|
| `event_id`, `event_type`, `aggregate_id`, `aggregate_type`, `sequence_number`, `payload`, `metadata`, `schema_version`, `occurred_at`, `recorded_at` | `stream_id`, `event_type`, `event_version`, `payload`, `recorded_at` only | Line 1 of `outputs/week5/events.jsonl`: `{"stream_id": "loan-APEX-0001", "event_type": "ApplicationSubmitted", "event_version": 1, "payload": {...}, "recorded_at": "2026-02-28T18:17:51.426037"}` |
| `recorded_at >= occurred_at` | No `occurred_at`, so ordering constraint cannot be checked | Same records |
| Monotonic `sequence_number` per aggregate | No `sequence_number` or `aggregate_id` | Same |

### LangSmith trace export — `trace_record` → `outputs/traces/run.jsonl`

| Canonical expectation | Actual | Evidence |
|----------------------|--------|----------|
| JSONL of `trace_record` rows with `id`, `name`, `run_type`, token fields, `total_cost`, `tags`, `parent_run_id`, `session_id` | One **single** JSON object with top-level `inputs`, `outputs`, `metadata`, `langsmith` (nested project/workspace metadata) — not a LangSmith `trace_record` stream | `outputs/extracted_keys.json` and `run.jsonl` structure (lines 1–4, 1090–1110) |

### `outputs/extracted_keys.json`

This file is a **key index** produced by `scripts/check_keys.py`, not a canonical contract artifact. It correctly reflects the heterogeneous shapes above (e.g. week4 graph keys, week5 event keys).

---

## Part B — Five domain questions (with evidence)

### 1. Backward-compatible vs breaking schema change (with examples from Weeks 1–5 outputs)

**Definitions**

- **Backward-compatible:** Existing consumers that only validate or read fields they already know about continue to accept **older** payloads without code changes, and/or new fields are optional and ignorable.
- **Breaking:** Existing consumers fail (validation, parsing, or business logic) on data that used to be valid, or required fields are removed/renamed, or types change incompatibly.

**Three backward-compatible examples (grounded in this repo’s shapes)**

1. **Week 5 — Add optional `correlation_id` next to `payload`**  
   Current rows: `stream_id`, `event_type`, `event_version`, `payload`, `recorded_at` (line 1 of `events.jsonl`). Adding an optional string `correlation_id` does not remove existing keys; readers that ignore unknown fields keep working.

2. **Week 3 — Add optional `notes` on the same flat extraction row**  
   Rows include `doc_id`, `strategy_used`, `confidence_score`, … (`extractions.jsonl`). A nullable `notes` string does not change types of existing fields.

3. **Week 4 — Add optional `owner` on graph nodes**  
   Some nodes already carry `owner`, `freshness_sla`, etc. (`lineage_snapshotsjsonl`). Adding another optional property to the node object is compatible for consumers that treat extra keys as opaque.

**Three breaking examples (grounded in this repo’s shapes)**

1. **Week 3 — Rename `confidence_score` → `confidence` only, without dual-publish**  
   Today `confidence_score` is present (e.g. `0.85` on line 11 of `extractions.jsonl`). Consumers keyed on `confidence_score` break if the field disappears.

2. **Week 5 — Replace `stream_id` with `aggregate_id` only (uuid rename, no alias)**  
   Current `stream_id` ties streams (e.g. `"loan-APEX-0001"`). Removing `stream_id` breaks any consumer selecting by that key.

3. **Week 4 — Change `edges[].source` from string path to a nested object**  
   Edges today use string `source` and `target` (`lineage_snapshotsjsonl`). Changing to an object would break string-based graph join logic.

---

### 2. Week 3 `confidence` float 0–1 → integer 0–100: failure in Week 4 Cartographer + Bitol clause

**Hypothesis consistent with `canonical_schema.md`:** Document Refinery exposes per-fact `confidence` as float 0–1; Cartographer (or a downstream step) **normalizes** or **compares** that value to edge `confidence` (0–1) in `lineage_snapshot` and to thresholds. (In this repo’s **actual** `outputs/week3/extractions.jsonl`, the analogous field is document-level `confidence_score` — e.g. `0.85` on line 11 — but the same failure mode applies if that float were replaced by integers `0–100`.)

**Trace of failure (logical, using this repo’s roles)**

1. **Refinery** starts emitting integers `85` instead of `0.85` for the same semantic confidence.
2. **Ingestion / schema validation** — If Cartographer’s pipeline uses JSON Schema `type: number`, `maximum: 1`, integer `85` **fails** validation (breaking change), or the run is **accepted** if validation is loose.
3. **If accepted without validation:** A Cartographer step that computes `min(edge.confidence, extraction.confidence)` or compares `extraction.confidence < 0.5` misclassifies: `85 < 0.5` is false, so “low confidence” facts are never flagged; or `log(confidence)` becomes invalid.
4. **Lineage graph** — In the canonical design, `edges[].confidence` is 0–1 (`canonical_schema.md` Week 4). Mixing 0–100 on the Refinery side and 0–1 on edges **breaks** any join or display that assumes a common scale.

**Evidence that this repo’s Week 4 file is not yet in canonical shape:** Actual `outputs/week4/lineage_snapshotsjsonl` has no `confidence` on edges (only `edge_type`, `source`, `target`, …), so the **first** failure in a fully implemented pipeline would be **Refinery output contract validation** before the graph is updated; the **second** failure would be **Cartographer merge logic** expecting float 0–1.

**Bitol-compatible YAML clause (ODCS-style schema)** — lock `extracted_facts[].confidence` to float in [0, 1] so a 0–100 integer cannot ship downstream:

PS D:\FDE-Training\data-contract-enforcer> uv run scripts/check_score.py
min=0.000 max=0.896 mean=0.388

```yaml
apiVersion: v3
kind: data-contract
metadata:
  name: document-refinery-extraction-record
  version: 1.0.0
  domain: fde-training
schema:
  - id: extraction_record_tbl
    name: extraction_record
    properties:
      - id: extracted_facts_field
        name: extracted_facts
        logicalType: array
        description: Facts extracted from source documents
        properties:
          - id: confidence_field
            name: confidence
            logicalType: float
            description: MUST be 0.0–1.0 inclusive; not percent integer
            constraints:
              - type: range
                min: 0.0
                max: 1.0
            customProperties:
              - property: breaking_change
                value: Changing logicalType to integer or range 0–100 is a breaking change for Cartographer and any consumer of normalized scores
```

ODCS references for stable, refactor-safe paths: [https://github.com/bitol-io/open-data-contract-standard/blob/main/docs/references.md](https://github.com/bitol-io/open-data-contract-standard/blob/main/docs/references.md).

---

### 3. Cartographer lineage graph → blame chain (specified architecture)

**Evidence scope:** This repository contains `scripts/check_keys.py` and schema documentation; it does **not** implement a runtime `Cartographer` or `Data Contract Enforcer` service in Python. The procedure below is the **specified** behavior aligned with `canonical_schema.md` Week 4 (`nodes[]`, `edges[]`, directed relationships) and standard graph-based attribution.

**Step-by-step (how the enforcer uses the graph)**

1. **Violation detection** — A validator flags an artifact (e.g. `extraction_record` fails `confidence` range, or an event fails `sequence_number` monotonicity).
2. **Anchor node** — Map the failing artifact to a **node** `node_id` (e.g. `file::src/pipelines/extract.py` or `TABLE::extractions`) using the producer’s registered path in the snapshot.
3. **Build reverse adjacency** — From `edges[]`, index **incoming** edges: `target → [(source, relationship, confidence)]`.
4. **Traversal (blame chain)** — Starting at the anchor node, walk **backward** along edges whose `relationship` is in the causal set (e.g. `PRODUCES`, `WRITES`, `CALLS`, `IMPORTS` as appropriate to your semantics), preferring **dataflow** edges (`CONSUMES`/`PRODUCES`) over config-only edges when both exist.
   - **Concrete logic:**  
     - `visited = {anchor}`  
     - `queue = [anchor]`  
     - While `queue` not empty: `n = pop(queue)`; for each edge `e` where `e.target == n`: `next = e.source`; if `next` not in `visited`, append `next` to `visited` and `queue`, and record `(edge.relationship, e.source, e.target, e.confidence)` on the blame chain.
   - Stop at **source nodes** (in-degree zero in the subgraph) or at depth/SLA limits.
5. **Emit blame chain** — Ordered list from **upstream producer** → … → **failing node**, with edge labels and optional `git_commit` from the snapshot for human escalation.

**Canonical graph constraints:** `edge.source` and `edge.target` must reference `nodes[].node_id` (`canonical_schema.md` Week 4), which makes the traversal well-defined.

---

### 4. Data contract for LangSmith `trace_record` (structural, statistical, AI-specific)

Aligned with `canonical_schema.md` LangSmith section (trace_record). Shown in ODCS-oriented YAML with three clause types:

```yaml
apiVersion: v3
kind: data-contract
metadata:
  name: langsmith-trace-record
  version: 1.0.0
  domain: observability
schema:
  - id: trace_record_tbl
    name: trace_record
    properties:
      - id: id_field
        name: id
        logicalType: string
        description: Unique run id (uuid)
      - id: run_type_field
        name: run_type
        logicalType: string
        description: Structural — must be one of llm|chain|tool|retriever|embedding
        constraints:
          - type: enum
            values: [llm, chain, tool, retriever, embedding]
      - id: start_time_field
        name: start_time
        logicalType: timestamp
      - id: end_time_field
        name: end_time
        logicalType: timestamp
      - id: total_tokens_field
        name: total_tokens
        logicalType: integer
      - id: prompt_tokens_field
        name: prompt_tokens
        logicalType: integer
      - id: completion_tokens_field
        name: completion_tokens
        logicalType: integer
      - id: total_cost_field
        name: total_cost
        logicalType: float
        description: USD; must be >= 0
        constraints:
          - type: range
            min: 0.0
      - id: parent_run_id_field
        name: parent_run_id
        logicalType: string
        description: Nullable parent; use for tree walks
        required: false
      - id: session_id_field
        name: session_id
        logicalType: string
        required: false
quality:
  - type: statistical
    name: token_balance
    description: total_tokens must equal prompt_tokens + completion_tokens within the same record
    expression: total_tokens == prompt_tokens + completion_tokens
  - type: statistical
    name: time_order
    description: end_time must be strictly after start_time
    expression: end_time > start_time
aiExtension:
  - type: ai-specific
    name: run_type_consistency
    description: If run_type is llm, prompt_tokens and completion_tokens should both be non-negative and total_cost should be present for cost-aware runs
    rules:
      - when: run_type == llm
        require: [prompt_tokens, completion_tokens, total_cost]
```

---

### 5. Production failure modes, stale contracts, and this architecture

**Most common failure mode (industry + this repo)**  
Systems most often fail when **validation is skipped or only runs in CI** on samples that do not match production shape, so bad data reaches consumers. Here, `outputs/` already diverges heavily from `canonical_schema.md` (Parts A); without an enforcer gate, **schema drift** is indistinguishable from intentional evolution.

**Why contracts get stale**  
- Producers change **field names or types** without bumping a contract (`confidence_score` vs `confidence`).  
- **No automated diff** against the declared contract on every build.  
- **Ownership** of the contract is not tied to **lineage** (who actually produces the artifact).

**How this architecture prevents drift (when fully implemented)**  
- **Single canonical definition** in `canonical_schema.md` plus machine-readable **Bitol YAML** contracts versioned with the repo.  
- **Validation at the boundary** (Refinery emit, Cartographer ingest, event store append) — the hypothetical `confidence` clause in §2 stops integer 0–100 before it reaches the graph.  
- **Lineage snapshots** (`lineage_snapshot`) tie violations to **files/services** via the blame traversal in §3.  
- **Evidence in-repo today:** `scripts/check_keys.py` and `outputs/extracted_keys.json` provide a **minimal** structural check (top-level keys); extending this to ODCS JSON Schema validation closes the gap between “keys exist” and “schema matches.”

---

## Part C — Contract registry subscriptions (minimum interfaces)

The following boundaries are registered for **audit** with explicit `fields_consumed` and `breaking_fields`. The authoritative YAML (including `validation_mode`, `registered_at`, and `contact`) is [`contract_registry/subscriptions.yaml`](contract_registry/subscriptions.yaml).

| Producer contract (`contract_id`) | Subscriber (`subscriber_id`) | Role |
|----------------------------------|------------------------------|------|
| `week3-document-refinery-extractions` | `week4-cartographer` | Week 3 extractions → Week 4 lineage / graph consumption |
| `week4-lineage-snapshot` | `week6-contract-enforcer` | Lineage snapshot → Week 6 Data Contract Enforcer |
| `week5-event-sourcing-events` | `week6-contract-enforcer` | Event stream → Week 6 Data Contract Enforcer |
| `langsmith-trace-export` | `week6-contract-enforcer` | LangSmith trace export → Week 6 Data Contract Enforcer |

**Week 3 → Week 4 (`week4-cartographer`)**

- **Fields consumed (declared):** `doc_id`, `fact_fact_id`, `fact_text`, `fact_confidence`, `fact_entity_refs`, `source_hash`, `extraction_model`.
- **Breaking fields (summary):** `doc_id` (join key / node linkage); `fact_confidence` (must remain 0.0–1.0 float scale for threshold logic).
- **Finding (uncertainty):** Raw `outputs/week3/extractions.jsonl` uses document-level `confidence_score` and no nested `extracted_facts`; migrated JSONL under `outputs/migrate/week3/` uses flat `fact_*` columns. It is **not** verified here which shape Week 4 ingestion implements — mismatch between nested vs flat (or slug vs UUID `doc_id`) breaks consumers without an explicit mapping.

**Week 4 → Week 6 (`week6-contract-enforcer`)**

- **Fields consumed (declared, canonical lineage snapshot):** `snapshot_id`, `nodes`, `edges`, `git_commit`, `captured_at`.
- **Breaking fields (summary):** Canonical `nodes[].node_id` vs actual graph `nodes[].id`; `edges[].source` / `edges[].target` as string endpoints (object or composite keys would break string joins).
- **Finding (uncertainty):** `generated_contracts/week4_lineage.yaml` is a **stub** (`schema: {}`). Part A documents NetworkX-style graph output vs the canonical `lineage_snapshot` envelope — **any** switch between shapes is breaking once Week 6 validates one of them.

**Week 5 → Week 6 (`week6-contract-enforcer`)**

- **Fields consumed (declared):** `event_id`, `event_type`, `aggregate_id`, `aggregate_type`, `sequence_number`, `payload`, `metadata`, `schema_version`, `occurred_at`, `recorded_at`.
- **Breaking fields (summary):** `event_id` (identity / dedup); `sequence_number` (monotonicity per aggregate); `payload` (migrated contract types it as string — serialization or type change breaks parsers).
- **Finding (uncertainty):** Raw `outputs/week5/events.jsonl` uses `stream_id`, not `aggregate_id` (Part A). Which identifier Week 6 uses for cross-system joins is **not** verified — removing `stream_id` or only publishing `aggregate_id` without alias breaks whichever consumer was built against raw output.

**LangSmith → Week 6 (`week6-contract-enforcer`)**

- **Fields consumed (declared, canonical `trace_record`):** `id`, `run_type`, `start_time`, `end_time`, `prompt_tokens`, `completion_tokens`, `total_tokens`, `total_cost`, `parent_run_id`, `session_id`.
- **Breaking fields (summary):** `total_tokens` vs `prompt_tokens` + `completion_tokens`; `run_type` enum; `start_time` / `end_time` ordering and types.
- **Finding (uncertainty):** `generated_contracts/langsmith_traces.yaml` is a **stub**; Part A documents a **single JSON object** in `outputs/traces/run.jsonl`, not JSONL `trace_record` rows. Week 6 cannot list production-verified breaking columns until the export shape is normalized — switching between “one blob” and per-run JSONL is a **consumption-model** breaking change.

---

## Summary table

| Week | Canonical file | Actual output | Match? |
|------|----------------|---------------|--------|
| 1 | `intent_records.jsonl` | `outputs/week1/intent_records.jsonl` | No — shape and JSONL quality issues |
| 2 | `verdicts.jsonl` | Traces in `outputs/traces/run.jsonl` (no `verdicts.jsonl`) | No |
| 3 | `extractions.jsonl` | `outputs/week3/extractions.jsonl` | No — flat metrics vs nested extraction |
| 4 | `lineage_snapshots.jsonl` | `outputs/week4/lineage_snapshotsjsonl` | No — filename + NetworkX vs snapshot |
| 5 | `events.jsonl` | `outputs/week5/events.jsonl` | No — stream/event vs full event sourcing |
| Trace | `runs.jsonl` trace_record | `outputs/traces/run.jsonl` | No — single workflow JSON |
