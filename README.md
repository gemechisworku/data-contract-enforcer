# Data Contract Enforcer — Running the System

Recipe card for evaluators: run commands from the **repository root** (`data-contract-enforcer/`). Paths assume the bundled course `outputs/` tree is present.

---

## Prerequisites

| Requirement | Notes |
|-------------|--------|
| **Python** 3.12+ | |
| **Dependencies** | Recommended: **[uv](https://docs.astral.sh/uv/)** (uses [`pyproject.toml`](pyproject.toml) + [`uv.lock`](uv.lock)). Alternative: `pip install -r requirements.txt` |
| **Data files** | `outputs/week3/extractions.jsonl` (≥50 lines typical), `outputs/week4/lineage_snapshots.jsonl`, `outputs/week5/events.jsonl` (≥50 lines typical). **Best contract match:** nested extraction shape in `outputs/migrate/week3/extractions.jsonl` (used by `--preset week3`). |
| **Optional: AI + PDF** | `OPENROUTER_API_KEY` or `OPENAI_API_KEY` in repo-root `.env` for embedding drift; `uv sync --extra ai --extra report` |

**Install (uv — recommended):**

```bash
cd data-contract-enforcer
uv venv
uv sync
uv sync --extra ai --extra report
```

**Install (pip — minimal core only):**

```bash
pip install -r requirements.txt
```

**Verify Python env:**

```bash
uv run python -c "import pandas, yaml; print('ok')"
```

**Expected:** prints `ok`.

---

## Data Contract Enforcer — Numbered steps

### Step 1: Bootstrap registry

The file [`contract_registry/subscriptions.yaml`](contract_registry/subscriptions.yaml) is committed. No generator flag consumes it today; it is used by **ViolationAttributor** and **Enforcer Report**.

**Verify subscriber rows:**

```bash
# Windows PowerShell
findstr subscriber_id contract_registry\subscriptions.yaml

# macOS / Linux
grep subscriber_id contract_registry/subscriptions.yaml
```

**Expected:** at least **4** lines containing `subscriber_id` (four interfaces: Week 3→4, Week 4→6, Week 5→6, LangSmith→6).

---

### Step 2: Generate contracts (Week 3)

The generator **does not** accept `--registry`; it needs `--file-stem` when not using a preset.

**Option A — preset (uses `outputs/migrate/week3/extractions.jsonl`, canonical nested facts):**

```bash
uv run python contracts/generator.py --preset week3 --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts
```

**Option B — explicit paths (matches course layout; use migrate file for richest schema):**

```bash
uv run python contracts/generator.py --source outputs/migrate/week3/extractions.jsonl --contract-id week3-document-refinery-extractions --file-stem week3_extractions --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts
```

**Expected console:** lines like `Wrote generated_contracts\week3_extractions.yaml`, `Wrote schema_snapshots\week3-document-refinery-extractions\<timestamp>.yaml`, `Wrote generated_contracts\week3_extractions_dbt.yml`.

**Verify success:**

```bash
uv run python -c "import yaml; d=yaml.safe_load(open('generated_contracts/week3_extractions.yaml')); print('schema fields:', len(d.get('schema',{})))"
```

**Expected:** `schema fields:` **≥ 8** (typically 10+ columns after flattening).

**Verify snapshot written:**

```bash
dir schema_snapshots\week3-document-refinery-extractions
```

**Expected:** at least one `*.yaml` timestamped file.

---

### Step 3: Validate clean data (AUDIT; establishes drift baselines if missing)

Runner uses `--source` / `--data` and `--report` / `--output` (synonyms).

```bash
uv run python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/migrate/week3/extractions.jsonl --mode AUDIT --output validation_reports/clean.json
```

Use **`outputs/migrate/week3/extractions.jsonl`** so row shapes match the generated contract (nested `extracted_facts` migrated format).

**Expected JSON:** open `validation_reports/clean.json` — `"overall"` is **`PASS`** or **`WARN`** (not `ERROR`). If `schema_snapshots/baselines.json` was absent, the report may include `"baselines_written"`.

**Verify baselines:**

```bash
uv run python -c "import json; print(json.load(open('schema_snapshots/baselines.json'))['columns'].keys())"
```

**Expected:** keys include **`fact_confidence`** (and others).

---

### Step 4: Inject violation and validate (ENFORCE)

**Create violated file (if not already present):**

```bash
uv run python scripts/create_violation.py
```

**Expected:** JSON with `"ok": true`, paths under `outputs/week3/extractions_violated.jsonl` and `violation_log/violations.jsonl`.

**Validate violated data:**

```bash
uv run python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions_violated.jsonl --mode ENFORCE --output validation_reports/violated_run.json
```

**Expected:** console JSON with `"overall": "FAIL"`; report lists **range** CRITICAL on **`fact_confidence`** and **statistical_drift** FAIL.

**Verify:**

```bash
uv run python -c "import json; r=json.load(open('validation_reports/violated_run.json')); print(r['overall'], [x.get('check') for x in r['statistical']])"
```

**Expected:** `FAIL` and checks include `range` / `statistical_drift`.

---

### Step 5: Attribute violations

```bash
uv run python contracts/attributor.py --violation validation_reports/violated_run.json --lineage outputs/migrate/week4/lineage_snapshots.jsonl --registry contract_registry/subscriptions.yaml --output violation_log/attributed_violations.jsonl
```

**Expected:** JSON with `"attributed":` number **≥ 1**, file `violation_log/attributed_violations.jsonl` appended.

**Verify:**

```bash
findstr blast_radius violation_log\attributed_violations.jsonl
```

**Expected:** lines contain `"blast_radius"` and `"week4-cartographer"`.

---

### Step 6: Schema evolution (needs ≥2 snapshots)

If you only have one snapshot, run **Step 1** again after changing source data (e.g. violated JSONL) to create a second timestamp under `schema_snapshots/week3-document-refinery-extractions/`.

```bash
uv run python contracts/generator.py --source outputs/week3/extractions_violated.jsonl --contract-id week3-document-refinery-extractions --file-stem week3_extractions --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts
```

```bash
uv run python contracts/schema_analyzer.py --contract-id week3-document-refinery-extractions --since "7 days ago" --output validation_reports/schema_evolution_week3.json
```

**Expected:** `validation_reports/schema_evolution_week3.json` with `"ok": true`, `"summary"` with field counts; may show **0 breaking** if inferred schema clauses are identical (statistical profile differs are in baselines, not always in YAML).

**Verify:**

```bash
uv run python -c "import json; s=json.load(open('validation_reports/schema_evolution_week3.json')); print(s['ok'], s['summary'])"
```

---

### Step 7: AI extensions bundle

```bash
uv run python contracts/ai_extensions.py run-all --extractions outputs/migrate/week3/extractions.jsonl --output validation_reports/ai_extensions.json
```

**Expected:** `validation_reports/ai_extensions.json` with `embedding_drift`, `prompt_validation` (counts only in file), optional `output_violation_rate` if you pass `--verdicts`.

**Verify:**

```bash
uv run python -c "import json; print(json.load(open('validation_reports/ai_extensions.json')).keys())"
```

---

### Step 8: Enforcer Report (JSON + Markdown + PDF)

```bash
uv run python contracts/report_generator.py --validation-dir validation_reports --schema-evolution validation_reports/schema_evolution_week3.json --ai-bundle validation_reports/ai_extensions.json --out-dir enforcer_report
```

**Expected:** `enforcer_report/report_<YYYY-MM-DD>.json`, `.md`, `.pdf` (PDF requires `uv sync --extra report`).

**Verify:**

```bash
dir enforcer_report
```

**Expected:** `report_*.json`, `report_*.md`, `report_*.pdf`.

---

## Quick reference

| Artifact | Purpose |
|----------|---------|
| [`generated_contracts/week3_extractions.yaml`](generated_contracts/week3_extractions.yaml) | Bitol-style contract |
| [`schema_snapshots/baselines.json`](schema_snapshots/baselines.json) | Drift statistics |
| [`validation_reports/*.json`](validation_reports/) | Runner / analyzer outputs |
| [`enforcer_report/`](enforcer_report/) | Stakeholder report |

## More documentation

- [`canonical_schema.md`](canonical_schema.md) — target record shapes  
- [`DOMAIN_NOTES.md`](DOMAIN_NOTES.md) — canonical vs actual outputs  
- [`outputs/migrate/README.md`](outputs/migrate/README.md) — migrated JSONL  

Preset shortcuts:

```bash
uv run python contracts/generator.py --preset week5 --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts
uv run python contracts/runner.py --source outputs/migrate/week5/events.jsonl --contract generated_contracts/week5_events.yaml --report validation_reports/week5_run.json
```
