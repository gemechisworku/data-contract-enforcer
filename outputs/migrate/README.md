# Canonical schema migrations

Scripts upcast traces under `outputs/` to the shapes defined in [`canonical_schema.md`](../../canonical_schema.md). Migrated artifacts are written under **`outputs/migrate/`** (mirrors `week1`…`week5` and `traces`); originals are not modified.

## Run (from repository root)

```text
.venv\Scripts\python.exe outputs\migrate\migrate_week1.py
.venv\Scripts\python.exe outputs\migrate\migrate_week2.py
.venv\Scripts\python.exe outputs\migrate\migrate_week3.py
.venv\Scripts\python.exe outputs\migrate\migrate_week4.py
.venv\Scripts\python.exe outputs\migrate\migrate_week5.py
.venv\Scripts\python.exe outputs\migrate\migrate_langsmith_trace.py
```

Optional: `--src` and `--dst` paths (see each script’s `--help`).

## Placeholder policy

| Area | Behavior |
|------|----------|
| UUID fields | Deterministic `uuid5` from stable source strings (`_common.uuid5_for`) unless the source already supplies a UUID-shaped value where applicable. |
| Missing paths / hashes | `"unknown"`, synthetic SHA-256 of slug (`week3`), or `git_commit` of 40×`0` when no commit exists (`week4`). |
| Tokens / costs (LangSmith) | `0` / `0.0` — not present in the workflow export. |
| Week 4 edges | NetworkX `CONFIGURES` mapped to canonical relationship **`PRODUCES`**. |
| `REPO_ROOT` | Set environment variable `REPO_ROOT` to override `codebase_root` in lineage snapshots (default `"unknown"`). |

## Source quirks

- **`outputs/week1/intent_records.jsonl`**: The first two trace rows may be **invalid JSON** (bracket mismatch). Those lines are skipped with a stderr warning; remaining lines migrate normally.
- **`outputs/traces/run.jsonl`**: A single JSON object (not NDJSON `trace_record` rows). Week 2 and LangSmith scripts both read this file with different mappings.

## Dependencies

Python 3.10+ with standard library only.
