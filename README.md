# Data Contract Enforcer

Python tooling for the **FDE Training** data-contract pipeline: infer **[Bitol / Open Data Contract Standard](https://github.com/bitol-io/open-data-contract-standard)**-style YAML from week outputs (JSONL and related artifacts), emit parallel **dbt** schema fragments, and **validate** data against those contracts (structural checks first, then statistical drift against baselines).

## Requirements

- **Python** 3.12+
- Dependencies: `pandas`, `pyyaml` (see [`pyproject.toml`](pyproject.toml))
- Optional: `ydata-profiling` for extended HTML profiles in the generator

## Setup

```bash
cd data-contract-enforcer
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate   # macOS / Linux
pip install "pandas>=2.0" "pyyaml>=6.0"
```

Optional profiling: `pip install "ydata-profiling>=4.0"`.

Run the CLI scripts from the **repository root** so paths like `outputs/` and `generated_contracts/` resolve as documented in the tools.

## Usage

**Generate contracts** (Bitol YAML under `generated_contracts/`, plus `*_dbt.yml` where applicable):

```bash
python contracts/generator.py --preset week3
python contracts/generator.py --preset week5
```

Custom sources and stems are supported; see `python contracts/generator.py --help`.

**Validate JSONL against a contract** (writes a JSON report, updates drift baselines in `schema_snapshots/` when configured):

```bash
python contracts/runner.py --source outputs/migrate/week3/extractions.jsonl --contract generated_contracts/week3_extractions.yaml --report validation_reports/validation_report.json
```

Other modules (`schema_analyzer`, `report_generator`, `attributor`, `ai_extensions`) follow the same pattern: `python contracts/<module>.py --help`.

## Repository layout

| Path | Purpose |
|------|--------|
| `contracts/` | `generator.py` (ContractGenerator), `runner.py` (ValidationRunner), helpers |
| `generated_contracts/` | Generated YAML contracts and dbt fragments |
| `validation_reports/` | JSON output from validation runs |
| `schema_snapshots/` | Profiles and `baselines.json` for drift checks |
| `outputs/` | Course artifacts (raw week outputs); `outputs/migrate/` holds normalized JSONL used by presets |
| `scripts/` | Small utilities (e.g. key/score checks) |

## Documentation

- [`canonical_schema.md`](canonical_schema.md) — target record shapes across weeks  
- [`DOMAIN_NOTES.md`](DOMAIN_NOTES.md) — canonical vs actual outputs, deviations  
- [`interim_report.md`](interim_report.md) — course interim deliverable (coverage, validation summary)  
- [`outputs/migrate/README.md`](outputs/migrate/README.md) — migration scripts for aligned JSONL
