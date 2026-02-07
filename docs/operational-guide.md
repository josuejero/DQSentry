# DQSentry Operational Guide

## Live experiences
- **DQ Scorecard** – the latest evaluation is automatically rendered and published from `reports/latest/index.html` via GitHub Pages. Visit the public view at `https://josuejero.github.io/DQSentry/` to see the most recent score, issue counts, recurrence trends, and check-level details.
- **CSV validator UI** – the Streamlit surface lets you upload the exported `districts.csv`, `users.csv`, `resources.csv`, `events.csv`, and `newsletter.csv` files (or reuse the provided sample) to run the ingest → validation → score cycle without cloning the repo. The hosted experience is at `https://dqsentry.streamlit.app/`.

## System snapshot
DQSentry codifies a full data-quality playbook in Python, DuckDB, and YAML so that each run is reproducible, explainable, and portable. The pipeline stages raw exports through ingestion, profiling, validation, scoring, publication, and monitoring while keeping all configuration (schema expectations, mapping tables, rules, root causes, thresholds, weights) under version control.

### Directory & artifact layout
- `data/raw/<dataset>/<seed>` – exports as CSV/JSON bundles. Never mutate this directory; ingestion always copies files out.
- `data/staging/<dataset>/<seed>` – DuckDB models, generated Parquet files, and run metadata (`run_metadata.json`, `ingest_metadata.json`).
- `data/marts/` – (e.g., `dq_check_results`, `dq_issue_log`, `dq_run_history`, `dq_issue_history`, `dq_metrics_history`, `dq_schema_drift`, `dq_anomalies`, `dq_issue_recurrence`, `score_history`) store the materialized validation artifacts used by scoring, history, and trend calculations.
- `dq/` – domain logic: configuration (`config/`), Great Expectations artifacts (`ge/`), validation helpers, anomaly/schema drift detection, Streamlit UI, and regression data.
- `scripts/` – CLI entry points (`ingest.py`, `profile_tables.py`, `validate_runner.py`, `score.py`, `publish.py`, `quality_gate.py`, `regression.py`, `get_run_id.py`) plus shared libraries that glue the pipeline together.
- `reports/` – templates (`templates/scorecard.html.jinja`), the current published scorecard (`latest/`), and archived run exports (`runs/run_id=<id>`).
- `tools/` – deterministic synthetic dataset generator for Phase 1 exports.

## Pipeline components
### 1. Ingestion
`tools/generate_synthetic.py` (or your own CSV bundle) seeds `data/raw/<dataset>/<seed>`. `scripts/ingest.py` calls `scripts/ingest_lib.ingest_dataset`, which runs every `TABLE_SPECS` definition in `scripts/ingest_tables.py`, canonicalizes states/grades/districts via `dq/config/mappings.yml`, parses timestamps, writes DuckDB staging tables `staging_<table>`, dumps them to Parquet, and persists `run_metadata.json`/`ingest_metadata.json`. The ingestion path becomes the single source-of-truth for every downstream step.

### 2. Profiling
`scripts/profile_tables.py` loads the DuckDB tables, collects column-level metrics via `scripts/profile_collector`, and writes Parquet/JSON artifacts into `data/marts/profiles/run_id=<id>`. It also generates an HTML summary inside `reports/runs/run_id=<id>/profile.html`, which is handy for human reviews or automated documentation.

### 3. Validation & scoring
`scripts/validate_runner.py` boots `dq.validate.runner.ValidationRunner`, which loads `dq/config/rules.yml`, evaluates every `CheckRule`, persists Great Expectations suites/results under `dq/ge/`, and records run metrics. It computes `dq_check_results`, `dq_issue_log`, `dq_run_history`, `dq_issue_history`, `dq_issue_recurrence`, and also kicks off `dq/anomaly.run_anomaly_detection` and `dq/schema_drift.run_schema_drift_detection` to flag shifts. `scripts/score.py` then reads the persisted checks, applies the penalty-based scoring formula (`normalized_penalty = total_penalty / total_weight`, `score = baseline - 100 * normalized_penalty` capped at the configured minimum), builds summary data (`score.json`, `issues.csv`), and appends to `data/marts/score_history`.

### 4. Scorecard publication
`reports/latest/score.json` feeds `scripts/publish.py`, which loads the score payload, uses `scripts/publish_helpers.mutate_context` to add trend HTML (from `dq_issue_history` and recurrence metrics), renders `reports/templates/scorecard.html.jinja`, and copies the artifacts into `reports/runs/run_id=<id>`. The `GH Pages` workflow (`.github/workflows/pages.yml`) waits for `make report` to finish, uploads `reports/latest`, and deploys it, keeping `https://josuejero.github.io/DQSentry/` in sync with the latest build.

### 5. Streamlit CSV validator
`dq/app/app.py` boots Streamlit and calls `dq/app/ui.main`, which handles file uploads (CSV/ZIP), copies raw files via `dq/app/processing.prepare_raw_source`, then runs `ingest_dataset` + `ValidationRunner` through `run_validation_pipeline`. Results, cleaned Parquet exports, `issues.csv`, `exceptions.csv`, and `ValidationSummary` flow back into the UI so users can download ready-to-use artifacts.

### 6. Automation, gatekeeping, and regression
- `.github/workflows/dq_push.yml` runs on pushes/PRs, while `.github/workflows/dq_scheduled.yml` runs nightly at 04:00 UTC. Each pipeline generates synthetic data (optional), ingests, validates, scores, renders the scoreboard, enforces `scripts/quality_gate.py` (defaults: score ≥ 90, no failed severity ≥ 5 checks), uploads artifacts, and publishes via `peaceiris/actions-gh-pages@v4`.
- `scripts/regression.py` reruns the pipeline against `dq/regression/golden_dataset.zip` and compares the score, issue counts, and subscores to `dq/regression/golden_expected.json`. Use `--update-expected` when intentional changes modify the golden baseline.

## Running locally
1. `make setup` – creates the virtual environment (`.venv`), upgrades `pip`, installs dependencies from `requirements.txt`, and ensures `cmake` is available (needed for `pyarrow`). This target runs `scripts/ensure_python_version.py` first to enforce CPython 3.9–3.13 (PyArrow 18 only ships wheels for those releases), so install a compatible interpreter if your system Python is newer.
2. `make sample` – populates `data/raw/phase1/<seed>` via `tools/generate_synthetic.py`. Skip if you have your own exports.
3. `make ingest` – copies `data/raw/<dataset>/<seed>` into `data/staging/<dataset>/<seed>`, writing DuckDB/Parquet artifacts. Use `DATASET`/`SEED` overrides or set environment variables (the defaults target `phase1/42`).
4. `make profile` – reads staging tables, emits profile artifacts, and drops an HTML summary in `reports/runs/run_id=<id>/profile.html`.
5. `make validate` – runs validation + scoring (`scripts/validate_runner.py`, `scripts/score.py`) and leaves `reports/latest/score.json` plus `reports/latest/issues.csv` ready.
6. `make report` – renders the scorecard HTML and archives artifacts for the run.
7. `make run` – shorthand for `sample`, `ingest`, `profile`, `validate`, `report` in sequence.

Override staging, run IDs, or dataset names by passing `DATASET=name SEED=99` before the `make` command, or run the scripts directly (e.g., `python scripts/ingest.py --dataset-name custom --seed 7 --force`). Use `scripts/get_run_id.py --stage-path data/staging/...` when you need the run identifier for downstream commands.
> **Python compatibility:** `scripts/ensure_python_version.py` can be executed manually to confirm your interpreter. PyArrow 18 publishes wheels for CPython 3.9–3.13 only, so if your global `python3` is 3.14+ install a supported release (e.g., `pyenv install 3.13.6`) and rerun `make setup` (prepend `PYTHON=python3.13` if needed).

## Artifacts & monitoring
- `reports/latest/index.html` – scoreboard consumed by GH Pages.
- `reports/latest/score.json` and `issues.csv` – backend payloads for the UI and the Streamlit artifact downloads or additional automation.
- `reports/runs/run_id=<id>/` – archived HTML scorecards, profile reports, and score payloads for historical comparisons.
- `data/marts/dq_check_results/run_id=<id>/` and `dq_issue_log/run_id=<id>/` – canonical Parquet tables containing every check detail and root-cause metadata.
- `data/marts/dq_issue_history/issue_history.parquet` – appended issue records powering recurrence, issue totals, and historical charts.
- `data/marts/score_history/score_history.parquet` plus per-run directories – timeline of overall scores and subscores (also used by the `scorecard` template). 
- `data/marts/dq_metrics_history` + `dq_anomalies`, `dq_schema_drift` – metrics for event volumes/completion rates and schema drift artifacts.

## Configuration & extension points
| artifact | role |
| --- | --- |
| `dq/config/mappings.yml` | normalizes states, grade bands, and district overrides during ingestion.
| `dq/config/schema.yml` | expected schema for schema-drift detection.
| `dq/config/rules.yml` | defines every `CheckRule`, default severity, weight, thresholds, and dimension (completeness, validity, etc.).
| `dq/config/root_causes.yml` | recommended fixes surfaced in issue logs and the UI.
| `dq/config/quality_gate.yml` | overrides the default score threshold (90) and critical severity (5).
| `reports/templates/scorecard.html.jinja` | where the scoreboard layout, CSS, charts, and issue tables live; mutate `scripts/publish_helpers.mutate_context` to feed new sections.

## Testing & experimentation
- Run `python scripts/regression.py` after tweaking rules, thresholds, or scoring to make sure golden runs still match expectations.
- Use `tools/generate_synthetic.py --dataset-name phase1 --seed <n> --force` to reproduce deterministic exports (they are also the uploads powering the Streamlit sample zip).
- Streamlit runs against in-memory staging directories, so artifacts stay isolated per session while still producing `issues.csv`, `exceptions.csv`, and cleaned Parquet downloads for end users.

## Next actions
- Tune rules or severity/weight combinations in `dq/config/rules.yml`, then rerun `make validate` to regenerate `score.json` and see the impact on the live scoreboard.
- Adjust `reports/templates/scorecard.html.jinja` and `scripts/publish_helpers/history.py` to highlight new metrics or charts before publishing.
- Keep `reports/runs/run_id=<id>` and `data/marts/*` under version control for human audits or compliance reporting.
