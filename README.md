# DQSentry: Phase 0 — Design, Contracts, Scoring

## What this proves
- The team can codify a full data quality playbook (schema expectations, validation rules, scoring, alerts) without writing any ad-hoc scripts.
- The foundation is repeatable: every run is reproducible, generates a `run_id`, and outputs portable Parquet artifacts for auditing.
- Quality decisions are explainable: checks, thresholds, severities, and weights live in YAML so non-engineers can adjust them and anyone can understand how a score was derived.

## Project goals for Phase 0
1. **Establish single source-of-truth configs** for schema contracts (columns/types/keys) and validation rules grouped by soundness dimensions.
2. **Document the scoring model** so it can be explained in under two minutes and updated without touching Python.
3. **Plan the delivery surface**: GitHub Pages for the public scorecard and Streamlit Community Cloud for the CSV upload validator.

## Architecture highlights
- **Storage/engine:** DuckDB reads CSV exports, writes Parquet to `data/staging/`, and keeps audit marts in `data/marts/`.
- **Validation:** Great Expectations suites live under `dq/ge/` and are driven by the YAML rules in `dq/config/`.
- **Orchestration:** Python scripts in `scripts/` (ingest, profile, validate, score, publish, run_all) plus `Makefile` and GitHub Actions under `.github/workflows/` will be wired in later phases.
- **Publishing:** Static HTML scorecards land in `reports/latest/` and per-run artifacts in `reports/runs/` for historical tracking.

## Demo links (Phase 0 placeholders)
- Public scorecard: GitHub Pages (e.g., `https://<org>.github.io/DQSentry/latest-scorecard/`).
- Interactive validator: Streamlit Community Cloud app (CSV upload → score + downloads).

## Scoring model (explainable and stable)
1. Each validation check returns a `failure_rate` between 0 (clean) and 1 (all rows fail).
2. Each check defines:
   - A **severity** (1=notice, 5=critical).
   - A **weight** to reflect relative importance when computing penalties.
3. **Penalty per check**: `failure_rate * (severity / 5) * weight` so every penalty already sits in `[0, weight]` and scales with severity.
4. **Aggregate penalty**: sum all check penalties, normalize by the total possible weight (sum of all `weight` values across active checks).
5. **Overall score**: `max(0, 100 - 100 * normalized_penalty_sum)`; baseline is 100 when there are zero failures.
6. **Sub-scores**: repeat the same calculation within each dimension (completeness, validity, consistency, uniqueness, integrity) by summing only the checks that belong there.

## How to run (future work)
- `scripts/ingest.py` will copy `data/raw/*` into typed Parquet under `data/staging/`.
- `scripts/validate.py` will run Great Expectations suites backed by `dq/config/rules.yml` and write failure rates to `data/marts/`.
- `scripts/score.py` will apply the scoring formula above and emit human-friendly scorecards and issue logs in `reports/latest/` plus run-specific folders under `reports/runs/`.
- GitHub Actions `dq_push.yml` and `dq_scheduled.yml` will orchestrate CI and scheduled audits.

## Run expectations
- Never mutate files in `data/raw/`; all cleansed outputs land in `data/staging/`.
- Each run captures a `run_id` and timestamp, stored alongside generated artifacts.
- Detection is separate from remediation: the toolkit flags issues but explicit cleaning steps log fixes to `data/staging/` or a dedicated `marts/fixes/` table.

