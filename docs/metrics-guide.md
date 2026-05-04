# DQSentry metrics guide

DQSentry records metrics across five areas:

1. Data-quality outcomes
2. Rule coverage
3. Issue explainability
4. Regression and testing
5. CI, reporting, and security hygiene

## Main public metrics

| Metric | Meaning | Source |
|---|---|---|
| Overall data quality score | Weighted score from validation checks | `reports/latest/score.json` |
| Automated checks | Number of configured validation checks | `dq/config/rules.yml` and check results |
| Quality gate | Pass/fail based on score threshold and critical failures | `scripts/quality_gate.py` |
| Critical failed checks | Failed checks at or above configured critical severity | `data/marts/dq_check_results` |
| Regression status | Whether golden dataset output matches expected output | `reports/latest/regression_metrics.json` |
| Coverage | Test coverage for project code | `reports/latest/coverage_summary.json` |
| Root-cause coverage | Share of issues with probable cause and recommended fix | `reports/latest/issues.csv` |
| Issue lifecycle | New, open, recurring, and not-seen issue counts | `data/marts/dq_issue_lifecycle` |
| Published artifacts | Generated report and audit files from the latest run | `reports/latest/artifact_manifest.json` |

## How to regenerate metrics locally

```bash
make setup
make run
make coverage
python scripts/regression.py
python scripts/security_summary.py
python scripts/collect_project_metrics.py --write-history
```

## Where to inspect results

- Human-readable summary: `reports/latest/employer_metrics.md`
- Machine-readable summary: `reports/latest/project_metrics.json`
- Scorecard: `reports/latest/index.html`
- Artifact manifest: `reports/latest/artifact_manifest.json`
- Historical metrics: `data/marts/project_metrics_history/project_metrics.parquet`
