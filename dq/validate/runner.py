"""Validation runner that combines DuckDB SQL with GE artifacts and reports."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import duckdb

from dq.validate.ge import (
    build_expectation_suite,
    build_validation_result,
    write_json,
)
from dq.validate.metadata import collect_stage_metadata
from dq.validate.models import ValidationSummary
from dq.validate.output import (
    append_issue_history,
    append_run_history,
    build_check_results,
    build_issue_log,
    compute_recurrence_metrics,
    persist_dataframe,
    persist_recurrence_summary,
)
from dq.validate.paths import GE_ARTIFACTS_BASE
from dq.validate.rule_executor import RuleEvaluator
from dq.validate.scoring import calculate_scores
from dq.validate.config import load_rules

RULES_PATH = Path(__file__).resolve().parents[2] / "dq" / "config" / "rules.yml"


class ValidationRunner:
    def __init__(self, dataset_name: str, run_id: str, stage_path: Path, duckdb_path: Path) -> None:
        self.dataset_name = dataset_name
        self.run_id = run_id
        self.stage_path = stage_path
        self.duckdb_path = duckdb_path
        self.run_ts = datetime.now(timezone.utc)
        self.rules, self.baseline, self.score_min = load_rules(RULES_PATH)

    def run(self) -> ValidationSummary:
        with duckdb.connect(str(self.duckdb_path)) as con:
            metadata = collect_stage_metadata(con)
            evaluator = RuleEvaluator(metadata)
            results = [evaluator.evaluate(con, rule) for rule in self.rules]

        check_df = build_check_results(results, self.run_id, self.dataset_name)
        issue_df = build_issue_log(results, self.run_id, self.dataset_name, self.run_ts)
        score, subscores = calculate_scores(results, self.baseline, self.score_min)

        check_path = persist_dataframe(check_df, self.run_id, "dq_check_results")
        issue_path = persist_dataframe(issue_df, self.run_id, "dq_issue_log")
        run_history_path = append_run_history(
            self.run_id, self.run_ts, self.dataset_name, metadata
        )
        issue_history_path = append_issue_history(issue_df)
        recurrence_path = persist_recurrence_summary(
            compute_recurrence_metrics(), self.run_id
        )

        suite = build_expectation_suite(results)
        suite_path = write_json(
            suite.to_json_dict(),
            GE_ARTIFACTS_BASE / "expectations" / f"{suite.name}.json",
        )
        validation = build_validation_result(
            suite,
            results,
            self.run_id,
            self.dataset_name,
            self.stage_path,
            self.duckdb_path,
            self.run_ts,
        )
        validation_path = write_json(
            validation.to_json_dict(),
            GE_ARTIFACTS_BASE / "validations" / f"{self.run_id}--{suite.name}.json",
        )

        return ValidationSummary(
            run_id=self.run_id,
            run_ts=self.run_ts,
            dataset_name=self.dataset_name,
            score=score,
            subscores=subscores,
            check_results_path=check_path,
            issue_log_path=issue_path,
            expectation_suite_path=suite_path,
            validation_result_path=validation_path,
            run_history_path=run_history_path,
            issue_history_path=issue_history_path,
            recurrence_summary_path=recurrence_path,
        )
