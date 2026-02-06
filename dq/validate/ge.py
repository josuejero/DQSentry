"""Great Expectations helpers for reporting."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from great_expectations import __version__ as ge_version
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchDefinition, BatchMarkers
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.id_dict import BatchSpec, IDDict
from great_expectations.core.run_identifier import RunIdentifier

from dq.validate.models import CheckResult
from dq.validate.paths import GE_ARTIFACTS_BASE


def build_expectation_suite(results: Iterable[CheckResult]) -> ExpectationSuite:
    expectations = [
        ExpectationConfiguration(
            expectation_type="dq_check",
            kwargs={
                "check_id": result.rule.id,
                "table": result.rule.table,
                "dimension": result.rule.dimension,
            },
        )
        for result in results
    ]
    return ExpectationSuite(expectation_suite_name="dq_checks", expectations=expectations)


def build_validation_result(
    suite: ExpectationSuite,
    results: Iterable[CheckResult],
    run_id: str,
    dataset_name: str,
    stage_path: Path,
    duckdb_path: Path,
    run_ts: datetime,
) -> ExpectationSuiteValidationResult:
    outcome: list[ExpectationValidationResult] = []
    for result in results:
        config = ExpectationConfiguration(
            expectation_type="dq_check",
            kwargs={
                "check_id": result.rule.id,
                "table": result.rule.table,
            },
        )
        outcome.append(
            ExpectationValidationResult(
                success=result.failure_count == 0,
                expectation_config=config,
                result={
                    "unexpected_count": result.failure_count,
                    "unexpected_percent": result.failure_rate * 100,
                },
                meta={
                    "issue_type": result.issue_type,
                    "samples": result.samples,
                },
            )
        )
    stats = {
        "evaluated_expectations": len(outcome),
        "success_percent": sum(1 for r in outcome if r.success) / len(outcome) * 100 if outcome else 100.0,
        "successful_expectations": sum(1 for r in outcome if r.success),
        "unsuccessful_expectations": sum(1 for r in outcome if not r.success),
    }
    run_identifier = RunIdentifier(run_name=run_id, run_time=run_ts)
    batch_definition = BatchDefinition(
        datasource_name="duckdb",
        data_connector_name="default_runtime_data_connector",
        data_asset_name=dataset_name,
        batch_identifiers=IDDict({}),
        batch_spec_passthrough={"path": str(stage_path)},
    )

    meta = {
        "active_batch_definition": batch_definition,
        "batch_markers": BatchMarkers({"ge_load_time": run_ts.isoformat()}),
        "batch_parameters": {"run_id": run_id},
        "batch_spec": BatchSpec({"path": str(duckdb_path)}),
        "checkpoint_id": None,
        "checkpoint_name": "dq_checkpoint",
        "expectation_suite_name": suite.name,
        "great_expectations_version": ge_version,
        "run_id": run_identifier,
        "validation_id": None,
        "validation_time": run_ts.isoformat(),
    }
    return ExpectationSuiteValidationResult(
        success=all(r.success for r in outcome),
        results=outcome,
        statistics=stats,
        meta=meta,
    )


def write_json(payload: dict[str, Any], target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    return target
