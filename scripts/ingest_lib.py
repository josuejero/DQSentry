"""Reusable helpers for ingesting raw exports into DuckDB staging artifacts."""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml
from dateutil import parser as date_parser

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_BASE = REPO_ROOT / "data" / "raw"
STAGING_BASE = REPO_ROOT / "data" / "staging"
MAPPINGS_FILE = REPO_ROOT / "dq" / "config" / "mappings.yml"

try:
    from scripts.ingest_tables import TABLE_SPECS
except ModuleNotFoundError:
    from ingest_tables import TABLE_SPECS


def sql_literal(value: str) -> str:
    return value.replace('"', '\\"').replace("'", "''")


def path_literal(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def load_mappings() -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    with MAPPINGS_FILE.open() as handle:
        raw = yaml.safe_load(handle)
    state_codes = raw.get("state_codes") or {}
    grade_bands = raw.get("grade_bands") or {}
    district_overrides = raw.get("district_overrides") or {}

    state_map: dict[str, str] = {}
    for code, name in state_codes.items():
        state_map[code.strip().lower()] = code
        state_map[name.strip().lower()] = code
    state_map.setdefault("ca", "CA")
    state_map.setdefault("cax", "CA")
    state_map.setdefault("nyc", "NY")
    state_map.setdefault("texas", "TX")

    grade_map: dict[str, str] = {}
    for canonical, payload in grade_bands.items():
        canonical_key = canonical.strip()
        grade_map[canonical_key.lower()] = canonical_key
        for synonym in payload.get("synonyms", []):
            grade_map[synonym.strip().lower()] = canonical_key

    override_map = {
        key.strip().lower(): value.strip() for key, value in district_overrides.items()
    }
    return state_map, grade_map, override_map


def build_case_expression(
    column: str, mapping: dict[str, str], fallback: str
) -> str:
    clauses = []
    for alias, canonical in mapping.items():
        alias_literal = sql_literal(alias)
        canonical_literal = sql_literal(canonical)
        clauses.append(
            f"WHEN lower(trim({column})) = '{alias_literal}' THEN '{canonical_literal}'"
        )
    if not clauses:
        return fallback
    clause_text = " ".join(clauses)
    return f"(CASE {clause_text} ELSE {fallback} END)"


def parse_timestamp(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = date_parser.parse(text)
    except (ValueError, OverflowError, date_parser.ParserError):
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def ingest_table(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    con.execute(sql)


def ingest_dataset(
    dataset_name: str, seed: int, force: bool = False
) -> dict[str, str]:
    raw_path = RAW_BASE / dataset_name / str(seed)
    if not raw_path.exists():
        raise SystemExit(f"Raw exports not found at {raw_path}")

    stage_path = STAGING_BASE / dataset_name / str(seed)
    if stage_path.exists():
        if force:
            shutil.rmtree(stage_path)
        else:
            raise SystemExit(f"Staging path {stage_path} already exists. Use --force to rebuild.")
    stage_path.mkdir(parents=True, exist_ok=True)

    state_map, grade_map, district_overrides = load_mappings()
    state_case_expr = build_case_expression("state", state_map, "upper(trim(state))")
    grade_case_expr = build_case_expression("grade_band", grade_map, "trim(grade_band)")
    district_case_expr = build_case_expression("district_name", district_overrides, "trim(district_name)")

    parquet_path = stage_path / "parquet"
    parquet_path.mkdir(exist_ok=True)
    db_path = stage_path / "staging.duckdb"
    con = duckdb.connect(str(db_path))
    con.create_function(
        "py_parse_ts", parse_timestamp, return_type=duckdb.sqltype("TIMESTAMP")
    )

    for spec in TABLE_SPECS:
        source_path = path_literal(raw_path / spec["source"])
        sql = f"""
        CREATE TABLE staging_{spec['name']} AS
        {spec['select'].format(
            state_case_expr=state_case_expr,
            grade_case_expr=grade_case_expr,
            district_case_expr=district_case_expr,
            source_path=source_path,
        )}
        """
        ingest_table(con, sql)

    for table in (spec["name"] for spec in TABLE_SPECS):
        dest = parquet_path / f"{table}.parquet"
        con.execute(f"COPY staging_{table} TO '{dest.as_posix()}' (FORMAT PARQUET)")

    raw_metadata_path = raw_path / "run_metadata.json"
    raw_metadata = {}
    if raw_metadata_path.exists():
        raw_metadata_text = raw_metadata_path.read_text()
        raw_metadata = json.loads(raw_metadata_text)
        (stage_path / "run_metadata.json").write_text(raw_metadata_text)

    ingest_metadata = {
        "dataset_name": dataset_name,
        "seed": seed,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "raw_metadata": raw_metadata,
        "duckdb_path": str(db_path),
        "parquet_path": str(parquet_path),
    }
    (stage_path / "ingest_metadata.json").write_text(json.dumps(ingest_metadata, indent=2))
    con.close()

    return {
        "stage_path": str(stage_path),
        "db_path": str(db_path),
        "parquet_path": str(parquet_path),
    }
