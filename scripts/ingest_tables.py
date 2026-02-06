"""Store shared table definitions used by the ingest pipeline."""

TABLE_SPECS = [
    {
        "name": "districts",
        "source": "districts.csv",
        "select": """
        SELECT
          NULLIF(trim(district_id), '') AS district_id,
          NULLIF({district_case_expr}, '') AS district_name,
          NULLIF({state_case_expr}, '') AS state
        FROM read_csv_auto('{source_path}')""",
    },
    {
        "name": "users",
        "source": "users.csv",
        "select": """
        SELECT
          NULLIF(trim(user_id), '') AS user_id,
          NULLIF(lower(trim(email)), '') AS email,
          NULLIF(trim(org_id), '') AS org_id,
          NULLIF(lower(trim(role)), '') AS role,
          NULLIF({state_case_expr}, '') AS state,
          NULLIF(trim(district_id), '') AS district_id
        FROM read_csv_auto('{source_path}')""",
    },
    {
        "name": "resources",
        "source": "resources.csv",
        "select": """
        SELECT
          NULLIF(trim(resource_id), '') AS resource_id,
          NULLIF(lower(trim(type)), '') AS type,
          NULLIF(trim(subject), '') AS subject,
          NULLIF({grade_case_expr}, '') AS grade_band
        FROM read_csv_auto('{source_path}')""",
    },
    {
        "name": "events",
        "source": "events.csv",
        "select": """
        SELECT
          NULLIF(trim(event_id), '') AS event_id,
          NULLIF(trim(user_id), '') AS user_id,
          NULLIF(trim(resource_id), '') AS resource_id,
          NULLIF(lower(trim(event_type)), '') AS event_type,
          py_parse_ts(event_ts) AS event_ts
        FROM read_csv_auto('{source_path}')""",
    },
    {
        "name": "newsletter",
        "source": "newsletter.csv",
        "select": """
        SELECT
          NULLIF(lower(trim(email)), '') AS email,
          py_parse_ts(subscribed_at) AS subscribed_at,
          py_parse_ts(opened_at) AS opened_at,
          py_parse_ts(clicked_at) AS clicked_at
        FROM read_csv_auto('{source_path}')""",
    },
]
