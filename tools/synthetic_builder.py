"""Helpers for building each table of the synthetic exports."""

import random
import uuid

NS = uuid.UUID("3c2f0d46-5df0-4c88-9a5d-dcbf6f3d43e9")

try:
    from tools.synthetic_templates import (
        DISTRICT_TEMPLATES,
        EVENT_TEMPLATES,
        NEWSLETTER_TEMPLATES,
        RESOURCE_TEMPLATES,
        USER_TEMPLATES,
    )
except ModuleNotFoundError:
    from synthetic_templates import (
        DISTRICT_TEMPLATES,
        EVENT_TEMPLATES,
        NEWSLETTER_TEMPLATES,
        RESOURCE_TEMPLATES,
        USER_TEMPLATES,
    )


def dataset_offset(dataset_name: str) -> int:
    return sum(ord(ch) for ch in (dataset_name or ""))


def stable_id(kind: str, identifier: str, dataset_name: str, seed: int) -> str:
    key = f"{dataset_name}-{seed}-{kind}-{identifier}"
    return str(uuid.uuid5(NS, key))


def shuffle_rows(rows: list[dict], dataset_name: str, seed: int, offset: int) -> list[dict]:
    rng = random.Random(seed + dataset_offset(dataset_name) + offset)
    shuffled = list(rows)
    rng.shuffle(shuffled)
    return shuffled


def build_districts(dataset_name: str, seed: int) -> list[dict]:
    rows = [
        {
            "district_id": stable_id("district", template["key"], dataset_name, seed),
            "district_name": template["name"],
            "state": template["state"],
        }
        for template in DISTRICT_TEMPLATES
    ]
    return shuffle_rows(rows, dataset_name, seed, offset=10)


def build_users(
    dataset_name: str, seed: int, district_ids: dict[str, str]
) -> list[dict]:
    org_names = {
        "Mastery Labs": "org-mastery",
        "NYC Elevate": "org-nyc",
        "Northside Alliance": "org-northside",
        "Sunrise Initiative": "org-sunrise",
    }
    rows = []
    for template in USER_TEMPLATES:
        org_id = stable_id("org", org_names[template["org"]], dataset_name, seed)
        user_id = stable_id("user", template["key"], dataset_name, seed)
        district_key = template["district"]
        district_id = district_ids.get(district_key) if district_key else ""
        rows.append(
            {
                "user_id": user_id,
                "email": template["email"],
                "org_id": org_id,
                "role": template["role"],
                "state": template["state"],
                "district_id": district_id,
            }
        )
    return shuffle_rows(rows, dataset_name, seed, offset=20)


def build_resources(dataset_name: str, seed: int) -> list[dict]:
    rows = []
    for template in RESOURCE_TEMPLATES:
        resource_id = stable_id("resource", template["key"], dataset_name, seed)
        rows.append(
            {
                "resource_id": resource_id,
                "type": template["type"],
                "subject": template["subject"],
                "grade_band": template["grade_band"],
            }
        )
    return shuffle_rows(rows, dataset_name, seed, offset=30)


def build_events(
    dataset_name: str,
    seed: int,
    user_ids: dict[str, str],
    resource_ids: dict[str, str],
) -> list[dict]:
    rows = []
    for template in EVENT_TEMPLATES:
        event_id = stable_id("event", template["key"], dataset_name, seed)
        rows.append(
            {
                "event_id": event_id,
                "user_id": user_ids[template["user"]],
                "resource_id": resource_ids.get(
                    template["resource"],
                    stable_id("resource", template["resource"], dataset_name, seed),
                ),
                "event_type": template["event_type"],
                "event_ts": template["event_ts"],
            }
        )
    return shuffle_rows(rows, dataset_name, seed, offset=40)


def build_newsletter(dataset_name: str, seed: int) -> list[dict]:
    return shuffle_rows(
        [
            {
                "email": template["email"],
                "subscribed_at": template["subscribed_at"],
                "opened_at": template["opened_at"],
                "clicked_at": template["clicked_at"],
            }
            for template in NEWSLETTER_TEMPLATES
        ],
        dataset_name,
        seed,
        offset=50,
    )
