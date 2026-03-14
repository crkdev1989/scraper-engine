from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from scraper_engine.crawl.url_utils import canonical_domain, normalize_url
from scraper_engine.outputs.writers import write_csv
from scraper_engine.utils.text_utils import dedupe_preserve_order

DEFAULT_OUTPUT_FILE_NAME = "cleaned_results.csv"
DEFAULT_SORT_FIELDS = ["firm", "email"]
DEFAULT_JUNK_EMAIL_SUBSTRINGS = [
    "sentry.io",
    "wixpress.com",
    "ingest.sentry",
    "@example.com",
    "johndoe",
]
DEFAULT_URL_FIELDS = {
    "website",
    "url",
    "contact_page_url",
    "profile_url",
    "source_directory",
    "input_url",
}
COMMON_SECOND_LEVEL_SUFFIXES = {
    "ac",
    "co",
    "com",
    "edu",
    "gov",
    "net",
    "org",
}
COMMON_COUNTRY_SUFFIXES = {
    "au",
    "jp",
    "nz",
    "uk",
}
PHONE_SPLIT_PATTERN = re.compile(r"[;,\n/|]+")
NON_DIGIT_PATTERN = re.compile(r"\D+")
COMPANY_CLEAN_PATTERN = re.compile(r"[^a-z0-9]+")


def clean_lead_rows(
    rows: list[dict[str, Any]],
    settings: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    normalized_settings = _normalize_settings(settings)
    prepared_rows: list[dict[str, Any]] = []
    stats = {
        "total_rows": len(rows),
        "filtered_missing_required": 0,
        "filtered_junk_email": 0,
        "duplicates_removed": 0,
        "written": 0,
    }

    for index, row in enumerate(rows):
        normalized_row = _normalize_row_strings(row)
        if _is_missing_required_field(normalized_row, normalized_settings["required_fields"]):
            stats["filtered_missing_required"] += 1
            continue

        email = _first_value(normalized_row, ("email",))
        normalized_email = _normalize_email(email)
        if normalized_email and _is_junk_email(
            normalized_email, normalized_settings["junk_email_substrings"]
        ):
            stats["filtered_junk_email"] += 1
            continue

        projected_row = _project_row(normalized_row, normalized_settings)
        dedupe_keys = _build_dedupe_keys(normalized_row, projected_row)
        score = _score_row(projected_row)
        prepared_rows.append(
            {
                "index": index,
                "row": projected_row,
                "dedupe_keys": dedupe_keys,
                "score": score,
            }
        )

    groups = _group_duplicate_candidates(prepared_rows)
    cleaned_rows: list[dict[str, Any]] = []
    for group_rows in groups:
        best = max(group_rows, key=lambda item: (item["score"], -item["index"]))
        cleaned_rows.append(best["row"])
        stats["duplicates_removed"] += max(0, len(group_rows) - 1)

    cleaned_rows.sort(
        key=lambda row: tuple(
            str(row.get(field, "") or "").casefold()
            for field in normalized_settings["sort_fields"]
        )
    )
    stats["written"] = len(cleaned_rows)
    return cleaned_rows, normalized_settings["output_fields"], stats


def write_cleaned_lead_csv(
    output_path: Path,
    rows: list[dict[str, Any]],
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cleaned_rows, columns, stats = clean_lead_rows(rows, settings)
    write_csv(output_path, cleaned_rows, columns)
    return {
        "path": str(output_path),
        "file_name": output_path.name,
        **stats,
    }


def cleaned_output_file_name(settings: dict[str, Any] | None = None) -> str:
    normalized_settings = _normalize_settings(settings)
    return normalized_settings["file_name"]


def _normalize_settings(settings: dict[str, Any] | None) -> dict[str, Any]:
    payload = settings if isinstance(settings, dict) else {}
    rename_fields = payload.get("rename_fields", {})
    include_fields = payload.get("include_fields", [])
    required_fields = payload.get("required_fields", [])
    sort_fields = payload.get("sort_fields", DEFAULT_SORT_FIELDS)
    junk_email_substrings = payload.get(
        "junk_email_substrings", DEFAULT_JUNK_EMAIL_SUBSTRINGS
    )

    if not isinstance(rename_fields, dict):
        raise ValueError("metadata.lead_cleaning.rename_fields must be an object when provided.")
    if not isinstance(include_fields, list):
        raise ValueError("metadata.lead_cleaning.include_fields must be a list when provided.")
    if not isinstance(required_fields, list):
        raise ValueError("metadata.lead_cleaning.required_fields must be a list when provided.")
    if not isinstance(sort_fields, list):
        raise ValueError("metadata.lead_cleaning.sort_fields must be a list when provided.")
    if not isinstance(junk_email_substrings, list):
        raise ValueError(
            "metadata.lead_cleaning.junk_email_substrings must be a list when provided."
        )

    return {
        "file_name": str(payload.get("file_name") or DEFAULT_OUTPUT_FILE_NAME).strip()
        or DEFAULT_OUTPUT_FILE_NAME,
        "rename_fields": {
            str(source): str(target)
            for source, target in rename_fields.items()
            if str(source).strip() and str(target).strip()
        },
        "output_fields": [str(field).strip() for field in include_fields if str(field).strip()],
        "required_fields": [
            str(field).strip() for field in required_fields if str(field).strip()
        ],
        "sort_fields": [str(field).strip() for field in sort_fields if str(field).strip()]
        or list(DEFAULT_SORT_FIELDS),
        "junk_email_substrings": [
            str(value).strip().lower()
            for value in junk_email_substrings
            if str(value).strip()
        ],
    }


def _normalize_row_strings(row: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, str):
            normalized[key] = value.strip()
            continue
        if isinstance(value, list):
            normalized[key] = [
                item.strip() if isinstance(item, str) else item
                for item in value
            ]
            continue
        normalized[key] = value
    return normalized


def _is_missing_required_field(row: dict[str, Any], required_fields: list[str]) -> bool:
    for field_name in required_fields:
        value = row.get(field_name)
        if _is_empty(value):
            return True
    return False


def _project_row(row: dict[str, Any], settings: dict[str, Any]) -> dict[str, Any]:
    rename_fields = settings["rename_fields"]
    projected: dict[str, Any] = {}
    for key, value in row.items():
        target_key = rename_fields.get(key, key)
        projected[target_key] = _normalize_output_value(target_key, value)

    if settings["output_fields"]:
        return {
            field_name: projected.get(field_name, "")
            for field_name in settings["output_fields"]
        }
    return projected


def _normalize_output_value(field_name: str, value: Any) -> Any:
    if isinstance(value, str):
        cleaned = value.strip()
        if field_name == "email":
            return _normalize_email(cleaned)
        if field_name in DEFAULT_URL_FIELDS:
            return _normalize_url_value(cleaned)
        return cleaned
    if isinstance(value, list):
        cleaned_items = [
            _normalize_output_value(field_name, item)
            for item in value
            if not _is_empty(item)
        ]
        if all(isinstance(item, str) for item in cleaned_items):
            return dedupe_preserve_order(cleaned_items)
        return cleaned_items
    return value


def _build_dedupe_keys(
    source_row: dict[str, Any],
    projected_row: dict[str, Any],
) -> list[tuple[str, str]]:
    email = _normalize_email(_first_value(source_row, ("email", "contact_email")))
    if email:
        return [("email", email)]

    domain = _normalize_domain(_first_value(source_row, ("website", "url", "input_url")))
    if domain:
        return [("domain", domain)]

    phones = _normalize_phone_values(
        [
            source_row.get("contact_phone"),
            source_row.get("phone"),
            source_row.get("phones"),
        ]
    )
    if phones:
        return [("phone", phone) for phone in phones]

    company = _normalize_company_name(
        _first_value(
            projected_row,
            ("firm", "law_firm", "company", "name"),
        )
    )
    if company:
        return [("company", company)]

    return []


def _group_duplicate_candidates(
    prepared_rows: list[dict[str, Any]],
) -> list[list[dict[str, Any]]]:
    if not prepared_rows:
        return []

    parent = list(range(len(prepared_rows)))
    first_seen_by_key: dict[tuple[str, str], int] = {}

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for index, prepared in enumerate(prepared_rows):
        for dedupe_key in prepared["dedupe_keys"]:
            first_seen = first_seen_by_key.get(dedupe_key)
            if first_seen is None:
                first_seen_by_key[dedupe_key] = index
                continue
            union(first_seen, index)

    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for index, prepared in enumerate(prepared_rows):
        grouped[find(index)].append(prepared)
    return list(grouped.values())


def _first_value(row: dict[str, Any], field_names: Iterable[str]) -> str:
    for field_name in field_names:
        value = row.get(field_name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _normalize_email(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lower()


def _is_junk_email(email: str, junk_email_substrings: list[str]) -> bool:
    return any(token in email for token in junk_email_substrings)


def _normalize_url_value(value: str) -> str:
    if not value:
        return ""
    try:
        return normalize_url(value)
    except ValueError:
        return value.strip().lower()


def _normalize_domain(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        return ""
    try:
        hostname = canonical_domain(normalize_url(value))
    except ValueError:
        return ""
    return _root_domain(hostname)


def _root_domain(hostname: str) -> str:
    if not hostname:
        return ""

    labels = [label for label in hostname.split(".") if label]
    if len(labels) <= 2:
        return ".".join(labels)

    if (
        labels[-1] in COMMON_COUNTRY_SUFFIXES
        and labels[-2] in COMMON_SECOND_LEVEL_SUFFIXES
        and len(labels) >= 3
    ):
        return ".".join(labels[-3:])

    return ".".join(labels[-2:])


def _normalize_phone_values(values: list[Any]) -> list[str]:
    normalized_numbers: list[str] = []
    for value in values:
        if isinstance(value, list):
            normalized_numbers.extend(_normalize_phone_values(value))
            continue
        if not isinstance(value, str) or not value.strip():
            continue
        for candidate in PHONE_SPLIT_PATTERN.split(value):
            digits = NON_DIGIT_PATTERN.sub("", candidate)
            if len(digits) == 11 and digits.startswith("1"):
                digits = digits[1:]
            if 7 <= len(digits) <= 15:
                normalized_numbers.append(digits)
    return dedupe_preserve_order(normalized_numbers)


def _normalize_company_name(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    lowered = value.strip().lower()
    if not lowered:
        return ""
    normalized = COMPANY_CLEAN_PATTERN.sub(" ", lowered)
    return " ".join(normalized.split())


def _score_row(row: dict[str, Any]) -> tuple[int, int, int]:
    important_fields = (
        "email",
        "url",
        "website",
        "firm",
        "law_firm",
        "company",
        "name",
        "contact_page_url",
        "profile_url",
        "source_directory",
        "contact_phone",
        "phone",
        "phones",
    )
    important_count = sum(1 for field in important_fields if not _is_empty(row.get(field)))
    total_count = sum(1 for value in row.values() if not _is_empty(value))
    total_length = sum(len(str(value).strip()) for value in row.values() if not _is_empty(value))
    return (important_count, total_count, total_length)


def _is_empty(value: Any) -> bool:
    return value in (None, "", [])
