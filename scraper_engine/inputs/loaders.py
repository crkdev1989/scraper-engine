from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import yaml

from scraper_engine.crawl.url_utils import normalize_url
from scraper_engine.core.models import EngineConfig, RuntimeOptions
from scraper_engine.utils.text_utils import dedupe_preserve_order


def load_targets(options: RuntimeOptions, config: EngineConfig) -> list[str]:
    targets: list[str] = []
    if options.single_url:
        targets.append(normalize_url(options.single_url))
    if options.input_path:
        targets.extend(_load_targets_from_file(options.input_path, config))
    if not targets:
        targets.extend(normalize_url(target) for target in config.static_targets)
    return dedupe_preserve_order(targets)


def load_target_contexts(
    options: RuntimeOptions,
    config: EngineConfig,
) -> dict[str, list[dict[str, Any]]]:
    if not options.input_path or options.input_path.suffix.lower() != ".csv":
        return {}

    input_csv_settings = _get_input_csv_settings(config)
    preserve_columns = input_csv_settings.get("preserve_columns")
    if preserve_columns is not None and not isinstance(preserve_columns, list):
        raise ValueError("metadata.input_csv.preserve_columns must be a list when provided.")

    rows = _load_csv_rows(options.input_path)
    url_column = _resolve_csv_url_column(rows, input_csv_settings)
    contexts: dict[str, list[dict[str, Any]]] = {}

    for row in rows:
        raw_url = (row.get(url_column) or "").strip()
        if not raw_url:
            continue
        target_url = normalize_url(raw_url)
        if preserve_columns:
            context = {
                column: row.get(column)
                for column in preserve_columns
                if column in row
            }
        else:
            context = {
                key: value
                for key, value in row.items()
                if key != url_column
            }
        contexts.setdefault(target_url, []).append(context)
    return contexts


def _load_targets_from_file(path: Path, config: EngineConfig) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".txt":
        return [
            normalize_url(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
    if suffix == ".csv":
        rows = _load_csv_rows(path)
        url_column = _resolve_csv_url_column(rows, _get_input_csv_settings(config))
        return [
            normalize_url((row.get(url_column) or "").strip())
            for row in rows
            if (row.get(url_column) or "").strip()
        ]
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
    elif suffix in {".yaml", ".yml"}:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    else:
        raise ValueError(f"Unsupported input format: {suffix}")

    if isinstance(payload, list):
        return [normalize_url(str(item)) for item in payload]
    if isinstance(payload, dict) and isinstance(payload.get("urls"), list):
        return [normalize_url(str(item)) for item in payload["urls"]]
    raise ValueError("Structured input files must contain a list or a 'urls' field.")


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def _resolve_csv_url_column(
    rows: list[dict[str, str]],
    input_csv_settings: dict[str, Any],
) -> str:
    requested_url_column = str(input_csv_settings.get("url_column") or "").strip()
    if requested_url_column:
        if rows and requested_url_column not in rows[0]:
            raise ValueError(
                f"Configured CSV URL column '{requested_url_column}' was not found in the input file."
            )
        return requested_url_column

    fallback_columns = ("url", "website")
    if not rows:
        return "url"
    available_columns = set(rows[0].keys())
    for column in fallback_columns:
        if column in available_columns:
            return column
    raise ValueError(
        "CSV input must contain a 'url' column, a 'website' column, or "
        "metadata.input_csv.url_column must be configured."
    )


def _get_input_csv_settings(config: EngineConfig) -> dict[str, Any]:
    settings = config.metadata.get("input_csv", {})
    return settings if isinstance(settings, dict) else {}
