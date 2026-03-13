from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import yaml

from scraper_engine.core.models import EngineConfig


SUPPORTED_CONFIG_SUFFIXES = {".json", ".yaml", ".yml"}


def load_raw_config(config_path: str | Path) -> dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    if path.suffix.lower() not in SUPPORTED_CONFIG_SUFFIXES:
        raise ValueError(
            f"Unsupported config format: {path.suffix}. Use JSON or YAML."
        )

    raw_text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        payload = json.loads(raw_text)
    else:
        payload = yaml.safe_load(raw_text)

    if not isinstance(payload, dict):
        raise ValueError("Config file must contain a top-level object.")
    return normalize_config_payload(payload)


def load_config(config_path: str | Path) -> EngineConfig:
    return EngineConfig.from_dict(load_raw_config(config_path))


def normalize_config_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = copy.deepcopy(payload)
    normalized.setdefault("mode", normalized.get("crawl", {}).get("mode", "site_scan"))
    normalized.setdefault("requests", {})
    normalized.setdefault("crawl", {})
    normalized.setdefault("extraction", {})
    normalized.setdefault("output", {})
    normalized.setdefault("logging", {})
    normalized.setdefault("sync", {})
    normalized.setdefault("schema", {})
    normalized.setdefault("metadata", {})
    normalized.setdefault("static_targets", [])

    normalized["crawl"].setdefault("mode", normalized["mode"])
    normalized["output"].setdefault("root_dir", "outputs")
    normalized["output"].setdefault("merge_rows", normalized["mode"] == "site_scan")
    normalized["logging"].setdefault("file_name", "run.log")
    if "record_selector" in normalized and "record_selector" not in normalized["extraction"]:
        normalized["extraction"]["record_selector"] = normalized.pop("record_selector")
    if "fields" in normalized and "fields" not in normalized["extraction"]:
        normalized["extraction"]["fields"] = normalized.pop("fields")

    record_selector = normalized["extraction"].get("record_selector")
    if isinstance(record_selector, str):
        normalized["extraction"]["record_selector"] = {"css": record_selector}

    fields_payload = normalized["extraction"].get("fields", [])
    if isinstance(fields_payload, dict):
        normalized["extraction"]["fields"] = [
            _normalize_dict_field_payload(field_name, field_payload)
            for field_name, field_payload in fields_payload.items()
        ]

    fields = normalized["extraction"].get("fields", [])
    normalized["extraction"]["fields"] = [
        _normalize_field_payload(field_payload)
        for field_payload in fields
    ]
    return normalized


def _normalize_field_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    field_type = normalized.get("type")
    if field_type == "phone_numbers":
        normalized["type"] = "phones"
    if field_type == "email":
        normalized["type"] = "emails"
    if "css" in normalized and "selector" not in normalized:
        normalized["selector"] = normalized.pop("css")
    if "attr" in normalized and "attribute" not in normalized:
        normalized["attribute"] = normalized.pop("attr")
    return normalized


def _normalize_dict_field_payload(field_name: str, payload: Any) -> dict[str, Any]:
    if isinstance(payload, str):
        return {"name": field_name, "selector": payload}
    if not isinstance(payload, dict):
        raise ValueError(f"Field '{field_name}' must be a string or object.")
    normalized = dict(payload)
    normalized["name"] = field_name
    return normalized
