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
    normalized.setdefault("normalization", {})
    normalized.setdefault("pagination", {})
    normalized.setdefault("output", {})
    normalized.setdefault("limits", {})
    normalized.setdefault("logging", {})
    normalized.setdefault("sync", {})
    normalized.setdefault("schema", {})
    normalized.setdefault("metadata", {})
    normalized.setdefault("static_targets", [])

    normalized["crawl"].setdefault("mode", normalized["mode"])
    normalized["output"].setdefault("root_dir", "outputs")
    normalized["output"].setdefault("merge_rows", normalized["mode"] == "site_scan")
    normalized["logging"].setdefault("file_name", "run.log")
    normalized["output"].setdefault("shaping", {})
    normalized["limits"].setdefault("max_records", None)
    normalized["limits"].setdefault("max_detail_pages", None)
    if "record_selector" in normalized and "record_selector" not in normalized["extraction"]:
        normalized["extraction"]["record_selector"] = normalized.pop("record_selector")
    if "fields" in normalized and "fields" not in normalized["extraction"]:
        normalized["extraction"]["fields"] = normalized.pop("fields")
    if "detail_page" in normalized and "detail_page" not in normalized["extraction"]:
        normalized["extraction"]["detail_page"] = normalized.pop("detail_page")
    if "shaping" in normalized:
        top_level_shaping = normalized.pop("shaping") or {}
        merged_shaping = dict(top_level_shaping)
        merged_shaping.update(normalized["output"].get("shaping", {}))
        normalized["output"]["shaping"] = merged_shaping

    record_selector = normalized["extraction"].get("record_selector")
    if isinstance(record_selector, str):
        normalized["extraction"]["record_selector"] = {"css": record_selector}

    detail_page = normalized["extraction"].get("detail_page")
    if detail_page is not None:
        detail_page.setdefault("enabled", True)
        detail_page.setdefault("fields", [])

    pagination = normalized.get("pagination", {})
    pagination.setdefault("enabled", False)
    pagination.setdefault("max_pages", 1)
    next_page = pagination.get("next_page")
    if isinstance(next_page, str):
        pagination["next_page"] = {"css": next_page, "attribute": "href"}
    elif isinstance(next_page, dict):
        next_page = dict(next_page)
        if "css" not in next_page and "selector" in next_page:
            next_page["css"] = next_page.pop("selector")
        if "attribute" not in next_page and "attr" in next_page:
            next_page["attribute"] = next_page.pop("attr")
        next_page.setdefault("attribute", "href")
        pagination["next_page"] = next_page

    shaping = normalized["output"].get("shaping", {})
    shaping.setdefault("include_fields", [])
    shaping.setdefault("exclude_fields", [])
    shaping.setdefault("field_order", [])
    shaping.setdefault("flatten_single_item_lists", False)
    shaping.setdefault("flatten_fields", [])
    shaping.setdefault("join_fields", {})
    shaping.setdefault("drop_empty_fields", False)
    shaping.setdefault("cleanup_common_fields", False)
    if isinstance(shaping.get("join_fields"), list):
        shaping["join_fields"] = {
            field_name: "; "
            for field_name in shaping["join_fields"]
        }
    elif isinstance(shaping.get("join_fields"), dict):
        shaping["join_fields"] = {
            field_name: (delimiter if delimiter is not None else "; ")
            for field_name, delimiter in shaping["join_fields"].items()
        }

    for limit_key in ("max_records", "max_detail_pages"):
        limit_value = normalized["limits"].get(limit_key)
        if limit_value is not None and int(limit_value) <= 0:
            normalized["limits"][limit_key] = None

    normalized["extraction"]["fields"] = _normalize_fields_collection(
        normalized["extraction"].get("fields", [])
    )
    if detail_page is not None:
        detail_page["fields"] = _normalize_fields_collection(detail_page.get("fields", []))
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
    normalized["transforms"] = _normalize_transforms_collection(
        normalized.get("transforms", [])
    )
    return normalized


def _normalize_dict_field_payload(field_name: str, payload: Any) -> dict[str, Any]:
    if isinstance(payload, str):
        return {"name": field_name, "selector": payload}
    if not isinstance(payload, dict):
        raise ValueError(f"Field '{field_name}' must be a string or object.")
    normalized = dict(payload)
    normalized["name"] = field_name
    return normalized


def _normalize_fields_collection(fields_payload: Any) -> list[dict[str, Any]]:
    if isinstance(fields_payload, dict):
        fields_payload = [
            _normalize_dict_field_payload(field_name, field_payload)
            for field_name, field_payload in fields_payload.items()
        ]
    return [_normalize_field_payload(field_payload) for field_payload in fields_payload]


def _normalize_transforms_collection(transforms_payload: Any) -> list[dict[str, Any]]:
    if transforms_payload is None:
        return []
    normalized_transforms: list[dict[str, Any]] = []
    for transform in transforms_payload:
        if isinstance(transform, str):
            normalized_transforms.append({"name": transform})
            continue
        if not isinstance(transform, dict):
            raise ValueError("Transforms must be strings or objects.")
        normalized = dict(transform)
        if "name" not in normalized:
            if "type" in normalized:
                normalized["name"] = normalized.pop("type")
            elif "transform" in normalized:
                normalized["name"] = normalized.pop("transform")
            else:
                raise ValueError("Transform objects must include a name.")
        normalized_transforms.append(normalized)
    return normalized_transforms
