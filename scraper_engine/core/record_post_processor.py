from __future__ import annotations

import copy
import logging
import re
from typing import Any

from scraper_engine.core.models import ExtractorFieldConfig, NormalizationConfig, OutputShapingConfig
from scraper_engine.utils.logging_utils import log_event
from scraper_engine.utils.text_utils import dedupe_preserve_order


class RecordPostProcessor:
    def __init__(self, logger=None) -> None:
        self.logger = logger

    def process_record(
        self,
        record: dict[str, Any],
        fields: list[ExtractorFieldConfig],
        normalization: NormalizationConfig,
    ) -> dict[str, Any]:
        processed = dict(record)
        field_map = {field.name: field for field in fields}

        for field_name, field in field_map.items():
            processed[field_name] = self._process_field_value(
                value=processed.get(field_name),
                field=field,
                normalization=normalization,
            )

        return processed

    def shape_record(
        self,
        record: dict[str, Any],
        shaping: OutputShapingConfig,
    ) -> dict[str, Any]:
        cleaned_items: list[tuple[str, Any]] = []
        for field_name, value in record.items():
            cleaned_value = self._cleanup_output_value(field_name, value, shaping)
            if shaping.drop_empty_fields and self._is_empty(cleaned_value):
                continue
            cleaned_items.append((field_name, cleaned_value))

        filtered_items = self._apply_field_filters(cleaned_items, shaping)
        ordered_items = self._apply_field_order(filtered_items, shaping)
        return dict(ordered_items)

    def _process_field_value(
        self,
        value: Any,
        field: ExtractorFieldConfig,
        normalization: NormalizationConfig,
    ) -> Any:
        if isinstance(value, list):
            processed_items = [
                self._process_scalar_value(item, field, normalization)
                for item in value
            ]
            normalized_items = [
                item
                for item in processed_items
                if item not in (None, "", [])
            ]
            if not normalized_items:
                return self._fallback_value(field)
            if all(isinstance(item, str) for item in normalized_items):
                return dedupe_preserve_order(normalized_items)
            return normalized_items

        processed_value = self._process_scalar_value(value, field, normalization)
        if processed_value in (None, "", []):
            return self._fallback_value(field)
        return processed_value

    def _process_scalar_value(
        self,
        value: Any,
        field: ExtractorFieldConfig,
        normalization: NormalizationConfig,
    ) -> Any:
        processed = self._normalize_empty_value(value, normalization)
        for transform in field.transforms:
            processed = self._apply_transform(
                processed,
                transform,
                field_name=field.name,
                normalization=normalization,
            )
            processed = self._normalize_empty_value(processed, normalization)
        return processed

    def _normalize_empty_value(
        self,
        value: Any,
        normalization: NormalizationConfig,
    ) -> Any:
        if value is None:
            return None
        if isinstance(value, list):
            normalized_items = [
                self._normalize_empty_value(item, normalization)
                for item in value
            ]
            normalized_items = [item for item in normalized_items if item is not None]
            return normalized_items or None
        if isinstance(value, str):
            stripped = value.strip()
            if stripped == "":
                return None
            if stripped.casefold() in {
                candidate.casefold() for candidate in normalization.empty_like_strings
            }:
                return None
            return value
        return value

    def _cleanup_output_value(
        self,
        field_name: str,
        value: Any,
        shaping: OutputShapingConfig,
    ) -> Any:
        if isinstance(value, list):
            cleaned_items = []
            for item in value:
                cleaned_item = self._cleanup_scalar_output_value(field_name, item, shaping)
                if self._is_empty(cleaned_item):
                    continue
                cleaned_items.append(cleaned_item)

            if all(isinstance(item, str) for item in cleaned_items):
                cleaned_items = dedupe_preserve_order(cleaned_items)

            if field_name in shaping.join_fields:
                delimiter = shaping.join_fields[field_name]
                return delimiter.join(str(item) for item in cleaned_items)
            if field_name in shaping.flatten_fields and len(cleaned_items) == 1:
                return cleaned_items[0]
            if shaping.flatten_single_item_lists and len(cleaned_items) == 1:
                return cleaned_items[0]
            return cleaned_items

        return self._cleanup_scalar_output_value(field_name, value, shaping)

    def _cleanup_scalar_output_value(
        self,
        field_name: str,
        value: Any,
        shaping: OutputShapingConfig,
    ) -> Any:
        if not isinstance(value, str):
            return value

        cleaned = value.strip()
        if not shaping.cleanup_common_fields:
            return cleaned

        lowered_field_name = field_name.lower()
        if "email" in lowered_field_name and cleaned.lower().startswith("mailto:"):
            cleaned = cleaned[7:]
        if any(token in lowered_field_name for token in ("phone", "phones", "tel")) and cleaned.lower().startswith("tel:"):
            cleaned = cleaned[4:]
        return cleaned

    def _apply_field_filters(
        self,
        items: list[tuple[str, Any]],
        shaping: OutputShapingConfig,
    ) -> list[tuple[str, Any]]:
        filtered = items
        if shaping.include_fields:
            include_set = set(shaping.include_fields)
            filtered = [(key, value) for key, value in filtered if key in include_set]
        if shaping.exclude_fields:
            exclude_set = set(shaping.exclude_fields)
            filtered = [(key, value) for key, value in filtered if key not in exclude_set]
        return filtered

    def _apply_field_order(
        self,
        items: list[tuple[str, Any]],
        shaping: OutputShapingConfig,
    ) -> list[tuple[str, Any]]:
        if not shaping.field_order:
            return items

        item_map = {key: value for key, value in items}
        ordered_items: list[tuple[str, Any]] = []
        used_keys: set[str] = set()

        for key in shaping.field_order:
            if key in item_map:
                ordered_items.append((key, item_map[key]))
                used_keys.add(key)

        for key, value in items:
            if key not in used_keys:
                ordered_items.append((key, value))
        return ordered_items

    def _is_empty(self, value: Any) -> bool:
        return value in (None, "", [])

    def _apply_transform(
        self,
        value: Any,
        transform: dict[str, Any],
        field_name: str,
        normalization: NormalizationConfig,
    ) -> Any:
        transform_name = transform.get("name")
        if not transform_name:
            return value

        try:
            if transform_name == "default_if_empty":
                if value is None:
                    return transform.get("value")
                return value
            if value is None or not isinstance(value, str):
                return value
            if transform_name == "trim":
                return value.strip()
            if transform_name == "collapse_whitespace":
                return " ".join(value.split())
            if transform_name == "lowercase":
                return value.lower()
            if transform_name == "uppercase":
                return value.upper()
            if transform_name == "titlecase":
                return value.title()
            if transform_name == "regex_replace":
                pattern = transform["pattern"]
                replacement = transform.get("replacement", transform.get("repl", ""))
                return re.sub(pattern, replacement, value)
            if transform_name == "regex_extract":
                pattern = transform["pattern"]
                group = transform.get("group", 0)
                match = re.search(pattern, value)
                if match is None:
                    return None
                return match.group(group)
            if transform_name == "strip_prefix":
                prefix = transform.get("value", transform.get("prefix", ""))
                if prefix and value.startswith(prefix):
                    return value[len(prefix) :]
                return value
            if transform_name == "strip_suffix":
                suffix = transform.get("value", transform.get("suffix", ""))
                if suffix and value.endswith(suffix):
                    return value[: -len(suffix)]
                return value

            self._warn_transform_issue(
                field_name,
                f"Unknown transform '{transform_name}' skipped.",
            )
            return value
        except Exception as error:
            self._warn_transform_issue(
                field_name,
                f"Transform '{transform_name}' failed safely: {error}",
            )
            return value

    def _fallback_value(self, field: ExtractorFieldConfig) -> Any:
        if field.default is not None:
            return copy.deepcopy(field.default)
        return None

    def _warn_transform_issue(self, field_name: str, message: str) -> None:
        log_event(
            self.logger,
            logging.WARNING,
            "FIELD TRANSFORM",
            message,
            field=field_name,
        )
