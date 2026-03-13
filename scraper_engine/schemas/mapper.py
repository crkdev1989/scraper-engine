from __future__ import annotations

from typing import Any

from scraper_engine.core.models import EngineConfig
from scraper_engine.utils.text_utils import dedupe_preserve_order


class SchemaMapper:
    def map_rows(
        self,
        seed_url: str,
        page_payloads: list[dict[str, Any]],
        config: EngineConfig,
    ) -> list[dict[str, Any]]:
        if not page_payloads:
            return []
        if config.output.merge_rows:
            return [self._merge_rows(seed_url, page_payloads, config)]
        return page_payloads

    def _merge_rows(
        self,
        seed_url: str,
        page_payloads: list[dict[str, Any]],
        config: EngineConfig,
    ) -> dict[str, Any]:
        merged: dict[str, Any] = {
            "input_url": seed_url,
            "source_urls": dedupe_preserve_order(
                [
                    payload.get("page_url")
                    for payload in page_payloads
                    if payload.get("page_url")
                ]
            ),
        }

        for payload in page_payloads:
            for key, value in payload.items():
                if key in {"input_url", "page_url", "status_code", "source_urls"}:
                    continue
                merged[key] = self._merge_value(merged.get(key), value)

        for field in config.extraction.fields:
            if field.many and merged.get(field.name) is None:
                merged[field.name] = list(field.default or [])
            elif merged.get(field.name) is None and field.default is not None:
                merged[field.name] = field.default

        return merged

    def _merge_value(self, current: Any, incoming: Any) -> Any:
        if incoming in (None, "", []):
            return current
        if isinstance(current, list) or isinstance(incoming, list):
            left = current if isinstance(current, list) else ([current] if current else [])
            right = incoming if isinstance(incoming, list) else [incoming]
            return dedupe_preserve_order([str(item) for item in [*left, *right] if item])
        return current or incoming
