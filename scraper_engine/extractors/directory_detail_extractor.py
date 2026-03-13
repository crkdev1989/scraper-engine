from __future__ import annotations

from typing import Any

from scraper_engine.core.models import DetailPageConfig
from scraper_engine.extractors.registry import ExtractorRegistry


class DirectoryDetailExtractor:
    def __init__(self, registry: ExtractorRegistry) -> None:
        self.registry = registry

    def extract_detail_fields(
        self,
        html: str,
        page_url: str,
        detail_page_config: DetailPageConfig,
    ) -> dict[str, Any]:
        return self.registry.extract_page(
            html=html,
            url=page_url,
            fields=detail_page_config.fields,
        )

    def merge_detail_fields(
        self,
        base_record: dict[str, Any],
        detail_fields: dict[str, Any],
        detail_page_url: str,
    ) -> dict[str, Any]:
        merged = dict(base_record)
        for key, value in detail_fields.items():
            if value not in (None, "", []):
                merged[key] = value
        merged["detail_url"] = detail_page_url
        return merged
