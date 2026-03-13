from __future__ import annotations

from typing import Any

from bs4 import BeautifulSoup

from scraper_engine.core.models import ExtractionConfig, ExtractorFieldConfig, PaginationConfig
from scraper_engine.extractors.selector_extractors import extract_css, extract_css_from_node


class DirectoryListExtractor:
    def extract_records(
        self,
        html: str,
        page_url: str,
        extraction_config: ExtractionConfig,
    ) -> list[dict[str, Any]]:
        record_selector = extraction_config.record_selector
        if record_selector is None or not record_selector.css:
            raise ValueError(
                "directory_list mode requires extraction.record_selector.css in the config."
            )

        soup = BeautifulSoup(html or "", "lxml")
        containers = soup.select(record_selector.css)
        records: list[dict[str, Any]] = []

        for container in containers:
            record = {
                field.name: self._extract_field(
                    field=field,
                    node=container,
                    base_url=page_url,
                )
                for field in extraction_config.fields
            }
            records.append(record)

        return records

    def _extract_field(
        self,
        field: ExtractorFieldConfig,
        node,
        base_url: str,
    ) -> Any:
        if not field.selector:
            return field.default

        value = extract_css_from_node(
            node,
            selector=field.selector,
            attribute=field.attribute,
            many=field.many,
            base_url=base_url,
        )
        if value in (None, "", []):
            return field.default
        return value

    def extract_next_page_url(
        self,
        html: str,
        page_url: str,
        pagination_config: PaginationConfig,
    ) -> str | None:
        if not pagination_config.enabled or pagination_config.next_page is None:
            return None
        if not pagination_config.next_page.css:
            return None

        soup = BeautifulSoup(html or "", "lxml")
        value = extract_css(
            soup,
            selector=pagination_config.next_page.css,
            attribute=pagination_config.next_page.attribute,
            many=False,
            base_url=page_url,
        )
        if value in (None, "", []):
            return None
        return value
