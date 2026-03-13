from __future__ import annotations

from typing import Any

from bs4 import BeautifulSoup

from scraper_engine.core.models import ExtractorFieldConfig
from scraper_engine.extractors.builtins import BUILTIN_EXTRACTORS
from scraper_engine.extractors.selector_extractors import (
    build_xpath_tree,
    extract_css,
    extract_xpath,
)


class ExtractorRegistry:
    def __init__(self) -> None:
        self.builtin_extractors = dict(BUILTIN_EXTRACTORS)

    def available_types(self) -> list[str]:
        return sorted(self.builtin_extractors)

    def extract_page(
        self,
        html: str,
        url: str,
        fields: list[ExtractorFieldConfig],
    ) -> dict[str, Any]:
        soup = BeautifulSoup(html or "", "lxml")
        tree = build_xpath_tree(html or "")
        results: dict[str, Any] = {}

        for field in fields:
            results[field.name] = self._extract_field(
                field=field,
                soup=soup,
                tree=tree,
                html=html,
                url=url,
            )
        return results

    def _extract_field(self, field: ExtractorFieldConfig, **context) -> Any:
        if field.type:
            extractor = self.builtin_extractors.get(field.type)
            if extractor is None:
                raise ValueError(f"Unknown extractor type: {field.type}")
            value = extractor(field=field, **context)
        elif field.selector:
            value = extract_css(
                context["soup"],
                selector=field.selector,
                attribute=field.attribute,
                many=field.many,
            )
        elif field.xpath:
            value = extract_xpath(context["tree"], expression=field.xpath, many=field.many)
        else:
            value = None

        if value in (None, "", []):
            return field.default
        return value
