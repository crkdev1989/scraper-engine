from __future__ import annotations

import logging
from typing import Any

from scraper_engine.core.models import EngineConfig, RunContext
from scraper_engine.crawl.crawler import Crawler
from scraper_engine.extractors.registry import ExtractorRegistry
from scraper_engine.schemas.mapper import SchemaMapper
from scraper_engine.utils.logging_utils import log_event


class JobRunner:
    def __init__(
        self,
        crawler: Crawler,
        extractor_registry: ExtractorRegistry,
        mapper: SchemaMapper,
        logger=None,
    ) -> None:
        self.crawler = crawler
        self.extractor_registry = extractor_registry
        self.mapper = mapper
        self.logger = logger

    def run(
        self,
        targets: list[str],
        config: EngineConfig,
        run_context: RunContext,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        target_reports: list[dict[str, Any]] = []
        notes: list[str] = []
        errors: list[str] = []
        pages_crawled = 0
        pages_succeeded = 0
        pages_failed = 0

        for target in targets:
            crawl_result = self.crawler.crawl(target, config.crawl, run_context)
            page_payloads: list[dict[str, Any]] = []
            pages_crawled += len(crawl_result.pages)
            target_pages_succeeded = 0
            target_pages_failed = 0

            for page in crawl_result.pages:
                if page.error:
                    pages_failed += 1
                    target_pages_failed += 1
                    continue

                try:
                    extracted = self.extractor_registry.extract_page(
                        html=page.html or "",
                        url=page.url,
                        fields=config.extraction.fields,
                    )
                    page_payloads.append(
                        {
                            "input_url": target,
                            "page_url": page.url,
                            "status_code": page.status_code,
                            "source_urls": [page.url],
                            **extracted,
                        }
                    )
                    pages_succeeded += 1
                    target_pages_succeeded += 1
                    log_event(
                        self.logger,
                        logging.INFO,
                        "EXTRACT FIELDS",
                        "Extracted fields from page.",
                        url=page.url,
                        field_count=len(extracted),
                        extracted_fields=", ".join(sorted(extracted.keys())),
                    )
                except Exception as error:
                    pages_failed += 1
                    target_pages_failed += 1
                    errors.append(f"{page.url}: extraction error: {error}")
                    if self.logger:
                        self.logger.exception("Extraction failed for %s", page.url)

            rows.extend(self.mapper.map_rows(target, page_payloads, config))
            target_reports.append(
                {
                    "target": target,
                    "pages_crawled": len(crawl_result.pages),
                    "pages_succeeded": target_pages_succeeded,
                    "pages_failed": target_pages_failed,
                    "queued_links": crawl_result.queued_links,
                    "notes": crawl_result.notes,
                    "errors": crawl_result.errors,
                    "source_urls": [page.url for page in crawl_result.pages if not page.error],
                }
            )
            notes.extend(crawl_result.notes)
            errors.extend(crawl_result.errors)

            log_event(
                self.logger,
                logging.INFO,
                "EXTRACT FIELDS",
                "Finished target extraction pass.",
                target=target,
                pages_crawled=len(crawl_result.pages),
                pages_succeeded=target_pages_succeeded,
                pages_failed=target_pages_failed,
            )

        report = {
            "target_count": len(targets),
            "row_count": len(rows),
            "error_count": len(errors),
            "pages_crawled": pages_crawled,
            "pages_succeeded": pages_succeeded,
            "pages_failed": pages_failed,
            "extracted_field_names": [field.name for field in config.extraction.fields],
            "targets": target_reports,
            "notes": notes,
            "errors": errors,
        }
        return rows, report
