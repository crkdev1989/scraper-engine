from __future__ import annotations

import logging
from typing import Any

from scraper_engine.core.models import CrawlPage, DetailPageConfig, EngineConfig, PaginationConfig, RunContext
from scraper_engine.crawl.crawler import Crawler
from scraper_engine.crawl.fetcher import Fetcher
from scraper_engine.crawl.url_utils import normalize_url
from scraper_engine.extractors.directory_detail_extractor import DirectoryDetailExtractor
from scraper_engine.extractors.directory_extractors import DirectoryListExtractor
from scraper_engine.extractors.registry import ExtractorRegistry
from scraper_engine.outputs.paths import raw_page_path
from scraper_engine.schemas.mapper import SchemaMapper
from scraper_engine.utils.logging_utils import log_event


class JobRunner:
    def __init__(
        self,
        fetcher: Fetcher,
        crawler: Crawler,
        extractor_registry: ExtractorRegistry,
        mapper: SchemaMapper,
        logger=None,
    ) -> None:
        self.fetcher = fetcher
        self.crawler = crawler
        self.extractor_registry = extractor_registry
        self.directory_extractor = DirectoryListExtractor()
        self.directory_detail_extractor = DirectoryDetailExtractor(extractor_registry)
        self.mapper = mapper
        self.logger = logger

    def run(
        self,
        targets: list[str],
        config: EngineConfig,
        run_context: RunContext,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if config.mode == "directory_list":
            return self._run_directory_list(targets, config, run_context)
        if config.mode == "directory_detail":
            return self._run_directory_detail(targets, config, run_context)
        return self._run_site_scan(targets, config, run_context)

    def _run_site_scan(
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
        pages_visited = 0

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
            "pages_visited": pages_visited,
            "listing_count": 0,
            "detail_pages_attempted": 0,
            "detail_pages_successful": 0,
            "detail_pages_failed": 0,
            "pagination_urls_followed": 0,
            "pagination_stopped_reasons": [],
            "extracted_field_names": [field.name for field in config.extraction.fields],
            "targets": target_reports,
            "notes": notes,
            "errors": errors,
        }
        return rows, report

    def _run_directory_list(
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
        pages_visited = 0
        pagination_urls_followed = 0
        pagination_stopped_reasons: list[str] = []
        page_number_counter = 0

        for target in targets:
            traversal, page_number_counter = self._walk_directory_pages(
                target,
                config.pagination,
                run_context,
                page_number_counter,
            )
            target_errors = list(traversal["errors"])
            target_source_urls: list[str] = []
            target_rows = 0

            pages_crawled += len(traversal["pages"])
            pages_visited += len(traversal["pages"])
            pagination_urls_followed += len(traversal["pagination_urls_followed"])
            pagination_stopped_reasons.append(traversal["stop_reason"])

            for page in traversal["pages"]:
                if page.error:
                    pages_failed += 1
                    target_errors.append(f"{page.url}: {page.error}")
                    continue

                pages_succeeded += 1
                target_source_urls.append(page.url)
                extracted_rows = self.directory_extractor.extract_records(
                    html=page.html or "",
                    page_url=page.url,
                    extraction_config=config.extraction,
                )
                for record in extracted_rows:
                    rows.append(
                        {
                            "input_url": target,
                            "source_url": page.url,
                            "page_url": page.url,
                            **record,
                        }
                    )
                target_rows += len(extracted_rows)
                log_event(
                    self.logger,
                    logging.INFO,
                    "EXTRACT FIELDS",
                    "Extracted directory listing records from page.",
                    target=target,
                    source_url=page.url,
                    row_count=len(extracted_rows),
                )

            target_reports.append(
                {
                    "target": target,
                    "pages_crawled": len(traversal["pages"]),
                    "pages_visited": len(traversal["pages"]),
                    "pages_succeeded": len([page for page in traversal["pages"] if not page.error]),
                    "pages_failed": len([page for page in traversal["pages"] if page.error]),
                    "row_count": target_rows,
                    "listing_count": target_rows,
                    "pagination_urls_followed": traversal["pagination_urls_followed"],
                    "pagination_stopped_reason": traversal["stop_reason"],
                    "errors": target_errors,
                    "source_urls": target_source_urls,
                }
            )
            errors.extend(target_errors)
            notes.append(
                f"Extracted {target_rows} listing record(s) from {len(traversal['pages'])} "
                f"directory page(s) for {target}; stop_reason={traversal['stop_reason']}."
            )

        report = {
            "target_count": len(targets),
            "row_count": len(rows),
            "error_count": len(errors),
            "pages_crawled": pages_crawled,
            "pages_succeeded": pages_succeeded,
            "pages_failed": pages_failed,
            "pages_visited": pages_visited,
            "listing_count": len(rows),
            "detail_pages_attempted": 0,
            "detail_pages_successful": 0,
            "detail_pages_failed": 0,
            "pagination_urls_followed": pagination_urls_followed,
            "pagination_stopped_reasons": pagination_stopped_reasons,
            "extracted_field_names": [field.name for field in config.extraction.fields],
            "targets": target_reports,
            "notes": notes,
            "errors": errors,
        }
        return rows, report

    def _run_directory_detail(
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
        detail_pages_attempted = 0
        detail_pages_successful = 0
        detail_pages_failed = 0
        listing_count = 0
        pages_visited = 0
        pagination_urls_followed = 0
        pagination_stopped_reasons: list[str] = []
        page_number_counter = 0

        detail_page_config = self._get_detail_page_config(config)

        for target in targets:
            traversal, page_number_counter = self._walk_directory_pages(
                target,
                config.pagination,
                run_context,
                page_number_counter,
            )
            target_listing_count = 0
            target_detail_attempted = 0
            target_detail_successful = 0
            target_detail_failed = 0
            target_errors: list[str] = list(traversal["errors"])
            target_source_urls: list[str] = []

            pages_crawled += len(traversal["pages"])
            pages_visited += len(traversal["pages"])
            pagination_urls_followed += len(traversal["pagination_urls_followed"])
            pagination_stopped_reasons.append(traversal["stop_reason"])

            for directory_page in traversal["pages"]:
                if directory_page.error:
                    pages_failed += 1
                    target_errors.append(f"{directory_page.url}: {directory_page.error}")
                    continue

                pages_succeeded += 1
                target_source_urls.append(directory_page.url)
                base_rows = self.directory_extractor.extract_records(
                    html=directory_page.html or "",
                    page_url=directory_page.url,
                    extraction_config=config.extraction,
                )
                target_listing_count += len(base_rows)
                listing_count += len(base_rows)

                for base_record in base_rows:
                    enriched_row = {
                        "input_url": target,
                        "source_url": directory_page.url,
                        "page_url": directory_page.url,
                        **base_record,
                    }

                    detail_url_value = self._get_record_detail_url(
                        enriched_row,
                        detail_page_config,
                    )
                    if detail_url_value:
                        detail_pages_attempted += 1
                        target_detail_attempted += 1
                        detail_page, page_number_counter = self._fetch_page_with_storage(
                            detail_url_value,
                            run_context,
                            page_number_counter,
                        )
                        pages_crawled += 1

                        if detail_page.error:
                            detail_pages_failed += 1
                            target_detail_failed += 1
                            pages_failed += 1
                            target_errors.append(f"{detail_page.url}: {detail_page.error}")
                            log_event(
                                self.logger,
                                logging.WARNING,
                                "DETAIL ENRICH",
                                "Detail page fetch failed; preserving base record.",
                                source_url=directory_page.url,
                                detail_url=detail_url_value,
                                error=detail_page.error,
                            )
                        else:
                            try:
                                detail_fields = self.directory_detail_extractor.extract_detail_fields(
                                    html=detail_page.html or "",
                                    page_url=detail_page.url,
                                    detail_page_config=detail_page_config,
                                )
                                enriched_row = self.directory_detail_extractor.merge_detail_fields(
                                    enriched_row,
                                    detail_fields,
                                    detail_page.url,
                                )
                                detail_pages_successful += 1
                                target_detail_successful += 1
                                pages_succeeded += 1
                                log_event(
                                    self.logger,
                                    logging.INFO,
                                    "DETAIL ENRICH",
                                    "Merged detail page fields into listing record.",
                                    source_url=directory_page.url,
                                    detail_url=detail_page.url,
                                    field_count=len(detail_fields),
                                )
                            except Exception as error:
                                detail_pages_failed += 1
                                target_detail_failed += 1
                                pages_failed += 1
                                target_errors.append(
                                    f"{detail_page.url}: detail extraction error: {error}"
                                )
                                log_event(
                                    self.logger,
                                    logging.WARNING,
                                    "DETAIL ENRICH",
                                    "Detail extraction failed; preserving base record.",
                                    source_url=directory_page.url,
                                    detail_url=detail_page.url,
                                    error=error,
                                )
                    else:
                        log_event(
                            self.logger,
                            logging.INFO,
                            "DETAIL ENRICH",
                            "Listing did not provide a detail URL; base record kept.",
                            source_url=directory_page.url,
                        )

                    rows.append(enriched_row)

            target_reports.append(
                {
                    "target": target,
                    "pages_crawled": len(traversal["pages"]) + target_detail_attempted,
                    "pages_visited": len(traversal["pages"]),
                    "pages_succeeded": len([page for page in traversal["pages"] if not page.error])
                    + target_detail_successful,
                    "pages_failed": len([page for page in traversal["pages"] if page.error])
                    + target_detail_failed,
                    "row_count": target_listing_count,
                    "listing_count": target_listing_count,
                    "detail_pages_attempted": target_detail_attempted,
                    "detail_pages_successful": target_detail_successful,
                    "detail_pages_failed": target_detail_failed,
                    "pagination_urls_followed": traversal["pagination_urls_followed"],
                    "pagination_stopped_reason": traversal["stop_reason"],
                    "errors": target_errors,
                    "source_urls": target_source_urls,
                }
            )
            errors.extend(target_errors)
            notes.append(
                f"Extracted {target_listing_count} listing record(s) from {len(traversal['pages'])} "
                f"directory page(s) for {target}, attempted {target_detail_attempted} detail page(s), "
                f"stop_reason={traversal['stop_reason']}."
            )
            log_event(
                self.logger,
                logging.INFO,
                "DETAIL ENRICH",
                "Finished directory detail enrichment for target.",
                target=target,
                listing_count=target_listing_count,
                detail_pages_attempted=target_detail_attempted,
                detail_pages_successful=target_detail_successful,
                detail_pages_failed=target_detail_failed,
            )

        report = {
            "target_count": len(targets),
            "row_count": len(rows),
            "error_count": len(errors),
            "pages_crawled": pages_crawled,
            "pages_succeeded": pages_succeeded,
            "pages_failed": pages_failed,
            "pages_visited": pages_visited,
            "listing_count": listing_count,
            "detail_pages_attempted": detail_pages_attempted,
            "detail_pages_successful": detail_pages_successful,
            "detail_pages_failed": detail_pages_failed,
            "pagination_urls_followed": pagination_urls_followed,
            "pagination_stopped_reasons": pagination_stopped_reasons,
            "extracted_field_names": [
                *[field.name for field in config.extraction.fields],
                *[field.name for field in detail_page_config.fields],
            ],
            "targets": target_reports,
            "notes": notes,
            "errors": errors,
        }
        return rows, report

    def _store_raw_page(self, page, run_context: RunContext, page_number: int) -> None:
        if not page.html or not run_context.raw_pages_dir:
            return
        output_path = raw_page_path(run_context.raw_pages_dir, page.url, page_number)
        output_path.write_text(page.html, encoding="utf-8")
        log_event(
            self.logger,
            logging.INFO,
            "WRITE OUTPUTS",
            "Stored raw HTML page.",
            url=page.url,
            path=output_path.name,
        )

    def _fetch_page_with_storage(
        self,
        url: str,
        run_context: RunContext,
        page_number_counter: int,
    ) -> tuple[CrawlPage, int]:
        page = self.fetcher.fetch(url)
        page_number_counter += 1
        self._store_raw_page(page, run_context, page_number_counter)
        return page, page_number_counter

    def _walk_directory_pages(
        self,
        target: str,
        pagination_config: PaginationConfig,
        run_context: RunContext,
        page_number_counter: int,
    ) -> tuple[dict[str, Any], int]:
        pages: list[CrawlPage] = []
        pagination_urls_followed: list[str] = []
        visited_urls: set[str] = set()
        errors: list[str] = []
        current_url = normalize_url(target)
        max_pages = max(
            1,
            pagination_config.max_pages if pagination_config.enabled else 1,
        )
        stop_reason = "pagination_disabled"

        while True:
            if current_url in visited_urls:
                stop_reason = "duplicate_next_page_detected"
                break

            visited_urls.add(current_url)
            log_event(
                self.logger,
                logging.INFO,
                "DIRECTORY PAGE",
                "Processing directory page.",
                url=current_url,
                page_number=len(pages) + 1,
            )
            page, page_number_counter = self._fetch_page_with_storage(
                current_url,
                run_context,
                page_number_counter,
            )
            pages.append(page)

            if page.error:
                stop_reason = "fetch_failure"
                break

            if not pagination_config.enabled:
                stop_reason = "pagination_disabled"
                break

            if len(pages) >= max_pages:
                stop_reason = "max_pages_reached"
                break

            next_page_url = self.directory_extractor.extract_next_page_url(
                page.html or "",
                page.url,
                pagination_config,
            )
            if not next_page_url:
                stop_reason = "no_next_page_found"
                break

            try:
                normalized_next_page_url = normalize_url(next_page_url)
            except ValueError as error:
                errors.append(f"{page.url}: invalid next page url: {error}")
                stop_reason = "invalid_next_page_url"
                break

            if normalized_next_page_url in visited_urls:
                stop_reason = "duplicate_next_page_detected"
                break

            pagination_urls_followed.append(normalized_next_page_url)
            log_event(
                self.logger,
                logging.INFO,
                "PAGINATION",
                "Found next directory page.",
                current_url=page.url,
                next_url=normalized_next_page_url,
            )
            current_url = normalized_next_page_url

        log_event(
            self.logger,
            logging.INFO,
            "PAGINATION",
            "Stopped directory pagination.",
            stop_reason=stop_reason,
            pages_visited=len(pages),
            pagination_urls_followed=len(pagination_urls_followed),
        )
        return (
            {
                "pages": pages,
                "pagination_urls_followed": pagination_urls_followed,
                "stop_reason": stop_reason,
                "errors": errors,
            },
            page_number_counter,
        )

    def _get_detail_page_config(self, config: EngineConfig) -> DetailPageConfig:
        detail_page_config = config.extraction.detail_page
        if detail_page_config is None or not detail_page_config.enabled:
            raise ValueError(
                "directory_detail mode requires extraction.detail_page.enabled=true."
            )
        if not detail_page_config.url_field:
            raise ValueError(
                "directory_detail mode requires extraction.detail_page.url_field."
            )
        return detail_page_config

    def _get_record_detail_url(
        self,
        record: dict[str, Any],
        detail_page_config: DetailPageConfig,
    ) -> str | None:
        raw_url = record.get(detail_page_config.url_field or "")
        if not raw_url:
            return None
        if isinstance(raw_url, list):
            raw_url = raw_url[0] if raw_url else None
        if not raw_url:
            return None
        try:
            return normalize_url(str(raw_url))
        except ValueError:
            return None
