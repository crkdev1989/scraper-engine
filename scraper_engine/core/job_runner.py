from __future__ import annotations

import logging
from typing import Any

from scraper_engine.core.models import CrawlPage, DetailPageConfig, EngineConfig, PaginationConfig, RunContext
from scraper_engine.core.record_post_processor import RecordPostProcessor
from scraper_engine.crawl.crawler import Crawler
from scraper_engine.crawl.fetcher import Fetcher
from scraper_engine.crawl.url_utils import normalize_url
from scraper_engine.extractors.directory_detail_extractor import DirectoryDetailExtractor
from scraper_engine.extractors.directory_extractors import DirectoryListExtractor
from scraper_engine.extractors.registry import ExtractorRegistry
from scraper_engine.outputs.paths import raw_page_path
from scraper_engine.schemas.mapper import SchemaMapper
from scraper_engine.utils.logging_utils import log_event
from scraper_engine.utils.playwright_directory import PlaywrightDirectoryClient


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
        self.post_processor = RecordPostProcessor(logger=logger)
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
        diagnostics = self._new_diagnostics()
        limits_state = self._new_limits_state(config)
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
                    self._record_diagnostic(
                        diagnostics,
                        "page_fetch_failed",
                        f"{page.url}: {page.error}",
                    )
                    continue

                try:
                    extracted = self.extractor_registry.extract_page(
                        html=page.html or "",
                        url=page.url,
                        fields=config.extraction.fields,
                    )
                    extracted = self.post_processor.process_record(
                        extracted,
                        config.extraction.fields,
                        config.normalization,
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
                    self._record_diagnostic(
                        diagnostics,
                        "page_extraction_failed",
                        f"{page.url}: extraction error: {error}",
                    )
                    if self.logger:
                        self.logger.exception("Extraction failed for %s", page.url)

            mapped_rows = self.mapper.map_rows(target, page_payloads, config)
            mapped_rows = self._cap_records(
                mapped_rows,
                current_count=len(rows),
                config=config,
                limits_state=limits_state,
                diagnostics=diagnostics,
                notes=notes,
                context=f"site_scan target {target}",
            )
            rows.extend(self._shape_final_rows(mapped_rows, config))
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
            if self._max_records_reached(limits_state):
                break

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
            "diagnostics": self._merge_diagnostics(diagnostics),
            "limits": self._build_limits_report(config, limits_state),
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
        diagnostics = self._new_diagnostics()
        limits_state = self._new_limits_state(config)
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
                diagnostics=diagnostics,
            )
            target_errors = list(traversal["errors"])
            target_source_urls: list[str] = []
            target_rows = 0
            target_output_rows: list[dict[str, Any]] = []

            pages_crawled += len(traversal["pages"])
            pages_visited += len(traversal["pages"])
            pagination_urls_followed += len(traversal["pagination_urls_followed"])
            pagination_stopped_reasons.append(traversal["stop_reason"])

            for page in traversal["pages"]:
                if page.error:
                    pages_failed += 1
                    target_errors.append(f"{page.url}: {page.error}")
                    self._record_diagnostic(
                        diagnostics,
                        "directory_page_fetch_failed",
                        f"{page.url}: {page.error}",
                    )
                    continue

                pages_succeeded += 1
                target_source_urls.append(page.url)
                extracted_rows = self.directory_extractor.extract_records(
                    html=page.html or "",
                    page_url=page.url,
                    extraction_config=config.extraction,
                )
                extracted_rows = self._cap_records(
                    extracted_rows,
                    current_count=len(rows) + len(target_output_rows),
                    config=config,
                    limits_state=limits_state,
                    diagnostics=diagnostics,
                    notes=notes,
                    context=f"directory_list page {page.url}",
                )
                for record in extracted_rows:
                    record = self.post_processor.process_record(
                        record,
                        config.extraction.fields,
                        config.normalization,
                    )
                    target_output_rows.append(
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
                if self._max_records_reached(limits_state):
                    break

            rows.extend(self._shape_final_rows(target_output_rows, config))
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
            if self._max_records_reached(limits_state):
                break

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
            "diagnostics": self._merge_diagnostics(diagnostics),
            "limits": self._build_limits_report(config, limits_state),
        }
        return rows, report

    def _run_directory_detail(
        self,
        targets: list[str],
        config: EngineConfig,
        run_context: RunContext,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if self._use_playwright_directory_renderer(config):
            return self._run_directory_detail_playwright(
                targets,
                config,
                run_context,
            )

        rows: list[dict[str, Any]] = []
        target_reports: list[dict[str, Any]] = []
        notes: list[str] = []
        errors: list[str] = []
        diagnostics = self._new_diagnostics()
        limits_state = self._new_limits_state(config)
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
                diagnostics=diagnostics,
            )
            target_listing_count = 0
            target_detail_attempted = 0
            target_detail_successful = 0
            target_detail_failed = 0
            target_errors: list[str] = list(traversal["errors"])
            target_source_urls: list[str] = []
            target_output_rows: list[dict[str, Any]] = []

            pages_crawled += len(traversal["pages"])
            pages_visited += len(traversal["pages"])
            pagination_urls_followed += len(traversal["pagination_urls_followed"])
            pagination_stopped_reasons.append(traversal["stop_reason"])

            for directory_page in traversal["pages"]:
                if directory_page.error:
                    pages_failed += 1
                    target_errors.append(f"{directory_page.url}: {directory_page.error}")
                    self._record_diagnostic(
                        diagnostics,
                        "directory_page_fetch_failed",
                        f"{directory_page.url}: {directory_page.error}",
                    )
                    continue

                pages_succeeded += 1
                target_source_urls.append(directory_page.url)
                base_rows = self.directory_extractor.extract_records(
                    html=directory_page.html or "",
                    page_url=directory_page.url,
                    extraction_config=config.extraction,
                )
                base_rows = self._cap_records(
                    base_rows,
                    current_count=len(rows) + len(target_output_rows),
                    config=config,
                    limits_state=limits_state,
                    diagnostics=diagnostics,
                    notes=notes,
                    context=f"directory_detail page {directory_page.url}",
                )
                target_listing_count += len(base_rows)
                listing_count += len(base_rows)

                for base_record in base_rows:
                    base_record = self.post_processor.process_record(
                        base_record,
                        config.extraction.fields,
                        config.normalization,
                    )
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
                        if self._detail_page_limit_reached(config, detail_pages_attempted):
                            limits_state["detail_pages_skipped_due_to_limit"] += 1
                            self._mark_detail_page_limit_hit(
                                config,
                                limits_state,
                                diagnostics,
                                notes,
                            )
                            self._record_diagnostic(
                                diagnostics,
                                "detail_page_limit_skipped",
                                "Detail page fetch skipped because max_detail_pages was reached.",
                            )
                        else:
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
                                self._record_diagnostic(
                                    diagnostics,
                                    "detail_page_fetch_failed",
                                    f"{detail_page.url}: {detail_page.error}",
                                )
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
                                    detail_fields = self.post_processor.process_record(
                                        detail_fields,
                                        detail_page_config.fields,
                                        config.normalization,
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
                                    self._record_diagnostic(
                                        diagnostics,
                                        "detail_field_extraction_failed",
                                        f"{detail_page.url}: detail extraction error: {error}",
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

                    target_output_rows.append(enriched_row)
                if self._max_records_reached(limits_state):
                    break

            rows.extend(self._shape_final_rows(target_output_rows, config))
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
            if self._max_records_reached(limits_state):
                break

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
            "diagnostics": self._merge_diagnostics(diagnostics),
            "limits": self._build_limits_report(config, limits_state),
        }
        return rows, report

    def _run_directory_detail_playwright(
        self,
        targets: list[str],
        config: EngineConfig,
        run_context: RunContext,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        target_reports: list[dict[str, Any]] = []
        notes: list[str] = []
        errors: list[str] = []
        diagnostics = self._new_diagnostics()
        limits_state = self._new_limits_state(config)
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
        playwright_settings = self._get_playwright_settings(config)

        for target in targets:
            target_listing_count = 0
            target_detail_attempted = 0
            target_detail_successful = 0
            target_detail_failed = 0
            target_errors: list[str] = []
            target_source_urls: list[str] = []
            target_output_rows: list[dict[str, Any]] = []

            log_event(
                self.logger,
                logging.INFO,
                "PLAYWRIGHT",
                "Rendering directory target with Playwright.",
                target=target,
            )
            try:
                with PlaywrightDirectoryClient(
                    settings=playwright_settings,
                    user_agent=config.requests.user_agent,
                    logger=self.logger,
                ) as client:
                    rendered_directory = client.open_directory(target)
                    page_number_counter += 1
                    self._store_raw_page(
                        rendered_directory.directory_page,
                        run_context,
                        page_number_counter,
                    )
                    pages_crawled += 1
                    pages_visited += 1
                    pagination_stopped_reasons.append(rendered_directory.stop_reason)

                    directory_page = rendered_directory.directory_page
                    if directory_page.error:
                        pages_failed += 1
                        target_errors.append(f"{target}: {directory_page.error}")
                        self._record_diagnostic(
                            diagnostics,
                            "playwright_directory_render_failed",
                            f"{target}: {directory_page.error}",
                        )
                        target_reports.append(
                            {
                                "target": target,
                                "pages_crawled": 1,
                                "pages_visited": 1,
                                "pages_succeeded": 0,
                                "pages_failed": 1,
                                "row_count": 0,
                                "listing_count": 0,
                                "detail_pages_attempted": 0,
                                "detail_pages_successful": 0,
                                "detail_pages_failed": 0,
                                "pagination_urls_followed": [],
                                "pagination_stopped_reason": rendered_directory.stop_reason,
                                "errors": target_errors,
                                "source_urls": [],
                            }
                        )
                        errors.extend(target_errors)
                        continue

                    pages_succeeded += 1
                    target_source_urls.append(directory_page.url)
                    base_rows = self._build_playwright_base_rows(
                        rendered_html=directory_page.html or "",
                        page_url=directory_page.url,
                        profile_urls=rendered_directory.profile_urls,
                        config=config,
                        detail_page_config=detail_page_config,
                    )
                    base_rows = self._cap_records(
                        base_rows,
                        current_count=len(rows) + len(target_output_rows),
                        config=config,
                        limits_state=limits_state,
                        diagnostics=diagnostics,
                        notes=notes,
                        context=f"playwright directory_detail target {target}",
                    )
                    target_listing_count += len(base_rows)
                    listing_count += len(base_rows)

                    notes.append(
                        f"Playwright rendered {target} with {rendered_directory.load_more_clicks} "
                        f"load-more click(s); stop_reason={rendered_directory.stop_reason}."
                    )

                    for base_record in base_rows:
                        base_record = self.post_processor.process_record(
                            base_record,
                            config.extraction.fields,
                            config.normalization,
                        )
                        enriched_row = {
                            "input_url": target,
                            "source_url": directory_page.url,
                            "source_directory": directory_page.url,
                            "page_url": directory_page.url,
                            **base_record,
                        }
                        detail_url_value = self._get_record_detail_url(
                            enriched_row,
                            detail_page_config,
                        )
                        if detail_url_value and not enriched_row.get("profile_url"):
                            enriched_row["profile_url"] = detail_url_value

                        if detail_url_value:
                            if self._detail_page_limit_reached(config, detail_pages_attempted):
                                limits_state["detail_pages_skipped_due_to_limit"] += 1
                                self._mark_detail_page_limit_hit(
                                    config,
                                    limits_state,
                                    diagnostics,
                                    notes,
                                )
                                self._record_diagnostic(
                                    diagnostics,
                                    "detail_page_limit_skipped",
                                    "Detail page fetch skipped because max_detail_pages was reached.",
                                )
                            else:
                                detail_pages_attempted += 1
                                target_detail_attempted += 1
                                detail_page = client.fetch_detail_page(detail_url_value)
                                page_number_counter += 1
                                self._store_raw_page(
                                    detail_page,
                                    run_context,
                                    page_number_counter,
                                )
                                pages_crawled += 1

                                if detail_page.error:
                                    detail_pages_failed += 1
                                    target_detail_failed += 1
                                    pages_failed += 1
                                    target_errors.append(f"{detail_page.url}: {detail_page.error}")
                                    self._record_diagnostic(
                                        diagnostics,
                                        "detail_page_fetch_failed",
                                        f"{detail_page.url}: {detail_page.error}",
                                    )
                                    log_event(
                                        self.logger,
                                        logging.WARNING,
                                        "DETAIL ENRICH",
                                        "Playwright detail page fetch failed; preserving base record.",
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
                                        detail_fields = self.post_processor.process_record(
                                            detail_fields,
                                            detail_page_config.fields,
                                            config.normalization,
                                        )
                                        enriched_row = self.directory_detail_extractor.merge_detail_fields(
                                            enriched_row,
                                            detail_fields,
                                            detail_page.url,
                                        )
                                        enriched_row["profile_url"] = detail_page.url
                                        enriched_row["source_directory"] = directory_page.url
                                        detail_pages_successful += 1
                                        target_detail_successful += 1
                                        pages_succeeded += 1
                                        log_event(
                                            self.logger,
                                            logging.INFO,
                                            "DETAIL ENRICH",
                                            "Merged Playwright detail page fields into listing record.",
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
                                        self._record_diagnostic(
                                            diagnostics,
                                            "detail_field_extraction_failed",
                                            f"{detail_page.url}: detail extraction error: {error}",
                                        )
                                        log_event(
                                            self.logger,
                                            logging.WARNING,
                                            "DETAIL ENRICH",
                                            "Playwright detail extraction failed; preserving base record.",
                                            source_url=directory_page.url,
                                            detail_url=detail_page.url,
                                            error=error,
                                        )
                        else:
                            log_event(
                                self.logger,
                                logging.INFO,
                                "DETAIL ENRICH",
                                "Rendered record did not provide a detail URL; base record kept.",
                                source_url=directory_page.url,
                            )

                        target_output_rows.append(enriched_row)
                    if self._max_records_reached(limits_state):
                        notes.append(
                            "Stopped additional Playwright directory processing after reaching max_records."
                        )

            except Exception as error:
                pages_failed += 1
                pages_crawled += 1
                pages_visited += 1
                pagination_stopped_reasons.append("playwright_session_failed")
                target_errors.append(f"{target}: Playwright session failed: {error}")
                self._record_diagnostic(
                    diagnostics,
                    "playwright_session_failed",
                    f"{target}: Playwright session failed: {error}",
                )
                target_reports.append(
                    {
                        "target": target,
                        "pages_crawled": 1,
                        "pages_visited": 1,
                        "pages_succeeded": 0,
                        "pages_failed": 1,
                        "row_count": 0,
                        "listing_count": 0,
                        "detail_pages_attempted": 0,
                        "detail_pages_successful": 0,
                        "detail_pages_failed": 0,
                        "pagination_urls_followed": [],
                        "pagination_stopped_reason": "playwright_session_failed",
                        "errors": target_errors,
                        "source_urls": [],
                    }
                )
                errors.extend(target_errors)
                continue

            rows.extend(self._shape_final_rows(target_output_rows, config))
            target_reports.append(
                {
                    "target": target,
                    "pages_crawled": 1 + target_detail_attempted,
                    "pages_visited": 1,
                    "pages_succeeded": 1 + target_detail_successful,
                    "pages_failed": target_detail_failed,
                    "row_count": target_listing_count,
                    "listing_count": target_listing_count,
                    "detail_pages_attempted": target_detail_attempted,
                    "detail_pages_successful": target_detail_successful,
                    "detail_pages_failed": target_detail_failed,
                    "pagination_urls_followed": [],
                    "pagination_stopped_reason": pagination_stopped_reasons[-1],
                    "errors": target_errors,
                    "source_urls": target_source_urls,
                }
            )
            errors.extend(target_errors)
            log_event(
                self.logger,
                logging.INFO,
                "DETAIL ENRICH",
                "Finished Playwright directory detail enrichment for target.",
                target=target,
                listing_count=target_listing_count,
                detail_pages_attempted=target_detail_attempted,
                detail_pages_successful=target_detail_successful,
                detail_pages_failed=target_detail_failed,
            )
            if self._max_records_reached(limits_state):
                break

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
                "profile_url",
                "source_directory",
            ],
            "targets": target_reports,
            "notes": notes,
            "errors": errors,
            "diagnostics": self._merge_diagnostics(diagnostics),
            "limits": self._build_limits_report(config, limits_state),
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
        diagnostics: dict[str, Any] | None = None,
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
                if diagnostics is not None:
                    self._record_diagnostic(
                        diagnostics,
                        "invalid_next_page_url",
                        f"{page.url}: invalid next page url: {error}",
                    )
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

    def _use_playwright_directory_renderer(self, config: EngineConfig) -> bool:
        renderer = str(config.metadata.get("renderer", "")).strip().lower()
        if renderer != "playwright":
            return False

        audience = str(config.metadata.get("audience", "")).strip().lower()
        private_only = bool(config.metadata.get("private_only"))
        if audience != "private" and not private_only:
            raise ValueError(
                "Playwright rendering is reserved for private configs and requires "
                "metadata.audience=private or metadata.private_only=true."
            )
        return True

    def _get_playwright_settings(self, config: EngineConfig) -> dict[str, Any]:
        settings = config.metadata.get("playwright")
        if not isinstance(settings, dict):
            raise ValueError(
                "Playwright rendering requires a metadata.playwright settings block."
            )
        return settings

    def _build_playwright_base_rows(
        self,
        rendered_html: str,
        page_url: str,
        profile_urls: list[str],
        config: EngineConfig,
        detail_page_config: DetailPageConfig,
    ) -> list[dict[str, Any]]:
        use_profile_urls_as_records = bool(
            self._get_playwright_settings(config).get(
                "use_profile_urls_as_records",
                True,
            )
        )
        if not use_profile_urls_as_records:
            try:
                base_rows = self.directory_extractor.extract_records(
                    html=rendered_html,
                    page_url=page_url,
                    extraction_config=config.extraction,
                )
            except ValueError:
                base_rows = []
            if base_rows:
                return base_rows

        detail_url_field = detail_page_config.url_field or "detail_url"
        return [
            {
                detail_url_field: profile_url,
                "profile_url": profile_url,
            }
            for profile_url in profile_urls
        ]

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

    def _shape_final_rows(
        self,
        rows: list[dict[str, Any]],
        config: EngineConfig,
    ) -> list[dict[str, Any]]:
        return [
            self.post_processor.shape_record(row, config.output.shaping)
            for row in rows
        ]

    def _new_diagnostics(self) -> dict[str, Any]:
        return {
            "non_fatal_issue_counts": {},
            "non_fatal_issue_messages": {},
        }

    def _record_diagnostic(
        self,
        diagnostics: dict[str, Any],
        category: str,
        message: str,
    ) -> None:
        issue_counts = diagnostics["non_fatal_issue_counts"]
        issue_counts[category] = issue_counts.get(category, 0) + 1

        issue_messages = diagnostics["non_fatal_issue_messages"]
        issue_messages[message] = issue_messages.get(message, 0) + 1

    def _merge_diagnostics(self, diagnostics: dict[str, Any]) -> dict[str, Any]:
        issue_messages = sorted(
            diagnostics["non_fatal_issue_messages"].items(),
            key=lambda item: (-item[1], item[0]),
        )
        merged = dict(self.post_processor.get_diagnostics())
        merged["non_fatal_issue_counts"] = dict(
            sorted(diagnostics["non_fatal_issue_counts"].items())
        )
        merged["non_fatal_issue_messages"] = [
            {"message": message, "count": count}
            for message, count in issue_messages[:10]
        ]
        return merged

    def _new_limits_state(self, config: EngineConfig) -> dict[str, Any]:
        return {
            "max_records": config.limits.max_records,
            "max_detail_pages": config.limits.max_detail_pages,
            "max_records_hit": False,
            "max_detail_pages_hit": False,
            "records_truncated": 0,
            "detail_pages_skipped_due_to_limit": 0,
        }

    def _cap_records(
        self,
        records: list[dict[str, Any]],
        current_count: int,
        config: EngineConfig,
        limits_state: dict[str, Any],
        diagnostics: dict[str, Any],
        notes: list[str],
        context: str,
    ) -> list[dict[str, Any]]:
        max_records = config.limits.max_records
        if max_records is None:
            return records

        remaining = max_records - current_count
        if remaining <= 0:
            limits_state["records_truncated"] += len(records)
            self._mark_max_records_hit(config, limits_state, diagnostics, notes, context)
            return []

        if len(records) <= remaining:
            return records

        limits_state["records_truncated"] += len(records) - remaining
        self._mark_max_records_hit(config, limits_state, diagnostics, notes, context)
        return records[:remaining]

    def _mark_max_records_hit(
        self,
        config: EngineConfig,
        limits_state: dict[str, Any],
        diagnostics: dict[str, Any],
        notes: list[str],
        context: str,
    ) -> None:
        self._record_diagnostic(
            diagnostics,
            "max_records_reached",
            f"Record output was capped at {config.limits.max_records} during {context}.",
        )
        if limits_state["max_records_hit"]:
            return

        limits_state["max_records_hit"] = True
        notes.append(
            f"Record output capped at {config.limits.max_records}; additional records were skipped."
        )
        log_event(
            self.logger,
            logging.WARNING,
            "RUN LIMIT",
            "Reached configured max_records limit.",
            max_records=config.limits.max_records,
            context=context,
        )

    def _detail_page_limit_reached(
        self,
        config: EngineConfig,
        detail_pages_attempted: int,
    ) -> bool:
        max_detail_pages = config.limits.max_detail_pages
        if max_detail_pages is None:
            return False
        return detail_pages_attempted >= max_detail_pages

    def _mark_detail_page_limit_hit(
        self,
        config: EngineConfig,
        limits_state: dict[str, Any],
        diagnostics: dict[str, Any],
        notes: list[str],
    ) -> None:
        if limits_state["max_detail_pages_hit"]:
            return

        limits_state["max_detail_pages_hit"] = True
        notes.append(
            "Detail page enrichment was capped by max_detail_pages; remaining rows kept base fields only."
        )
        log_event(
            self.logger,
            logging.WARNING,
            "RUN LIMIT",
            "Reached configured max_detail_pages limit.",
            max_detail_pages=config.limits.max_detail_pages,
        )
        self._record_diagnostic(
            diagnostics,
            "max_detail_pages_reached",
            f"Detail page enrichment was capped at {config.limits.max_detail_pages}.",
        )

    def _max_records_reached(self, limits_state: dict[str, Any]) -> bool:
        return bool(limits_state["max_records_hit"])

    def _build_limits_report(
        self,
        config: EngineConfig,
        limits_state: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "max_records": config.limits.max_records,
            "max_detail_pages": config.limits.max_detail_pages,
            "max_records_hit": limits_state["max_records_hit"],
            "max_detail_pages_hit": limits_state["max_detail_pages_hit"],
            "records_truncated": limits_state["records_truncated"],
            "detail_pages_skipped_due_to_limit": limits_state["detail_pages_skipped_due_to_limit"],
        }
