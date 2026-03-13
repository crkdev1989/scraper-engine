from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from scraper_engine.core.config_loader import normalize_config_payload
from scraper_engine.core.models import (
    CrawlPage,
    DetailPageConfig,
    EngineConfig,
    ExtractionConfig,
    ExtractorFieldConfig,
    LoggingConfig,
    OutputConfig,
    PaginationConfig,
    RecordSelectorConfig,
    RequestConfig,
    RunContext,
    SyncConfig,
    NextPageConfig,
)
from scraper_engine.core.job_runner import JobRunner
from scraper_engine.extractors.registry import ExtractorRegistry
from scraper_engine.schemas.mapper import SchemaMapper


PAGE_ONE_HTML = """
<html>
  <body>
    <div class="listing-card">
      <h2 class="listing-title">Alpha Realty</h2>
    </div>
    <a class="next" href="/directory?page=2">Next</a>
  </body>
</html>
"""

PAGE_TWO_HTML = """
<html>
  <body>
    <div class="listing-card">
      <h2 class="listing-title">Beta Realty</h2>
    </div>
  </body>
</html>
"""

LOOP_PAGE_TWO_HTML = """
<html>
  <body>
    <div class="listing-card">
      <h2 class="listing-title">Beta Realty</h2>
    </div>
    <a class="next" href="/directory">Back</a>
  </body>
</html>
"""

DETAIL_PAGE_ONE_HTML = """
<html><body><a href="mailto:alpha@example.com">Email</a></body></html>
"""

DETAIL_PAGE_TWO_HTML = """
<html><body><a href="mailto:beta@example.com">Email</a></body></html>
"""

DETAIL_DIRECTORY_PAGE_ONE = """
<html>
  <body>
    <div class="profile-card">
      <h2 class="profile-name">Jane Smith</h2>
      <a class="profile" href="/profiles/jane-smith">Profile</a>
    </div>
    <a class="next" href="/directory?page=2">Next</a>
  </body>
</html>
"""

DETAIL_DIRECTORY_PAGE_TWO = """
<html>
  <body>
    <div class="profile-card">
      <h2 class="profile-name">John Doe</h2>
      <a class="profile" href="/profiles/john-doe">Profile</a>
    </div>
  </body>
</html>
"""


class MappingFetcher:
    def __init__(self, pages: dict[str, CrawlPage]) -> None:
        self.pages = pages

    def fetch(self, url: str) -> CrawlPage:
        page = self.pages[url]
        return CrawlPage(
            requested_url=url,
            url=page.url,
            status_code=page.status_code,
            html=page.html,
            error=page.error,
        )


class DummyCrawler:
    def crawl(self, *args, **kwargs):
        raise AssertionError("directory pagination tests should not use the site_scan crawler")


class DirectoryPaginationMilestoneTests(unittest.TestCase):
    def test_config_normalization_supports_pagination_block(self) -> None:
        normalized = normalize_config_payload(
            {
                "name": "paginated_directory",
                "mode": "directory_list",
                "record_selector": ".listing-card",
                "fields": {"business_name": {"css": ".listing-title"}},
                "pagination": {
                    "enabled": True,
                    "max_pages": 5,
                    "next_page": {"css": "a.next", "attr": "href"},
                },
            }
        )

        self.assertTrue(normalized["pagination"]["enabled"])
        self.assertEqual(5, normalized["pagination"]["max_pages"])
        self.assertEqual("a.next", normalized["pagination"]["next_page"]["css"])
        self.assertEqual("href", normalized["pagination"]["next_page"]["attribute"])

    def test_directory_list_pagination_collects_rows_from_multiple_pages(self) -> None:
        config = EngineConfig(
            name="paginated_directory",
            mode="directory_list",
            requests=RequestConfig(),
            extraction=ExtractionConfig(
                record_selector=RecordSelectorConfig(css=".listing-card"),
                fields=[ExtractorFieldConfig(name="business_name", selector=".listing-title")],
            ),
            pagination=PaginationConfig(
                enabled=True,
                max_pages=5,
                next_page=NextPageConfig(css="a.next", attribute="href"),
            ),
            output=OutputConfig(merge_rows=False),
            logging=LoggingConfig(),
            sync=SyncConfig(),
        )
        fetcher = MappingFetcher(
            {
                "https://example.com/directory": CrawlPage(
                    requested_url="https://example.com/directory",
                    url="https://example.com/directory",
                    status_code=200,
                    html=PAGE_ONE_HTML,
                ),
                "https://example.com/directory?page=2": CrawlPage(
                    requested_url="https://example.com/directory?page=2",
                    url="https://example.com/directory?page=2",
                    status_code=200,
                    html=PAGE_TWO_HTML,
                ),
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            runner = JobRunner(
                fetcher=fetcher,
                crawler=DummyCrawler(),
                extractor_registry=ExtractorRegistry(),
                mapper=SchemaMapper(),
                logger=None,
            )
            rows, report = runner.run(
                targets=["https://example.com/directory"],
                config=config,
                run_context=RunContext(
                    run_name="test_paginated_directory",
                    started_at=datetime.now(timezone.utc),
                    output_root=Path(temp_dir),
                    output_dir=Path(temp_dir),
                    log_file=Path(temp_dir) / "run.log",
                    raw_pages_dir=None,
                ),
            )

        self.assertEqual(2, len(rows))
        self.assertEqual(2, report["pages_visited"])
        self.assertEqual(1, report["pagination_urls_followed"])
        self.assertEqual(["no_next_page_found"], report["pagination_stopped_reasons"])
        self.assertEqual("https://example.com/directory?page=2", rows[1]["source_url"])

    def test_directory_list_pagination_stops_on_duplicate_next_page(self) -> None:
        config = EngineConfig(
            name="paginated_directory",
            mode="directory_list",
            requests=RequestConfig(),
            extraction=ExtractionConfig(
                record_selector=RecordSelectorConfig(css=".listing-card"),
                fields=[ExtractorFieldConfig(name="business_name", selector=".listing-title")],
            ),
            pagination=PaginationConfig(
                enabled=True,
                max_pages=5,
                next_page=NextPageConfig(css="a.next", attribute="href"),
            ),
            output=OutputConfig(merge_rows=False),
            logging=LoggingConfig(),
            sync=SyncConfig(),
        )
        fetcher = MappingFetcher(
            {
                "https://example.com/directory": CrawlPage(
                    requested_url="https://example.com/directory",
                    url="https://example.com/directory",
                    status_code=200,
                    html=PAGE_ONE_HTML,
                ),
                "https://example.com/directory?page=2": CrawlPage(
                    requested_url="https://example.com/directory?page=2",
                    url="https://example.com/directory?page=2",
                    status_code=200,
                    html=LOOP_PAGE_TWO_HTML,
                ),
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            runner = JobRunner(
                fetcher=fetcher,
                crawler=DummyCrawler(),
                extractor_registry=ExtractorRegistry(),
                mapper=SchemaMapper(),
                logger=None,
            )
            _, report = runner.run(
                targets=["https://example.com/directory"],
                config=config,
                run_context=RunContext(
                    run_name="test_paginated_directory",
                    started_at=datetime.now(timezone.utc),
                    output_root=Path(temp_dir),
                    output_dir=Path(temp_dir),
                    log_file=Path(temp_dir) / "run.log",
                    raw_pages_dir=None,
                ),
            )

        self.assertEqual(["duplicate_next_page_detected"], report["pagination_stopped_reasons"])
        self.assertEqual(2, report["pages_visited"])

    def test_directory_detail_pagination_enriches_records_from_multiple_pages(self) -> None:
        config = EngineConfig(
            name="law_firms",
            mode="directory_detail",
            requests=RequestConfig(),
            extraction=ExtractionConfig(
                record_selector=RecordSelectorConfig(css=".profile-card"),
                fields=[
                    ExtractorFieldConfig(name="attorney_name", selector=".profile-name"),
                    ExtractorFieldConfig(name="detail_url", selector="a.profile", attribute="href"),
                ],
                detail_page=DetailPageConfig(
                    enabled=True,
                    url_field="detail_url",
                    fields=[
                        ExtractorFieldConfig(name="email", selector="a[href^='mailto:']", attribute="href")
                    ],
                ),
            ),
            pagination=PaginationConfig(
                enabled=True,
                max_pages=5,
                next_page=NextPageConfig(css="a.next", attribute="href"),
            ),
            output=OutputConfig(merge_rows=False),
            logging=LoggingConfig(),
            sync=SyncConfig(),
        )
        fetcher = MappingFetcher(
            {
                "https://example.com/directory": CrawlPage(
                    requested_url="https://example.com/directory",
                    url="https://example.com/directory",
                    status_code=200,
                    html=DETAIL_DIRECTORY_PAGE_ONE,
                ),
                "https://example.com/directory?page=2": CrawlPage(
                    requested_url="https://example.com/directory?page=2",
                    url="https://example.com/directory?page=2",
                    status_code=200,
                    html=DETAIL_DIRECTORY_PAGE_TWO,
                ),
                "https://example.com/profiles/jane-smith": CrawlPage(
                    requested_url="https://example.com/profiles/jane-smith",
                    url="https://example.com/profiles/jane-smith",
                    status_code=200,
                    html=DETAIL_PAGE_ONE_HTML,
                ),
                "https://example.com/profiles/john-doe": CrawlPage(
                    requested_url="https://example.com/profiles/john-doe",
                    url="https://example.com/profiles/john-doe",
                    status_code=200,
                    html=DETAIL_PAGE_TWO_HTML,
                ),
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            runner = JobRunner(
                fetcher=fetcher,
                crawler=DummyCrawler(),
                extractor_registry=ExtractorRegistry(),
                mapper=SchemaMapper(),
                logger=None,
            )
            rows, report = runner.run(
                targets=["https://example.com/directory"],
                config=config,
                run_context=RunContext(
                    run_name="test_paginated_detail",
                    started_at=datetime.now(timezone.utc),
                    output_root=Path(temp_dir),
                    output_dir=Path(temp_dir),
                    log_file=Path(temp_dir) / "run.log",
                    raw_pages_dir=None,
                ),
            )

        self.assertEqual(2, len(rows))
        self.assertEqual(2, report["pages_visited"])
        self.assertEqual(4, report["pages_crawled"])
        self.assertEqual(2, report["detail_pages_attempted"])
        self.assertEqual(2, report["detail_pages_successful"])
        self.assertEqual(0, report["detail_pages_failed"])
        self.assertEqual("mailto:alpha@example.com", rows[0]["email"])
        self.assertEqual("mailto:beta@example.com", rows[1]["email"])


if __name__ == "__main__":
    unittest.main()
