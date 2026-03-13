from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from scraper_engine.core.models import (
    CrawlPage,
    DetailPageConfig,
    EngineConfig,
    ExtractionConfig,
    ExtractorFieldConfig,
    LoggingConfig,
    OutputConfig,
    RequestConfig,
    RunContext,
    SyncConfig,
)
from scraper_engine.core.job_runner import JobRunner
from scraper_engine.extractors.registry import ExtractorRegistry
from scraper_engine.schemas.mapper import SchemaMapper
from scraper_engine.utils.playwright_directory import PlaywrightDirectoryResult


DETAIL_HTML_ONE = """
<html>
  <body>
    <main>
      <h1>Jane Smith</h1>
      <div class="firm">Smith Law</div>
      <div class="location">Baltimore, MD</div>
      <a href="https://smithlaw.example.com">Website</a>
      <a href="tel:4105550100">Phone</a>
    </main>
  </body>
</html>
"""

DETAIL_HTML_TWO = """
<html>
  <body>
    <main>
      <h1>John Doe</h1>
      <div class="firm">Doe Legal Group</div>
      <div class="location">Rockville, MD</div>
    </main>
  </body>
</html>
"""


class DummyFetcher:
    def fetch(self, url: str) -> CrawlPage:
        raise AssertionError("Playwright directory_detail should not use the standard fetcher")


class DummyCrawler:
    def crawl(self, *args, **kwargs):
        raise AssertionError("Playwright directory_detail should not use the site_scan crawler")


class FakePlaywrightDirectoryClient:
    detail_fetches: list[str] = []

    def __init__(self, settings, user_agent=None, logger=None) -> None:
        self.settings = settings
        self.user_agent = user_agent
        self.logger = logger

    def __enter__(self):
        type(self).detail_fetches = []
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def open_directory(self, start_url: str) -> PlaywrightDirectoryResult:
        return PlaywrightDirectoryResult(
            directory_page=CrawlPage(
                requested_url=start_url,
                url=start_url,
                status_code=200,
                html="<html><body>Rendered directory</body></html>",
            ),
            profile_urls=[
                "https://example.com/members/jane-smith",
                "https://example.com/members/john-doe",
            ],
            cookie_accepted=True,
            load_more_clicks=4,
            stop_reason="load_more_not_found",
        )

    def fetch_detail_page(self, url: str) -> CrawlPage:
        type(self).detail_fetches.append(url)
        detail_html = DETAIL_HTML_ONE if "jane-smith" in url else DETAIL_HTML_TWO
        return CrawlPage(
            requested_url=url,
            url=url,
            status_code=200,
            html=detail_html,
        )


class PrivatePlaywrightDirectoryDetailTests(unittest.TestCase):
    def test_playwright_renderer_requires_private_metadata(self) -> None:
        config = EngineConfig(
            name="not_private",
            mode="directory_detail",
            requests=RequestConfig(),
            extraction=ExtractionConfig(
                fields=[],
                detail_page=DetailPageConfig(
                    enabled=True,
                    url_field="detail_url",
                    fields=[ExtractorFieldConfig(name="name", selector="h1")],
                ),
            ),
            output=OutputConfig(merge_rows=False),
            logging=LoggingConfig(),
            sync=SyncConfig(),
            metadata={
                "renderer": "playwright",
                "playwright": {"profile_link_selector": "a[href]"},
            },
        )
        runner = JobRunner(
            fetcher=DummyFetcher(),
            crawler=DummyCrawler(),
            extractor_registry=ExtractorRegistry(),
            mapper=SchemaMapper(),
            logger=None,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "reserved for private configs"):
                runner.run(
                    targets=["https://example.com/directory"],
                    config=config,
                    run_context=RunContext(
                        run_name="private_guard",
                        started_at=datetime.now(timezone.utc),
                        output_root=Path(temp_dir),
                        output_dir=Path(temp_dir),
                        log_file=Path(temp_dir) / "run.log",
                        raw_pages_dir=None,
                    ),
                )

    def test_playwright_directory_detail_uses_rendered_profile_urls(self) -> None:
        config = EngineConfig(
            name="private_playwright_directory",
            mode="directory_detail",
            requests=RequestConfig(),
            extraction=ExtractionConfig(
                fields=[],
                detail_page=DetailPageConfig(
                    enabled=True,
                    url_field="detail_url",
                    fields=[
                        ExtractorFieldConfig(name="name", selector="h1"),
                        ExtractorFieldConfig(name="law_firm", selector=".firm"),
                        ExtractorFieldConfig(name="city", selector=".location"),
                        ExtractorFieldConfig(name="website", selector="a[href^='https://']", attribute="href"),
                        ExtractorFieldConfig(name="phone", selector="a[href^='tel:']", attribute="href"),
                    ],
                ),
            ),
            output=OutputConfig(merge_rows=False),
            logging=LoggingConfig(),
            sync=SyncConfig(),
            metadata={
                "audience": "private",
                "private_only": True,
                "renderer": "playwright",
                "playwright": {
                    "profile_link_selector": "a[href*='/members/']",
                    "use_profile_urls_as_records": True,
                },
            },
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            runner = JobRunner(
                fetcher=DummyFetcher(),
                crawler=DummyCrawler(),
                extractor_registry=ExtractorRegistry(),
                mapper=SchemaMapper(),
                logger=None,
            )
            with patch(
                "scraper_engine.core.job_runner.PlaywrightDirectoryClient",
                FakePlaywrightDirectoryClient,
            ):
                rows, report = runner.run(
                    targets=["https://example.com/directory"],
                    config=config,
                    run_context=RunContext(
                        run_name="private_playwright_directory",
                        started_at=datetime.now(timezone.utc),
                        output_root=Path(temp_dir),
                        output_dir=Path(temp_dir),
                        log_file=Path(temp_dir) / "run.log",
                        raw_pages_dir=None,
                    ),
                )

        self.assertEqual(2, len(rows))
        self.assertEqual(
            [
                "https://example.com/members/jane-smith",
                "https://example.com/members/john-doe",
            ],
            FakePlaywrightDirectoryClient.detail_fetches,
        )
        self.assertEqual(
            "https://example.com/members/jane-smith",
            rows[0]["profile_url"],
        )
        self.assertEqual(
            "https://example.com/directory",
            rows[0]["source_directory"],
        )
        self.assertEqual("Jane Smith", rows[0]["name"])
        self.assertEqual("Smith Law", rows[0]["law_firm"])
        self.assertEqual("Baltimore, MD", rows[0]["city"])
        self.assertEqual("https://smithlaw.example.com", rows[0]["website"])
        self.assertEqual("tel:4105550100", rows[0]["phone"])
        self.assertEqual(2, report["listing_count"])
        self.assertEqual(2, report["detail_pages_attempted"])
        self.assertEqual(2, report["detail_pages_successful"])
        self.assertEqual(["load_more_not_found"], report["pagination_stopped_reasons"])
        self.assertIn("profile_url", report["extracted_field_names"])
        self.assertIn("source_directory", report["extracted_field_names"])


if __name__ == "__main__":
    unittest.main()
