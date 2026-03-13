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
    RecordSelectorConfig,
    RequestConfig,
    RunContext,
    SyncConfig,
)
from scraper_engine.core.job_runner import JobRunner
from scraper_engine.extractors.registry import ExtractorRegistry
from scraper_engine.schemas.mapper import SchemaMapper


DIRECTORY_HTML = """
<html>
  <body>
    <div class="profile-card">
      <h2 class="profile-name">Jane Smith</h2>
      <span class="firm-name">Smith Law</span>
      <a class="profile" href="/profiles/jane-smith">Profile</a>
    </div>
    <div class="profile-card">
      <h2 class="profile-name">John Doe</h2>
      <span class="firm-name">Doe Legal</span>
      <a class="profile" href="/profiles/john-doe">Profile</a>
    </div>
  </body>
</html>
"""

DETAIL_HTML_SUCCESS = """
<html>
  <body>
    <a href="mailto:jane@example.com">Email</a>
    <a class="website" href="https://smithlaw.example.com">Website</a>
    <div class="address">123 Main St, Austin, TX</div>
    <div class="profile-bio">Trusted business attorney.</div>
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
        raise AssertionError("directory_detail should not use the site_scan crawler")


class DirectoryDetailMilestoneTests(unittest.TestCase):
    def test_config_normalization_supports_detail_page_dict_style(self) -> None:
        normalized = normalize_config_payload(
            {
                "name": "law_firms",
                "mode": "directory_detail",
                "record_selector": ".profile-card",
                "fields": {
                    "attorney_name": {"css": ".profile-name"},
                    "detail_url": {"css": "a.profile", "attr": "href"},
                },
                "detail_page": {
                    "enabled": True,
                    "url_field": "detail_url",
                    "fields": {
                        "email": {"css": "a[href^='mailto:']", "attr": "href"},
                        "address": {"css": ".address", "attr": "text"},
                    },
                },
            }
        )

        self.assertEqual("detail_url", normalized["extraction"]["detail_page"]["url_field"])
        self.assertEqual(
            "a[href^='mailto:']",
            normalized["extraction"]["detail_page"]["fields"][0]["selector"],
        )

    def test_directory_detail_enriches_records_and_keeps_base_rows_on_failure(self) -> None:
        config = EngineConfig(
            name="law_firms",
            mode="directory_detail",
            requests=RequestConfig(),
            extraction=ExtractionConfig(
                record_selector=RecordSelectorConfig(css=".profile-card"),
                fields=[
                    ExtractorFieldConfig(name="attorney_name", selector=".profile-name"),
                    ExtractorFieldConfig(name="firm_name", selector=".firm-name"),
                    ExtractorFieldConfig(name="detail_url", selector="a.profile", attribute="href"),
                ],
                detail_page=DetailPageConfig(
                    enabled=True,
                    url_field="detail_url",
                    fields=[
                        ExtractorFieldConfig(name="email", selector="a[href^='mailto:']", attribute="href"),
                        ExtractorFieldConfig(name="website", selector="a.website", attribute="href"),
                        ExtractorFieldConfig(name="address", selector=".address"),
                        ExtractorFieldConfig(name="bio", selector=".profile-bio"),
                    ],
                ),
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
                    html=DIRECTORY_HTML,
                ),
                "https://example.com/profiles/jane-smith": CrawlPage(
                    requested_url="https://example.com/profiles/jane-smith",
                    url="https://example.com/profiles/jane-smith",
                    status_code=200,
                    html=DETAIL_HTML_SUCCESS,
                ),
                "https://example.com/profiles/john-doe": CrawlPage(
                    requested_url="https://example.com/profiles/john-doe",
                    url="https://example.com/profiles/john-doe",
                    error="404 Client Error",
                ),
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            run_context = RunContext(
                run_name="test_detail",
                started_at=datetime.now(timezone.utc),
                output_root=Path(temp_dir),
                output_dir=Path(temp_dir),
                log_file=Path(temp_dir) / "run.log",
                raw_pages_dir=None,
            )
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
                run_context=run_context,
            )

        self.assertEqual(2, len(rows))
        self.assertEqual("mailto:jane@example.com", rows[0]["email"])
        self.assertEqual("https://example.com/profiles/jane-smith", rows[0]["detail_url"])
        self.assertEqual("John Doe", rows[1]["attorney_name"])
        self.assertEqual("https://example.com/profiles/john-doe", rows[1]["detail_url"])
        self.assertNotIn("email", rows[1])
        self.assertEqual(2, report["listing_count"])
        self.assertEqual(2, report["detail_pages_attempted"])
        self.assertEqual(1, report["detail_pages_successful"])
        self.assertEqual(1, report["detail_pages_failed"])


if __name__ == "__main__":
    unittest.main()
