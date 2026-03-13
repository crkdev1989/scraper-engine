from __future__ import annotations

import csv
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from scraper_engine.core.config_loader import normalize_config_payload
from scraper_engine.core.models import (
    CrawlPage,
    CrawlResult,
    DetailPageConfig,
    EngineConfig,
    ExtractionConfig,
    ExtractorFieldConfig,
    LoggingConfig,
    OutputConfig,
    RecordSelectorConfig,
    RequestConfig,
    RunContext,
    RunLimitsConfig,
    SyncConfig,
)
from scraper_engine.core.job_runner import JobRunner
from scraper_engine.extractors.registry import ExtractorRegistry
from scraper_engine.outputs.reporters import write_summary
from scraper_engine.outputs.writers import write_csv, write_json
from scraper_engine.schemas.mapper import SchemaMapper


SITE_SCAN_PAGE_ONE = """
<html>
  <head><title>Example Company</title></head>
  <body>
    Contact us at hello@example.com
  </body>
</html>
"""

SITE_SCAN_PAGE_TWO = """
<html>
  <head><title>Contact Example Company</title></head>
  <body>
    Contact us at hello@example.com and sales@example.com
  </body>
</html>
"""

DIRECTORY_LIST_HTML = """
<html>
  <body>
    <div class="listing-card"><h2 class="listing-title">One Co</h2></div>
    <div class="listing-card"><h2 class="listing-title">Two Co</h2></div>
    <div class="listing-card"><h2 class="listing-title">Three Co</h2></div>
  </body>
</html>
"""

DIRECTORY_DETAIL_HTML = """
<html>
  <body>
    <div class="profile-card">
      <h2 class="profile-name">Jane Smith</h2>
      <a class="profile" href="/profiles/jane-smith">Profile</a>
    </div>
    <div class="profile-card">
      <h2 class="profile-name">John Doe</h2>
      <a class="profile" href="/profiles/john-doe">Profile</a>
    </div>
  </body>
</html>
"""

DETAIL_HTML_ONE = """
<html><body><a href="mailto:jane@example.com">Email</a></body></html>
"""

DETAIL_HTML_TWO = """
<html><body><a href="mailto:john@example.com">Email</a></body></html>
"""


class StaticFetcher:
    def __init__(self, page: CrawlPage) -> None:
        self.page = page

    def fetch(self, url: str) -> CrawlPage:
        return CrawlPage(
            requested_url=url,
            url=self.page.url,
            status_code=self.page.status_code,
            html=self.page.html,
            error=self.page.error,
        )


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
        raise AssertionError("directory tests should not use the site_scan crawler")


class FakeCrawler:
    def crawl(self, target: str, crawl_config, run_context) -> CrawlResult:
        return CrawlResult(
            seed_url=target,
            pages=[
                CrawlPage(
                    requested_url=target,
                    url="https://example.com",
                    status_code=200,
                    html=SITE_SCAN_PAGE_ONE,
                ),
                CrawlPage(
                    requested_url="https://example.com/contact",
                    url="https://example.com/contact",
                    status_code=200,
                    html=SITE_SCAN_PAGE_TWO,
                ),
            ],
        )


class LargeRunSafetyMilestoneTests(unittest.TestCase):
    def test_config_normalization_supports_limits_block(self) -> None:
        normalized = normalize_config_payload(
            {
                "name": "limits_test",
                "mode": "directory_detail",
                "limits": {
                    "max_records": 25,
                    "max_detail_pages": 10,
                },
            }
        )

        self.assertEqual(25, normalized["limits"]["max_records"])
        self.assertEqual(10, normalized["limits"]["max_detail_pages"])

    def test_site_scan_regression_still_merges_results(self) -> None:
        config = EngineConfig(
            name="site_scan_test",
            mode="site_scan",
            requests=RequestConfig(),
            extraction=ExtractionConfig(
                fields=[
                    ExtractorFieldConfig(name="page_title", type="page_title"),
                    ExtractorFieldConfig(name="emails", type="emails", many=True),
                ]
            ),
            output=OutputConfig(merge_rows=True),
            logging=LoggingConfig(),
            sync=SyncConfig(),
        )

        runner = JobRunner(
            fetcher=None,
            crawler=FakeCrawler(),
            extractor_registry=ExtractorRegistry(),
            mapper=SchemaMapper(),
            logger=None,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            rows, report = runner.run(
                targets=["https://example.com"],
                config=config,
                run_context=RunContext(
                    run_name="site_scan_regression",
                    started_at=datetime.now(timezone.utc),
                    output_root=Path(temp_dir),
                    output_dir=Path(temp_dir),
                    log_file=Path(temp_dir) / "run.log",
                    raw_pages_dir=None,
                ),
            )

        self.assertEqual(1, len(rows))
        self.assertEqual(1, report["row_count"])
        self.assertEqual(
            ["https://example.com", "https://example.com/contact"],
            rows[0]["source_urls"],
        )
        self.assertEqual(
            ["hello@example.com", "sales@example.com"],
            rows[0]["emails"],
        )

    def test_directory_list_max_records_caps_output_and_reports_limit(self) -> None:
        config = EngineConfig(
            name="directory_limit_test",
            mode="directory_list",
            requests=RequestConfig(),
            extraction=ExtractionConfig(
                record_selector=RecordSelectorConfig(css=".listing-card"),
                fields=[ExtractorFieldConfig(name="business_name", selector=".listing-title")],
            ),
            limits=RunLimitsConfig(max_records=2),
            output=OutputConfig(merge_rows=False),
            logging=LoggingConfig(),
            sync=SyncConfig(),
        )

        fetcher = StaticFetcher(
            CrawlPage(
                requested_url="https://example.com/directory",
                url="https://example.com/directory",
                status_code=200,
                html=DIRECTORY_LIST_HTML,
            )
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
                    run_name="directory_limit",
                    started_at=datetime.now(timezone.utc),
                    output_root=Path(temp_dir),
                    output_dir=Path(temp_dir),
                    log_file=Path(temp_dir) / "run.log",
                    raw_pages_dir=None,
                ),
            )

        self.assertEqual(2, len(rows))
        self.assertEqual(2, report["row_count"])
        self.assertTrue(report["limits"]["max_records_hit"])
        self.assertEqual(1, report["limits"]["records_truncated"])
        self.assertEqual(1, report["diagnostics"]["non_fatal_issue_counts"]["max_records_reached"])

    def test_directory_detail_max_detail_pages_keeps_base_rows(self) -> None:
        config = EngineConfig(
            name="detail_limit_test",
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
                        ExtractorFieldConfig(name="email", selector="a[href^='mailto:']", attribute="href"),
                    ],
                ),
            ),
            limits=RunLimitsConfig(max_detail_pages=1),
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
                    html=DIRECTORY_DETAIL_HTML,
                ),
                "https://example.com/profiles/jane-smith": CrawlPage(
                    requested_url="https://example.com/profiles/jane-smith",
                    url="https://example.com/profiles/jane-smith",
                    status_code=200,
                    html=DETAIL_HTML_ONE,
                ),
                "https://example.com/profiles/john-doe": CrawlPage(
                    requested_url="https://example.com/profiles/john-doe",
                    url="https://example.com/profiles/john-doe",
                    status_code=200,
                    html=DETAIL_HTML_TWO,
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
                    run_name="detail_limit",
                    started_at=datetime.now(timezone.utc),
                    output_root=Path(temp_dir),
                    output_dir=Path(temp_dir),
                    log_file=Path(temp_dir) / "run.log",
                    raw_pages_dir=None,
                ),
            )

        self.assertEqual(2, len(rows))
        self.assertEqual(1, report["detail_pages_attempted"])
        self.assertEqual(1, report["detail_pages_successful"])
        self.assertTrue(report["limits"]["max_detail_pages_hit"])
        self.assertEqual(1, report["limits"]["detail_pages_skipped_due_to_limit"])
        self.assertEqual("mailto:jane@example.com", rows[0]["email"])
        self.assertNotIn("email", rows[1])

    def test_atomic_writers_overwrite_existing_files_with_valid_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            json_path = output_dir / "results.json"
            csv_path = output_dir / "results.csv"
            summary_path = output_dir / "summary.txt"

            json_path.write_text("broken", encoding="utf-8")
            csv_path.write_text("broken", encoding="utf-8")

            rows = [
                {"name": "Alpha", "emails": ["a@example.com", "b@example.com"]},
                {"name": "Beta", "emails": []},
            ]
            write_json(json_path, rows)
            write_csv(csv_path, rows, preferred_columns=["name", "emails"])
            write_summary(
                summary_path,
                {
                    "run_name": "writer_test",
                    "config_name": "demo",
                    "mode": "directory_list",
                    "input_targets": [],
                    "target_count": 1,
                    "duration_seconds": 0.1,
                    "duration": "0.100s",
                    "pages_crawled": 1,
                    "pages_visited": 1,
                    "pages_succeeded": 1,
                    "pages_failed": 0,
                    "listing_count": 2,
                    "pagination_urls_followed": 0,
                    "pagination_stopped_reasons": [],
                    "detail_pages_attempted": 0,
                    "detail_pages_successful": 0,
                    "detail_pages_failed": 0,
                    "row_count": 2,
                    "extracted_field_names": ["name", "emails"],
                    "sync_attempted": False,
                    "sync_success": False,
                    "errors": [],
                    "notes": [],
                    "diagnostics": {},
                    "quality_metrics": {},
                    "limits": {},
                },
            )

            json_payload = json.loads(json_path.read_text(encoding="utf-8"))
            with csv_path.open("r", encoding="utf-8", newline="") as handle:
                csv_rows = list(csv.DictReader(handle))
            summary_text = summary_path.read_text(encoding="utf-8")

        self.assertEqual(2, len(json_payload))
        self.assertEqual("Alpha", json_payload[0]["name"])
        self.assertEqual("a@example.com; b@example.com", csv_rows[0]["emails"])
        self.assertIn("Run Limits:", summary_text)


if __name__ == "__main__":
    unittest.main()
