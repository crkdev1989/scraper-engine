from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from scraper_engine.core.job_runner import JobRunner
from scraper_engine.core.models import (
    CrawlConfig,
    CrawlPage,
    CrawlResult,
    EngineConfig,
    ExtractionConfig,
    ExtractorFieldConfig,
    LoggingConfig,
    OutputConfig,
    RequestConfig,
    RunContext,
    RuntimeOptions,
    SyncConfig,
)
from scraper_engine.core.pipeline import Pipeline
from scraper_engine.crawl.crawler import Crawler
from scraper_engine.extractors.registry import ExtractorRegistry
from scraper_engine.inputs.loaders import load_target_contexts, load_targets
from scraper_engine.outputs.lead_cleaner import clean_lead_rows
from scraper_engine.schemas.mapper import SchemaMapper


HOME_HTML = """
<html>
  <body>
    <a href="/contact">Contact Us</a>
    <a href="mailto:jane@firm-example.com">Email Jane</a>
  </body>
</html>
"""

CONTACT_HTML = """
<html>
  <body>
    <p>Contact our office at info@firm-example.com</p>
    <p>Call us at (410) 555-0100</p>
  </body>
</html>
"""


class DummyFetcher:
    def fetch(self, url: str) -> CrawlPage:
        raise AssertionError("site_scan summary tests should use the fake crawler directly")


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


class FakeCrawler:
    def crawl(self, seed_url: str, crawl_config: CrawlConfig, run_context: RunContext) -> CrawlResult:
        return CrawlResult(
            seed_url=seed_url,
            pages=[
                CrawlPage(
                    requested_url=seed_url,
                    url=seed_url,
                    status_code=200,
                    html=HOME_HTML,
                ),
                CrawlPage(
                    requested_url=f"{seed_url}/contact",
                    url=f"{seed_url}/contact",
                    status_code=200,
                    html=CONTACT_HTML,
                ),
            ],
        )


class PrivateCsvContactEnrichmentTests(unittest.TestCase):
    def test_csv_loader_supports_configured_website_column_and_contexts(self) -> None:
        config = EngineConfig(
            name="website_contact_csv_enrich",
            mode="site_scan",
            requests=RequestConfig(),
            extraction=ExtractionConfig(),
            output=OutputConfig(),
            logging=LoggingConfig(),
            sync=SyncConfig(),
            metadata={
                "input_csv": {
                    "url_column": "website",
                    "preserve_columns": ["name", "law_firm", "website"],
                }
            },
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "input.csv"
            csv_path.write_text(
                "name,law_firm,website\n"
                "Jane Smith,Smith Law,example.com\n"
                "John Doe,Doe Legal,https://example.org/contact\n",
                encoding="utf-8",
            )
            options = RuntimeOptions(config_path=Path("dummy.yaml"), input_path=csv_path)

            targets = load_targets(options, config)
            contexts = load_target_contexts(options, config)

        self.assertEqual(
            ["https://example.com/", "https://example.org/contact"],
            targets,
        )
        self.assertEqual(
            [{"name": "Jane Smith", "law_firm": "Smith Law", "website": "example.com"}],
            contexts["https://example.com/"],
        )

    def test_site_scan_contact_discovery_adds_outreach_summary_fields(self) -> None:
        config = EngineConfig(
            name="website_contact_csv_enrich",
            mode="site_scan",
            requests=RequestConfig(),
            crawl=CrawlConfig(mode="site_scan", max_pages=5),
            extraction=ExtractionConfig(
                fields=[
                    ExtractorFieldConfig(name="page_title", type="page_title"),
                    ExtractorFieldConfig(name="contact_pages", type="contact_links", many=True, default=[]),
                    ExtractorFieldConfig(name="emails", type="emails", many=True, default=[]),
                    ExtractorFieldConfig(name="phones", type="phones", many=True, default=[]),
                ]
            ),
            output=OutputConfig(merge_rows=True),
            logging=LoggingConfig(),
            sync=SyncConfig(),
            metadata={"contact_discovery": {"enabled": True}},
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            runner = JobRunner(
                fetcher=DummyFetcher(),
                crawler=FakeCrawler(),
                extractor_registry=ExtractorRegistry(),
                mapper=SchemaMapper(),
                logger=None,
            )
            rows, _report = runner.run(
                targets=["https://firm-example.com"],
                config=config,
                run_context=RunContext(
                    run_name="contact_enrich",
                    started_at=datetime.now(timezone.utc),
                    output_root=Path(temp_dir),
                    output_dir=Path(temp_dir),
                    log_file=Path(temp_dir) / "run.log",
                    raw_pages_dir=None,
                ),
            )

        self.assertEqual(1, len(rows))
        row = rows[0]
        self.assertEqual("info@firm-example.com", row["email"])
        self.assertEqual("https://firm-example.com/contact", row["contact_page_url"])
        self.assertEqual("https://firm-example.com/contact", row["email_found_on"])
        self.assertEqual("(410) 555-0100", row["contact_phone"])

    def test_pipeline_expands_rows_for_multiple_source_contexts(self) -> None:
        pipeline = Pipeline()
        rows = [{"input_url": "https://example.com/", "email": "info@example.com"}]
        contexts = {
            "https://example.com/": [
                {"name": "Jane Smith", "website": "https://example.com/"},
                {"name": "John Doe", "website": "https://example.com/"},
            ]
        }

        expanded = pipeline._expand_rows_with_input_context(rows, contexts)

        self.assertEqual(2, len(expanded))
        self.assertEqual("Jane Smith", expanded[0]["name"])
        self.assertEqual("John Doe", expanded[1]["name"])
        self.assertEqual("info@example.com", expanded[0]["email"])

    def test_pipeline_uses_configured_output_file_names_when_present(self) -> None:
        pipeline = Pipeline()
        config = EngineConfig(
            name="website_contact_csv_enrich",
            mode="site_scan",
            requests=RequestConfig(),
            extraction=ExtractionConfig(),
            output=OutputConfig(),
            logging=LoggingConfig(),
            sync=SyncConfig(),
            metadata={
                "output_files": {
                    "csv": "CONTACT_ENRICHED.csv",
                    "json": "CONTACT_ENRICHED.json",
                    "summary": "CONTACT_ENRICHED_SUMMARY.txt",
                    "report": "CONTACT_ENRICHED_REPORT.json",
                }
            },
        )

        self.assertEqual(
            "CONTACT_ENRICHED.csv",
            pipeline._get_output_file_name(config, "csv", "results.csv"),
        )
        self.assertEqual(
            "CONTACT_ENRICHED_REPORT.json",
            pipeline._get_output_file_name(config, "report", "run_report.json"),
        )
        self.assertEqual(
            "results.json",
            pipeline._get_output_file_name(config, "missing", "results.json"),
        )

    def test_lead_cleaner_keeps_best_duplicate_row_and_avoids_weak_name_dedupes(self) -> None:
        rows = [
            {
                "name": "Jane Smith",
                "law_firm": "Smith Law",
                "website": "https://www.smithlaw.com/",
                "email": " INFO@SMITHLAW.COM ",
                "contact_page_url": "",
                "profile_url": "https://directory.test/jane",
                "source_directory": "https://directory.test/listing",
            },
            {
                "name": "Jane Smith",
                "law_firm": "Smith Law",
                "website": "https://smithlaw.com/contact",
                "email": "info@smithlaw.com",
                "contact_page_url": "https://smithlaw.com/contact",
                "profile_url": "https://directory.test/jane",
                "source_directory": "https://directory.test/listing",
            },
            {
                "name": "Jane Smith",
                "law_firm": "Smith Law",
                "website": "https://othersmithlaw.com/",
                "email": "team@othersmithlaw.com",
                "contact_page_url": "https://othersmithlaw.com/contact",
                "profile_url": "https://directory.test/other-jane",
                "source_directory": "https://directory.test/listing",
            },
        ]
        settings = {
            "rename_fields": {
                "law_firm": "firm",
                "website": "url",
            },
            "include_fields": [
                "firm",
                "name",
                "url",
                "email",
                "contact_page_url",
                "profile_url",
                "source_directory",
            ],
            "sort_fields": ["firm", "email"],
        }

        cleaned_rows, columns, stats = clean_lead_rows(rows, settings)

        self.assertEqual(
            ["firm", "name", "url", "email", "contact_page_url", "profile_url", "source_directory"],
            columns,
        )
        self.assertEqual(2, len(cleaned_rows))
        self.assertEqual(1, stats["duplicates_removed"])
        self.assertEqual("https://smithlaw.com/contact", cleaned_rows[0]["url"])
        self.assertEqual("info@smithlaw.com", cleaned_rows[0]["email"])
        self.assertEqual("https://smithlaw.com/contact", cleaned_rows[0]["contact_page_url"])
        self.assertEqual("team@othersmithlaw.com", cleaned_rows[1]["email"])

    def test_pipeline_writes_cleaned_results_into_run_output_dir(self) -> None:
        pipeline = Pipeline()
        rows = [
            {
                "name": "Jane Smith",
                "law_firm": "Smith Law",
                "website": "https://smithlaw.com/",
                "email": "info@smithlaw.com",
                "contact_page_url": "https://smithlaw.com/contact",
                "profile_url": "https://directory.test/jane",
                "source_directory": "https://directory.test/listing",
            },
            {
                "name": "Jane Smith",
                "law_firm": "Smith Law",
                "website": "https://smithlaw.com/contact",
                "email": "info@smithlaw.com",
                "contact_page_url": "",
                "profile_url": "",
                "source_directory": "https://directory.test/listing",
            },
        ]
        config = EngineConfig(
            name="website_contact_csv_enrich",
            mode="site_scan",
            requests=RequestConfig(),
            extraction=ExtractionConfig(),
            output=OutputConfig(),
            logging=LoggingConfig(),
            sync=SyncConfig(),
            metadata={
                "lead_cleaning": {
                    "enabled": True,
                    "file_name": "cleaned_results.csv",
                    "required_fields": ["website", "email"],
                    "rename_fields": {
                        "law_firm": "firm",
                        "website": "url",
                    },
                    "include_fields": [
                        "firm",
                        "name",
                        "url",
                        "email",
                        "contact_page_url",
                        "profile_url",
                        "source_directory",
                    ],
                    "sort_fields": ["firm", "email"],
                }
            },
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            run_context = RunContext(
                run_name="contact_enrich",
                started_at=datetime.now(timezone.utc),
                output_root=Path(temp_dir),
                output_dir=Path(temp_dir),
                log_file=Path(temp_dir) / "run.log",
                raw_pages_dir=None,
            )

            cleaned_output = pipeline._write_configured_cleaned_output(
                rows=rows,
                config=config,
                run_context=run_context,
                logger=None,
            )

            cleaned_csv = Path(temp_dir) / "cleaned_results.csv"
            self.assertTrue(cleaned_csv.exists())
            self.assertIsNotNone(cleaned_output)
            self.assertTrue(cleaned_output["success"])
            self.assertEqual(str(cleaned_csv), cleaned_output["path"])

            lines = cleaned_csv.read_text(encoding="utf-8").splitlines()

        self.assertEqual(
            "firm,name,url,email,contact_page_url,profile_url,source_directory",
            lines[0],
        )
        self.assertEqual(2, len(lines))
        self.assertIn("Smith Law,Jane Smith,https://smithlaw.com/", lines[1])

    def test_crawler_include_exclude_keywords_limit_broad_site_traversal(self) -> None:
        homepage_html = """
        <html>
          <body>
            <a href="/contact">Contact</a>
            <a href="/about">About</a>
            <a href="/blog">Blog</a>
            <a href="/news">News</a>
          </body>
        </html>
        """
        fetcher = MappingFetcher(
            {
                "https://example.com/": CrawlPage(
                    requested_url="https://example.com/",
                    url="https://example.com/",
                    status_code=200,
                    html=homepage_html,
                ),
                "https://example.com/contact": CrawlPage(
                    requested_url="https://example.com/contact",
                    url="https://example.com/contact",
                    status_code=200,
                    html="<html><body>contact@example.com</body></html>",
                ),
                "https://example.com/about": CrawlPage(
                    requested_url="https://example.com/about",
                    url="https://example.com/about",
                    status_code=200,
                    html="<html><body>About us</body></html>",
                ),
            }
        )
        crawler = Crawler(fetcher=fetcher, logger=None)

        with tempfile.TemporaryDirectory() as temp_dir:
            result = crawler.crawl(
                seed_url="https://example.com/",
                crawl_config=CrawlConfig(
                    mode="site_scan",
                    max_pages=4,
                    concurrency=1,
                    priority_keywords=["contact", "about"],
                    include_url_keywords=["contact", "about"],
                    exclude_url_keywords=["blog", "news"],
                ),
                run_context=RunContext(
                    run_name="crawl_filters",
                    started_at=datetime.now(timezone.utc),
                    output_root=Path(temp_dir),
                    output_dir=Path(temp_dir),
                    log_file=Path(temp_dir) / "run.log",
                    raw_pages_dir=None,
                ),
            )

        self.assertEqual(
            [
                "https://example.com/",
                "https://example.com/contact",
                "https://example.com/about",
            ],
            [page.url for page in result.pages],
        )


if __name__ == "__main__":
    unittest.main()
