from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from scraper_engine.core.models import (
    CrawlPage,
    DetailPageConfig,
    EngineConfig,
    ExtractionConfig,
    ExtractorFieldConfig,
    LoggingConfig,
    NormalizationConfig,
    OutputConfig,
    OutputShapingConfig,
    RequestConfig,
    RunContext,
    SyncConfig,
    RecordSelectorConfig,
)
from scraper_engine.core.job_runner import JobRunner
from scraper_engine.core.pipeline import Pipeline
from scraper_engine.core.record_post_processor import RecordPostProcessor
from scraper_engine.extractors.registry import ExtractorRegistry
from scraper_engine.outputs.reporters import write_summary
from scraper_engine.schemas.mapper import SchemaMapper


DIRECTORY_HTML = """
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


DETAIL_HTML_SUCCESS = """
<html>
  <body>
    <a href="mailto:jane@example.com">Email</a>
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
        raise AssertionError("milestone 3 diagnostics tests should not use the site_scan crawler")


class ReportingDiagnosticsMilestoneTests(unittest.TestCase):
    def test_post_processor_collects_warning_and_cleanup_diagnostics(self) -> None:
        processor = RecordPostProcessor(logger=None)
        fields = [
            ExtractorFieldConfig(
                name="email",
                transforms=[{"name": "unknown_transform"}],
            )
        ]

        processed = processor.process_record(
            {"email": "mailto:test@example.com"},
            fields,
            normalization=NormalizationConfig(),
        )
        shaped = processor.shape_record(
            {"email": [processed["email"], " mailto:test@example.com ", " "]},
            OutputShapingConfig(
                cleanup_common_fields=True,
                flatten_single_item_lists=True,
            ),
        )
        diagnostics = processor.get_diagnostics()

        self.assertEqual("test@example.com", shaped["email"])
        self.assertEqual(1, diagnostics["warning_total"])
        self.assertEqual(1, diagnostics["warning_counts_by_category"]["field_transform"])
        self.assertEqual(1, diagnostics["warning_counts_by_field"]["email"])
        self.assertGreaterEqual(diagnostics["cleanup_actions"]["list_items_dropped"], 1)
        self.assertGreaterEqual(diagnostics["cleanup_actions"]["duplicate_list_items_removed"], 1)
        self.assertGreaterEqual(diagnostics["cleanup_actions"]["mailto_prefix_removed"], 1)

    def test_pipeline_builds_quality_metrics(self) -> None:
        pipeline = Pipeline()
        metrics = pipeline._build_quality_metrics(
            rows=[
                {"input_url": "https://example.com", "email": "a@example.com", "phone": ""},
                {"input_url": "https://example.com/2", "email": None, "phone": "555"},
            ],
            configured_fields=["email", "phone", "website"],
        )

        self.assertEqual(2, metrics["records_produced"])
        self.assertEqual(1, metrics["configured_field_coverage"]["email"]["filled"])
        self.assertEqual(1, metrics["configured_field_coverage"]["phone"]["filled"])
        self.assertEqual(2, metrics["configured_field_coverage"]["website"]["empty"])
        self.assertEqual(0, metrics["records_with_all_configured_fields_empty"])
        self.assertIn("input_url", metrics["final_output_fields_seen"])

    def test_directory_detail_report_contains_non_fatal_issue_diagnostics(self) -> None:
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
                        ExtractorFieldConfig(name="email", selector="a[href^='mailto:']", attribute="href"),
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
                run_name="test_detail_diagnostics",
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
            _, report = runner.run(
                targets=["https://example.com/directory"],
                config=config,
                run_context=run_context,
            )

        self.assertEqual(1, report["diagnostics"]["non_fatal_issue_counts"]["detail_page_fetch_failed"])
        self.assertTrue(report["diagnostics"]["non_fatal_issue_messages"])

    def test_summary_includes_diagnostics_and_quality_sections(self) -> None:
        report = {
            "run_name": "summary_test",
            "config_name": "demo",
            "mode": "directory_detail",
            "input_targets": ["https://example.com/directory"],
            "target_count": 1,
            "duration_seconds": 1.234,
            "duration": "1.234s",
            "pages_crawled": 3,
            "pages_visited": 1,
            "pages_succeeded": 2,
            "pages_failed": 1,
            "listing_count": 2,
            "pagination_urls_followed": 0,
            "pagination_stopped_reasons": ["no_next_page_found"],
            "detail_pages_attempted": 2,
            "detail_pages_successful": 1,
            "detail_pages_failed": 1,
            "row_count": 2,
            "extracted_field_names": ["attorney_name", "email"],
            "sync_attempted": False,
            "sync_success": False,
            "errors": ["https://example.com/profiles/john-doe: 404 Client Error"],
            "notes": ["detail page failure preserved base record"],
            "diagnostics": {
                "warning_total": 1,
                "warning_counts_by_category": {"field_transform": 1},
                "non_fatal_issue_counts": {"detail_page_fetch_failed": 1},
                "pagination_stop_reason_counts": {"no_next_page_found": 1},
                "cleanup_actions": {"duplicate_list_items_removed": 2},
                "warning_messages": [{"message": "email: Unknown transform", "count": 1}],
                "non_fatal_issue_messages": [
                    {
                        "message": "https://example.com/profiles/john-doe: 404 Client Error",
                        "count": 1,
                    }
                ],
            },
            "quality_metrics": {
                "records_produced": 2,
                "records_with_all_configured_fields_empty": 0,
                "final_output_fields_seen": ["input_url", "attorney_name", "email"],
                "configured_field_coverage": {
                    "attorney_name": {
                        "filled": 2,
                        "empty": 0,
                        "fill_rate": 1.0,
                        "empty_rate": 0.0,
                    },
                    "email": {
                        "filled": 1,
                        "empty": 1,
                        "fill_rate": 0.5,
                        "empty_rate": 0.5,
                    },
                },
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            summary_path = Path(temp_dir) / "summary.txt"
            write_summary(summary_path, report)
            summary_text = summary_path.read_text(encoding="utf-8")

        self.assertIn("Diagnostics:", summary_text)
        self.assertIn("Quality Metrics:", summary_text)
        self.assertIn("Non-fatal issue detail_page_fetch_failed: 1", summary_text)
        self.assertIn("Field coverage:", summary_text)


if __name__ == "__main__":
    unittest.main()
