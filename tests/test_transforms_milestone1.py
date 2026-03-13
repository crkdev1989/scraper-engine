from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from scraper_engine.core.config_loader import normalize_config_payload
from scraper_engine.core.models import (
    CrawlPage,
    EngineConfig,
    ExtractionConfig,
    ExtractorFieldConfig,
    LoggingConfig,
    NormalizationConfig,
    OutputConfig,
    RecordSelectorConfig,
    RequestConfig,
    RunContext,
    SyncConfig,
)
from scraper_engine.core.job_runner import JobRunner
from scraper_engine.core.record_post_processor import RecordPostProcessor
from scraper_engine.extractors.registry import ExtractorRegistry
from scraper_engine.schemas.mapper import SchemaMapper


DIRECTORY_HTML = """
<html>
  <body>
    <div class="listing-card">
      <h2 class="listing-title">  EXAMPLE COMPANY  </h2>
      <div class="status"> n/a </div>
      <a class="website" href=" https://Example.com/Test/ ">Visit</a>
    </div>
  </body>
</html>
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


class DummyCrawler:
    def crawl(self, *args, **kwargs):
        raise AssertionError("directory transform tests should not use the site_scan crawler")


class TransformMilestoneTests(unittest.TestCase):
    def test_config_normalization_keeps_transform_order(self) -> None:
        normalized = normalize_config_payload(
            {
                "name": "transform_test",
                "mode": "directory_list",
                "record_selector": ".listing-card",
                "fields": {
                    "business_name": {
                        "css": ".listing-title",
                        "transforms": [
                            "trim",
                            "collapse_whitespace",
                            {"transform": "titlecase"},
                        ],
                    }
                },
            }
        )

        transforms = normalized["extraction"]["fields"][0]["transforms"]
        self.assertEqual(
            ["trim", "collapse_whitespace", "titlecase"],
            [transform["name"] for transform in transforms],
        )

    def test_post_processor_normalizes_empty_like_values_and_applies_transforms(self) -> None:
        processor = RecordPostProcessor(logger=None)
        fields = [
            ExtractorFieldConfig(
                name="business_name",
                transforms=[
                    {"name": "trim"},
                    {"name": "collapse_whitespace"},
                    {"name": "titlecase"},
                ],
            ),
            ExtractorFieldConfig(
                name="status",
                transforms=[{"name": "default_if_empty", "value": "Unknown"}],
            ),
            ExtractorFieldConfig(
                name="website",
                transforms=[
                    {"name": "trim"},
                    {"name": "strip_suffix", "value": "/"},
                    {"name": "lowercase"},
                ],
            ),
            ExtractorFieldConfig(
                name="tagline",
                transforms=[
                    {"name": "regex_replace", "pattern": r"\s+", "replacement": " "},
                    {"name": "trim"},
                    {"name": "regex_extract", "pattern": r"(great .*?)$", "group": 1},
                ],
            ),
        ]
        record = {
            "business_name": "  example   company ",
            "status": " n/a ",
            "website": " HTTPS://Example.com/Test/ ",
            "tagline": "We build   great tools   ",
        }

        processed = processor.process_record(
            record,
            fields,
            NormalizationConfig(empty_like_strings=["n/a", "na", "null", "none"]),
        )

        self.assertEqual("Example Company", processed["business_name"])
        self.assertEqual("Unknown", processed["status"])
        self.assertEqual("https://example.com/test", processed["website"])
        self.assertEqual("great tools", processed["tagline"])

    def test_directory_runner_applies_transforms_before_output_rows(self) -> None:
        config = EngineConfig(
            name="transform_directory",
            mode="directory_list",
            requests=RequestConfig(),
            extraction=ExtractionConfig(
                record_selector=RecordSelectorConfig(css=".listing-card"),
                fields=[
                    ExtractorFieldConfig(
                        name="business_name",
                        selector=".listing-title",
                        transforms=[
                            {"name": "trim"},
                            {"name": "collapse_whitespace"},
                            {"name": "titlecase"},
                        ],
                    ),
                    ExtractorFieldConfig(
                        name="status",
                        selector=".status",
                        transforms=[{"name": "default_if_empty", "value": "Unknown"}],
                    ),
                    ExtractorFieldConfig(
                        name="website",
                        selector=".website",
                        attribute="href",
                        transforms=[
                            {"name": "trim"},
                            {"name": "strip_suffix", "value": "/"},
                            {"name": "lowercase"},
                        ],
                    ),
                ],
            ),
            normalization=NormalizationConfig(empty_like_strings=["n/a", "na", "null", "none"]),
            output=OutputConfig(merge_rows=False),
            logging=LoggingConfig(),
            sync=SyncConfig(),
        )

        fetcher = StaticFetcher(
            CrawlPage(
                requested_url="https://example.com/directory",
                url="https://example.com/directory",
                status_code=200,
                html=DIRECTORY_HTML,
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
                    run_name="transform_test",
                    started_at=datetime.now(timezone.utc),
                    output_root=Path(temp_dir),
                    output_dir=Path(temp_dir),
                    log_file=Path(temp_dir) / "run.log",
                    raw_pages_dir=None,
                ),
            )

        self.assertEqual(1, report["row_count"])
        self.assertEqual("Example Company", rows[0]["business_name"])
        self.assertEqual("Unknown", rows[0]["status"])
        self.assertEqual("https://example.com/test", rows[0]["website"])


if __name__ == "__main__":
    unittest.main()
