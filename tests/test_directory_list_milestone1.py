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
    OutputConfig,
    RecordSelectorConfig,
    RequestConfig,
    RunContext,
    SyncConfig,
)
from scraper_engine.core.job_runner import JobRunner
from scraper_engine.extractors.directory_extractors import DirectoryListExtractor
from scraper_engine.extractors.registry import ExtractorRegistry
from scraper_engine.schemas.mapper import SchemaMapper


DIRECTORY_HTML = """
<html>
  <body>
    <div class="listing-card">
      <h2 class="listing-title">Alpha Realty</h2>
      <a class="website" href="/alpha">Visit</a>
      <div class="phone">(555) 111-2222</div>
      <span class="city">Austin</span>
      <span class="state">TX</span>
    </div>
    <div class="listing-card">
      <h2 class="listing-title">Beta Realty</h2>
      <a class="website" href="https://example.com/beta">Visit</a>
      <div class="phone">(555) 333-4444</div>
      <span class="city">Dallas</span>
      <span class="state">TX</span>
    </div>
  </body>
</html>
"""


class FakeFetcher:
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
        raise AssertionError("directory_list should not use the site_scan crawler")


class DirectoryListMilestoneTests(unittest.TestCase):
    def test_directory_extractor_returns_multiple_records(self) -> None:
        extractor = DirectoryListExtractor()
        extraction_config = ExtractionConfig(
            record_selector=RecordSelectorConfig(css=".listing-card"),
            fields=[
                ExtractorFieldConfig(name="business_name", selector=".listing-title"),
                ExtractorFieldConfig(name="website", selector="a.website", attribute="href"),
                ExtractorFieldConfig(name="phone", selector=".phone"),
            ],
        )

        records = extractor.extract_records(
            html=DIRECTORY_HTML,
            page_url="https://example.com/directory",
            extraction_config=extraction_config,
        )

        self.assertEqual(2, len(records))
        self.assertEqual("Alpha Realty", records[0]["business_name"])
        self.assertEqual("https://example.com/alpha", records[0]["website"])
        self.assertEqual("(555) 333-4444", records[1]["phone"])

    def test_config_normalization_supports_directory_dict_style(self) -> None:
        normalized = normalize_config_payload(
            {
                "name": "sample_directory",
                "mode": "directory_list",
                "record_selector": ".listing-card",
                "fields": {
                    "business_name": {"css": ".listing-title", "attr": "text"},
                    "website": {"css": "a.website", "attr": "href"},
                },
            }
        )

        self.assertEqual(
            {"css": ".listing-card"},
            normalized["extraction"]["record_selector"],
        )
        self.assertEqual("business_name", normalized["extraction"]["fields"][0]["name"])
        self.assertEqual(".listing-title", normalized["extraction"]["fields"][0]["selector"])
        self.assertEqual("text", normalized["extraction"]["fields"][0]["attribute"])

    def test_job_runner_directory_list_outputs_multiple_rows(self) -> None:
        config = EngineConfig(
            name="sample_directory",
            mode="directory_list",
            requests=RequestConfig(),
            extraction=ExtractionConfig(
                record_selector=RecordSelectorConfig(css=".listing-card"),
                fields=[
                    ExtractorFieldConfig(name="business_name", selector=".listing-title"),
                    ExtractorFieldConfig(name="website", selector="a.website", attribute="href"),
                ],
            ),
            output=OutputConfig(merge_rows=False),
            logging=LoggingConfig(),
            sync=SyncConfig(),
        )
        fetcher = FakeFetcher(
            CrawlPage(
                requested_url="https://example.com/directory",
                url="https://example.com/directory",
                status_code=200,
                html=DIRECTORY_HTML,
            )
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            run_context = RunContext(
                run_name="test_directory",
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
        self.assertEqual(2, report["row_count"])
        self.assertEqual(1, report["pages_crawled"])
        self.assertEqual("https://example.com/directory", rows[0]["source_url"])


if __name__ == "__main__":
    unittest.main()
