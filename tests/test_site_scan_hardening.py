from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from scraper_engine.core.models import EngineConfig, ExtractionConfig, ExtractorFieldConfig
from scraper_engine.core.models import OutputConfig
from scraper_engine.core.models import RequestConfig, CrawlConfig, LoggingConfig, SyncConfig
from scraper_engine.core.models import DiscoveredLink
from scraper_engine.crawl.link_scoring import sort_links
from scraper_engine.outputs.paths import raw_page_path
from scraper_engine.outputs.writers import write_csv
from scraper_engine.schemas.mapper import SchemaMapper


class SiteScanHardeningTests(unittest.TestCase):
    def test_priority_keywords_sort_before_generic_links(self) -> None:
        links = [
            DiscoveredLink(url="https://example.com/services", anchor_text="Services"),
            DiscoveredLink(url="https://example.com/contact", anchor_text="Contact"),
            DiscoveredLink(url="https://example.com/about", anchor_text="About Us"),
            DiscoveredLink(url="https://example.com/blog", anchor_text="Blog"),
        ]

        ranked = sort_links(links, ["contact", "about", "team"])
        ranked_urls = [link.url for link in ranked]

        self.assertEqual("https://example.com/contact", ranked_urls[0])
        self.assertIn("https://example.com/about", ranked_urls[:2])

    def test_raw_page_path_uses_unique_numbered_filenames(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            raw_dir = Path(temp_dir)
            path_one = raw_page_path(raw_dir, "https://example.com/", 1)
            path_two = raw_page_path(raw_dir, "https://example.com/contact", 2)
            path_three = raw_page_path(raw_dir, "https://example.com/contact?tab=team", 3)

            self.assertEqual("page_001_home.html", path_one.name)
            self.assertEqual("page_002_contact.html", path_two.name)
            self.assertTrue(path_three.name.startswith("page_003_contact_tab_team"))
            self.assertNotEqual(path_two.name, path_three.name)

    def test_csv_writer_flattens_list_values_with_semicolons(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "results.csv"
            write_csv(
                output_path,
                [
                    {
                        "input_url": "https://example.com/",
                        "emails": ["one@example.com", "two@example.com"],
                        "phones": ["111-111-1111", "222-222-2222"],
                    }
                ],
                preferred_columns=["input_url", "emails", "phones"],
            )

            with output_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual("one@example.com; two@example.com", rows[0]["emails"])
            self.assertEqual("111-111-1111; 222-222-2222", rows[0]["phones"])
            self.assertNotIn("[", rows[0]["emails"])

    def test_schema_mapper_merges_single_record_and_dedupes_lists(self) -> None:
        config = EngineConfig(
            name="website_contact",
            mode="site_scan",
            preset="website_contact",
            requests=RequestConfig(),
            crawl=CrawlConfig(),
            extraction=ExtractionConfig(
                fields=[
                    ExtractorFieldConfig(name="emails", many=True, default=[]),
                    ExtractorFieldConfig(name="phones", many=True, default=[]),
                    ExtractorFieldConfig(name="contact_pages", many=True, default=[]),
                    ExtractorFieldConfig(name="social_links", many=True, default=[]),
                ]
            ),
            output=OutputConfig(merge_rows=True),
            logging=LoggingConfig(),
            sync=SyncConfig(),
        )

        rows = SchemaMapper().map_rows(
            "https://example.com/",
            [
                {
                    "input_url": "https://example.com/",
                    "page_url": "https://example.com/",
                    "emails": ["hello@example.com"],
                    "phones": ["111-111-1111"],
                    "contact_pages": ["https://example.com/contact"],
                    "social_links": ["https://linkedin.com/company/example"],
                },
                {
                    "input_url": "https://example.com/",
                    "page_url": "https://example.com/about",
                    "emails": ["hello@example.com", "team@example.com"],
                    "phones": ["111-111-1111", "222-222-2222"],
                    "contact_pages": ["https://example.com/contact"],
                    "social_links": ["https://linkedin.com/company/example"],
                },
            ],
            config,
        )

        self.assertEqual(1, len(rows))
        merged = rows[0]
        self.assertEqual(
            ["hello@example.com", "team@example.com"],
            merged["emails"],
        )
        self.assertEqual(
            ["111-111-1111", "222-222-2222"],
            merged["phones"],
        )
        self.assertEqual(
            ["https://example.com/", "https://example.com/about"],
            merged["source_urls"],
        )


if __name__ == "__main__":
    unittest.main()
