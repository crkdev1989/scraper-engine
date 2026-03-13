from __future__ import annotations

import unittest

from scraper_engine.core.config_loader import normalize_config_payload
from scraper_engine.core.models import OutputShapingConfig
from scraper_engine.core.pipeline import Pipeline
from scraper_engine.core.record_post_processor import RecordPostProcessor


class OutputShapingMilestoneTests(unittest.TestCase):
    def test_config_normalization_supports_top_level_shaping(self) -> None:
        normalized = normalize_config_payload(
            {
                "name": "shape_test",
                "mode": "directory_list",
                "record_selector": ".listing-card",
                "fields": {"emails": {"css": ".email"}},
                "shaping": {
                    "include_fields": ["input_url", "emails"],
                    "field_order": ["emails", "input_url"],
                    "join_fields": ["emails"],
                    "cleanup_common_fields": True,
                },
            }
        )

        shaping = normalized["output"]["shaping"]
        self.assertEqual(["input_url", "emails"], shaping["include_fields"])
        self.assertEqual(["emails", "input_url"], shaping["field_order"])
        self.assertEqual({"emails": "; "}, shaping["join_fields"])
        self.assertTrue(shaping["cleanup_common_fields"])

    def test_shape_record_cleans_lists_and_common_field_prefixes(self) -> None:
        processor = RecordPostProcessor(logger=None)
        shaping = OutputShapingConfig(
            cleanup_common_fields=True,
            flatten_single_item_lists=True,
            drop_empty_fields=True,
            field_order=["emails", "phones", "website"],
        )
        record = {
            "emails": [" mailto:test@example.com ", "mailto:test@example.com", "", " "],
            "phones": [" tel:+1-555-111-2222 ", None],
            "website": [" https://example.com "],
            "notes": [],
        }

        shaped = processor.shape_record(record, shaping)

        self.assertEqual("test@example.com", shaped["emails"])
        self.assertEqual("+1-555-111-2222", shaped["phones"])
        self.assertEqual("https://example.com", shaped["website"])
        self.assertNotIn("notes", shaped)
        self.assertEqual(["emails", "phones", "website"], list(shaped.keys()))

    def test_shape_record_supports_join_include_exclude(self) -> None:
        processor = RecordPostProcessor(logger=None)
        shaping = OutputShapingConfig(
            include_fields=["input_url", "social_links", "source_url"],
            exclude_fields=["source_url"],
            field_order=["social_links", "input_url"],
            join_fields={"social_links": " | "},
        )
        record = {
            "input_url": "https://example.com",
            "source_url": "https://example.com/about",
            "social_links": [" https://x.com/example ", "https://x.com/example", "https://linkedin.com/company/example"],
            "ignored": "value",
        }

        shaped = processor.shape_record(record, shaping)

        self.assertEqual(
            "https://x.com/example | https://linkedin.com/company/example",
            shaped["social_links"],
        )
        self.assertEqual(["social_links", "input_url"], list(shaped.keys()))
        self.assertNotIn("source_url", shaped)
        self.assertNotIn("ignored", shaped)

    def test_pipeline_preferred_columns_respects_shaping(self) -> None:
        pipeline = Pipeline()

        config = type("Config", (), {})()
        config.schema = {"columns": ["input_url", "source_url", "emails", "phones"]}
        config.extraction = type("Extraction", (), {"fields": [], "detail_page": None})()
        config.output = type(
            "Output",
            (),
            {
                "shaping": OutputShapingConfig(
                    include_fields=["input_url", "emails"],
                    field_order=["emails", "input_url"],
                )
            },
        )()

        columns = pipeline._resolve_preferred_columns(
            config,
            [{"input_url": "https://example.com", "emails": "a@example.com", "phones": "111"}],
        )

        self.assertEqual(["emails", "input_url"], columns)


if __name__ == "__main__":
    unittest.main()
