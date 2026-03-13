from __future__ import annotations

import argparse
from pathlib import Path

from scraper_engine.core.models import RuntimeOptions


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scraper-engine",
        description="CLI-first foundation for a config-driven scraping engine.",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to a JSON or YAML job config file.",
    )
    parser.add_argument(
        "--url",
        help="Single seed URL to process.",
    )
    parser.add_argument(
        "--input",
        help="Path to a text, JSON, YAML, or CSV file containing URLs.",
    )
    parser.add_argument(
        "--run-name",
        help="Optional custom run name prefix for the output folder.",
    )
    parser.add_argument(
        "--output-root",
        help="Override the output root directory from the config.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        help="Override configured concurrency for the current run.",
    )
    parser.add_argument(
        "--log-level",
        help="Override configured log level for the current run.",
    )
    parser.add_argument(
        "--debug-html",
        action="store_true",
        help="Persist fetched HTML into a raw_pages folder for the run.",
    )
    return parser


def parse_args(argv: list[str] | None = None) -> RuntimeOptions:
    parser = build_parser()
    namespace = parser.parse_args(argv)
    if not namespace.url and not namespace.input:
        parser.error("one of --url or --input is required")

    return RuntimeOptions(
        config_path=Path(namespace.config),
        single_url=namespace.url,
        input_path=Path(namespace.input) if namespace.input else None,
        run_name=namespace.run_name,
        output_root=Path(namespace.output_root) if namespace.output_root else None,
        concurrency=namespace.concurrency,
        log_level=namespace.log_level,
        debug_html=namespace.debug_html,
    )
