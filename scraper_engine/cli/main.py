from __future__ import annotations

import sys

from scraper_engine.cli.args import parse_args


def main(argv: list[str] | None = None) -> int:
    try:
        options = parse_args(argv)
        from scraper_engine.core.pipeline import Pipeline

        pipeline = Pipeline()
        result = pipeline.run(options)
        print(f"Run complete: {result.run_context.output_dir}")
        return 0
    except Exception as error:
        print(f"Run failed: {error}", file=sys.stderr)
        return 1
