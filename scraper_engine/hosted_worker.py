from __future__ import annotations

import argparse
from pathlib import Path

from scraper_engine.hosted_service import run_hosted_job_process


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scraper-engine-hosted-worker")
    parser.add_argument("--metadata-path", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_hosted_job_process(Path(args.metadata_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
