from __future__ import annotations

import argparse
from pathlib import Path, PurePosixPath
import csv

from scraper_engine.outputs.lead_cleaner import write_cleaned_lead_csv

DEFAULT_SETTINGS = {
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
    "junk_email_substrings": [
        "sentry.io",
        "wixpress.com",
        "ingest.sentry",
        "@example.com",
        "johndoe",
    ],
}


def resolve_input_path(raw_path: str) -> Path:
    """Accept native paths and WSL-style /mnt/<drive>/... paths."""
    candidate = Path(raw_path).expanduser()
    if candidate.exists():
        return candidate

    if raw_path.startswith("/mnt/"):
        posix_path = PurePosixPath(raw_path)
        parts = posix_path.parts
        if len(parts) >= 4 and parts[1] == "mnt":
            drive = parts[2].upper() + ":"
            windows_path = Path(f"{drive}\\", *parts[3:])
            if windows_path.exists():
                return windows_path

    return candidate


def default_output_path(input_path: Path) -> Path:
    return input_path.with_name(f"cleaned_{input_path.name}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create a clean email outreach CSV from scraper-engine output."
    )
    parser.add_argument("input_csv", help="Path to the raw scraper CSV.")
    parser.add_argument(
        "--output",
        help="Optional output path. Defaults to cleaned_<input filename>.csv next to the input file.",
    )
    args = parser.parse_args()

    input_path = resolve_input_path(args.input_csv)
    if not input_path.exists():
        raise SystemExit(f"Input CSV not found: {args.input_csv}")

    output_path = (
        Path(args.output).expanduser()
        if args.output
        else default_output_path(input_path)
    )

    with input_path.open("r", encoding="utf-8-sig", newline="") as infile:
        reader = csv.DictReader(infile)
        if reader.fieldnames is None:
            raise SystemExit("Input CSV is empty or missing a header row.")
        rows = list(reader)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    stats = write_cleaned_lead_csv(output_path, rows, DEFAULT_SETTINGS)

    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print(
        "Rows:"
        f" total={stats['total_rows']}"
        f", missing_required={stats['filtered_missing_required']}"
        f", junk_email={stats['filtered_junk_email']}"
        f", duplicates={stats['duplicates_removed']}"
        f", written={stats['written']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
