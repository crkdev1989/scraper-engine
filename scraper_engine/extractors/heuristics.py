from __future__ import annotations

import re

from scraper_engine.utils.text_utils import clean_whitespace, dedupe_preserve_order


ADDRESS_PATTERN = re.compile(
    r"\b\d{1,6}\s+[A-Z0-9][A-Z0-9\s.,#-]{6,120}\b(?:,\s*[A-Z .'-]+,\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?)?",
    re.IGNORECASE,
)


def derive_business_name(title: str | None, headings: list[str] | None = None) -> str | None:
    heading_values = [value for value in (headings or []) if value]
    if heading_values:
        first_heading = heading_values[0]
        if 2 <= len(first_heading.split()) <= 8:
            return first_heading

    if not title:
        return None

    separators = [" | ", " - ", " :: ", " / "]
    candidate = title
    for separator in separators:
        if separator in title:
            candidate = title.split(separator)[0]
            break

    candidate = clean_whitespace(candidate)
    if not candidate:
        return None
    return candidate


def extract_address(text: str, candidates: list[str] | None = None) -> str | None:
    options = dedupe_preserve_order(candidates or [])
    for option in options:
        cleaned = clean_whitespace(option)
        if 10 <= len(cleaned) <= 180 and any(char.isdigit() for char in cleaned):
            return cleaned

    match = ADDRESS_PATTERN.search(text or "")
    if match:
        return clean_whitespace(match.group(0))
    return None
