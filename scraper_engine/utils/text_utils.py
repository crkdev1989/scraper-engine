from __future__ import annotations

import re
from typing import Iterable


EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_PATTERN = re.compile(
    r"(?:\+?1[\s\-.]?)?(?:\(?\d{3}\)?[\s\-.]?)\d{3}[\s\-.]?\d{4}"
)


def clean_whitespace(value: str) -> str:
    return " ".join(value.split())


def dedupe_preserve_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        results.append(normalized)
    return results


def extract_emails(text: str) -> list[str]:
    return dedupe_preserve_order(EMAIL_PATTERN.findall(text or ""))


def extract_phone_numbers(text: str) -> list[str]:
    return dedupe_preserve_order(PHONE_PATTERN.findall(text or ""))
