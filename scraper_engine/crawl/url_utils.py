from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from scraper_engine.core.models import DiscoveredLink
from scraper_engine.utils.text_utils import clean_whitespace, dedupe_preserve_order


SKIPPED_SCHEMES = ("mailto:", "tel:", "javascript:", "data:")


def normalize_url(value: str, base_url: str | None = None) -> str:
    value = value.strip()
    if not value:
        raise ValueError("URL value is empty.")
    if base_url:
        value = urljoin(base_url, value)
    if not value.startswith(("http://", "https://")):
        value = f"https://{value}"

    parsed = urlparse(value)
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    if ":" in netloc:
        host, _, port = netloc.partition(":")
        if (scheme == "http" and port == "80") or (scheme == "https" and port == "443"):
            netloc = host

    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    normalized_query = urlencode(query_pairs)
    normalized = parsed._replace(
        scheme=scheme,
        netloc=netloc,
        path=path,
        query=normalized_query,
        fragment="",
    )
    return urlunparse(normalized)


def is_http_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"}


def should_skip_url(value: str) -> bool:
    lowered = value.lower()
    return not value or lowered.startswith(("#", *SKIPPED_SCHEMES))


def canonical_domain(url: str) -> str:
    hostname = (urlparse(url).hostname or "").lower()
    if hostname.startswith("www."):
        hostname = hostname[4:]
    return hostname


def same_domain(source_url: str, candidate_url: str) -> bool:
    return canonical_domain(source_url) == canonical_domain(candidate_url)


def extract_links(base_url: str, html: str) -> list[DiscoveredLink]:
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    links: list[DiscoveredLink] = []
    for tag in soup.select("a[href]"):
        href = (tag.get("href") or "").strip()
        if should_skip_url(href):
            continue
        absolute = normalize_url(href, base_url=base_url)
        if not is_http_url(absolute):
            continue
        links.append(
            DiscoveredLink(
                url=absolute,
                anchor_text=clean_whitespace(tag.get_text(" ", strip=True)),
            )
        )
    return links


def unique_urls(values: list[str]) -> list[str]:
    return dedupe_preserve_order(values)
