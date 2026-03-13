from __future__ import annotations

from bs4 import BeautifulSoup

from scraper_engine.crawl.url_utils import extract_links, same_domain
from scraper_engine.extractors.heuristics import derive_business_name, extract_address
from scraper_engine.utils.text_utils import (
    clean_whitespace,
    dedupe_preserve_order,
    extract_emails,
    extract_phone_numbers,
)

SOCIAL_DOMAINS = (
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "tiktok.com",
)

CONTACT_KEYWORDS = (
    "contact",
    "about",
    "team",
    "staff",
    "office",
    "location",
    "get-in-touch",
)


def get_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html or "", "lxml")


def page_title(*, soup, **_) -> str | None:
    if soup.title and soup.title.string:
        return clean_whitespace(soup.title.string)
    return None


def meta_description(*, soup, **_) -> str | None:
    tag = soup.select_one("meta[name='description'], meta[property='og:description']")
    if tag:
        return clean_whitespace(tag.get("content", ""))
    return None


def emails(*, html, **_) -> list[str]:
    return extract_emails(html or "")


def phone_numbers(*, html, **_) -> list[str]:
    return extract_phone_numbers(html or "")


def phones(*, html, **_) -> list[str]:
    return phone_numbers(html=html)


def headings(*, soup, **_) -> list[str]:
    values = [
        clean_whitespace(node.get_text(" ", strip=True))
        for node in soup.select("h1, h2, h3")
    ]
    return dedupe_preserve_order([value for value in values if value])


def internal_links(*, html, url, **_) -> list[str]:
    links = [link.url for link in extract_links(url, html or "") if same_domain(url, link.url)]
    return dedupe_preserve_order(links)


def contact_links(*, soup, url, **_) -> list[str]:
    values = [
        link.url
        for link in extract_links(url, str(soup))
        if same_domain(url, link.url)
        and any(
            keyword in link.url.lower() or keyword in link.anchor_text.lower()
            for keyword in CONTACT_KEYWORDS
        )
    ]
    return dedupe_preserve_order(values)


def social_links(*, soup, url, **_) -> list[str]:
    values = []
    for link in extract_links(url, str(soup)):
        lowered = link.url.lower()
        if any(domain in lowered for domain in SOCIAL_DOMAINS):
            values.append(link.url)
    return dedupe_preserve_order(values)


def address(*, soup, html, **_) -> str | None:
    candidates = [
        clean_whitespace(node.get_text(" ", strip=True))
        for node in soup.select(
            "address, [itemprop='address'], .address, .location, .contact-address, .footer-address"
        )
    ]
    return extract_address(html or "", candidates)


def business_name(*, soup, html, **_) -> str | None:
    title = page_title(soup=soup)
    heading_values = headings(soup=soup, html=html)
    meta_site_name = soup.select_one("meta[property='og:site_name']")
    if meta_site_name:
        site_name = clean_whitespace(meta_site_name.get("content", ""))
        if site_name:
            return site_name
    return derive_business_name(title=title, headings=heading_values)


BUILTIN_EXTRACTORS = {
    "page_title": page_title,
    "meta_description": meta_description,
    "emails": emails,
    "email": emails,
    "phone_numbers": phone_numbers,
    "phones": phones,
    "phone": phones,
    "contact_links": contact_links,
    "social_links": social_links,
    "internal_links": internal_links,
    "address": address,
    "headings": headings,
    "business_name": business_name,
}
