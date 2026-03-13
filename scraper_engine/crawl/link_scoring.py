from __future__ import annotations

from scraper_engine.core.models import DiscoveredLink


def score_link(url: str, priority_keywords: list[str], anchor_text: str = "") -> int:
    lowered = url.lower()
    anchor_lowered = anchor_text.lower()
    score = 0
    for keyword in priority_keywords:
        normalized_keyword = keyword.lower()
        if normalized_keyword in lowered:
            score += 10
        if normalized_keyword in anchor_lowered:
            score += 5
    if lowered.count("/") <= 3:
        score += 2
    if "contact" in lowered or "contact" in anchor_lowered:
        score += 3
    return score


def sort_links(links: list[DiscoveredLink], priority_keywords: list[str]) -> list[DiscoveredLink]:
    deduped: dict[str, DiscoveredLink] = {}
    for link in links:
        link.score = score_link(link.url, priority_keywords, link.anchor_text)
        existing = deduped.get(link.url)
        if existing is None or link.score > existing.score:
            deduped[link.url] = link

    return sorted(
        deduped.values(),
        key=lambda link: (link.score, -len(link.url)),
        reverse=True,
    )
