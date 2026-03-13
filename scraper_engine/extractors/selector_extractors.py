from __future__ import annotations

from lxml import html as lxml_html

from scraper_engine.utils.text_utils import clean_whitespace, dedupe_preserve_order


def build_xpath_tree(html: str):
    if not html:
        return None
    try:
        return lxml_html.fromstring(html)
    except (ValueError, TypeError):
        return None


def extract_css(soup, selector: str, attribute: str | None = None, many: bool = False):
    nodes = soup.select(selector) if soup else []
    if attribute:
        values = [node.get(attribute, "") for node in nodes]
    else:
        values = [clean_whitespace(node.get_text(" ", strip=True)) for node in nodes]

    values = dedupe_preserve_order([value for value in values if value])
    if many:
        return values
    return values[0] if values else None


def extract_xpath(tree, expression: str, many: bool = False):
    if tree is None:
        return [] if many else None

    raw_values = tree.xpath(expression)
    values = []
    for value in raw_values:
        if hasattr(value, "text_content"):
            normalized = clean_whitespace(value.text_content())
        else:
            normalized = clean_whitespace(str(value))
        if normalized:
            values.append(normalized)

    values = dedupe_preserve_order(values)
    if many:
        return values
    return values[0] if values else None
