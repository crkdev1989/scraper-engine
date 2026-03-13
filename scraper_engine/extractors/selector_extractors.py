from __future__ import annotations

from urllib.parse import urljoin

from lxml import html as lxml_html

from scraper_engine.utils.text_utils import clean_whitespace, dedupe_preserve_order


def build_xpath_tree(html: str):
    if not html:
        return None
    try:
        return lxml_html.fromstring(html)
    except (ValueError, TypeError):
        return None


def extract_css(
    soup,
    selector: str,
    attribute: str | None = None,
    many: bool = False,
    base_url: str | None = None,
):
    nodes = soup.select(selector) if soup else []
    values = [_extract_css_node_value(node, attribute=attribute, base_url=base_url) for node in nodes]

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


def extract_css_from_node(
    node,
    selector: str,
    attribute: str | None = None,
    many: bool = False,
    base_url: str | None = None,
):
    return extract_css(
        node,
        selector=selector,
        attribute=attribute,
        many=many,
        base_url=base_url,
    )


def _extract_css_node_value(node, attribute: str | None = None, base_url: str | None = None):
    if attribute and attribute not in {"text", "text_content"}:
        raw_value = node.get(attribute, "")
        if attribute in {"href", "src"} and raw_value and base_url:
            return urljoin(base_url, raw_value)
        return clean_whitespace(raw_value)
    return clean_whitespace(node.get_text(" ", strip=True))
