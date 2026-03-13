from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from scraper_engine.core.models import RunContext


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_") or "run"


def build_run_context(
    output_root: Path,
    config_name: str,
    requested_run_name: str | None,
    log_file_name: str,
    save_raw_html: bool,
) -> RunContext:
    started_at = datetime.now(timezone.utc)
    run_label = slugify(requested_run_name or config_name)
    timestamp = started_at.strftime("%Y%m%d_%H%M%S")
    run_name = f"{run_label}_{timestamp}"
    output_dir = output_root / run_name
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_pages_dir = output_dir / "raw_pages" if save_raw_html else None
    if raw_pages_dir is not None:
        raw_pages_dir.mkdir(parents=True, exist_ok=True)

    return RunContext(
        run_name=run_name,
        started_at=started_at,
        output_root=output_root,
        output_dir=output_dir,
        log_file=output_dir / log_file_name,
        raw_pages_dir=raw_pages_dir,
    )


def raw_page_path(raw_pages_dir: Path, url: str, page_number: int) -> Path:
    parsed = urlparse(url)
    path_parts = [part for part in parsed.path.split("/") if part]
    if not path_parts:
        label = "home"
    else:
        label = "_".join(path_parts[-2:]) if len(path_parts) > 1 else path_parts[-1]

    query_parts = parse_qs(parsed.query)
    if query_parts:
        query_suffix = "_".join(
            f"{key}_{'_'.join(values)}"
            for key, values in sorted(query_parts.items())
        )
        label = f"{label}_{query_suffix}"

    safe_label = slugify(label) or "page"
    return raw_pages_dir / f"page_{page_number:03d}_{safe_label}.html"
