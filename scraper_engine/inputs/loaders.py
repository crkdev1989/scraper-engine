from __future__ import annotations

import csv
import json
from pathlib import Path

import yaml

from scraper_engine.crawl.url_utils import normalize_url
from scraper_engine.core.models import EngineConfig, RuntimeOptions
from scraper_engine.utils.text_utils import dedupe_preserve_order


def load_targets(options: RuntimeOptions, config: EngineConfig) -> list[str]:
    targets: list[str] = []
    if options.single_url:
        targets.append(normalize_url(options.single_url))
    if options.input_path:
        targets.extend(_load_targets_from_file(options.input_path))
    if not targets:
        targets.extend(normalize_url(target) for target in config.static_targets)
    return dedupe_preserve_order(targets)


def _load_targets_from_file(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".txt":
        return [
            normalize_url(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            return [
                normalize_url(row["url"])
                for row in reader
                if row.get("url")
            ]
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
    elif suffix in {".yaml", ".yml"}:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    else:
        raise ValueError(f"Unsupported input format: {suffix}")

    if isinstance(payload, list):
        return [normalize_url(str(item)) for item in payload]
    if isinstance(payload, dict) and isinstance(payload.get("urls"), list):
        return [normalize_url(str(item)) for item in payload["urls"]]
    raise ValueError("Structured input files must contain a list or a 'urls' field.")
