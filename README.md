# scraper-engine

Stage 2 foundation for a production-minded, config-driven Python scraping engine.

This repository is intentionally CLI-first, but the code is structured so the same core engine can later be called by a FastAPI or web layer without major refactoring.

## Current Scope

This stage includes:

- repo and package structure
- runnable CLI entrypoint
- JSON and YAML config loading
- seed URL and file-based input handling
- working first-pass `site_scan` crawl flow
- built-in extractor registry for public preset use cases
- per-run output folders with logs and reports
- starter public and private configs

This stage does **not** yet include browser automation, a frontend, FastAPI, or JS rendering.

## Project Layout

```text
scraper-engine/
  run.py
  requirements.txt
  .gitignore
  README.md
  configs/
    public/
    private/
  scraper_engine/
    cli/
    core/
    crawl/
    extractors/
    inputs/
    outputs/
    schemas/
    sync/
    utils/
  tests/
```

## Install

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Public Preset Test Commands

Website contact:

```bash
python run.py --config configs/public/website_contact.yaml --url https://example.com --run-name test_scan
```

Business info:

```bash
python run.py --config configs/public/business_info.yaml --url https://example.com --run-name test_business
```

Page data:

```bash
python run.py --config configs/public/page_data.yaml --url https://example.com --run-name test_page
```

Debug raw HTML storage:

```bash
python run.py --config configs/public/page_data.yaml --url https://example.com --run-name test_page_debug --debug-html
```

## Additional Input Examples

Input file run:

```bash
python run.py --config configs/private/contractors.yaml --input seeds.txt --run-name contractors_batch
```

CSV input with a `url` column:

```bash
python run.py --config configs/private/real_estate_agents.yaml --input seeds.csv
```

Override concurrency for a run:

```bash
python run.py --config configs/public/page_data.yaml --url https://example.com --concurrency 8
```

## Output Artifacts

Each run writes to:

```text
outputs/<custom-run-name-or-config>_<timestamp>/
```

Artifacts created for every run:

- `results.csv`
- `results.json`
- `summary.txt`
- `run_report.json`
- `run.log`

Optional when enabled:

- `raw_pages/`

## Config Notes

The engine currently supports:

- JSON config files
- YAML config files
- reusable extractor types such as `page_title`, `business_name`, `emails`, `phones`, `contact_links`, `social_links`, `internal_links`, `address`, and `headings`
- selector-based extraction with CSS or XPath
- selector-based `directory_list` extraction with per-listing CSS field extraction
- sequential `directory_detail` enrichment using config-defined detail URLs and full-document field extraction
- sequential pagination for `directory_list` and `directory_detail` using config-defined next-page selectors
- `--url` input
- `--input` file input from `.txt`, `.csv`, `.json`, `.yaml`, `.yml`
- same-domain prioritized site crawling with retries, timeout handling, duplicate URL prevention, relative URL resolution, and merged public outputs
- optional insecure SSL fallback in config for environments with broken local certificate trust

## Implemented vs Not Yet

Implemented now:

- real v1 `site_scan` execution from the CLI
- public presets for `website_contact`, `business_info`, and `page_data`
- minimum viable `directory_list` mode for extracting multiple records from a single directory page
- minimum viable `directory_detail` mode for enriching listings from fetched detail pages
- safe sequential pagination for directory modes with loop prevention and bounded traversal
- merged single-row public outputs with CSV/JSON/report artifacts
- optional `raw_pages/` storage
- per-run console and file logging
- graceful sync hook handling

Not implemented yet:

- browser-rendered scraping
- advanced directory crawl strategies beyond selector-based next-page traversal
- API or web wrapper
- advanced anti-bot handling
- polished sync deployment automation

## Sample Directory List Config

See `configs/private/real_estate_agents.yaml` for a Stage 3 paginated `directory_list` example that extracts one row per listing container using CSS selectors and follows next-page links.

## Sample Directory Detail Config

See `configs/private/law_firms.yaml` for a Stage 3 paginated `directory_detail` example that extracts listing rows, follows next-page links, and enriches each listing from fetched detail pages.

Public presets are separated from private presets from day one to keep the shared engine clean while supporting different product surfaces later.
