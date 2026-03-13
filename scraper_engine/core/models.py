from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


DEFAULT_PRIORITY_KEYWORDS = [
    "contact",
    "about",
    "team",
    "staff",
    "office",
    "location",
    "get-in-touch",
]


@dataclass
class RequestConfig:
    timeout_seconds: int = 20
    retries: int = 3
    backoff_seconds: float = 1.0
    user_agent: str = "scraper-engine/0.1"
    verify_ssl: bool = True
    allow_insecure_fallback: bool = False


@dataclass
class CrawlConfig:
    mode: str = "site_scan"
    same_domain_only: bool = True
    max_pages: int = 10
    concurrency: int = 5
    priority_keywords: list[str] = field(default_factory=lambda: list(DEFAULT_PRIORITY_KEYWORDS))
    store_debug_html: bool = False


@dataclass
class ExtractorFieldConfig:
    name: str
    type: str | None = None
    selector: str | None = None
    xpath: str | None = None
    attribute: str | None = None
    many: bool = False
    default: Any = None
    transforms: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class RecordSelectorConfig:
    css: str | None = None


@dataclass
class DetailPageConfig:
    enabled: bool = False
    url_field: str | None = None
    fields: list[ExtractorFieldConfig] = field(default_factory=list)


@dataclass
class NextPageConfig:
    css: str | None = None
    attribute: str = "href"


@dataclass
class PaginationConfig:
    enabled: bool = False
    max_pages: int = 1
    next_page: NextPageConfig | None = None


@dataclass
class ExtractionConfig:
    fields: list[ExtractorFieldConfig] = field(default_factory=list)
    record_selector: RecordSelectorConfig | None = None
    detail_page: DetailPageConfig | None = None


@dataclass
class NormalizationConfig:
    empty_like_strings: list[str] = field(default_factory=list)


@dataclass
class OutputShapingConfig:
    include_fields: list[str] = field(default_factory=list)
    exclude_fields: list[str] = field(default_factory=list)
    field_order: list[str] = field(default_factory=list)
    flatten_single_item_lists: bool = False
    flatten_fields: list[str] = field(default_factory=list)
    join_fields: dict[str, str] = field(default_factory=dict)
    drop_empty_fields: bool = False
    cleanup_common_fields: bool = False


@dataclass
class OutputConfig:
    root_dir: str = "outputs"
    merge_rows: bool = True
    write_csv: bool = True
    write_json: bool = True
    write_summary: bool = True
    write_report: bool = True
    shaping: OutputShapingConfig = field(default_factory=OutputShapingConfig)


@dataclass
class RunLimitsConfig:
    max_records: int | None = None
    max_detail_pages: int | None = None


@dataclass
class LoggingConfig:
    level: str = "INFO"
    file_name: str = "run.log"


@dataclass
class SyncConfig:
    enabled: bool = False
    strategy: str = "rsync"
    host: str | None = None
    username: str | None = None
    destination_path: str | None = None
    dry_run: bool = True


@dataclass
class EngineConfig:
    name: str
    mode: str
    description: str = ""
    preset: str | None = None
    requests: RequestConfig = field(default_factory=RequestConfig)
    crawl: CrawlConfig = field(default_factory=CrawlConfig)
    extraction: ExtractionConfig = field(default_factory=ExtractionConfig)
    normalization: NormalizationConfig = field(default_factory=NormalizationConfig)
    pagination: PaginationConfig = field(default_factory=PaginationConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    limits: RunLimitsConfig = field(default_factory=RunLimitsConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    sync: SyncConfig = field(default_factory=SyncConfig)
    schema: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    static_targets: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "EngineConfig":
        extraction = payload.get("extraction", {})
        fields = [
            ExtractorFieldConfig(**field_payload)
            for field_payload in extraction.get("fields", [])
        ]
        record_selector_payload = extraction.get("record_selector")
        detail_page_payload = extraction.get("detail_page")
        pagination_payload = payload.get("pagination", {})
        output_payload = payload.get("output", {})
        limits_payload = payload.get("limits", {})
        detail_page_fields = [
            ExtractorFieldConfig(**field_payload)
            for field_payload in (detail_page_payload or {}).get("fields", [])
        ]
        return cls(
            name=payload["name"],
            mode=payload.get("mode", "site_scan"),
            description=payload.get("description", ""),
            preset=payload.get("preset"),
            requests=RequestConfig(**payload.get("requests", {})),
            crawl=CrawlConfig(**payload.get("crawl", {})),
            extraction=ExtractionConfig(
                fields=fields,
                record_selector=RecordSelectorConfig(**record_selector_payload)
                if record_selector_payload
                else None,
                detail_page=DetailPageConfig(
                    enabled=detail_page_payload.get("enabled", False),
                    url_field=detail_page_payload.get("url_field"),
                    fields=detail_page_fields,
                )
                if detail_page_payload
                else None,
            ),
            normalization=NormalizationConfig(**payload.get("normalization", {})),
            pagination=PaginationConfig(
                enabled=pagination_payload.get("enabled", False),
                max_pages=pagination_payload.get("max_pages", 1),
                next_page=NextPageConfig(**pagination_payload["next_page"])
                if pagination_payload.get("next_page")
                else None,
            ),
            output=OutputConfig(
                root_dir=output_payload.get("root_dir", "outputs"),
                merge_rows=output_payload.get("merge_rows", True),
                write_csv=output_payload.get("write_csv", True),
                write_json=output_payload.get("write_json", True),
                write_summary=output_payload.get("write_summary", True),
                write_report=output_payload.get("write_report", True),
                shaping=OutputShapingConfig(**output_payload.get("shaping", {})),
            ),
            limits=RunLimitsConfig(**limits_payload),
            logging=LoggingConfig(**payload.get("logging", {})),
            sync=SyncConfig(**payload.get("sync", {})),
            schema=payload.get("schema", {}),
            metadata=payload.get("metadata", {}),
            static_targets=list(payload.get("static_targets", [])),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RuntimeOptions:
    config_path: Path
    single_url: str | None = None
    input_path: Path | None = None
    run_name: str | None = None
    output_root: Path | None = None
    concurrency: int | None = None
    log_level: str | None = None
    debug_html: bool = False


@dataclass
class RunContext:
    run_name: str
    started_at: datetime
    output_root: Path
    output_dir: Path
    log_file: Path
    raw_pages_dir: Path | None = None


@dataclass
class DiscoveredLink:
    url: str
    anchor_text: str = ""
    score: int = 0


@dataclass
class CrawlPage:
    requested_url: str
    url: str
    status_code: int | None = None
    html: str | None = None
    error: str | None = None
    links: list[str] = field(default_factory=list)
    depth: int = 0
    redirected: bool = False


@dataclass
class CrawlResult:
    seed_url: str
    pages: list[CrawlPage] = field(default_factory=list)
    queued_links: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass
class PipelineResult:
    run_context: RunContext
    rows: list[dict[str, Any]]
    report: dict[str, Any]
    sync_result: dict[str, Any]
