from __future__ import annotations

import logging
from datetime import datetime, timezone

from scraper_engine.core.config_loader import load_config
from scraper_engine.core.job_runner import JobRunner
from scraper_engine.core.models import PipelineResult, RuntimeOptions
from scraper_engine.crawl.crawler import Crawler
from scraper_engine.crawl.fetcher import Fetcher
from scraper_engine.extractors.registry import ExtractorRegistry
from scraper_engine.inputs.loaders import load_targets
from scraper_engine.outputs.paths import build_run_context
from scraper_engine.outputs.reporters import write_run_report, write_summary
from scraper_engine.outputs.writers import write_csv, write_json
from scraper_engine.schemas.mapper import SchemaMapper
from scraper_engine.sync.pi_sync import run_sync_hook
from scraper_engine.utils.logging_utils import configure_logging, log_event


class Pipeline:
    def run(self, options: RuntimeOptions) -> PipelineResult:
        config = load_config(options.config_path)
        if options.concurrency is not None:
            config.crawl.concurrency = options.concurrency
        if options.debug_html:
            config.crawl.store_debug_html = True

        output_root = options.output_root or self._default_output_root(options)
        run_context = build_run_context(
            output_root=output_root,
            config_name=config.name,
            requested_run_name=options.run_name,
            log_file_name=config.logging.file_name,
            save_raw_html=config.crawl.store_debug_html,
        )

        log_level = options.log_level or config.logging.level
        logger = configure_logging(level=log_level, log_file=run_context.log_file)
        log_event(
            logger,
            logging.INFO,
            "START RUN",
            "Initialized run.",
            run_name=run_context.run_name,
            mode=config.mode,
            config_path=options.config_path.resolve(),
            output_dir=run_context.output_dir,
        )
        if config.requests.allow_insecure_fallback:
            log_event(
                logger,
                logging.WARNING,
                "START RUN",
                "Insecure HTTPS fallback is enabled for this run.",
                verify_ssl=config.requests.verify_ssl,
                allow_insecure_fallback=config.requests.allow_insecure_fallback,
            )

        targets = load_targets(options, config)
        log_event(
            logger,
            logging.INFO,
            "START RUN",
            "Loaded crawl targets.",
            target_count=len(targets),
        )
        fetcher = Fetcher(config.requests, logger=logger)
        crawler = Crawler(fetcher, logger=logger)
        runner = JobRunner(
            fetcher=fetcher,
            crawler=crawler,
            extractor_registry=ExtractorRegistry(),
            mapper=SchemaMapper(),
            logger=logger,
        )

        started_at = datetime.now(timezone.utc)
        rows, runner_report = runner.run(targets=targets, config=config, run_context=run_context)
        ended_at = datetime.now(timezone.utc)
        sync_result = run_sync_hook(config.sync, run_context, logger=logger)
        duration_seconds = round((ended_at - started_at).total_seconds(), 3)

        report = {
            "run_name": run_context.run_name,
            "config_name": config.name,
            "mode": config.mode,
            "config_path": str(options.config_path.resolve()),
            "input_targets": targets,
            "started_at": started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
            "duration_seconds": duration_seconds,
            "duration": f"{duration_seconds:.3f}s",
            "target_count": runner_report["target_count"],
            "row_count": runner_report["row_count"],
            "error_count": runner_report["error_count"],
            "pages_crawled": runner_report["pages_crawled"],
            "pages_succeeded": runner_report["pages_succeeded"],
            "pages_failed": runner_report["pages_failed"],
            "listing_count": runner_report["listing_count"],
            "detail_pages_attempted": runner_report["detail_pages_attempted"],
            "detail_pages_successful": runner_report["detail_pages_successful"],
            "detail_pages_failed": runner_report["detail_pages_failed"],
            "extracted_field_names": runner_report["extracted_field_names"],
            "notes": runner_report["notes"],
            "errors": runner_report["errors"],
            "targets": runner_report["targets"],
            "sync_attempted": bool(sync_result.get("attempted")),
            "sync_success": bool(sync_result.get("success")),
            "sync_result": sync_result,
            "config_snapshot": config.to_dict(),
        }

        preferred_columns = list(config.schema.get("columns", []))
        if not preferred_columns:
            preferred_columns = ["input_url", "source_url", "page_url", "status_code"]
            preferred_columns.extend(field.name for field in config.extraction.fields)
            preferred_columns.append("source_urls")
        if config.output.write_csv:
            write_csv(run_context.output_dir / "results.csv", rows, preferred_columns)
        if config.output.write_json:
            write_json(run_context.output_dir / "results.json", rows)
        if config.output.write_summary:
            write_summary(run_context.output_dir / "summary.txt", report)
        if config.output.write_report:
            write_run_report(run_context.output_dir / "run_report.json", report)
        log_event(
            logger,
            logging.INFO,
            "WRITE OUTPUTS",
            "Wrote run outputs.",
            results_csv=config.output.write_csv,
            results_json=config.output.write_json,
            summary=config.output.write_summary,
            run_report=config.output.write_report,
            row_count=report["row_count"],
        )
        log_event(
            logger,
            logging.INFO,
            "END RUN",
            "Run complete.",
            run_name=run_context.run_name,
            row_count=report["row_count"],
            error_count=report["error_count"],
            duration=report["duration"],
        )

        return PipelineResult(
            run_context=run_context,
            rows=rows,
            report=report,
            sync_result=sync_result,
        )

    def _default_output_root(self, options: RuntimeOptions):
        config_path = options.config_path.resolve()
        if (
            config_path.parent.name in {"public", "private"}
            and config_path.parent.parent.name == "configs"
        ):
            return config_path.parent.parent.parent / "outputs"
        return config_path.parent / "outputs"
