from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from scraper_engine.core.clean_csv_runner import run_clean_csv_job
from scraper_engine.core.config_loader import load_config
from scraper_engine.core.job_runner import JobRunner
from scraper_engine.core.models import PipelineResult, RuntimeOptions
from scraper_engine.crawl.crawler import Crawler
from scraper_engine.crawl.fetcher import Fetcher
from scraper_engine.extractors.registry import ExtractorRegistry
from scraper_engine.inputs.loaders import load_target_contexts, load_targets
from scraper_engine.outputs.lead_cleaner import cleaned_output_file_name, write_cleaned_lead_csv
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

        if config.mode == "clean_csv":
            return self._run_clean_csv_mode(
                options=options,
                config=config,
                run_context=run_context,
                logger=logger,
            )

        targets = load_targets(options, config)
        target_contexts = load_target_contexts(options, config)
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
        rows = self._expand_rows_with_input_context(rows, target_contexts)
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
            "row_count": len(rows),
            "error_count": runner_report["error_count"],
            "pages_crawled": runner_report["pages_crawled"],
            "pages_succeeded": runner_report["pages_succeeded"],
            "pages_failed": runner_report["pages_failed"],
            "pages_visited": runner_report["pages_visited"],
            "listing_count": runner_report["listing_count"],
            "detail_pages_attempted": runner_report["detail_pages_attempted"],
            "detail_pages_successful": runner_report["detail_pages_successful"],
            "detail_pages_failed": runner_report["detail_pages_failed"],
            "pagination_urls_followed": runner_report["pagination_urls_followed"],
            "pagination_stopped_reasons": runner_report["pagination_stopped_reasons"],
            "extracted_field_names": runner_report["extracted_field_names"],
            "notes": runner_report["notes"],
            "errors": runner_report["errors"],
            "targets": runner_report["targets"],
            "diagnostics": runner_report.get("diagnostics", {}),
            "limits": runner_report.get("limits", {}),
            "sync_attempted": bool(sync_result.get("attempted")),
            "sync_success": bool(sync_result.get("success")),
            "sync_result": sync_result,
            "config_snapshot": config.to_dict(),
        }
        if target_contexts:
            report["notes"].append("Preserved input CSV row context on enriched output rows.")
        report["diagnostics"]["pagination_stop_reason_counts"] = dict(
            sorted(Counter(report.get("pagination_stopped_reasons", [])).items())
        )
        report["quality_metrics"] = self._build_quality_metrics(
            rows,
            runner_report["extracted_field_names"],
        )

        preferred_columns = self._resolve_preferred_columns(config, rows)
        if config.output.write_csv:
            write_csv(
                run_context.output_dir / self._get_output_file_name(config, "csv", "results.csv"),
                rows,
                preferred_columns,
            )
        cleaned_output = self._write_configured_cleaned_output(
            rows=rows,
            config=config,
            run_context=run_context,
            logger=logger,
        )
        if cleaned_output is not None:
            report["cleaned_output"] = cleaned_output
        if config.output.write_json:
            write_json(
                run_context.output_dir / self._get_output_file_name(config, "json", "results.json"),
                rows,
            )
        if config.output.write_summary:
            write_summary(
                run_context.output_dir / self._get_output_file_name(config, "summary", "summary.txt"),
                report,
            )
        if config.output.write_report:
            write_run_report(
                run_context.output_dir / self._get_output_file_name(config, "report", "run_report.json"),
                report,
            )
        log_event(
            logger,
            logging.INFO,
            "WRITE OUTPUTS",
            "Wrote run outputs.",
            results_csv=config.output.write_csv,
            results_json=config.output.write_json,
            summary=config.output.write_summary,
            run_report=config.output.write_report,
            cleaned_csv=bool(cleaned_output),
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
        if config_path.parent.parent.name == "configs":
            return config_path.parent.parent.parent / "outputs"
        return config_path.parent / "outputs"

    def _resolve_preferred_columns(self, config, rows: list[dict[str, object]]) -> list[str]:
        columns = list(config.schema.get("columns", []))
        if not columns:
            columns = ["input_url", "source_url", "page_url", "status_code"]
            columns.extend(field.name for field in config.extraction.fields)
            if config.extraction.detail_page is not None:
                columns.extend(field.name for field in config.extraction.detail_page.fields)
            columns.append("source_urls")

        row_keys: list[str] = []
        for row in rows:
            for key in row.keys():
                if key not in row_keys:
                    row_keys.append(key)

        if config.output.shaping.include_fields:
            include_set = set(config.output.shaping.include_fields)
            columns = [column for column in columns if column in include_set]
            row_keys = [key for key in row_keys if key in include_set]
        if config.output.shaping.exclude_fields:
            exclude_set = set(config.output.shaping.exclude_fields)
            columns = [column for column in columns if column not in exclude_set]
            row_keys = [key for key in row_keys if key not in exclude_set]

        for key in row_keys:
            if key not in columns:
                columns.append(key)

        if config.output.shaping.field_order:
            order = config.output.shaping.field_order
            ordered = [column for column in order if column in columns]
            ordered.extend(column for column in columns if column not in ordered)
            columns = ordered

        return columns

    def _get_output_file_name(
        self,
        config,
        output_key: str,
        default_name: str,
    ) -> str:
        output_files = config.metadata.get("output_files", {})
        if not isinstance(output_files, dict):
            return default_name
        file_name = str(output_files.get(output_key) or "").strip()
        return file_name or default_name

    def _run_clean_csv_mode(
        self,
        *,
        options: RuntimeOptions,
        config,
        run_context,
        logger,
    ) -> PipelineResult:
        started_at = datetime.now(timezone.utc)
        cleaned_rows, clean_csv = run_clean_csv_job(
            config=config,
            config_path=options.config_path,
            input_path=options.input_path,
            run_context=run_context,
        )
        ended_at = datetime.now(timezone.utc)
        sync_result = run_sync_hook(config.sync, run_context, logger=logger)
        duration_seconds = round((ended_at - started_at).total_seconds(), 3)

        report = {
            "run_name": run_context.run_name,
            "config_name": config.name,
            "mode": config.mode,
            "config_path": str(options.config_path.resolve()),
            "input_targets": [str(options.input_path.resolve())] if options.input_path else [],
            "started_at": started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
            "duration_seconds": duration_seconds,
            "duration": f"{duration_seconds:.3f}s",
            "target_count": 1 if options.input_path else 0,
            "row_count": len(cleaned_rows),
            "error_count": 0,
            "pages_crawled": 0,
            "pages_succeeded": 0,
            "pages_failed": 0,
            "pages_visited": 0,
            "listing_count": 0,
            "detail_pages_attempted": 0,
            "detail_pages_successful": 0,
            "detail_pages_failed": 0,
            "pagination_urls_followed": 0,
            "pagination_stopped_reasons": [],
            "extracted_field_names": list(cleaned_rows[0].keys()) if cleaned_rows else [],
            "notes": [
                f"Applied cleaner config {clean_csv['cleaner_config_path']}.",
                f"Wrote cleaned dataset to {clean_csv['cleaned_output_path']}.",
            ],
            "errors": [],
            "targets": [],
            "diagnostics": {"pagination_stop_reason_counts": {}},
            "limits": {},
            "sync_attempted": bool(sync_result.get("attempted")),
            "sync_success": bool(sync_result.get("success")),
            "sync_result": sync_result,
            "config_snapshot": config.to_dict(),
            "clean_csv": clean_csv,
        }
        report["quality_metrics"] = self._build_quality_metrics(
            cleaned_rows,
            report["extracted_field_names"],
        )

        if config.output.write_summary:
            write_summary(
                run_context.output_dir / self._get_output_file_name(config, "summary", "summary.txt"),
                report,
            )
        if config.output.write_report:
            write_run_report(
                run_context.output_dir / self._get_output_file_name(config, "report", "run_report.json"),
                report,
            )
        log_event(
            logger,
            logging.INFO,
            "WRITE OUTPUTS",
            "Wrote clean_csv outputs.",
            cleaned_output=clean_csv["cleaned_output_path"],
            cleaned_rows=len(cleaned_rows),
            summary=config.output.write_summary,
            run_report=config.output.write_report,
        )
        log_event(
            logger,
            logging.INFO,
            "END RUN",
            "Clean CSV run complete.",
            run_name=run_context.run_name,
            row_count=report["row_count"],
            duration=report["duration"],
        )
        return PipelineResult(
            run_context=run_context,
            rows=cleaned_rows,
            report=report,
            sync_result=sync_result,
        )

    def _write_configured_cleaned_output(
        self,
        rows: list[dict[str, Any]],
        config,
        run_context,
        logger,
    ) -> dict[str, Any] | None:
        settings = self._get_lead_cleaning_settings(config)
        if not settings.get("enabled"):
            return None

        output_path = run_context.output_dir / cleaned_output_file_name(settings)
        try:
            cleaned_output = write_cleaned_lead_csv(output_path, rows, settings)
        except Exception as error:
            log_event(
                logger,
                logging.WARNING,
                "WRITE OUTPUTS",
                "Failed to write cleaned lead output; continuing with standard outputs.",
                cleaned_output_file=output_path,
                error=str(error),
            )
            return {
                "enabled": True,
                "path": str(output_path),
                "file_name": output_path.name,
                "success": False,
                "error": str(error),
            }

        cleaned_output["enabled"] = True
        cleaned_output["success"] = True
        return cleaned_output

    def _get_lead_cleaning_settings(self, config) -> dict[str, Any]:
        settings = config.metadata.get("lead_cleaning", {})
        return settings if isinstance(settings, dict) else {}

    def _expand_rows_with_input_context(
        self,
        rows: list[dict[str, Any]],
        target_contexts: dict[str, list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        if not target_contexts:
            return rows

        expanded_rows: list[dict[str, Any]] = []
        for row in rows:
            input_url = str(row.get("input_url") or "")
            contexts = target_contexts.get(input_url)
            if not contexts:
                expanded_rows.append(row)
                continue
            for context in contexts:
                expanded_rows.append({**context, **row})
        return expanded_rows

    def _build_quality_metrics(
        self,
        rows: list[dict[str, Any]],
        configured_fields: list[str],
    ) -> dict[str, Any]:
        record_count = len(rows)
        final_output_fields_seen: list[str] = []
        for row in rows:
            for key in row.keys():
                if key not in final_output_fields_seen:
                    final_output_fields_seen.append(key)

        field_coverage: dict[str, dict[str, Any]] = {}
        for field_name in configured_fields:
            filled = sum(1 for row in rows if not self._is_empty(row.get(field_name)))
            empty = max(0, record_count - filled)
            field_coverage[field_name] = {
                "filled": filled,
                "empty": empty,
                "fill_rate": round((filled / record_count), 3) if record_count else 0.0,
                "empty_rate": round((empty / record_count), 3) if record_count else 0.0,
            }

        records_with_all_configured_fields_empty = 0
        if configured_fields:
            records_with_all_configured_fields_empty = sum(
                1
                for row in rows
                if all(self._is_empty(row.get(field_name)) for field_name in configured_fields)
            )

        return {
            "records_produced": record_count,
            "configured_field_count": len(configured_fields),
            "configured_fields": list(configured_fields),
            "final_output_fields_seen": final_output_fields_seen,
            "records_with_all_configured_fields_empty": records_with_all_configured_fields_empty,
            "configured_field_coverage": field_coverage,
        }

    def _is_empty(self, value: Any) -> bool:
        return value in (None, "", [])
