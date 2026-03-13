from __future__ import annotations

import ipaddress
import json
import logging
import os
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Callable
from urllib.parse import urlparse

from scraper_engine.core.config_loader import load_config, load_raw_config
from scraper_engine.core.models import PipelineResult, RuntimeOptions
from scraper_engine.core.pipeline import Pipeline
from scraper_engine.crawl.url_utils import normalize_url
from scraper_engine.outputs.writers import write_json


PUBLIC_OUTPUT_FILES = (
    "results.csv",
    "results.json",
    "summary.txt",
    "run_report.json",
)

ADVANCED_HELP_MESSAGE = (
    "This site may require advanced scraping or custom extraction. "
    "If you need help scraping this data, CRK Dev offers custom scraping solutions."
)
DEFAULT_MAX_PAGES = 10
DEFAULT_MAX_RECORDS = 500
DEFAULT_MAX_RUNTIME_SECONDS = 120
DEFAULT_MAX_DOWNLOAD_BYTES = 15 * 1024 * 1024
DEFAULT_MAX_JOBS_PER_HOUR_PER_IP = 5
DEFAULT_RETENTION_HOURS = 48
RATE_LIMIT_WINDOW_SECONDS = 3600
WORKER_LOG_FILE = "worker.log"

LOGGER = logging.getLogger("scraper_engine.hosted")
DEFAULT_HOSTED_STATS = {
    "total_jobs_run": 0,
    "total_records_extracted": 0,
    "total_completed_jobs": 0,
}
_STATS_LOCK = Lock()


class HostedServiceError(Exception):
    def __init__(
        self,
        *,
        status_code: int,
        reason: str,
        message: str,
        suggestion: str,
        help_message: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.reason = reason
        self.message = message
        self.suggestion = suggestion
        self.help_message = help_message

    def to_failure(self, status: str = "failed") -> dict[str, Any]:
        payload = {
            "status": status,
            "reason": self.reason,
            "message": self.message,
            "suggestion": self.suggestion,
        }
        if self.help_message:
            payload["help"] = self.help_message
        return payload


def failure_payload(
    *,
    reason: str,
    message: str,
    suggestion: str,
    help_message: str | None = None,
) -> dict[str, Any]:
    payload = {
        "status": "failed",
        "reason": reason,
        "message": message,
        "suggestion": suggestion,
    }
    if help_message:
        payload["help"] = help_message
    return payload


def classify_report_failure(report: dict[str, Any]) -> dict[str, Any] | None:
    row_count = int(report.get("row_count", 0) or 0)
    if row_count > 0:
        return None

    limits = report.get("limits", {})
    diagnostics = report.get("diagnostics", {})
    stop_reasons = set(report.get("pagination_stopped_reasons", []))
    errors = " | ".join(report.get("errors", [])).lower()
    mode = report.get("mode", "")
    listing_count = int(report.get("listing_count", 0) or 0)

    if limits.get("max_records_hit"):
        return failure_payload(
            reason="max_records_reached",
            message="The hosted scraper hit the record limit before it could return useful results.",
            suggestion="Narrow the target scope or request a custom scraping workflow.",
        )
    if "max_pages_reached" in stop_reasons:
        return failure_payload(
            reason="max_pages_reached",
            message="The hosted scraper stopped after reaching the page limit for this preset.",
            suggestion="Try a narrower target or request a custom scraping workflow for larger sites.",
        )
    if "duplicate_next_page_detected" in stop_reasons:
        return failure_payload(
            reason="pagination_loop_detected",
            message="The scraper detected a pagination loop and stopped safely.",
            suggestion="The website structure may not match this preset.",
            help_message=ADVANCED_HELP_MESSAGE,
        )
    if "no_next_page_found" in stop_reasons and mode in {"directory_list", "directory_detail"}:
        return failure_payload(
            reason="pagination_selector_not_found",
            message="The scraper could not find the next page selector defined in the config.",
            suggestion="The website structure may not match this preset.",
            help_message=ADVANCED_HELP_MESSAGE,
        )
    if any(token in errors for token in ("cloudflare", "captcha", "403", "429", "forbidden", "blocked")):
        return failure_payload(
            reason="blocked_by_site",
            message="The target site appears to be blocking automated requests.",
            suggestion="Try a different site or contact CRK Dev for a custom scraping solution.",
            help_message=ADVANCED_HELP_MESSAGE,
        )
    if any(token in errors for token in ("timed out", "timeout")):
        return failure_payload(
            reason="timeout",
            message="The request timed out before the hosted scraper could finish loading the site.",
            suggestion="Try again later or verify that the target site is responsive.",
        )
    if any(
        token in errors
        for token in (
            "connection",
            "dns",
            "name or service not known",
            "getaddrinfo",
            "failed to establish a new connection",
        )
    ):
        return failure_payload(
            reason="connection_failed",
            message="The hosted scraper could not connect to the target site.",
            suggestion="Verify the URL and confirm the site is publicly reachable.",
        )
    if diagnostics.get("non_fatal_issue_counts", {}).get("page_extraction_failed") or diagnostics.get(
        "non_fatal_issue_counts", {}
    ).get("detail_field_extraction_failed"):
        return failure_payload(
            reason="extraction_failed",
            message="The scraper could not extract usable data from the target site.",
            suggestion="The website structure may not match this preset.",
            help_message=ADVANCED_HELP_MESSAGE,
        )
    if diagnostics.get("non_fatal_issue_counts", {}).get("detail_page_fetch_failed") and listing_count > 0:
        return failure_payload(
            reason="extraction_failed",
            message="The scraper found listings but could not load any detail pages successfully.",
            suggestion="The site may have blocked detail-page access or changed structure.",
            help_message=ADVANCED_HELP_MESSAGE,
        )
    if listing_count == 0 and mode in {"directory_list", "directory_detail"}:
        return failure_payload(
            reason="no_results_found",
            message="The website structure did not match the selected hosted preset.",
            suggestion="Try a different preset or request a custom extraction setup.",
            help_message=ADVANCED_HELP_MESSAGE,
        )

    return failure_payload(
        reason="no_results_found",
        message="The hosted scraper completed but did not produce useful results.",
        suggestion="Verify that the site content is public and that the selected preset matches the site.",
        help_message=ADVANCED_HELP_MESSAGE,
    )


def classify_exception_failure(error: Exception) -> dict[str, Any]:
    message = str(error).lower()
    if any(token in message for token in ("cloudflare", "captcha", "403", "429", "forbidden", "blocked")):
        return failure_payload(
            reason="blocked_by_site",
            message="The target site appears to be blocking automated requests.",
            suggestion="Try a different site or contact CRK Dev for a custom scraping solution.",
            help_message=ADVANCED_HELP_MESSAGE,
        )
    if any(token in message for token in ("timed out", "timeout")):
        return failure_payload(
            reason="timeout",
            message="The hosted scraper timed out while requesting the target site.",
            suggestion="Try again later or verify that the target site is responsive.",
        )
    if any(
        token in message
        for token in (
            "connection",
            "dns",
            "name or service not known",
            "getaddrinfo",
            "failed to establish a new connection",
        )
    ):
        return failure_payload(
            reason="connection_failed",
            message="The hosted scraper could not connect to the target site.",
            suggestion="Verify the URL and confirm the site is publicly reachable.",
        )
    if "extract" in message:
        return failure_payload(
            reason="extraction_failed",
            message="The hosted scraper could not extract usable data from the target site.",
            suggestion="The website structure may not match this preset.",
            help_message=ADVANCED_HELP_MESSAGE,
        )
    return failure_payload(
        reason="extraction_failed",
        message="The hosted scraper could not complete this run.",
        suggestion="Review the run status and try again, or contact CRK Dev if the issue persists.",
    )


def build_file_availability(output_dir: Path | None) -> dict[str, Any]:
    files: dict[str, Any] = {}
    for file_name in PUBLIC_OUTPUT_FILES:
        available = bool(output_dir and (output_dir / file_name).exists())
        files[file_name] = {
            "available": available,
            "download_path": None if not available else f"/api/jobs/__JOB_ID__/download/{file_name}",
        }
    return files


def run_hosted_job_process(metadata_path: Path) -> None:
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    try:
        pipeline = Pipeline()
        result = pipeline.run(
            RuntimeOptions(
                config_path=Path(metadata["config_path"]),
                single_url=metadata["target_url"],
                run_name=metadata["requested_run_name"] or metadata["job_id"],
                output_root=Path(metadata["output_root"]),
            )
        )
        failure = classify_report_failure(result.report)
        metadata.update(
            {
                "status": "failed" if failure else "completed",
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "run_name": result.run_context.run_name,
                "output_dir": str(result.run_context.output_dir.resolve()),
                "files": build_file_availability(result.run_context.output_dir),
                "failure": failure,
            }
        )
    except Exception as error:
        LOGGER.exception("Hosted worker failed for job %s", metadata.get("job_id"))
        metadata.update(
            {
                "status": "failed",
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "failure": classify_exception_failure(error),
            }
        )

    _update_stats_from_metadata(
        stats_path=Path(metadata["output_root"]) / "hosted_stats.json",
        metadata=metadata,
    )
    write_json(metadata_path, metadata)


class HostedJobService:
    def __init__(
        self,
        *,
        public_config_root: Path,
        output_root: Path,
        pipeline_runner: Callable[[RuntimeOptions], PipelineResult] | None = None,
        run_jobs_inline: bool = False,
        max_pages: int | None = None,
        max_records: int | None = None,
        max_runtime_seconds: int | None = None,
        max_download_bytes: int | None = None,
        max_jobs_per_hour_per_ip: int | None = None,
        retention_hours: int | None = None,
    ) -> None:
        self.public_config_root = public_config_root
        self.output_root = output_root
        self.jobs_root = output_root / "hosted_jobs"
        self.stats_path = self.output_root / "hosted_stats.json"
        self.jobs_root.mkdir(parents=True, exist_ok=True)
        self._run_jobs_inline = run_jobs_inline
        self._pipeline_runner = pipeline_runner or self._default_pipeline_runner
        self._metadata_lock = Lock()
        self._stats_lock = _STATS_LOCK
        self._rate_limit_lock = Lock()
        self._submission_history: dict[str, list[float]] = {}
        self.max_pages = (
            max_pages
            if max_pages is not None
            else _env_int("SCRAPER_ENGINE_HOSTED_MAX_PAGES", DEFAULT_MAX_PAGES)
        )
        self.max_records = (
            max_records
            if max_records is not None
            else _env_int("SCRAPER_ENGINE_HOSTED_MAX_RECORDS", DEFAULT_MAX_RECORDS)
        )
        self.max_runtime_seconds = (
            max_runtime_seconds
            if max_runtime_seconds is not None
            else _env_int("SCRAPER_ENGINE_HOSTED_MAX_RUNTIME_SECONDS", DEFAULT_MAX_RUNTIME_SECONDS)
        )
        self.max_download_bytes = (
            max_download_bytes
            if max_download_bytes is not None
            else _env_int("SCRAPER_ENGINE_HOSTED_MAX_DOWNLOAD_BYTES", DEFAULT_MAX_DOWNLOAD_BYTES)
        )
        self.max_jobs_per_hour_per_ip = (
            max_jobs_per_hour_per_ip
            if max_jobs_per_hour_per_ip is not None
            else _env_int(
                "SCRAPER_ENGINE_HOSTED_MAX_JOBS_PER_HOUR_PER_IP",
                DEFAULT_MAX_JOBS_PER_HOUR_PER_IP,
            )
        )
        self.retention_hours = (
            retention_hours
            if retention_hours is not None
            else _env_int("SCRAPER_ENGINE_HOSTED_RETENTION_HOURS", DEFAULT_RETENTION_HOURS)
        )
        self._load_stats()
        self._recover_incomplete_jobs()
        self._cleanup_old_jobs()

    def capabilities(self) -> dict[str, Any]:
        presets = self.list_presets()
        return {
            "allowed_presets": presets,
            "job_statuses": ["queued", "running", "completed", "failed"],
            "downloadable_files": list(PUBLIC_OUTPUT_FILES),
        }

    def limits(self) -> dict[str, Any]:
        presets = self.list_presets()
        return {
            "allowed_presets": [preset["preset"] for preset in presets],
            "max_pages": self.max_pages,
            "max_records": self.max_records,
            "max_runtime_seconds": self.max_runtime_seconds,
            "retention_hours": self.retention_hours,
            "presets": [
                {
                    "preset": preset["preset"],
                    "mode": preset["mode"],
                    "limits": preset["limits"],
                }
                for preset in presets
            ],
        }

    def health(self) -> dict[str, Any]:
        writable = self._storage_writable()
        return {
            "status": "ok" if writable else "degraded",
            "ready": writable,
            "storage": {
                "jobs_root": str(self.jobs_root.resolve()),
                "writable": writable,
            },
        }

    def get_stats(self) -> dict[str, int]:
        stats = self._load_stats()
        return {
            "jobs_run": int(stats.get("total_jobs_run", 0) or 0),
            "records_extracted": int(stats.get("total_records_extracted", 0) or 0),
            "completed_scrapes": int(stats.get("total_completed_jobs", 0) or 0),
        }

    def list_presets(self) -> list[dict[str, Any]]:
        preset_paths = sorted(self.public_config_root.glob("*.yaml"))
        preset_paths.extend(sorted(self.public_config_root.glob("*.yml")))
        preset_paths.extend(sorted(self.public_config_root.glob("*.json")))

        seen_ids: set[str] = set()
        presets: list[dict[str, Any]] = []
        for config_path in preset_paths:
            preset_id = config_path.stem
            if preset_id in seen_ids:
                continue
            seen_ids.add(preset_id)

            config = load_config(config_path)
            presets.append(
                {
                    "preset": preset_id,
                    "config_name": config.name,
                    "description": config.description,
                    "mode": config.mode,
                    "limits": {
                        "max_pages": (
                            config.pagination.max_pages
                            if config.pagination.enabled
                            else config.crawl.max_pages
                        ),
                        "max_records": config.limits.max_records,
                        "max_detail_pages": config.limits.max_detail_pages,
                    },
                }
            )
        return presets

    def submit_job(
        self,
        *,
        preset: str,
        target_url: str,
        run_name: str | None = None,
        client_ip: str | None = None,
    ) -> dict[str, Any]:
        self._cleanup_old_jobs()
        config_path = self._resolve_public_config_path(preset)
        normalized_target = self._validate_target_url(target_url)
        self._enforce_rate_limit(client_ip)

        job_id = uuid.uuid4().hex[:12]
        job_dir = self.jobs_root / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        config = load_config(config_path)
        runtime_config_path = self._build_runtime_config(config_path, job_dir)

        metadata = {
            "job_id": job_id,
            "status": "queued",
            "preset": preset,
            "config_name": config.name,
            "mode": config.mode,
            "target_url": normalized_target,
            "client_ip": client_ip,
            "requested_run_name": (run_name or "").strip() or None,
            "config_path": str(runtime_config_path.resolve()),
            "source_config_path": str(config_path.resolve()),
            "output_root": str(self.output_root.resolve()),
            "created_at": self._utc_now(),
            "started_at": None,
            "finished_at": None,
            "run_name": None,
            "output_dir": None,
            "worker_pid": None,
            "worker_log": str((job_dir / WORKER_LOG_FILE).resolve()),
            "files": self._build_file_availability(None),
            "failure": None,
            "stats_counted": False,
        }
        self._write_job_metadata(job_id, metadata)
        LOGGER.info("Hosted job submitted: %s preset=%s target=%s", job_id, preset, normalized_target)

        if self._run_jobs_inline:
            self._run_job_inline(job_id)
        else:
            self._start_job_process(job_id)

        return self.get_job(job_id)

    def get_job(self, job_id: str) -> dict[str, Any]:
        self._cleanup_old_jobs()
        metadata = self._read_job_metadata(job_id)
        if metadata is None:
            raise HostedServiceError(
                status_code=404,
                reason="job_not_found",
                message="The requested job ID does not exist.",
                suggestion="Submit a new job and use the returned job ID.",
            )
        metadata = self._refresh_job_state(job_id, metadata)
        return self._build_job_response(job_id, metadata)

    def get_download_path(self, job_id: str, file_name: str) -> Path:
        if file_name not in PUBLIC_OUTPUT_FILES:
            raise HostedServiceError(
                status_code=404,
                reason="file_not_supported",
                message="That output file is not available for download.",
                suggestion="Use one of the supported hosted output files.",
            )

        metadata = self.get_job(job_id)
        if metadata["status"] != "completed":
            raise HostedServiceError(
                status_code=409,
                reason="job_not_completed",
                message="Outputs are only available after a job completes successfully.",
                suggestion="Poll the job status until it reaches completed.",
            )

        output_dir_value = metadata.get("output_dir")
        if not output_dir_value:
            raise HostedServiceError(
                status_code=404,
                reason="output_not_available",
                message="This job does not have an output directory.",
                suggestion="Re-run the job or inspect the job status response.",
            )

        output_dir = Path(output_dir_value).resolve()
        output_root = self.output_root.resolve()
        if not output_dir.is_relative_to(output_root):
            raise HostedServiceError(
                status_code=400,
                reason="unsafe_output_path",
                message="The requested output path is not available for hosted download.",
                suggestion="Submit the job again and download from the returned output links.",
            )

        file_path = (output_dir / file_name).resolve()
        if not file_path.exists():
            raise HostedServiceError(
                status_code=404,
                reason="file_missing",
                message="The requested output file is missing.",
                suggestion="Check the job status or run report for file availability.",
            )
        if file_path.stat().st_size > self.max_download_bytes:
            raise HostedServiceError(
                status_code=413,
                reason="limit_exceeded",
                message="This output file exceeds the hosted download size limit.",
                suggestion="Reduce the scope of the scrape or contact CRK Dev for custom scraping.",
            )
        return file_path

    def _start_job_process(self, job_id: str) -> None:
        metadata = self._read_job_metadata(job_id)
        if metadata is None:
            raise HostedServiceError(
                status_code=404,
                reason="job_not_found",
                message="The requested job ID does not exist.",
                suggestion="Submit a new job and use the returned job ID.",
            )

        worker_log_path = Path(metadata["worker_log"])
        worker_log_path.parent.mkdir(parents=True, exist_ok=True)
        with worker_log_path.open("a", encoding="utf-8") as handle:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "scraper_engine.hosted_worker",
                    "--metadata-path",
                    str(self._metadata_path(job_id)),
                ],
                stdout=handle,
                stderr=subprocess.STDOUT,
                cwd=str(self.output_root.parent.resolve()),
            )

        metadata["status"] = "running"
        metadata["started_at"] = self._utc_now()
        metadata["worker_pid"] = process.pid
        self._write_job_metadata(job_id, metadata)
        LOGGER.info("Hosted job started: %s pid=%s", job_id, process.pid)

    def _refresh_job_state(self, job_id: str, metadata: dict[str, Any]) -> dict[str, Any]:
        status = metadata.get("status")
        if status not in {"queued", "running"}:
            return metadata

        if status == "running" and self._runtime_exceeded(metadata):
            self._terminate_worker(metadata.get("worker_pid"))
            metadata.update(
                {
                    "status": "failed",
                    "finished_at": self._utc_now(),
                    "failure": failure_payload(
                        reason="limit_exceeded",
                        message="This run exceeded the limits of the hosted tool.",
                        suggestion="Reduce the scope of the scrape or contact CRK Dev for custom scraping.",
                    ),
                }
            )
            self._write_job_metadata(job_id, metadata)
            LOGGER.warning("Hosted job exceeded runtime limit: %s", job_id)
            return metadata

        worker_pid = metadata.get("worker_pid")
        if status == "running" and worker_pid and self._is_process_running(worker_pid):
            return metadata

        failure_reason = "service_restarted" if status == "queued" else "interrupted_run"
        failure_message = (
            "This scrape was interrupted because the service restarted before the run completed."
            if status == "running"
            else "This scrape did not start before the service restarted."
        )
        metadata.update(
            {
                "status": "failed",
                "finished_at": self._utc_now(),
                "failure": failure_payload(
                    reason=failure_reason,
                    message=failure_message,
                    suggestion="Please resubmit the scrape.",
                    help_message=ADVANCED_HELP_MESSAGE,
                ),
            }
        )
        self._write_job_metadata(job_id, metadata)
        LOGGER.warning("Hosted job recovered as failed: %s reason=%s", job_id, failure_reason)
        return metadata

    def _run_job_inline(self, job_id: str) -> None:
        metadata = self._read_job_metadata(job_id)
        if metadata is None:
            raise HostedServiceError(
                status_code=404,
                reason="job_not_found",
                message="The requested job ID does not exist.",
                suggestion="Submit a new job and use the returned job ID.",
            )
        metadata["status"] = "running"
        metadata["started_at"] = self._utc_now()
        self._write_job_metadata(job_id, metadata)
        LOGGER.info("Hosted job started inline: %s", job_id)

        try:
            started_at = time.monotonic()
            result = self._pipeline_runner(
                RuntimeOptions(
                    config_path=Path(metadata["config_path"]),
                    single_url=metadata["target_url"],
                    run_name=metadata["requested_run_name"] or metadata["job_id"],
                    output_root=self.output_root,
                )
            )
            elapsed_seconds = time.monotonic() - started_at
            if elapsed_seconds > self.max_runtime_seconds:
                metadata.update(
                    {
                        "status": "failed",
                        "finished_at": self._utc_now(),
                        "failure": failure_payload(
                            reason="limit_exceeded",
                            message="This run exceeded the limits of the hosted tool.",
                            suggestion="Reduce the scope of the scrape or contact CRK Dev for custom scraping.",
                        ),
                    }
                )
                self._write_job_metadata(job_id, metadata)
                LOGGER.warning("Hosted inline job exceeded runtime limit: %s", job_id)
                return
            failure = self._classify_report_failure(result.report)
            metadata.update(
                {
                    "status": "failed" if failure else "completed",
                    "finished_at": self._utc_now(),
                    "run_name": result.run_context.run_name,
                    "output_dir": str(result.run_context.output_dir.resolve()),
                    "files": self._build_file_availability(result.run_context.output_dir),
                    "failure": failure,
                }
            )
        except HostedServiceError as error:
            metadata.update(
                {
                    "status": "failed",
                    "finished_at": self._utc_now(),
                    "failure": error.to_failure(),
                }
            )
        except Exception as error:
            LOGGER.exception("Hosted inline job failed: %s", job_id)
            metadata.update(
                {
                    "status": "failed",
                    "finished_at": self._utc_now(),
                    "failure": self._classify_exception_failure(error),
                }
            )

        self._write_job_metadata(job_id, metadata)
        if metadata["status"] == "completed":
            LOGGER.info("Hosted job completed: %s", job_id)
        else:
            LOGGER.warning("Hosted job failed: %s", job_id)
        self._update_stats_from_job(job_id, metadata)

    def _resolve_public_config_path(self, preset: str) -> Path:
        normalized = preset.strip()
        if not normalized:
            raise HostedServiceError(
                status_code=400,
                reason="unsupported_config",
                message="A public preset is required.",
                suggestion="Choose one of the supported public presets.",
            )

        candidates = [
            self.public_config_root / f"{normalized}.yaml",
            self.public_config_root / f"{normalized}.yml",
            self.public_config_root / f"{normalized}.json",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate

        raise HostedServiceError(
            status_code=400,
            reason="unsupported_config",
            message="The requested preset is not available in the hosted tool.",
            suggestion="Choose one of the supported public presets exposed by the backend.",
        )

    def _validate_target_url(self, target_url: str) -> str:
        raw_target = (target_url or "").strip()
        parsed_input = urlparse(raw_target)
        if parsed_input.scheme and parsed_input.scheme.lower() not in {"http", "https"}:
            raise HostedServiceError(
                status_code=400,
                reason="invalid_target",
                message="The submitted URL is not allowed.",
                suggestion="Please submit a valid public website URL.",
            )
        try:
            normalized = normalize_url(target_url)
        except Exception:
            raise HostedServiceError(
                status_code=400,
                reason="invalid_target",
                message="The submitted URL is not allowed.",
                suggestion="Please submit a valid public website URL.",
            ) from None
        parsed = urlparse(normalized)
        if parsed.scheme not in {"http", "https"}:
            raise HostedServiceError(
                status_code=400,
                reason="invalid_target",
                message="The submitted URL is not allowed.",
                suggestion="Please submit a valid public website URL.",
            )
        hostname = (parsed.hostname or "").lower()
        if hostname in {"localhost", "127.0.0.1", "0.0.0.0"}:
            raise HostedServiceError(
                status_code=400,
                reason="invalid_target",
                message="The submitted URL is not allowed.",
                suggestion="Please submit a valid public website URL.",
            )
        try:
            ip = ipaddress.ip_address(hostname)
        except ValueError:
            return normalized
        if not ip.is_global:
            raise HostedServiceError(
                status_code=400,
                reason="invalid_target",
                message="The submitted URL is not allowed.",
                suggestion="Please submit a valid public website URL.",
            )
        return normalized

    def _classify_report_failure(self, report: dict[str, Any]) -> dict[str, Any] | None:
        return classify_report_failure(report)

    def _classify_exception_failure(self, error: Exception) -> dict[str, Any]:
        return classify_exception_failure(error)

    def _failure_payload(
        self,
        *,
        reason: str,
        message: str,
        suggestion: str,
        help_message: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "status": "failed",
            "reason": reason,
            "message": message,
            "suggestion": suggestion,
        }
        if help_message:
            payload["help"] = help_message
        return payload

    def _build_file_availability(self, output_dir: Path | None) -> dict[str, Any]:
        return build_file_availability(output_dir)

    def _materialize_download_paths(
        self,
        job_id: str,
        files: dict[str, Any],
    ) -> dict[str, Any]:
        materialized: dict[str, Any] = {}
        for file_name, payload in files.items():
            item = dict(payload)
            if item.get("download_path"):
                item["download_path"] = str(item["download_path"]).replace("__JOB_ID__", job_id)
            materialized[file_name] = item
        return materialized

    def _metadata_path(self, job_id: str) -> Path:
        return self.jobs_root / job_id / "job.json"

    def _write_job_metadata(self, job_id: str, metadata: dict[str, Any]) -> None:
        path = self._metadata_path(job_id)
        with self._metadata_lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            write_json(path, metadata)

    def _read_job_metadata(self, job_id: str) -> dict[str, Any] | None:
        path = self._metadata_path(job_id)
        if not path.exists():
            return None
        with self._metadata_lock:
            return json.loads(path.read_text(encoding="utf-8"))

    def _default_pipeline_runner(self, options: RuntimeOptions) -> PipelineResult:
        pipeline = Pipeline()
        return pipeline.run(options)

    def _utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _build_job_response(self, job_id: str, metadata: dict[str, Any]) -> dict[str, Any]:
        files = self._materialize_download_paths(job_id, metadata.get("files", {}))
        run_report = self._load_run_report(metadata.get("output_dir"))
        response = dict(metadata)
        response["files"] = files
        response["files_available"] = {
            file_name: file_info.get("available", False)
            for file_name, file_info in files.items()
        }
        response["progress"] = self._build_progress(run_report, response["status"])
        response["current_phase"] = self._determine_current_phase(response["status"])

        if run_report:
            response["pages_visited"] = run_report.get("pages_visited")
            response["pagination_urls_followed"] = run_report.get("pagination_urls_followed")
            response["records_extracted"] = run_report.get("row_count")
            response["crawl_pages_scanned"] = run_report.get("pages_crawled")
            response["run_duration"] = run_report.get("duration") or run_report.get("duration_seconds")
            if response.get("status") == "failed" and not response.get("failure"):
                response["failure"] = self._classify_report_failure(run_report)
        else:
            response["pages_visited"] = None
            response["pagination_urls_followed"] = None
            response["records_extracted"] = None
            response["crawl_pages_scanned"] = None
            response["run_duration"] = self._duration_from_metadata(
                response.get("started_at"),
                response.get("finished_at"),
            )

        return response

    def _build_progress(
        self,
        run_report: dict[str, Any] | None,
        status: str,
    ) -> dict[str, Any] | None:
        if run_report is None and status == "queued":
            return None

        report = run_report or {}
        return {
            "pages_scanned": report.get("pages_visited", report.get("pages_crawled", 0)),
            "records_extracted": report.get("row_count", 0),
            "pagination_pages": report.get("pagination_urls_followed", 0),
        }

    def _determine_current_phase(self, status: str) -> str:
        if status == "queued":
            return "queued"
        if status == "running":
            return "running"
        if status == "completed":
            return "completed"
        return "failed"

    def _load_run_report(self, output_dir_value: str | None) -> dict[str, Any] | None:
        if not output_dir_value:
            return None
        report_path = Path(output_dir_value) / "run_report.json"
        if not report_path.exists():
            return None
        try:
            return json.loads(report_path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _duration_from_metadata(
        self,
        started_at: str | None,
        finished_at: str | None,
    ) -> str | None:
        if not started_at or not finished_at:
            return None
        try:
            started = datetime.fromisoformat(started_at)
            finished = datetime.fromisoformat(finished_at)
        except ValueError:
            return None
        duration_seconds = round((finished - started).total_seconds(), 3)
        return f"{duration_seconds:.3f}s"

    def _build_runtime_config(self, config_path: Path, job_dir: Path) -> Path:
        payload = load_raw_config(config_path)
        payload.setdefault("crawl", {})
        payload.setdefault("pagination", {})
        payload.setdefault("limits", {})

        payload["crawl"]["max_pages"] = min(
            int(payload["crawl"].get("max_pages", self.max_pages) or self.max_pages),
            self.max_pages,
        )
        payload["pagination"]["max_pages"] = min(
            int(payload["pagination"].get("max_pages", self.max_pages) or self.max_pages),
            self.max_pages,
        )

        existing_max_records = payload["limits"].get("max_records")
        if existing_max_records is None:
            payload["limits"]["max_records"] = self.max_records
        else:
            payload["limits"]["max_records"] = min(int(existing_max_records), self.max_records)

        runtime_config_path = job_dir / "runtime_config.json"
        write_json(runtime_config_path, payload)
        return runtime_config_path

    def _enforce_rate_limit(self, client_ip: str | None) -> None:
        key = client_ip or "unknown"
        now = datetime.now(timezone.utc).timestamp()
        window_start = now - RATE_LIMIT_WINDOW_SECONDS
        with self._rate_limit_lock:
            history = [
                timestamp
                for timestamp in self._submission_history.get(key, [])
                if timestamp >= window_start
            ]
            if len(history) >= self.max_jobs_per_hour_per_ip:
                self._submission_history[key] = history
                raise HostedServiceError(
                    status_code=429,
                    reason="rate_limited",
                    message="Too many jobs have been submitted from this client.",
                    suggestion="Wait before submitting another hosted scrape.",
                )
            history.append(now)
            self._submission_history[key] = history

    def _recover_incomplete_jobs(self) -> None:
        for metadata_path in self._iter_job_metadata_paths():
            try:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            except Exception:
                continue

            status = metadata.get("status")
            if status not in {"queued", "running"}:
                continue

            worker_pid = metadata.get("worker_pid")
            if status == "running" and worker_pid and self._is_process_running(worker_pid):
                continue

            reason = "service_restarted" if status == "queued" else "interrupted_run"
            message = (
                "This scrape was interrupted because the service restarted before the run completed."
                if status == "running"
                else "This scrape did not start before the service restarted."
            )
            metadata.update(
                {
                    "status": "failed",
                    "finished_at": self._utc_now(),
                    "failure": failure_payload(
                        reason=reason,
                        message=message,
                        suggestion="Please resubmit the scrape.",
                        help_message=ADVANCED_HELP_MESSAGE,
                    ),
                }
            )
            write_json(metadata_path, metadata)
            LOGGER.warning("Recovered stale hosted job: %s reason=%s", metadata.get("job_id"), reason)

    def _cleanup_old_jobs(self) -> None:
        cutoff = datetime.now(timezone.utc).timestamp() - (self.retention_hours * 3600)
        for metadata_path in self._iter_job_metadata_paths():
            job_dir = metadata_path.parent
            try:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            except Exception:
                continue

            if metadata.get("status") in {"queued", "running"}:
                continue

            timestamp = self._metadata_timestamp(metadata)
            if timestamp is None or timestamp >= cutoff:
                continue

            shutil.rmtree(job_dir, ignore_errors=True)
            LOGGER.info("Cleaned up hosted job artifacts: %s", job_dir)

    def _iter_job_metadata_paths(self) -> list[Path]:
        return sorted(self.jobs_root.glob("*/job.json"))

    def _metadata_timestamp(self, metadata: dict[str, Any]) -> float | None:
        for key in ("finished_at", "created_at"):
            value = metadata.get(key)
            if not value:
                continue
            try:
                return datetime.fromisoformat(value).timestamp()
            except ValueError:
                continue
        return None

    def _runtime_exceeded(self, metadata: dict[str, Any]) -> bool:
        started_at = metadata.get("started_at")
        if not started_at:
            return False
        try:
            started = datetime.fromisoformat(started_at)
        except ValueError:
            return False
        return (datetime.now(timezone.utc) - started).total_seconds() > self.max_runtime_seconds

    def _terminate_worker(self, worker_pid: int | None) -> None:
        if not worker_pid:
            return
        try:
            os.kill(worker_pid, 9)
        except OSError:
            return

    def _is_process_running(self, worker_pid: int) -> bool:
        try:
            os.kill(worker_pid, 0)
        except PermissionError:
            return True
        except OSError:
            return False
        return True

    def _storage_writable(self) -> bool:
        probe_path = self.jobs_root / ".write_probe"
        try:
            probe_path.write_text("ok", encoding="utf-8")
            probe_path.unlink(missing_ok=True)
            return True
        except OSError:
            return False

    def _load_stats(self) -> dict[str, int]:
        with self._stats_lock:
            stats = _load_stats_from_path(self.stats_path)
            _save_stats_to_path(self.stats_path, stats)
            return dict(stats)

    def _save_stats(self, stats: dict[str, int]) -> None:
        with self._stats_lock:
            _save_stats_to_path(self.stats_path, stats)

    def _increment_stats(self, records_extracted: int, completed: bool) -> dict[str, int]:
        with self._stats_lock:
            stats = _load_stats_from_path(self.stats_path)
            stats["total_jobs_run"] += 1
            if completed:
                stats["total_completed_jobs"] += 1
                stats["total_records_extracted"] += max(0, int(records_extracted))
            _save_stats_to_path(self.stats_path, stats)
            return dict(stats)

    def _update_stats_from_job(self, job_id: str, metadata: dict[str, Any]) -> None:
        metadata_path = self._metadata_path(job_id)
        with self._metadata_lock:
            current = self._read_job_metadata_unlocked(metadata_path)
            if current is None:
                return
            if current.get("stats_counted"):
                return
            records_extracted = 0
            if current.get("status") == "completed":
                run_report = self._load_run_report(current.get("output_dir"))
                if run_report:
                    records_extracted = int(run_report.get("row_count", 0) or 0)
            self._increment_stats(
                records_extracted=records_extracted,
                completed=current.get("status") == "completed",
            )
            current["stats_counted"] = True
            write_json(metadata_path, current)

    def _read_job_metadata_unlocked(self, metadata_path: Path) -> dict[str, Any] | None:
        if not metadata_path.exists():
            return None
        return json.loads(metadata_path.read_text(encoding="utf-8"))


def _env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name, "").strip()
    if not raw_value:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        return default
    return value if value > 0 else default


def _load_stats_from_path(stats_path: Path) -> dict[str, int]:
    if not stats_path.exists():
        return dict(DEFAULT_HOSTED_STATS)
    try:
        payload = json.loads(stats_path.read_text(encoding="utf-8"))
    except Exception:
        LOGGER.warning("Hosted stats file was malformed and has been reset: %s", stats_path)
        return dict(DEFAULT_HOSTED_STATS)
    if not isinstance(payload, dict):
        return dict(DEFAULT_HOSTED_STATS)
    return {
        "total_jobs_run": _safe_stat_int(payload.get("total_jobs_run")),
        "total_records_extracted": _safe_stat_int(payload.get("total_records_extracted")),
        "total_completed_jobs": _safe_stat_int(payload.get("total_completed_jobs")),
    }


def _save_stats_to_path(stats_path: Path, stats: dict[str, int]) -> None:
    stats_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "total_jobs_run": _safe_stat_int(stats.get("total_jobs_run")),
        "total_records_extracted": _safe_stat_int(stats.get("total_records_extracted")),
        "total_completed_jobs": _safe_stat_int(stats.get("total_completed_jobs")),
    }
    write_json(stats_path, payload)


def _update_stats_from_metadata(stats_path: Path, metadata: dict[str, Any]) -> None:
    if metadata.get("stats_counted"):
        return
    stats = _load_stats_from_path(stats_path)
    stats["total_jobs_run"] += 1
    if metadata.get("status") == "completed":
        stats["total_completed_jobs"] += 1
        records_extracted = 0
        output_dir = metadata.get("output_dir")
        if output_dir:
            report_path = Path(output_dir) / "run_report.json"
            if report_path.exists():
                try:
                    report = json.loads(report_path.read_text(encoding="utf-8"))
                    records_extracted = int(report.get("row_count", 0) or 0)
                except Exception:
                    records_extracted = 0
        stats["total_records_extracted"] += max(0, records_extracted)
    _save_stats_to_path(stats_path, stats)
    metadata["stats_counted"] = True


def _safe_stat_int(value: Any) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed >= 0 else 0
