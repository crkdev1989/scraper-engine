from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Callable

from scraper_engine.core.config_loader import load_config
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


class HostedJobService:
    def __init__(
        self,
        *,
        public_config_root: Path,
        output_root: Path,
        pipeline_runner: Callable[[RuntimeOptions], PipelineResult] | None = None,
        run_jobs_inline: bool = False,
    ) -> None:
        self.public_config_root = public_config_root
        self.output_root = output_root
        self.jobs_root = output_root / "hosted_jobs"
        self.jobs_root.mkdir(parents=True, exist_ok=True)
        self._run_jobs_inline = run_jobs_inline
        self._pipeline_runner = pipeline_runner or self._default_pipeline_runner
        self._metadata_lock = Lock()
        self._executor = None if run_jobs_inline else ThreadPoolExecutor(max_workers=2)

    def capabilities(self) -> dict[str, Any]:
        presets = self.list_presets()
        return {
            "allowed_presets": presets,
            "job_statuses": ["queued", "running", "completed", "failed"],
            "downloadable_files": list(PUBLIC_OUTPUT_FILES),
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
    ) -> dict[str, Any]:
        config_path = self._resolve_public_config_path(preset)
        normalized_target = self._validate_target_url(target_url)

        job_id = uuid.uuid4().hex[:12]
        job_dir = self.jobs_root / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        config = load_config(config_path)

        metadata = {
            "job_id": job_id,
            "status": "queued",
            "preset": preset,
            "config_name": config.name,
            "mode": config.mode,
            "target_url": normalized_target,
            "requested_run_name": (run_name or "").strip() or None,
            "config_path": str(config_path.resolve()),
            "created_at": self._utc_now(),
            "started_at": None,
            "finished_at": None,
            "run_name": None,
            "output_dir": None,
            "files": self._build_file_availability(None),
            "failure": None,
        }
        self._write_job_metadata(job_id, metadata)

        if self._run_jobs_inline:
            self._run_job(job_id)
        else:
            assert self._executor is not None
            self._executor.submit(self._run_job, job_id)

        return self.get_job(job_id)

    def get_job(self, job_id: str) -> dict[str, Any]:
        metadata = self._read_job_metadata(job_id)
        if metadata is None:
            raise HostedServiceError(
                status_code=404,
                reason="job_not_found",
                message="The requested job ID does not exist.",
                suggestion="Submit a new job and use the returned job ID.",
            )
        metadata["files"] = self._materialize_download_paths(job_id, metadata.get("files", {}))
        return metadata

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
        return file_path

    def _run_job(self, job_id: str) -> None:
        metadata = self.get_job(job_id)
        metadata["status"] = "running"
        metadata["started_at"] = self._utc_now()
        self._write_job_metadata(job_id, metadata)

        try:
            result = self._pipeline_runner(
                RuntimeOptions(
                    config_path=Path(metadata["config_path"]),
                    single_url=metadata["target_url"],
                    run_name=metadata["requested_run_name"] or metadata["job_id"],
                    output_root=self.output_root,
                )
            )
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
            metadata.update(
                {
                    "status": "failed",
                    "finished_at": self._utc_now(),
                    "failure": self._classify_exception_failure(error),
                }
            )

        self._write_job_metadata(job_id, metadata)

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
        try:
            return normalize_url(target_url)
        except Exception:
            raise HostedServiceError(
                status_code=400,
                reason="invalid_url",
                message="The submitted target URL is not valid.",
                suggestion="Enter a full public website URL such as https://example.com.",
            ) from None

    def _classify_report_failure(self, report: dict[str, Any]) -> dict[str, Any] | None:
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
            return self._failure_payload(
                reason="max_records_reached",
                message="The hosted scraper hit the record limit before it could return useful results.",
                suggestion="Narrow the target scope or request a custom scraping workflow.",
            )
        if "max_pages_reached" in stop_reasons:
            return self._failure_payload(
                reason="max_pages_reached",
                message="The hosted scraper stopped after reaching the page limit for this preset.",
                suggestion="Try a narrower target or request a custom scraping workflow for larger sites.",
            )
        if "duplicate_next_page_detected" in stop_reasons:
            return self._failure_payload(
                reason="pagination_loop_detected",
                message="The scraper detected a pagination loop and stopped safely.",
                suggestion="The website structure may not match this preset.",
                help_message=ADVANCED_HELP_MESSAGE,
            )
        if "no_next_page_found" in stop_reasons and mode in {"directory_list", "directory_detail"}:
            return self._failure_payload(
                reason="pagination_selector_not_found",
                message="The scraper could not find the next page selector defined in the config.",
                suggestion="The website structure may not match this preset.",
                help_message=ADVANCED_HELP_MESSAGE,
            )
        if any(token in errors for token in ("cloudflare", "captcha", "403", "429", "forbidden", "blocked")):
            return self._failure_payload(
                reason="blocking_or_anti_bot",
                message="The target site appears to be blocking automated requests.",
                suggestion="Try a different site or contact CRK Dev for a custom scraping solution.",
                help_message=ADVANCED_HELP_MESSAGE,
            )
        if any(token in errors for token in ("timed out", "timeout")):
            return self._failure_payload(
                reason="fetch_timeout",
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
            return self._failure_payload(
                reason="connectivity_failure",
                message="The hosted scraper could not connect to the target site.",
                suggestion="Verify the URL and confirm the site is publicly reachable.",
            )
        if diagnostics.get("non_fatal_issue_counts", {}).get("detail_page_fetch_failed") and listing_count > 0:
            return self._failure_payload(
                reason="detail_page_fetch_failed",
                message="The scraper found listings but could not load any detail pages successfully.",
                suggestion="The site may have blocked detail-page access or changed structure.",
                help_message=ADVANCED_HELP_MESSAGE,
            )
        if listing_count == 0 and mode in {"directory_list", "directory_detail"}:
            return self._failure_payload(
                reason="config_mismatch",
                message="The website structure did not match the selected hosted preset.",
                suggestion="Try a different preset or request a custom extraction setup.",
                help_message=ADVANCED_HELP_MESSAGE,
            )

        return self._failure_payload(
            reason="no_useful_results",
            message="The hosted scraper completed but did not produce useful results.",
            suggestion="Verify that the site content is public and that the selected preset matches the site.",
            help_message=ADVANCED_HELP_MESSAGE,
        )

    def _classify_exception_failure(self, error: Exception) -> dict[str, Any]:
        message = str(error).lower()
        if any(token in message for token in ("cloudflare", "captcha", "403", "429", "forbidden", "blocked")):
            return self._failure_payload(
                reason="blocking_or_anti_bot",
                message="The target site appears to be blocking automated requests.",
                suggestion="Try a different site or contact CRK Dev for a custom scraping solution.",
                help_message=ADVANCED_HELP_MESSAGE,
            )
        if any(token in message for token in ("timed out", "timeout")):
            return self._failure_payload(
                reason="fetch_timeout",
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
            return self._failure_payload(
                reason="connectivity_failure",
                message="The hosted scraper could not connect to the target site.",
                suggestion="Verify the URL and confirm the site is publicly reachable.",
            )
        return self._failure_payload(
            reason="run_failed",
            message="The hosted scraper could not complete this run.",
            suggestion="Review the run status and try again, or contact CRK Dev if the issue persists.",
        )

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
        files: dict[str, Any] = {}
        for file_name in PUBLIC_OUTPUT_FILES:
            available = bool(output_dir and (output_dir / file_name).exists())
            files[file_name] = {
                "available": available,
                "download_path": None if not available else f"/api/jobs/__JOB_ID__/files/{file_name}",
            }
        return files

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
            import json

            return json.loads(path.read_text(encoding="utf-8"))

    def _default_pipeline_runner(self, options: RuntimeOptions) -> PipelineResult:
        pipeline = Pipeline()
        return pipeline.run(options)

    def _utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()
