from __future__ import annotations

import tempfile
import textwrap
import time
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from scraper_engine.api_app import create_app
from scraper_engine.core.models import PipelineResult, RuntimeOptions
from scraper_engine.outputs.paths import build_run_context
from scraper_engine.outputs.writers import write_csv, write_json


def _write_public_config(config_root: Path, file_name: str, content: str) -> None:
    path = config_root / file_name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")


def _fast_runner(options: RuntimeOptions) -> PipelineResult:
    run_context = build_run_context(
        output_root=options.output_root or Path.cwd() / "outputs",
        config_name="website_contact",
        requested_run_name=options.run_name,
        log_file_name="run.log",
        save_raw_html=False,
    )
    rows = [{"input_url": options.single_url, "emails": ["hello@example.com"]}]
    report = {
        "run_name": run_context.run_name,
        "config_name": "website_contact",
        "mode": "site_scan",
        "row_count": 1,
        "listing_count": 0,
        "pages_crawled": 2,
        "pages_visited": 2,
        "pagination_urls_followed": 0,
        "duration_seconds": 1.0,
        "pagination_stopped_reasons": [],
        "errors": [],
        "diagnostics": {},
        "limits": {},
    }
    write_csv(run_context.output_dir / "results.csv", rows, preferred_columns=["input_url", "emails"])
    write_json(run_context.output_dir / "results.json", rows)
    (run_context.output_dir / "summary.txt").write_text("summary\n", encoding="utf-8")
    write_json(run_context.output_dir / "run_report.json", report)
    return PipelineResult(run_context=run_context, rows=rows, report=report, sync_result={})


def _slow_runner(options: RuntimeOptions) -> PipelineResult:
    time.sleep(1.2)
    return _fast_runner(options)


def _large_file_runner(options: RuntimeOptions) -> PipelineResult:
    result = _fast_runner(options)
    big_text = "x" * 2048
    (result.run_context.output_dir / "results.json").write_text(big_text, encoding="utf-8")
    return result


class HostedApiSafetyMilestoneTests(unittest.TestCase):
    def test_rejects_private_and_unsafe_targets(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_root = root / "configs" / "public"
            output_root = root / "outputs"
            _write_public_config(
                config_root,
                "website_contact.yaml",
                """
                name: website_contact
                mode: site_scan
                fields:
                  emails:
                    type: emails
                    many: true
                """,
            )
            client = TestClient(
                create_app(
                    public_config_root=config_root,
                    output_root=output_root,
                    pipeline_runner=_fast_runner,
                    run_jobs_inline=True,
                )
            )

            responses = [
                client.post("/api/jobs", json={"preset": "website_contact", "target_url": "http://localhost"}),
                client.post("/api/jobs", json={"preset": "website_contact", "target_url": "http://127.0.0.1"}),
                client.post("/api/jobs", json={"preset": "website_contact", "target_url": "http://192.168.1.10"}),
                client.post("/api/jobs", json={"preset": "website_contact", "target_url": "file:///etc/passwd"}),
                client.post("/api/jobs", json={"preset": "website_contact", "target_url": "ftp://example.com"}),
            ]

        for response in responses:
            self.assertEqual(400, response.status_code)
            self.assertEqual("invalid_target", response.json()["detail"]["reason"])

    def test_rate_limits_job_submission_by_ip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_root = root / "configs" / "public"
            output_root = root / "outputs"
            _write_public_config(
                config_root,
                "website_contact.yaml",
                """
                name: website_contact
                mode: site_scan
                fields:
                  emails:
                    type: emails
                    many: true
                """,
            )
            client = TestClient(
                create_app(
                    public_config_root=config_root,
                    output_root=output_root,
                    pipeline_runner=_fast_runner,
                    run_jobs_inline=True,
                    max_jobs_per_hour_per_ip=2,
                )
            )

            first = client.post("/api/jobs", json={"preset": "website_contact", "target_url": "https://example.com/1"})
            second = client.post("/api/jobs", json={"preset": "website_contact", "target_url": "https://example.com/2"})
            third = client.post("/api/jobs", json={"preset": "website_contact", "target_url": "https://example.com/3"})

        self.assertEqual(202, first.status_code)
        self.assertEqual(202, second.status_code)
        self.assertEqual(429, third.status_code)
        self.assertEqual("rate_limited", third.json()["detail"]["reason"])

    def test_runtime_limit_returns_limit_exceeded(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_root = root / "configs" / "public"
            output_root = root / "outputs"
            _write_public_config(
                config_root,
                "website_contact.yaml",
                """
                name: website_contact
                mode: site_scan
                fields:
                  emails:
                    type: emails
                    many: true
                """,
            )
            client = TestClient(
                create_app(
                    public_config_root=config_root,
                    output_root=output_root,
                    pipeline_runner=_slow_runner,
                    run_jobs_inline=True,
                    max_runtime_seconds=1,
                )
            )

            response = client.post(
                "/api/jobs",
                json={"preset": "website_contact", "target_url": "https://example.com"},
            )

        self.assertEqual(202, response.status_code)
        self.assertEqual("failed", response.json()["status"])
        self.assertEqual("limit_exceeded", response.json()["failure"]["reason"])

    def test_download_blocks_oversized_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_root = root / "configs" / "public"
            output_root = root / "outputs"
            _write_public_config(
                config_root,
                "website_contact.yaml",
                """
                name: website_contact
                mode: site_scan
                fields:
                  emails:
                    type: emails
                    many: true
                """,
            )
            client = TestClient(
                create_app(
                    public_config_root=config_root,
                    output_root=output_root,
                    pipeline_runner=_large_file_runner,
                    run_jobs_inline=True,
                    max_download_bytes=1024,
                )
            )

            job_response = client.post(
                "/api/jobs",
                json={"preset": "website_contact", "target_url": "https://example.com"},
            )
            job_id = job_response.json()["job_id"]
            download_response = client.get(f"/api/jobs/{job_id}/download/results.json")

        self.assertEqual(413, download_response.status_code)
        self.assertEqual("limit_exceeded", download_response.json()["detail"]["reason"])

    def test_limits_endpoint_exposes_hosted_tool_limits(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_root = root / "configs" / "public"
            output_root = root / "outputs"
            _write_public_config(
                config_root,
                "website_contact.yaml",
                """
                name: website_contact
                mode: site_scan
                crawl:
                  max_pages: 25
                fields:
                  emails:
                    type: emails
                    many: true
                limits:
                  max_records: 700
                """,
            )
            client = TestClient(
                create_app(
                    public_config_root=config_root,
                    output_root=output_root,
                    pipeline_runner=_fast_runner,
                    run_jobs_inline=True,
                    max_pages=10,
                    max_records=500,
                    max_runtime_seconds=120,
                )
            )

            response = client.get("/api/limits")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(["website_contact"], payload["allowed_presets"])
        self.assertEqual(10, payload["max_pages"])
        self.assertEqual(500, payload["max_records"])
        self.assertEqual(120, payload["max_runtime_seconds"])


if __name__ == "__main__":
    unittest.main()
