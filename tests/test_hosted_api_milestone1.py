from __future__ import annotations

import tempfile
import textwrap
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


def _successful_runner(options: RuntimeOptions) -> PipelineResult:
    run_context = build_run_context(
        output_root=options.output_root or Path.cwd() / "outputs",
        config_name="website_contact",
        requested_run_name=options.run_name,
        log_file_name="run.log",
        save_raw_html=False,
    )
    rows = [
        {
            "input_url": options.single_url,
            "source_urls": [options.single_url],
            "emails": ["hello@example.com"],
        }
    ]
    report = {
        "run_name": run_context.run_name,
        "config_name": "website_contact",
        "mode": "site_scan",
        "row_count": 1,
        "listing_count": 0,
        "pages_crawled": 2,
        "pages_visited": 0,
        "pagination_urls_followed": 0,
        "duration": "0.100s",
        "pagination_stopped_reasons": [],
        "errors": [],
        "diagnostics": {},
        "limits": {},
    }

    write_csv(run_context.output_dir / "results.csv", rows, preferred_columns=["input_url", "emails"])
    write_json(run_context.output_dir / "results.json", rows)
    (run_context.output_dir / "summary.txt").write_text("summary\n", encoding="utf-8")
    write_json(run_context.output_dir / "run_report.json", report)

    return PipelineResult(
        run_context=run_context,
        rows=rows,
        report=report,
        sync_result={},
    )


def _pagination_failure_runner(options: RuntimeOptions) -> PipelineResult:
    run_context = build_run_context(
        output_root=options.output_root or Path.cwd() / "outputs",
        config_name="directory_public",
        requested_run_name=options.run_name,
        log_file_name="run.log",
        save_raw_html=False,
    )
    report = {
        "run_name": run_context.run_name,
        "config_name": "directory_public",
        "mode": "directory_list",
        "row_count": 0,
        "listing_count": 0,
        "pages_crawled": 1,
        "pages_visited": 1,
        "pagination_urls_followed": 0,
        "duration": "0.200s",
        "pagination_stopped_reasons": ["no_next_page_found"],
        "errors": [],
        "diagnostics": {},
        "limits": {},
    }

    write_csv(run_context.output_dir / "results.csv", [], preferred_columns=["business_name"])
    write_json(run_context.output_dir / "results.json", [])
    (run_context.output_dir / "summary.txt").write_text("summary\n", encoding="utf-8")
    write_json(run_context.output_dir / "run_report.json", report)

    return PipelineResult(
        run_context=run_context,
        rows=[],
        report=report,
        sync_result={},
    )


def _blocking_runner(options: RuntimeOptions) -> PipelineResult:
    raise RuntimeError("403 Forbidden - Cloudflare challenge")


class HostedApiMilestoneTests(unittest.TestCase):
    def test_capabilities_and_presets_endpoints(self) -> None:
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
                description: Hosted contact preset
                fields:
                  emails:
                    type: emails
                    many: true
                limits:
                  max_records: 25
                """,
            )

            client = TestClient(
                create_app(
                    public_config_root=config_root,
                    output_root=output_root,
                    pipeline_runner=_successful_runner,
                    run_jobs_inline=True,
                )
            )

            capabilities = client.get("/api/capabilities")
            presets = client.get("/api/presets")

        self.assertEqual(200, capabilities.status_code)
        self.assertEqual(200, presets.status_code)
        self.assertEqual("website_contact", capabilities.json()["allowed_presets"][0]["preset"])
        self.assertEqual(25, capabilities.json()["allowed_presets"][0]["limits"]["max_records"])
        self.assertEqual("website_contact", presets.json()["presets"][0]["preset"])

    def test_submit_status_and_download_completed_job(self) -> None:
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
                    pipeline_runner=_successful_runner,
                    run_jobs_inline=True,
                )
            )

            submit_response = client.post(
                "/api/jobs",
                json={
                    "preset": "website_contact",
                    "target_url": "example.com",
                    "run_name": "hosted_test",
                },
            )
            job_id = submit_response.json()["job_id"]
            status_response = client.get(f"/api/jobs/{job_id}")
            download_response = client.get(f"/api/jobs/{job_id}/files/results.csv")

        self.assertEqual(202, submit_response.status_code)
        self.assertEqual("completed", submit_response.json()["status"])
        self.assertEqual(200, status_response.status_code)
        self.assertTrue(status_response.json()["files"]["results.csv"]["available"])
        self.assertEqual(200, download_response.status_code)
        self.assertIn("hello@example.com", download_response.text)

    def test_submission_validation_rejects_bad_preset_and_url(self) -> None:
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
                    pipeline_runner=_successful_runner,
                    run_jobs_inline=True,
                )
            )

            bad_preset = client.post(
                "/api/jobs",
                json={"preset": "missing_preset", "target_url": "https://example.com"},
            )
            bad_url = client.post(
                "/api/jobs",
                json={"preset": "website_contact", "target_url": "   "},
            )

        self.assertEqual(400, bad_preset.status_code)
        self.assertEqual("unsupported_config", bad_preset.json()["detail"]["reason"])
        self.assertEqual(400, bad_url.status_code)
        self.assertEqual("invalid_url", bad_url.json()["detail"]["reason"])

    def test_failed_job_maps_user_readable_reason_and_blocks_download(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_root = root / "configs" / "public"
            output_root = root / "outputs"
            _write_public_config(
                config_root,
                "directory_public.yaml",
                """
                name: directory_public
                mode: directory_list
                record_selector: ".listing-card"
                fields:
                  business_name:
                    css: ".listing-title"
                pagination:
                  enabled: true
                  max_pages: 3
                  next_page: "a.next"
                """,
            )

            client = TestClient(
                create_app(
                    public_config_root=config_root,
                    output_root=output_root,
                    pipeline_runner=_pagination_failure_runner,
                    run_jobs_inline=True,
                )
            )

            submit_response = client.post(
                "/api/jobs",
                json={"preset": "directory_public", "target_url": "https://example.com/directory"},
            )
            job_id = submit_response.json()["job_id"]
            download_response = client.get(f"/api/jobs/{job_id}/files/results.csv")

        self.assertEqual(202, submit_response.status_code)
        self.assertEqual("failed", submit_response.json()["status"])
        self.assertEqual(
            "pagination_selector_not_found",
            submit_response.json()["failure"]["reason"],
        )
        self.assertIn("CRK Dev", submit_response.json()["failure"]["help"])
        self.assertEqual(409, download_response.status_code)

    def test_blocking_failure_returns_advanced_scraping_help(self) -> None:
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
                    pipeline_runner=_blocking_runner,
                    run_jobs_inline=True,
                )
            )

            submit_response = client.post(
                "/api/jobs",
                json={"preset": "website_contact", "target_url": "https://example.com"},
            )

        self.assertEqual(202, submit_response.status_code)
        self.assertEqual("failed", submit_response.json()["status"])
        self.assertEqual("blocked_by_site", submit_response.json()["failure"]["reason"])
        self.assertIn("custom scraping solutions", submit_response.json()["failure"]["help"])


if __name__ == "__main__":
    unittest.main()
