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


def _detailed_runner(options: RuntimeOptions) -> PipelineResult:
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
        "pages_crawled": 3,
        "pages_visited": 3,
        "pagination_urls_followed": 1,
        "duration": "1.234s",
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


class HostedApiFrontendMilestoneTests(unittest.TestCase):
    def test_job_status_includes_frontend_fields_and_progress(self) -> None:
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
                """,
            )
            client = TestClient(
                create_app(
                    public_config_root=config_root,
                    output_root=output_root,
                    pipeline_runner=_detailed_runner,
                    run_jobs_inline=True,
                    cors_origins=["https://scraper.crkdev.com"],
                )
            )

            submit_response = client.post(
                "/api/jobs",
                json={"preset": "website_contact", "target_url": "https://example.com"},
            )
            job_id = submit_response.json()["job_id"]
            status_response = client.get(f"/api/jobs/{job_id}")

        payload = status_response.json()
        self.assertEqual(202, submit_response.status_code)
        self.assertEqual(200, status_response.status_code)
        self.assertEqual(job_id, payload["job_id"])
        self.assertEqual("completed", payload["status"])
        self.assertEqual("website_contact", payload["preset"])
        self.assertEqual("https://example.com/", payload["target_url"])
        self.assertIn("created_at", payload)
        self.assertIn("started_at", payload)
        self.assertIn("finished_at", payload)
        self.assertEqual("completed", payload["current_phase"])
        self.assertEqual(3, payload["pages_visited"])
        self.assertEqual(1, payload["pagination_urls_followed"])
        self.assertEqual(1, payload["records_extracted"])
        self.assertEqual(3, payload["crawl_pages_scanned"])
        self.assertEqual("1.234s", payload["run_duration"])
        self.assertEqual(
            {"pages_scanned": 3, "records_extracted": 1, "pagination_pages": 1},
            payload["progress"],
        )
        self.assertTrue(payload["files_available"]["results.csv"])

    def test_limits_health_and_download_alias_endpoints_work(self) -> None:
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
                limits:
                  max_records: 25
                """,
            )
            client = TestClient(
                create_app(
                    public_config_root=config_root,
                    output_root=output_root,
                    pipeline_runner=_detailed_runner,
                    run_jobs_inline=True,
                    cors_origins=["https://scraper.crkdev.com"],
                )
            )

            job_response = client.post(
                "/api/jobs",
                json={"preset": "website_contact", "target_url": "https://example.com"},
            )
            job_id = job_response.json()["job_id"]
            limits_response = client.get("/api/limits")
            health_response = client.get("/health")
            download_response = client.get(f"/api/jobs/{job_id}/download/results.csv")

        self.assertEqual(200, limits_response.status_code)
        self.assertEqual(["website_contact"], limits_response.json()["allowed_presets"])
        self.assertEqual(25, limits_response.json()["presets"][0]["limits"]["max_records"])
        self.assertEqual(200, health_response.status_code)
        self.assertEqual("ok", health_response.json()["status"])
        self.assertTrue(health_response.json()["ready"])
        self.assertEqual(200, download_response.status_code)
        self.assertIn("hello@example.com", download_response.text)

    def test_cors_headers_are_applied_for_allowed_origin(self) -> None:
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
                    pipeline_runner=_detailed_runner,
                    run_jobs_inline=True,
                    cors_origins=["https://scraper.crkdev.com"],
                )
            )

            response = client.get(
                "/health",
                headers={"Origin": "https://scraper.crkdev.com"},
            )

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            "https://scraper.crkdev.com",
            response.headers.get("access-control-allow-origin"),
        )


if __name__ == "__main__":
    unittest.main()
