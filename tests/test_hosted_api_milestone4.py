from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scraper_engine.hosted_service import HostedJobService
from scraper_engine.outputs.writers import write_json


def _write_public_config(config_root: Path) -> None:
    path = config_root / "website_contact.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "name: website_contact",
                "mode: site_scan",
                "fields:",
                "  emails:",
                "    type: emails",
                "    many: true",
                "",
            ]
        ),
        encoding="utf-8",
    )


class HostedApiDeploymentMilestoneTests(unittest.TestCase):
    def test_startup_recovery_marks_stale_running_jobs_failed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_root = root / "configs" / "public"
            output_root = root / "outputs"
            jobs_root = output_root / "hosted_jobs"
            _write_public_config(config_root)

            job_dir = jobs_root / "job123"
            job_dir.mkdir(parents=True, exist_ok=True)
            write_json(
                job_dir / "job.json",
                {
                    "job_id": "job123",
                    "status": "running",
                    "preset": "website_contact",
                    "target_url": "https://example.com/",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "finished_at": None,
                    "worker_pid": 999999,
                    "files": {},
                    "failure": None,
                },
            )

            service = HostedJobService(
                public_config_root=config_root,
                output_root=output_root,
                run_jobs_inline=True,
            )
            job = service.get_job("job123")

        self.assertEqual("failed", job["status"])
        self.assertEqual("interrupted_run", job["failure"]["reason"])

    def test_retention_cleanup_removes_old_hosted_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_root = root / "configs" / "public"
            output_root = root / "outputs"
            jobs_root = output_root / "hosted_jobs"
            _write_public_config(config_root)

            old_job_dir = jobs_root / "oldjob"
            old_job_dir.mkdir(parents=True, exist_ok=True)
            old_finished_at = (datetime.now(timezone.utc) - timedelta(hours=72)).isoformat()
            write_json(
                old_job_dir / "job.json",
                {
                    "job_id": "oldjob",
                    "status": "completed",
                    "preset": "website_contact",
                    "target_url": "https://example.com/",
                    "created_at": old_finished_at,
                    "finished_at": old_finished_at,
                    "files": {},
                    "failure": None,
                },
            )

            HostedJobService(
                public_config_root=config_root,
                output_root=output_root,
                run_jobs_inline=True,
                retention_hours=24,
            )

        self.assertFalse(old_job_dir.exists())


if __name__ == "__main__":
    unittest.main()
