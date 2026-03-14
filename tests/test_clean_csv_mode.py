from __future__ import annotations

import json
import re
import tempfile
import unittest
from pathlib import Path

from scraper_engine.core.models import RuntimeOptions
from scraper_engine.core.pipeline import Pipeline


class CleanCsvModeTests(unittest.TestCase):
    def test_clean_csv_mode_runs_csv_cleaner_rules_and_writes_expected_files(self) -> None:
        temp_root = Path(tempfile.mkdtemp())
        cleaner_project_root = temp_root / "fake-cleaner"
        cleaner_package = cleaner_project_root / "cleaner"
        cleaner_loaders = cleaner_package / "loaders"
        cleaner_configs = cleaner_project_root / "configs"
        cleaner_loaders.mkdir(parents=True, exist_ok=True)
        cleaner_configs.mkdir(parents=True, exist_ok=True)

        (cleaner_package / "__init__.py").write_text("", encoding="utf-8")
        (cleaner_package / "simple_frame.py").write_text(
                """
import csv


class SimpleFrame:
    def __init__(self, rows):
        self.rows = list(rows)

    def __len__(self):
        return len(self.rows)

    def to_dict(self, orient="records"):
        if orient != "records":
            raise ValueError("Only records orient is supported")
        return list(self.rows)


def load_csv_rows(path):
    with open(path, "r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))
""".strip(),
            encoding="utf-8",
        )
        (cleaner_package / "config.py").write_text(
                """
import json
from pathlib import Path


def load_config(path):
    path = Path(path)
    data = json.loads(path.read_text(encoding="utf-8"))
    base = path.parent.resolve()
    for section in ("input", "output", "report"):
        raw_path = data.get(section, {}).get("path")
        if raw_path:
            data[section]["path"] = str((base / raw_path).resolve())
    return data


def infer_format(path):
    return "csv"
""".strip(),
            encoding="utf-8",
        )
        (cleaner_loaders / "__init__.py").write_text(
                """
from cleaner.simple_frame import SimpleFrame, load_csv_rows


def load_data(path, format=None):
    return SimpleFrame(load_csv_rows(path))


def load_data_chunked(path, chunk_size, format=None):
    rows = load_csv_rows(path)
    for index in range(0, len(rows), chunk_size):
        yield SimpleFrame(rows[index:index + chunk_size])
""".strip(),
            encoding="utf-8",
        )
        (cleaner_package / "pipeline.py").write_text(
                """
from cleaner.simple_frame import SimpleFrame


def run_pipeline(df, config, report, modules_root=None):
    rows = df.to_dict(orient="records")
    for spec in config.get("modules", []):
        module_id = spec["id"] if isinstance(spec, dict) else spec
        options = spec.get("options", {}) if isinstance(spec, dict) else {}
        if module_id == "core.rename_columns":
            mapping = options.get("mapping", {})
            rows = [
                {mapping.get(key, key): value for key, value in row.items()}
                for row in rows
            ]
        elif module_id == "text.normalize_case":
            columns = options.get("columns", [])
            case = options.get("case")
            for row in rows:
                for column in columns:
                    value = row.get(column)
                    if isinstance(value, str) and case == "lower":
                        row[column] = value.lower()
        report.record_module(module_id, {"rows_after": len(rows)})
    return SimpleFrame(rows)
""".strip(),
            encoding="utf-8",
        )
        (cleaner_package / "report.py").write_text(
                """
import time


class CleaningReport:
    def __init__(self):
        self.rows_loaded = 0
        self.rows_output = 0
        self.duplicates_removed = 0
        self.rows_dropped = 0
        self.modules_executed = []
        self.module_stats = {}
        self.processing_time_seconds = 0.0
        self.output_path = None
        self.report_path = None
        self.input_path = None
        self._start_time = None

    def start_timer(self):
        self._start_time = time.perf_counter()

    def stop_timer(self):
        if self._start_time is not None:
            self.processing_time_seconds = time.perf_counter() - self._start_time

    def record_module(self, module_id, stats=None):
        self.modules_executed.append(module_id)
        if stats is not None:
            self.module_stats[module_id] = stats

    def to_dict(self):
        return {
            "rows_loaded": self.rows_loaded,
            "rows_output": self.rows_output,
            "modules_executed": self.modules_executed,
            "module_stats": self.module_stats,
            "output_path": self.output_path,
            "report_path": self.report_path,
        }
""".strip(),
            encoding="utf-8",
        )
        (cleaner_package / "writers.py").write_text(
                """
import csv
import json
from pathlib import Path


def write_data(df, path, format=None, **kwargs):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = df.to_dict(orient="records")
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_report(report_dict, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report_dict, indent=2), encoding="utf-8")
""".strip(),
            encoding="utf-8",
        )

        cleaner_config_path = cleaner_configs / "law_firm_leads.json"
        cleaner_config_path.write_text(
            json.dumps(
                {
                    "input": {"path": "./placeholder.csv", "format": "csv"},
                    "output": {"path": "./placeholder.csv", "format": "csv"},
                    "report": {"path": "./placeholder_report.json"},
                    "modules": [
                        {
                            "id": "core.rename_columns",
                            "options": {"mapping": {"law_firm": "firm_name"}},
                        },
                        {
                            "id": "text.normalize_case",
                            "options": {"columns": ["email"], "case": "lower"},
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )

        input_csv = temp_root / "results.csv"
        input_csv.write_text(
            "name,law_firm,website,email,phone,city,state\n"
            "Jane Smith,Smith Law,https://smithlaw.example,INFO@SMITHLAW.EXAMPLE,(410) 555-0100,washington,district of columbia\n",
            encoding="utf-8",
        )

        config_dir = temp_root / "configs" / "cleaners"
        config_dir.mkdir(parents=True, exist_ok=True)
        wrapper_config = config_dir / "law_firm_leads.json"
        wrapper_config.write_text(
            json.dumps(
                {
                    "name": "law_firm_leads_cleaner",
                    "mode": "clean_csv",
                    "output": {
                        "write_csv": False,
                        "write_json": False,
                        "write_summary": True,
                        "write_report": True,
                    },
                    "logging": {"level": "INFO", "file_name": "run.log"},
                    "sync": {"enabled": False, "strategy": "rsync", "dry_run": True},
                    "metadata": {
                        "output_files": {
                            "summary": "LEADS_CLEAN_SUMMARY.txt",
                            "report": "LEADS_CLEAN_REPORT.json"
                        },
                        "clean_csv": {
                            "cleaner_project_root": str(cleaner_project_root),
                            "cleaner_config_path": str(cleaner_config_path),
                            "output_file_name": "LEADS_CLEAN.csv",
                            "report_file_name": "cleaner_report.json",
                        }
                    },
                }
            ),
            encoding="utf-8",
        )

        pipeline = Pipeline()
        result = pipeline.run(
            RuntimeOptions(
                config_path=wrapper_config,
                input_path=input_csv,
                run_name="ntl_dc_20260313_215524",
                output_root=temp_root / "outputs",
            )
        )

        output_dir = result.run_context.output_dir
        cleaned_csv_path = output_dir / "LEADS_CLEAN.csv"
        cleaner_report_path = output_dir / "cleaner_report.json"
        run_report_path = output_dir / "LEADS_CLEAN_REPORT.json"
        summary_path = output_dir / "LEADS_CLEAN_SUMMARY.txt"

        self.assertTrue(cleaned_csv_path.exists())
        self.assertTrue(cleaner_report_path.exists())
        self.assertTrue(run_report_path.exists())
        self.assertTrue(summary_path.exists())
        self.assertEqual("clean_csv", result.report["mode"])
        self.assertEqual(str(cleaned_csv_path), result.report["clean_csv"]["cleaned_output_path"])
        self.assertEqual(1, result.report["row_count"])
        self.assertRegex(result.run_context.run_name, r"^ntl_dc_\d{8}_\d{6}$")
        self.assertTrue(re.fullmatch(r"ntl_dc_\d{8}_\d{6}", output_dir.name))

        cleaned_csv = cleaned_csv_path.read_text(encoding="utf-8")
        self.assertIn("firm_name", cleaned_csv)
        self.assertIn("Smith Law", cleaned_csv)
        self.assertIn("info@smithlaw.example", cleaned_csv)

    def test_default_output_root_uses_repo_outputs_for_cleaner_configs(self) -> None:
        pipeline = Pipeline()
        config_path = Path(
            "C:/Users/crk24/Craig/Dev/crk-dev/scraper-engine/configs/cleaners/law_firm_leads.json"
        )

        output_root = pipeline._default_output_root(
            RuntimeOptions(config_path=config_path, input_path=Path("dummy.csv"))
        )

        self.assertEqual(
            Path("C:/Users/crk24/Craig/Dev/crk-dev/scraper-engine/outputs"),
            output_root,
        )


if __name__ == "__main__":
    unittest.main()
