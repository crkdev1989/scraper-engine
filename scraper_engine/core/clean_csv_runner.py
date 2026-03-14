from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sys
from typing import Any


DEFAULT_CLEANED_FILE_NAME = "DC_CLEAN.csv"
DEFAULT_REPORT_FILE_NAME = "cleaner_report.json"


def run_clean_csv_job(
    *,
    config,
    config_path: Path,
    input_path: Path | None,
    run_context,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    settings = _resolve_clean_csv_settings(config, config_path)
    if input_path is None:
        raise ValueError("clean_csv mode requires --input with a source dataset path.")
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    cleaner_project_root = settings["cleaner_project_root"]
    cleaner_config_path = settings["cleaner_config_path"]
    output_path = run_context.output_dir / settings["output_file_name"]
    report_path = (
        run_context.output_dir / settings["report_file_name"]
        if settings["report_file_name"]
        else None
    )

    with _prepend_sys_path(cleaner_project_root):
        from cleaner.config import infer_format, load_config as load_cleaner_config
        from cleaner.loaders import load_data, load_data_chunked
        from cleaner.pipeline import run_pipeline
        from cleaner.report import CleaningReport
        from cleaner.writers import write_data, write_report

        cleaner_config = load_cleaner_config(cleaner_config_path)
        cleaner_config["input"]["path"] = str(input_path.resolve())
        cleaner_config["output"]["path"] = str(output_path)
        cleaner_config["report"]["path"] = str(report_path) if report_path else None

        report = CleaningReport()
        report.start_timer()
        report.input_path = cleaner_config["input"]["path"]
        report.output_path = str(output_path)
        report.report_path = str(report_path) if report_path else None

        input_fmt = cleaner_config["input"].get("format") or infer_format(input_path)
        output_fmt = cleaner_config["output"].get("format") or infer_format(output_path)
        chunk_size = cleaner_config.get("chunk_size")

        if chunk_size and chunk_size > 0:
            import pandas as pd

            total_loaded = 0
            out_chunks: list[pd.DataFrame] = []
            for chunk in load_data_chunked(
                cleaner_config["input"]["path"],
                chunk_size=chunk_size,
                format=input_fmt,
            ):
                total_loaded += len(chunk)
                cleaned_chunk = run_pipeline(chunk, cleaner_config, report)
                if len(cleaned_chunk) > 0:
                    out_chunks.append(cleaned_chunk)
            report.rows_loaded = total_loaded
            if out_chunks:
                cleaned_df = pd.concat(out_chunks, ignore_index=True)
            else:
                cleaned_df = pd.DataFrame()
            report.rows_output = len(cleaned_df)
        else:
            cleaned_df = load_data(cleaner_config["input"]["path"], format=input_fmt)
            report.rows_loaded = len(cleaned_df)
            cleaned_df = run_pipeline(cleaned_df, cleaner_config, report)
            report.rows_output = len(cleaned_df)

        write_data(cleaned_df, output_path, format=output_fmt)
        report.stop_timer()
        if report_path is not None:
            write_report(report.to_dict(), report_path)

    rows = cleaned_df.to_dict(orient="records")
    return rows, {
        "cleaner_project_root": str(cleaner_project_root),
        "cleaner_config_path": str(cleaner_config_path),
        "input_path": str(input_path.resolve()),
        "cleaned_output_path": str(output_path),
        "cleaner_report_path": str(report_path) if report_path else None,
        "rows_loaded": report.rows_loaded,
        "rows_output": report.rows_output,
        "duplicates_removed": report.duplicates_removed,
        "rows_dropped": report.rows_dropped,
        "modules_executed": list(report.modules_executed),
        "module_stats": dict(report.module_stats),
        "processing_time_seconds": round(report.processing_time_seconds, 4),
        "chunk_size": chunk_size,
    }


def _resolve_clean_csv_settings(config, config_path: Path) -> dict[str, Path | str]:
    settings = config.metadata.get("clean_csv", {})
    if not isinstance(settings, dict):
        raise ValueError("metadata.clean_csv must be an object when mode=clean_csv.")

    cleaner_config_value = str(settings.get("cleaner_config_path") or "").strip()
    if not cleaner_config_value:
        raise ValueError(
            "clean_csv mode requires metadata.clean_csv.cleaner_config_path."
        )

    base_dir = config_path.resolve().parent
    cleaner_config_path = Path(cleaner_config_value)
    if not cleaner_config_path.is_absolute():
        cleaner_config_path = (base_dir / cleaner_config_path).resolve()
    if not cleaner_config_path.exists():
        raise FileNotFoundError(
            f"Cleaner config not found: {cleaner_config_path}"
        )

    cleaner_project_value = str(settings.get("cleaner_project_root") or "").strip()
    if cleaner_project_value:
        cleaner_project_root = Path(cleaner_project_value)
        if not cleaner_project_root.is_absolute():
            cleaner_project_root = (base_dir / cleaner_project_root).resolve()
    else:
        cleaner_project_root = cleaner_config_path.parent.parent.resolve()
    if not cleaner_project_root.exists():
        raise FileNotFoundError(
            f"Cleaner project root not found: {cleaner_project_root}"
        )

    return {
        "cleaner_project_root": cleaner_project_root,
        "cleaner_config_path": cleaner_config_path,
        "output_file_name": str(settings.get("output_file_name") or DEFAULT_CLEANED_FILE_NAME),
        "report_file_name": str(settings.get("report_file_name") or DEFAULT_REPORT_FILE_NAME),
    }


@contextmanager
def _prepend_sys_path(path: Path):
    path_str = str(path)
    removed = False
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
        removed = True
    try:
        yield
    finally:
        if removed:
            try:
                sys.path.remove(path_str)
            except ValueError:
                pass
