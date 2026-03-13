from __future__ import annotations

from pathlib import Path
from typing import Any

from scraper_engine.outputs.writers import write_json


def write_summary(path: Path, report: dict[str, Any]) -> None:
    targets = ", ".join(report.get("input_targets", []))
    fields = ", ".join(report.get("extracted_field_names", []))
    duration = report.get("duration") or f"{report['duration_seconds']}s"
    pagination_stopped_reasons = ", ".join(report.get("pagination_stopped_reasons", [])) or "n/a"
    diagnostics = report.get("diagnostics", {})
    quality_metrics = report.get("quality_metrics", {})
    limits = report.get("limits", {})
    lines = [
        f"Run: {report['run_name']}",
        f"Config: {report['config_name']}",
        f"Mode: {report['mode']}",
        f"Targets: {targets or report['target_count']}",
        f"Duration: {duration}",
        f"Pages crawled: {report['pages_crawled']}",
        f"Pages visited: {report.get('pages_visited', 0)}",
        f"Pages succeeded: {report['pages_succeeded']}",
        f"Pages failed: {report['pages_failed']}",
        f"Listings found: {report.get('listing_count', 0)}",
        f"Pagination URLs followed: {report.get('pagination_urls_followed', 0)}",
        f"Pagination stop reasons: {pagination_stopped_reasons}",
        f"Detail pages attempted: {report.get('detail_pages_attempted', 0)}",
        f"Detail pages successful: {report.get('detail_pages_successful', 0)}",
        f"Detail pages failed: {report.get('detail_pages_failed', 0)}",
        f"Rows written: {report['row_count']}",
        f"Extracted fields: {fields}",
        f"Sync attempted: {report['sync_attempted']}",
        f"Sync success: {report['sync_success']}",
    ]
    lines.extend(["", "Run Limits:"])
    lines.append(f"- Max records: {limits.get('max_records', 'none')}")
    lines.append(f"- Max detail pages: {limits.get('max_detail_pages', 'none')}")
    lines.append(f"- Max records hit: {limits.get('max_records_hit', False)}")
    lines.append(f"- Max detail pages hit: {limits.get('max_detail_pages_hit', False)}")
    lines.append(f"- Records truncated: {limits.get('records_truncated', 0)}")
    lines.append(
        "- Detail pages skipped due to limit: "
        f"{limits.get('detail_pages_skipped_due_to_limit', 0)}"
    )
    lines.extend(["", "Diagnostics:"])
    lines.append(f"- Warning total: {diagnostics.get('warning_total', 0)}")
    warning_counts = diagnostics.get("warning_counts_by_category", {})
    if warning_counts:
        for category, count in warning_counts.items():
            lines.append(f"- Warning category {category}: {count}")
    else:
        lines.append("- Warning categories: none")

    non_fatal_issue_counts = diagnostics.get("non_fatal_issue_counts", {})
    if non_fatal_issue_counts:
        for category, count in non_fatal_issue_counts.items():
            lines.append(f"- Non-fatal issue {category}: {count}")
    else:
        lines.append("- Non-fatal issues: none")

    pagination_stop_reason_counts = diagnostics.get("pagination_stop_reason_counts", {})
    if pagination_stop_reason_counts:
        for reason, count in pagination_stop_reason_counts.items():
            lines.append(f"- Pagination stop reason {reason}: {count}")

    cleanup_actions = diagnostics.get("cleanup_actions", {})
    if cleanup_actions:
        cleanup_summary = ", ".join(
            f"{key}={value}"
            for key, value in cleanup_actions.items()
            if value
        )
        lines.append(f"- Cleanup actions: {cleanup_summary or 'none'}")

    warning_messages = diagnostics.get("warning_messages", [])
    if warning_messages:
        lines.append("- Warning samples:")
        lines.extend(
            f"  - {item['message']} ({item['count']})"
            for item in warning_messages[:5]
        )

    non_fatal_issue_messages = diagnostics.get("non_fatal_issue_messages", [])
    if non_fatal_issue_messages:
        lines.append("- Non-fatal issue samples:")
        lines.extend(
            f"  - {item['message']} ({item['count']})"
            for item in non_fatal_issue_messages[:5]
        )

    lines.extend(["", "Quality Metrics:"])
    lines.append(f"- Records produced: {quality_metrics.get('records_produced', report['row_count'])}")
    lines.append(
        "- Records with all configured fields empty: "
        f"{quality_metrics.get('records_with_all_configured_fields_empty', 0)}"
    )
    final_output_fields = ", ".join(quality_metrics.get("final_output_fields_seen", []))
    lines.append(f"- Final output fields seen: {final_output_fields or 'none'}")

    field_coverage = quality_metrics.get("configured_field_coverage", {})
    if field_coverage:
        lines.append("- Field coverage:")
        for field_name, metrics in field_coverage.items():
            lines.append(
                "  - "
                f"{field_name}: filled={metrics['filled']} empty={metrics['empty']} "
                f"fill_rate={metrics['fill_rate']:.1%}"
            )
    else:
        lines.append("- Field coverage: none")

    lines.extend(["", "Errors:"])
    error_lines = report.get("errors", [])
    if error_lines:
        lines.extend(f"- {error}" for error in error_lines)
    else:
        lines.append("- none")

    lines.extend(["", "Notes:"])
    notes = report.get("notes", [])
    if notes:
        lines.extend(f"- {note}" for note in notes)
    else:
        lines.append("- none")
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def write_run_report(path: Path, report: dict[str, Any]) -> None:
    write_json(path, report)
