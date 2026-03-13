from __future__ import annotations

import logging
import subprocess

from scraper_engine.core.models import RunContext, SyncConfig
from scraper_engine.utils.logging_utils import log_event


def run_sync_hook(sync_config: SyncConfig, run_context: RunContext, logger=None) -> dict[str, object]:
    if not sync_config.enabled:
        log_event(
            logger,
            logging.INFO,
            "SYNC ATTEMPT",
            "Sync skipped because it is disabled.",
        )
        return {
            "enabled": False,
            "attempted": False,
            "executed": False,
            "success": False,
            "status": "skipped",
        }

    result = {
        "enabled": True,
        "attempted": True,
        "executed": False,
        "success": False,
        "status": "pending",
        "strategy": sync_config.strategy,
        "host": sync_config.host,
        "username": sync_config.username,
        "destination_path": sync_config.destination_path,
        "source_path": str(run_context.output_dir),
        "dry_run": sync_config.dry_run,
    }

    try:
        command = _build_sync_command(sync_config, run_context)
        result["command"] = command
    except Exception as error:
        result["status"] = "error"
        result["error"] = str(error)
        log_event(
            logger,
            logging.WARNING,
            "SYNC ATTEMPT",
            "Sync hook failed before execution.",
            error=error,
        )
        return result

    if sync_config.dry_run:
        result["success"] = True
        result["status"] = "dry_run"
        log_event(
            logger,
            logging.INFO,
            "SYNC ATTEMPT",
            "Prepared sync dry run.",
            strategy=sync_config.strategy,
            command=" ".join(command),
        )
        return result

    try:
        log_event(
            logger,
            logging.INFO,
            "SYNC ATTEMPT",
            "Executing sync command.",
            strategy=sync_config.strategy,
            command=" ".join(command),
        )
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
        result["executed"] = True
        result["success"] = completed.returncode == 0
        result["status"] = "completed" if result["success"] else "failed"
        result["return_code"] = completed.returncode
        result["stdout"] = completed.stdout.strip()
        result["stderr"] = completed.stderr.strip()
        if result["success"]:
            log_event(
                logger,
                logging.INFO,
                "SYNC ATTEMPT",
                "Sync completed successfully.",
                return_code=completed.returncode,
            )
        else:
            log_event(
                logger,
                logging.WARNING,
                "SYNC ATTEMPT",
                "Sync command failed.",
                return_code=completed.returncode,
            )
        return result
    except Exception as error:
        result["status"] = "error"
        result["error"] = str(error)
        log_event(
            logger,
            logging.WARNING,
            "SYNC ATTEMPT",
            "Sync hook raised an exception.",
            error=error,
        )
        return result


def _build_sync_command(sync_config: SyncConfig, run_context: RunContext) -> list[str]:
    if not sync_config.host or not sync_config.destination_path:
        raise ValueError("Sync enabled but host or destination_path is missing.")

    remote = (
        f"{sync_config.username}@{sync_config.host}:{sync_config.destination_path}"
        if sync_config.username
        else f"{sync_config.host}:{sync_config.destination_path}"
    )
    source = str(run_context.output_dir)

    if sync_config.strategy == "scp":
        return ["scp", "-r", source, remote]
    return ["rsync", "-avz", source, remote]
