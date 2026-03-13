from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from scraper_engine.hosted_service import HostedJobService, HostedServiceError


class JobSubmissionRequest(BaseModel):
    preset: str = Field(..., min_length=1)
    target_url: str = Field(..., min_length=1)
    run_name: str | None = None


def _get_cors_origins_from_env() -> list[str]:
    configured = os.getenv("SCRAPER_ENGINE_CORS_ORIGINS", "").strip()
    if configured:
        return [origin.strip() for origin in configured.split(",") if origin.strip()]
    return [
        "https://scraper.crkdev.com",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]


def create_app(
    *,
    public_config_root: Path | None = None,
    output_root: Path | None = None,
    pipeline_runner=None,
    run_jobs_inline: bool = False,
    cors_origins: list[str] | None = None,
    max_pages: int | None = None,
    max_records: int | None = None,
    max_runtime_seconds: int | None = None,
    max_download_bytes: int | None = None,
    max_jobs_per_hour_per_ip: int | None = None,
) -> FastAPI:
    repo_root = Path(__file__).resolve().parent.parent
    service = HostedJobService(
        public_config_root=public_config_root or (repo_root / "configs" / "public"),
        output_root=output_root or (repo_root / "outputs"),
        pipeline_runner=pipeline_runner,
        run_jobs_inline=run_jobs_inline,
        max_pages=max_pages,
        max_records=max_records,
        max_runtime_seconds=max_runtime_seconds,
        max_download_bytes=max_download_bytes,
        max_jobs_per_hour_per_ip=max_jobs_per_hour_per_ip,
    )

    app = FastAPI(title="scraper-engine hosted backend", version="0.1.0")
    app.state.hosted_service = service
    allowed_origins = cors_origins if cors_origins is not None else _get_cors_origins_from_env()
    if allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=allowed_origins,
            allow_credentials=False,
            allow_methods=["GET", "POST", "OPTIONS"],
            allow_headers=["*"],
        )

    @app.get("/api/capabilities")
    def get_capabilities() -> dict[str, Any]:
        return service.capabilities()

    @app.get("/api/presets")
    def get_presets() -> dict[str, Any]:
        return {"presets": service.list_presets()}

    @app.get("/api/limits")
    def get_limits() -> dict[str, Any]:
        return service.limits()

    @app.get("/health")
    def get_health() -> dict[str, Any]:
        return {"status": "ok"}

    @app.post("/api/jobs", status_code=202)
    def submit_job(payload: JobSubmissionRequest, request: Request) -> dict[str, Any]:
        try:
            return service.submit_job(
                preset=payload.preset,
                target_url=payload.target_url,
                run_name=payload.run_name,
                client_ip=request.client.host if request.client else None,
            )
        except HostedServiceError as error:
            raise HTTPException(status_code=error.status_code, detail=error.to_failure()) from error

    @app.get("/api/jobs/{job_id}")
    def get_job(job_id: str) -> dict[str, Any]:
        try:
            return service.get_job(job_id)
        except HostedServiceError as error:
            raise HTTPException(status_code=error.status_code, detail=error.to_failure()) from error

    @app.get("/api/jobs/{job_id}/files/{file_name}")
    def download_file(job_id: str, file_name: str):
        try:
            file_path = service.get_download_path(job_id, file_name)
        except HostedServiceError as error:
            raise HTTPException(status_code=error.status_code, detail=error.to_failure()) from error
        return FileResponse(path=file_path, filename=file_name)

    @app.get("/api/jobs/{job_id}/download/{file_name}")
    def download_file_compat(job_id: str, file_name: str):
        return download_file(job_id, file_name)

    return app


app = create_app()
