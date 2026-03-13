from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from scraper_engine.hosted_service import HostedJobService, HostedServiceError


class JobSubmissionRequest(BaseModel):
    preset: str = Field(..., min_length=1)
    target_url: str = Field(..., min_length=1)
    run_name: str | None = None


def create_app(
    *,
    public_config_root: Path | None = None,
    output_root: Path | None = None,
    pipeline_runner=None,
    run_jobs_inline: bool = False,
) -> FastAPI:
    repo_root = Path(__file__).resolve().parent.parent
    service = HostedJobService(
        public_config_root=public_config_root or (repo_root / "configs" / "public"),
        output_root=output_root or (repo_root / "outputs"),
        pipeline_runner=pipeline_runner,
        run_jobs_inline=run_jobs_inline,
    )

    app = FastAPI(title="scraper-engine hosted backend", version="0.1.0")
    app.state.hosted_service = service

    @app.get("/api/capabilities")
    def get_capabilities() -> dict[str, Any]:
        return service.capabilities()

    @app.get("/api/presets")
    def get_presets() -> dict[str, Any]:
        return {"presets": service.list_presets()}

    @app.post("/api/jobs", status_code=202)
    def submit_job(payload: JobSubmissionRequest) -> dict[str, Any]:
        try:
            return service.submit_job(
                preset=payload.preset,
                target_url=payload.target_url,
                run_name=payload.run_name,
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

    return app


app = create_app()
