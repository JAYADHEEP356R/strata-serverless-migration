"""Read-only FastAPI service for the migration control plane."""
import json
import logging
import os
import re
import sys
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import migration_agent

ROOT = Path(__file__).resolve().parent
MAX_SNAPSHOT_BYTES = 20 * 1024 * 1024
logger = logging.getLogger("migration.app")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from plan_api import register_planning
from discovery_api import register_discovery
from execution_api import register_execution
from fleet_api import register_fleet
from waves_api import register_waves
from migration_center_api import register_migration_center
from post_migration_api import register_post_migration
from migration_agent.presentation import visible_report


class SnapshotError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def validate_report(report):
    """Reject malformed snapshots rather than presenting invented empty results."""
    if not isinstance(report, dict) or not isinstance(report.get("scan_id"), str) or not report["scan_id"]:
        raise SnapshotError("INVALID_SNAPSHOT")
    if report.get("scope") not in {"project", "workspace"}:
        raise SnapshotError("INVALID_SNAPSHOT")
    if not isinstance(report.get("scan_time"), str) or report.get("coverage") not in {"PARTIAL", "COMPLETE_WITHIN_SELECTED_VISIBILITY"}:
        raise SnapshotError("INVALID_SNAPSHOT")
    for field in ("assets", "findings", "relationships"):
        if not isinstance(report.get(field), list):
            raise SnapshotError("INVALID_SNAPSHOT")
    for field in ("errors", "validation_evidence"):
        report.setdefault(field, [])
        if not isinstance(report[field], list):
            raise SnapshotError("INVALID_SNAPSHOT")
    keys = set()
    for asset in report["assets"]:
        if not isinstance(asset, dict) or not all(isinstance(asset.get(k), str) for k in ("asset_key", "asset_type", "name")):
            raise SnapshotError("INVALID_SNAPSHOT")
        if asset["asset_key"] in keys:
            raise SnapshotError("INVALID_SNAPSHOT")
        keys.add(asset["asset_key"])
        score = asset.get("readiness_score")
        if score is not None and (isinstance(score, bool) or not isinstance(score, (int, float)) or not 0 <= score <= 100):
            raise SnapshotError("INVALID_SNAPSHOT")
        asset.setdefault("resource_id", asset["asset_key"])
        asset.setdefault("compute_kind", "UNKNOWN")
        asset.setdefault("assessment_coverage", "METADATA_ONLY")
        asset.setdefault("migration_status", "DISCOVERED")
        asset.setdefault("is_fixture", False)
        asset.setdefault("eligible", False)
        asset.setdefault("readiness_score", None)
        if not all(isinstance(asset.get(k), str) for k in ("resource_id", "compute_kind", "assessment_coverage", "migration_status")):
            raise SnapshotError("INVALID_SNAPSHOT")
        if not all(isinstance(asset.get(k), bool) for k in ("eligible", "is_fixture")):
            raise SnapshotError("INVALID_SNAPSHOT")
        if not all(asset.get(k) is None or isinstance(asset.get(k), str) for k in ("path", "owner", "language", "source_fingerprint")):
            raise SnapshotError("INVALID_SNAPSHOT")
    for finding in report["findings"]:
        if not isinstance(finding, dict) or not all(isinstance(finding.get(k), str) for k in ("asset_key", "rule", "severity", "limitation", "evidence", "recommendation")):
            raise SnapshotError("INVALID_SNAPSHOT")
        if finding["asset_key"] not in keys:
            raise SnapshotError("INVALID_SNAPSHOT")
        finding.setdefault("location", "configuration")
    for relation in report["relationships"]:
        if not isinstance(relation, dict) or not all(isinstance(relation.get(k), str) for k in ("from_key", "to_key", "relation")):
            raise SnapshotError("INVALID_SNAPSHOT")
    for event in report["validation_evidence"]:
        if not isinstance(event, dict) or not all(isinstance(event.get(k), str) for k in ("run_key", "status", "checked_at")):
            raise SnapshotError("INVALID_SNAPSHOT")
        for field in ("classic_rows", "serverless_rows", "classic_sales", "serverless_sales", "left_only", "right_only"):
            value = event.get(field)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                raise SnapshotError("INVALID_SNAPSHOT")
        if event.get("schema_match") is not None and not isinstance(event["schema_match"], bool):
            raise SnapshotError("INVALID_SNAPSHOT")
        if not all(event.get(k) is None or isinstance(event.get(k), str) for k in ("classic_table", "serverless_table", "comparison")):
            raise SnapshotError("INVALID_SNAPSHOT")
    for error in report["errors"]:
        if not isinstance(error, dict) or not all(isinstance(error.get(k), str) for k in ("operation", "resource", "error_type", "message")):
            raise SnapshotError("INVALID_SNAPSHOT")
    return report


class SnapshotStore:
    """The only data source is the configured project volume or explicit local file."""
    def __init__(self, local=None, volume=None, ttl=30, workspace_factory=None):
        self.local = local
        self.volume = volume or ""
        self.ttl = ttl
        self.workspace_factory = workspace_factory
        self._report = None
        self._loaded = 0
        self._lock = threading.Lock()

    def get(self, force=False):
        with self._lock:
            if self._report is not None and not force and time.monotonic() - self._loaded < self.ttl:
                return self._report
            try:
                if self.local:
                    path = Path(self.local)
                    if path.stat().st_size > MAX_SNAPSHOT_BYTES:
                        raise SnapshotError("SNAPSHOT_TOO_LARGE")
                    data = path.read_bytes()
                else:
                    if not re.fullmatch(r"/Volumes/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/?", self.volume):
                        raise SnapshotError("VOLUME_NOT_CONFIGURED")
                    if self.workspace_factory is None:
                        from databricks.sdk import WorkspaceClient
                        workspace = WorkspaceClient()
                    else:
                        workspace = self.workspace_factory()
                    response = workspace.files.download(self.volume.rstrip("/") + "/discovery/latest.json")
                    with response.contents as content:
                        data = content.read(MAX_SNAPSHOT_BYTES + 1)
                if len(data) > MAX_SNAPSHOT_BYTES:
                    raise SnapshotError("SNAPSHOT_TOO_LARGE")
                report = validate_report(json.loads(data))
            except SnapshotError:
                raise
            except Exception as exc:
                logger.warning("snapshot_read_failed error_type=%s", type(exc).__name__)
                raise SnapshotError("SNAPSHOT_UNAVAILABLE") from None
            self._report, self._loaded = report, time.monotonic()
            return report


def create_app(store=None, planning_service=None, identity_resolver=None, discovery_service=None, execution_service=None, fleet_service=None, wave_service=None):
    store = store or SnapshotStore(os.environ.get("MIGRATION_LOCAL_REPORT"), os.environ.get("MIGRATION_ARTIFACTS_VOLUME"))

    get_planner = None

    @asynccontextmanager
    async def lifespan(application):
        try:
            report = store.get()
            print(json.dumps({"event": "project_snapshot_read_verified", "scan_id": report["scan_id"], "asset_count": len(report["assets"])}), flush=True)
        except SnapshotError as exc:
            logger.warning("snapshot_initialization_pending code=%s", exc.code)
            if os.environ.get("MIGRATION_REQUIRE_SNAPSHOT") == "true":
                raise RuntimeError("Project snapshot verification failed: " + exc.code) from None
        if os.environ.get("MIGRATION_REQUIRE_PLANNER") == "true":
            planner = get_planner()
            planner.store.list_ids(limit=1)
            report = store.get(force=True)
            if report.get("scope") == "workspace" and planner.workspace_planning:
                for job_id in report.get("planning_sources", {}):
                    planner.source(report, job_id)
                print(json.dumps({"event": "workspace_planning_evidence_verified", "supported_jobs": len(report.get("planning_sources", {}))}), flush=True)
            else:
                source = planner.source(report)
                print(json.dumps({"event": "project_planning_sources_verified", "job_id": source["job_id"], "task_count": len(source["tasks"])}), flush=True)
        if os.environ.get("MIGRATION_REQUIRE_EXECUTOR") == "true":
            from migration_agent.execution import guarded_runner
            planner = get_planner()
            report = store.get(force=True)
            guarded_runner(planner.workspace, os.environ.get("MIGRATION_EXECUTION_JOB", ""), report["scope_root"], report["actor"])
            print(json.dumps({"event": "dedicated_demo_executor_access_verified", "trial_started": False}), flush=True)
        yield

    application = FastAPI(title="Migration control plane", version="0.3.0", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    register_discovery(application, store, discovery_service, identity_resolver)

    @application.middleware("http")
    async def response_headers(request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "same-origin"
        if request.url.path.startswith("/api/") or request.url.path in {"/", "/index.html"}:
            response.headers["Cache-Control"] = "no-store"
        return response

    @application.get("/api/health")
    def health():
        try:
            report = store.get()
            return {"status": "ready", "mode": "demo_trial_available" if os.environ.get("MIGRATION_EXECUTION_JOB") else "planning_only" if os.environ.get("MIGRATION_PLANS_VOLUME") else "read_only", "execution_enabled": False, "demo_execution_available": bool(os.environ.get("MIGRATION_EXECUTION_JOB")), "scan_id": report["scan_id"], "version": "0.3.0"}
        except SnapshotError as exc:
            return JSONResponse({"status": "degraded", "mode": "read_only", "code": exc.code}, status_code=503)

    @application.get("/api/report")
    def report(refresh: bool = Query(False)):
        try:
            current = store.get(force=True)
            return visible_report(current) if os.environ.get("MIGRATION_HIDE_LEGACY_DEMOS") == "true" else current
        except SnapshotError as exc:
            return JSONResponse({"error": {"code": exc.code, "message": "The published project assessment is unavailable. Check the discovery run and project volume access."}}, status_code=503)

    @application.get("/api/report/download")
    def download_report():
        try:
            value = store.get()
            if os.environ.get("MIGRATION_HIDE_LEGACY_DEMOS") == "true": value = visible_report(value)
            return JSONResponse(value, headers={"Content-Disposition": 'attachment; filename="migration-assessment.json"'})
        except SnapshotError as exc:
            return JSONResponse({"error": {"code": exc.code}}, status_code=503)

    static = ROOT / "static"
    if (static / "assets").is_dir():
        application.mount("/assets", StaticFiles(directory=static / "assets"), name="assets")

    @application.get("/favicon.svg", include_in_schema=False)
    def favicon():
        if not (static / "favicon.svg").is_file():
            raise HTTPException(404)
        return FileResponse(static / "favicon.svg", media_type="image/svg+xml")

    @application.get("/", include_in_schema=False)
    def index():
        if not (static / "index.html").is_file():
            return JSONResponse({"error": "Frontend build missing. Run npm ci and npm run build in frontend/."}, status_code=503)
        return FileResponse(static / "index.html", headers={"Cache-Control": "no-store"})

    get_planner = register_planning(application, store, planning_service, identity_resolver)
    register_execution(application, get_planner, identity_resolver, execution_service)
    get_fleet = register_fleet(application, store, get_planner, identity_resolver, fleet_service)
    register_waves(application, get_planner, get_fleet, identity_resolver, wave_service)
    register_migration_center(application, get_planner, identity_resolver)
    register_post_migration(application, get_planner, identity_resolver)
    return application


app = create_app()
