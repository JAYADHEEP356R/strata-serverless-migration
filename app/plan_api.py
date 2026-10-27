"""Authenticated planning API; no migration execution endpoint is exposed."""
import logging
import json
import os
import re
import threading
from urllib.parse import urlsplit

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from migration_agent.plan_store import LocalPlanStore, VolumePlanStore
from migration_agent.presentation import legacy_name
from migration_agent.workflow import PlanningService, WorkflowError

logger = logging.getLogger("migration.planning")


class GenerateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    asset_key: str = Field(min_length=1, max_length=100)
    scan_id: str = Field(min_length=1, max_length=100)
    request_id: str = Field(min_length=36, max_length=36)


class DecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    decision: str = Field(pattern="^(APPROVED|REJECTED)$")
    plan_hash: str = Field(pattern="^[a-f0-9]{64}$")
    source_fingerprint: str = Field(pattern="^[a-f0-9]{64}$")
    acknowledgements: list[str] = Field(default_factory=list, max_length=20)
    reason: str = Field(default="", max_length=1000)


def request_identity(request):
    mode = os.environ.get("MIGRATION_AUTH_MODE", "disabled")
    if mode == "local" and not any(os.environ.get(k) for k in ("DATABRICKS_CLIENT_ID", "DATABRICKS_APP_NAME")):
        actor = os.environ.get("MIGRATION_LOCAL_ACTOR", "").strip().casefold()
        if actor:
            return actor
    if mode == "databricks":
        # These headers are supplied by the Databricks Apps authenticated proxy.
        # Local development cannot opt into proxy identity through request headers.
        actor = request.headers.get("x-forwarded-email", "").strip().casefold()
        subject = request.headers.get("x-forwarded-user", "").strip()
        proxy_host = request.headers.get("x-forwarded-host", "").split(",")[0].strip().lower()
        if actor and "@" in actor and subject and proxy_host.endswith(".databricksapps.com"):
            return actor
    raise WorkflowError("IDENTITY_REQUIRED", "Sign in through Databricks Apps to create or review plan decisions.", 401)


def verify_write_request(request):
    if request.headers.get("x-migration-request") != "1" or request.headers.get("content-type", "").split(";")[0].strip().lower() != "application/json":
        raise WorkflowError("INVALID_REQUEST_ORIGIN", "Use the application's planning controls for this action.", 403)
    origin = urlsplit(request.headers.get("origin", ""))
    target = (request.headers.get("x-forwarded-host", "") if os.environ.get("MIGRATION_AUTH_MODE") == "databricks" else request.headers.get("host", "")).split(",")[0].strip().lower()
    if origin.scheme not in {"https", "http"} or origin.netloc.lower() != target or origin.username or origin.password:
        raise WorkflowError("INVALID_REQUEST_ORIGIN", "The request origin does not match this application.", 403)
    if os.environ.get("MIGRATION_AUTH_MODE") == "databricks" and origin.scheme != "https":
        raise WorkflowError("INVALID_REQUEST_ORIGIN", "Planning decisions require the secure application origin.", 403)


def register_planning(application, snapshots, service=None, identity=None):
    router = APIRouter()
    lock = threading.Lock()
    state = {"service": service}
    identity = identity or request_identity

    def get_service():
        with lock:
            if state["service"] is None:
                source_job = os.environ.get("MIGRATION_SOURCE_JOB", "")
                volume = os.environ.get("MIGRATION_PLANS_VOLUME", "")
                if not volume:
                    raise WorkflowError("PLANNING_NOT_CONFIGURED", "The planning job and plans volume resources are not configured.", 503)
                from databricks.sdk import WorkspaceClient
                workspace = WorkspaceClient()
                if workspace.config.workspace_id:
                    workspace_id = str(workspace.config.workspace_id)
                    workspace = WorkspaceClient(custom_headers={"X-Databricks-Workspace-Id": workspace_id, "X-Databricks-Org-Id": workspace_id})
                storage = VolumePlanStore(workspace, volume)
                def evidence(cluster_id):
                    if not re.fullmatch(r"[A-Za-z0-9-]+", cluster_id):
                        raise WorkflowError("INVALID_CLUSTER", "Invalid classic source reference.", 400)
                    artifacts = os.environ.get("MIGRATION_ARTIFACTS_VOLUME", "").rstrip("/")
                    if not re.fullmatch(r"/Volumes/[A-Za-z0-9_-]+/migration_agent_jr_[A-Za-z0-9_]+/artifacts", artifacts):
                        raise WorkflowError("INVALID_EVIDENCE_VOLUME", "The read-only project evidence volume is not configured.", 503)
                    response = workspace.files.download(artifacts + "/planning_evidence/cluster_" + cluster_id + ".json")
                    with response.contents as content:
                        return json.load(content)
                state["service"] = PlanningService(workspace, storage, lambda: snapshots.get(force=True), source_job, evidence, workspace_planning=True)
            return state["service"]

    def perform(operation):
        try:
            return operation()
        except WorkflowError:
            raise
        except Exception as exc:
            logger.warning("planning_operation_failed error_type=%s", type(exc).__name__)
            raise WorkflowError("SOURCE_OR_STORAGE_UNAVAILABLE", "The live source or planning storage could not be verified. No approval or workload change was made; check project resource access and retry.", 503) from None

    @application.exception_handler(WorkflowError)
    async def workflow_error(request, exc):
        return JSONResponse({"error": {"code": exc.code, "message": exc.message}}, status_code=exc.status)

    @router.get("/api/planning/session")
    def session(request: Request):
        actor = identity(request)
        return perform(lambda: {**get_service().policy(actor), "source_job_id": get_service().allowed_job_id, "workspace_planning": get_service().workspace_planning, "demo_execution_available": bool(os.environ.get("MIGRATION_EXECUTION_JOB"))})

    @router.get("/api/plans")
    def plans(request: Request):
        identity(request)
        return perform(lambda: {"plans": [p for p in get_service().list() if os.environ.get("MIGRATION_HIDE_LEGACY_DEMOS") != "true" or not legacy_name(p.get("source_name"))], "execution_enabled": False, "limit": 100})

    @router.get("/api/plans/{plan_id}")
    def plan(plan_id: str, request: Request):
        identity(request)
        value = perform(lambda: get_service().get(plan_id))
        if os.environ.get("MIGRATION_HIDE_LEGACY_DEMOS") == "true" and legacy_name(value.get("source_name")):
            raise WorkflowError("PLAN_NOT_FOUND", "This retired demonstration is no longer shown in the App.", 404)
        return value

    @router.post("/api/plans", status_code=201)
    def generate(payload: GenerateRequest, request: Request):
        verify_write_request(request)
        actor = identity(request)
        return perform(lambda: get_service().generate(actor, payload.asset_key, payload.scan_id, payload.request_id))

    @router.post("/api/plans/{plan_id}/decision")
    def decision(plan_id: str, payload: DecisionRequest, request: Request):
        verify_write_request(request)
        actor = identity(request)
        return perform(lambda: get_service().decide(actor, plan_id, payload.decision, payload.plan_hash, payload.source_fingerprint, payload.acknowledgements, payload.reason))

    application.include_router(router)
    return get_service
