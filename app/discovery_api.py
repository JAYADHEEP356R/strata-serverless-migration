"""Start only the project's read-only scanner, never a discovered workload."""
import logging
import os
import uuid
from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field
from migration_agent.workflow import WorkflowError, PROJECT
from plan_api import request_identity, verify_write_request


class ScanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str = Field(min_length=36, max_length=36)


class DiscoveryService:
    def __init__(self, workspace, job_id, snapshots):
        self.workspace, self.job_id, self.snapshots = workspace, int(job_id), snapshots

    def require_operator(self, actor):
        owner = self.snapshots.get(force=True).get("actor", "")
        if not owner or actor.casefold() != owner.casefold():
            raise WorkflowError("NOT_AUTHORIZED", "Only the project operator can request a workspace scan.", 403)

    def start(self, actor, request_id):
        self.require_operator(actor)
        try:
            request_id = str(uuid.UUID(request_id))
        except ValueError:
            raise WorkflowError("INVALID_REQUEST_ID", "A UUID scan request is required.", 400) from None
        report = self.snapshots.get(force=True)
        job = self.workspace.jobs.get(self.job_id)
        settings = job.settings
        tags = settings.tags or {}
        tasks = settings.tasks or []
        if tags.get("Project") != PROJECT or tags.get("Purpose") != "discovery-assessment" or (job.creator_user_name or "").casefold() != actor.casefold():
            raise WorkflowError("INVALID_SCANNER", "The configured resource is not the project discovery job.", 403)
        if len(tasks) != 1 or tasks[0].task_key != "discover_assess" or not tasks[0].notebook_task or tasks[0].notebook_task.notebook_path.removesuffix(".py") != report["scope_root"] + "/notebooks/discover_assess" or any(getattr(tasks[0], k, None) for k in ("existing_cluster_id", "new_cluster", "job_cluster_key")):
            raise WorkflowError("INVALID_SCANNER", "Discovery must use only the dedicated serverless scanner notebook.", 403)
        if settings.schedule or settings.trigger or settings.continuous:
            raise WorkflowError("INVALID_SCANNER", "Discovery must remain manually triggered.", 403)
        active = list(self.workspace.jobs.list_runs(job_id=self.job_id, active_only=True, limit=1))
        if active:
            return self.status(actor, active[0].run_id)
        run = self.workspace.jobs.run_now(job_id=self.job_id, job_parameters={"scope": "workspace"}, idempotency_token=request_id).response
        return self.status(actor, run.run_id)

    def status(self, actor, run_id):
        self.require_operator(actor)
        run = self.workspace.jobs.get_run(int(run_id))
        if run.job_id != self.job_id:
            raise WorkflowError("OUTSIDE_PROJECT", "This run is not a project discovery run.", 403)
        state = run.state
        return {"run_id": str(run.run_id), "state": state.life_cycle_state.value, "result": state.result_state.value if state.result_state else None, "scan_compute": "SERVERLESS", "workload_execution_enabled": False}


def register_discovery(application, snapshots, service=None, identity=None):
    router = APIRouter()
    identity = identity or request_identity
    cached = [service]

    def get_service():
        if cached[0] is None:
            job = os.environ.get("MIGRATION_DISCOVERY_JOB", "")
            if not job.isdigit():
                raise WorkflowError("DISCOVERY_NOT_CONFIGURED", "The project scanner resource is not configured.", 503)
            from databricks.sdk import WorkspaceClient
            cached[0] = DiscoveryService(WorkspaceClient(), job, snapshots)
        return cached[0]

    def perform(operation):
        try:
            return operation()
        except WorkflowError:
            raise
        except Exception as exc:
            logging.getLogger("migration.discovery").warning("discovery_request_failed error_type=%s", type(exc).__name__)
            raise WorkflowError("DISCOVERY_UNAVAILABLE", "The scanner could not be contacted. Retry with the same request; no migration workload was started.", 503) from None

    @router.post("/api/discovery/scans", status_code=202)
    def start(payload: ScanRequest, request: Request):
        verify_write_request(request)
        actor = identity(request)
        return perform(lambda: get_service().start(actor, payload.request_id))

    @router.get("/api/discovery/scans/{run_id}")
    def status(run_id: int, request: Request):
        actor = identity(request)
        return perform(lambda: get_service().status(actor, run_id))

    application.include_router(router)
