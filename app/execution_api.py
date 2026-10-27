"""Explicit approval-bound demo execution, separate from plan approval."""
import json
import logging
import os
import re
from fastapi import APIRouter, Request
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field
from migration_agent.execution import ExecutionService
from migration_agent.workflow import WorkflowError
from plan_api import request_identity, verify_write_request


class ExecuteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    plan_id: str = Field(pattern="^[a-f0-9]{32}$")
    plan_hash: str = Field(pattern="^[a-f0-9]{64}$")
    source_fingerprint: str = Field(pattern="^[a-f0-9]{64}$")
    request_id: str = Field(min_length=36, max_length=36)
    acknowledgements: list[str] = Field(max_length=3)
    baseline_run_id: str | None = Field(default=None, pattern=r"^[0-9]+$")


def register_execution(application, get_planner, identity=None, service=None):
    router = APIRouter()
    identity = identity or request_identity
    state = [service]

    def get_service():
        if state[0] is None:
            runner = os.environ.get("MIGRATION_EXECUTION_JOB", "")
            if not runner.isdigit():
                raise WorkflowError("EXECUTION_NOT_CONFIGURED", "The dedicated demo executor is not configured.", 503)
            planner = get_planner()
            volume = os.environ.get("MIGRATION_PLANS_VOLUME", "")
            matched = re.fullmatch(r"/Volumes/([A-Za-z0-9_-]+)/(migration_agent_jr_[A-Za-z0-9_]+)/plans/?", volume)
            scope = matched.groups() if matched else None
            state[0] = ExecutionService(planner.workspace, planner, runner, os.environ.get("MIGRATION_BASELINE_MODE", "legacy_copy"), scope)
        return state[0]

    def perform(action):
        try:
            return action()
        except WorkflowError:
            raise
        except Exception as exc:
            logging.getLogger("migration.execution").warning("execution_request_failed error_type=%s", type(exc).__name__)
            raise WorkflowError("EXECUTION_UNAVAILABLE", "The execution request or status could not be confirmed. Reload this plan's execution status before retrying; retries reuse the existing request.", 503) from None

    @router.post("/api/executions", status_code=202)
    def execute(payload: ExecuteRequest, request: Request):
        verify_write_request(request)
        actor = identity(request)
        values = {"baseline_run_id": payload.baseline_run_id} if payload.baseline_run_id is not None else {}
        return perform(lambda: get_service().start(actor, payload.plan_id, payload.plan_hash, payload.source_fingerprint, payload.request_id, payload.acknowledgements, **values))

    @router.get("/api/plans/{plan_id}/baseline-runs")
    def baseline_runs(plan_id: str, request: Request):
        actor = identity(request)
        return perform(lambda: get_service().baseline_runs(actor, plan_id))

    @router.get("/api/plans/{plan_id}/execution")
    def status(plan_id: str, request: Request):
        actor = identity(request)
        return perform(lambda: get_service().get(actor, plan_id))

    @router.get("/api/plans/{plan_id}/candidate-bundle")
    def candidate_bundle(plan_id: str, request: Request):
        identity(request)
        service = get_service()
        current = perform(lambda: service.get(identity(request), plan_id))
        execution = current.get("execution") or {}
        result = (execution.get("stages") or {}).get("results") or {}
        if result.get("status") != "PASS":
            raise WorkflowError("VALIDATION_REQUIRED", "Validate the candidate before exporting its bundle.")
        bundle = service.store.read("execution_candidate_bundle", execution["execution_id"])
        if not bundle or bundle.get("plan_id") != plan_id or bundle.get("plan_hash") != execution["plan_hash"]:
            raise WorkflowError("BUNDLE_UNAVAILABLE", "The validated candidate bundle is not available for this execution.")
        return Response(json.dumps(bundle["config"], indent=2), media_type="application/yaml", headers={"Content-Disposition": 'attachment; filename="databricks.yml"'})

    application.include_router(router)
