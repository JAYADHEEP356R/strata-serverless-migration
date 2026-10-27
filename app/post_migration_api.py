"""Post-migration validation requests and recorded application activity."""
import logging
import os
import re

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field

from migration_agent.activity_log import activity
from migration_agent.post_validation import ValidationService
from migration_agent.workflow import WorkflowError
from plan_api import request_identity, verify_write_request


class ValidateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    definition_id: str = Field(pattern="^[a-f0-9]{32}$")
    asset_key: str = Field(min_length=1, max_length=200)
    mode: str = Field(pattern="^(CONFIGURATION|OUTPUT)$")
    baseline_run_id: str | None = Field(default=None, pattern="^[0-9]+$")
    request_id: str = Field(min_length=36, max_length=36)
    acknowledged: bool


def register_post_migration(application, get_planner, identity=None):
    router = APIRouter()
    identify = identity or request_identity
    def service():
        runner = os.environ.get("MIGRATION_EXECUTION_JOB", "")
        if not runner.isdigit():
            raise WorkflowError("VALIDATION_UNAVAILABLE", "The validation runner is not configured.", 503)
        return ValidationService(get_planner(), runner)
    def perform(action):
        try:
            return action()
        except WorkflowError:
            raise
        except Exception as exc:
            logging.getLogger("migration.validation").warning("post_migration_failed error_type=%s", type(exc).__name__)
            raise WorkflowError("POST_MIGRATION_UNAVAILABLE", "Recorded activity or validation could not be read. Refresh before retrying.", 503) from None

    @router.get("/api/post-validation/items")
    def items(request: Request):
        identify(request)
        validator = service()
        host = (validator.workspace.config.host or "").rstrip("/")
        if not re.fullmatch(r"https://[A-Za-z0-9.-]+\.(?:databricks\.com|azuredatabricks\.net)", host):
            host = None
        return {**perform(validator.list), "workspace_host": host}

    @router.post("/api/post-validation", status_code=202)
    def start(payload: ValidateRequest, request: Request):
        verify_write_request(request)
        return perform(lambda: service().start(identify(request), **payload.model_dump()))

    @router.get("/api/post-validation/{validation_id}")
    def status(validation_id: str, request: Request):
        identify(request)
        return perform(lambda: service().status(validation_id))

    @router.get("/api/activity")
    def events(request: Request):
        identify(request)
        planner = get_planner()
        return perform(lambda: activity(planner.store, planner.report_loader()))

    application.include_router(router)
