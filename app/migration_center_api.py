"""Authenticated create, save and run controls for the migration workspace."""
import logging
import os

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field

from migration_agent.migration_center import MigrationCenter
from migration_agent.workflow import WorkflowError
from plan_api import request_identity, verify_write_request


class InspectRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    asset_keys: list[str] = Field(min_length=1, max_length=50)
    scan_id: str = Field(min_length=1, max_length=100)


class SaveRequest(InspectRequest):
    inspection_hash: str = Field(pattern="^[a-f0-9]{64}$")
    mode: str = Field(pattern="^(WAVE|INDIVIDUAL)$")
    name: str = Field(default="", max_length=80)
    request_id: str = Field(min_length=36, max_length=36)


class StartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    definition_hash: str = Field(pattern="^[a-f0-9]{64}$")
    acknowledged: bool


def register_migration_center(application, get_planner, identity=None):
    router = APIRouter(prefix="/api/migration-center")
    identify = identity or request_identity

    def service():
        runner = os.environ.get("MIGRATION_EXECUTION_JOB", "")
        if not runner.isdigit():
            raise WorkflowError("EXECUTOR_UNAVAILABLE", "The migration executor is not configured.", 503)
        return MigrationCenter(get_planner(), runner)

    def perform(action):
        try:
            return action()
        except WorkflowError:
            raise
        except Exception as exc:
            logging.getLogger("migration.center").warning("migration_center_failed error_type=%s", type(exc).__name__)
            raise WorkflowError("MIGRATION_UNAVAILABLE", "The migration operation could not be confirmed. Refresh its status before retrying.", 503) from None

    @router.get("/plans")
    def listing(request: Request):
        identify(request)
        return perform(lambda: service().list())

    @router.post("/inspect")
    def inspect(payload: InspectRequest, request: Request):
        verify_write_request(request)
        return perform(lambda: service().inspect(identify(request), **payload.model_dump()))

    @router.post("/plans", status_code=201)
    def save(payload: SaveRequest, request: Request):
        verify_write_request(request)
        return perform(lambda: service().save(identify(request), **payload.model_dump()))

    @router.get("/plans/{reference}")
    def detail(reference: str, request: Request):
        identify(request)
        return perform(lambda: service().get(reference))

    @router.post("/plans/{reference}/run", status_code=202)
    def run(reference: str, payload: StartRequest, request: Request):
        verify_write_request(request)
        return perform(lambda: service().start(identify(request), reference, **payload.model_dump()))

    application.include_router(router)
