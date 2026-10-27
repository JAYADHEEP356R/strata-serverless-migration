"""Authenticated persistent review groups using existing project storage."""
import logging
import os
from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field
from migration_agent.waves import WaveService
from migration_agent.direct_wave import DirectWaveService
from migration_agent.workflow import WorkflowError
from plan_api import request_identity, verify_write_request


class CreateWave(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=80)
    description: str = Field(default="", max_length=500)
    asset_keys: list[str] = Field(min_length=1, max_length=50)
    scan_id: str = Field(min_length=1, max_length=100)
    request_id: str = Field(min_length=36, max_length=36)


class DirectPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")
    plan_id: str = Field(pattern="^[a-f0-9]{32}$")
    plan_hash: str = Field(pattern="^[a-f0-9]{64}$")
    source_fingerprint: str = Field(pattern="^[a-f0-9]{64}$")

class DirectWaveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    wave_hash: str = Field(pattern="^[a-f0-9]{64}$")
    plans: list[DirectPlan] = Field(min_length=1, max_length=50)
    request_id: str = Field(min_length=36, max_length=36)
    acknowledged: bool


def register_waves(application, get_planner, get_fleet, identity=None, service=None):
    router = APIRouter()
    identity = identity or request_identity
    state = [service]

    def get_service():
        if state[0] is None:
            state[0] = WaveService(get_planner(), get_fleet())
        return state[0]

    def perform(action):
        try:
            return action()
        except WorkflowError:
            raise
        except Exception as exc:
            logging.getLogger("migration.waves").warning("wave_request_failed error_type=%s", type(exc).__name__)
            raise WorkflowError("WAVES_UNAVAILABLE", "The saved wave could not be confirmed. Reload waves before retrying the same request.", 503) from None

    @router.get("/api/waves")
    def listing(request: Request):
        identity(request)
        return perform(lambda: get_service().list())

    @router.get("/api/waves/{wave_id}")
    def detail(wave_id: str, request: Request, refresh: bool = False):
        identity(request)
        return perform(lambda: get_service().get(wave_id, refresh))

    @router.post("/api/waves", status_code=201)
    def create(payload: CreateWave, request: Request):
        verify_write_request(request)
        actor = identity(request)
        return perform(lambda: get_service().create(actor=actor, **payload.model_dump()))

    def direct_service():
        runner = os.environ.get("MIGRATION_EXECUTION_JOB", "")
        if not runner.isdigit(): raise WorkflowError("EXECUTION_NOT_CONFIGURED", "The migration executor is unavailable.", 503)
        return DirectWaveService(get_planner(), get_fleet(), runner)

    @router.get("/api/waves/{wave_id}/direct-preview")
    def direct_preview(wave_id: str, request: Request):
        return perform(lambda: direct_service().preview(identity(request), wave_id))

    @router.get("/api/waves/{wave_id}/direct-status")
    def direct_status(wave_id: str, request: Request):
        return perform(lambda: direct_service().status(identity(request), wave_id))

    @router.post("/api/waves/{wave_id}/direct-start", status_code=202)
    def direct_start(wave_id: str, payload: DirectWaveRequest, request: Request):
        verify_write_request(request)
        return perform(lambda: direct_service().start(identity(request), wave_id, **payload.model_dump()))

    application.include_router(router)
