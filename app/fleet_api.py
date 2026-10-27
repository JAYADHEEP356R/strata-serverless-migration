"""Authenticated, read-only workspace migration progress."""
import logging
import os
from fastapi import APIRouter, Request
from migration_agent.fleet import FleetService
from migration_agent.workflow import WorkflowError
from plan_api import request_identity


def register_fleet(application, snapshots, get_planner, identity=None, service=None):
    router = APIRouter()
    identity = identity or request_identity
    state = [service]

    def get_service():
        if state[0] is None:
            state[0] = FleetService(get_planner(), lambda: snapshots.get(force=True), os.environ.get("MIGRATION_EXECUTION_JOB"))
        return state[0]

    @router.get("/api/fleet")
    def fleet(request: Request, refresh: bool = False):
        identity(request)
        try:
            return get_service().get(refresh=refresh)
        except WorkflowError:
            raise
        except Exception as exc:
            logging.getLogger("migration.fleet").warning("fleet_read_failed error_type=%s", type(exc).__name__)
            raise WorkflowError("FLEET_UNAVAILABLE", "Saved migration progress could not be confirmed. The published inventory remains available; refresh progress to retry.", 503) from None

    application.include_router(router)
    return get_service
