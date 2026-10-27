"""Bind execution authorization to the exact assessed configuration and source."""
from dataclasses import dataclass
from .assessment import assess, fingerprint


@dataclass(frozen=True)
class MigrationPlan:
    fingerprint: str
    source_name: str
    eligible: bool


def prepare_plan(config: dict, source: str, source_name: str) -> MigrationPlan:
    result = assess(config, source)
    return MigrationPlan(result.source_fingerprint, source_name, result.eligible)


def authorize_execution(plan: MigrationPlan, config: dict, source: str, approved_fingerprint: str) -> None:
    if not plan.eligible:
        raise PermissionError("Unresolved findings block automatic migration")
    if approved_fingerprint != plan.fingerprint or fingerprint(config, source) != plan.fingerprint:
        raise PermissionError("Plan is stale or authorization does not match")
