"""Publish sanitized planning evidence using the scanner's existing read rights.

No workload mutation, permission updates, or source-code persistence occurs here.
"""
from .workflow import WorkflowError, read_source
from .experiment_profile import PROFILE as EXPERIMENT_PROFILE, source_supported as experiment_supported, parameters as experiment_parameters
from .proof_profile import BATCH_PROFILE_ID, batch_source_supported


def enrich_planning(workspace, report):
    sources = {}
    for asset in report["assets"]:
        if asset["asset_type"] != "JOB" or asset.get("is_fixture"):
            continue
        if asset.get("compute_kind") not in {"CLASSIC", "MIXED"}:
            asset.update(planning_supported=False, planning_reason="No classic compute assignment was found; this is not a classic migration target.")
            continue
        try:
            source = read_source(workspace, asset["resource_id"], report["scope_root"], report["actor"], workspace_scope=True)
            if batch_source_supported(source, report["scope_root"]):
                source["execution_profile"] = BATCH_PROFILE_ID
                asset["execution_profile"] = BATCH_PROFILE_ID
            if experiment_supported(source):
                source["experiment"] = experiment_parameters(source)
                source["execution_profile"] = EXPERIMENT_PROFILE
                asset["execution_profile"] = EXPERIMENT_PROFILE
            # Full settings, parameters and source are hashed in memory only.
            # Retain environment keys for collision avoidance, never dependency URLs.
            source["settings"] = {"environments": [{"environment_key": e["environment_key"]} for e in source["settings"].get("environments", [])]}
            source["source_checks"].update(job_configuration="PUBLISHED_EVIDENCE", notebook_source="PUBLISHED_EVIDENCE", classic_configuration="PUBLISHED_EVIDENCE", captured_at=report["scan_time"])
            sources[str(asset["resource_id"])] = source
            asset.update(planning_supported=True, planning_reason="Notebook job inspected. Review findings and define isolated outputs before execution.")
        except WorkflowError as exc:
            asset.update(planning_supported=False, planning_reason=exc.message, planning_error=exc.code)
        except Exception as exc:
            asset.update(planning_supported=False, planning_reason="Source or dependency inspection failed. Check scanner access and rescan.", planning_error=type(exc).__name__)
            report["coverage"] = "PARTIAL"
            report["errors"].append({"operation": "planning-source", "resource": str(asset["resource_id"]), "error_type": type(exc).__name__, "message": "Planning source inspection is incomplete."})
    report["planning_sources"] = sources
    report["planning_evidence_mode"] = "PUBLISHED_SCAN"
    return report
