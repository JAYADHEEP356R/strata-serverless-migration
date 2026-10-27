"""Provision project-only planning access, generate a real draft, and inspect it.

There is intentionally no CLI approval or execution command in this milestone.
"""
import argparse
import json
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
from migration_agent.plan_store import VolumePlanStore
from migration_agent.workflow import PlanningService, PROJECT, capture_cluster_evidence
from smoke import CONTEXT, LOCAL, client, guarded_cluster, guarded_job


def resources(workspace, context):
    guarded_job(workspace, context)
    guarded_cluster(workspace, context["classic_cluster_id"])
    schema_name = context["catalog"] + "." + context["schema"]
    schema = workspace.schemas.get(schema_name)
    if (schema.properties or {}).get("migration_agent_project") != PROJECT:
        raise PermissionError("Planning storage requires the marked project schema")
    volume_name = schema_name + ".plans"
    try:
        volume = workspace.volumes.read(volume_name)
        if volume.comment != PROJECT:
            raise PermissionError("Existing plans volume is not marked for this project")
    except NotFound:
        volume = workspace.volumes.create(context["catalog"], context["schema"], "plans", VolumeType.MANAGED, comment=PROJECT)
    app = workspace.apps.get(context["app_name"])
    if app.creator != workspace.current_user.me().user_name or not (app.description or "").startswith("Read-only migration readiness"):
        raise PermissionError("Not this project's App")
    principal = app.service_principal_client_id
    source_path = context["workspace_root"] + "/files/notebooks"
    if "/.bundle/workspace-migration-agent-jr/dev/files/notebooks" not in source_path:
        raise PermissionError("Invalid project source folder")
    folder = workspace.workspace.get_status(source_path)
    workspace.permissions.update("directories", str(folder.object_id), access_control_list=[AccessControlRequest(service_principal_name=principal, permission_level=PermissionLevel.CAN_READ)])
    # Capture source facts with the operator's existing read rights. Do not grant
    # the App any compute permission: cluster ACLs have no read-only level.
    import io
    actor = workspace.current_user.me().user_name
    evidence = capture_cluster_evidence(workspace, context["classic_cluster_id"], actor, context["job_id"])
    evidence_root = f"/Volumes/{context['catalog']}/{context['schema']}/artifacts/planning_evidence"
    workspace.files.create_directory(evidence_root)
    workspace.files.upload(evidence_root + "/cluster_" + context["classic_cluster_id"] + ".json", io.BytesIO(json.dumps(evidence, sort_keys=True).encode()), overwrite=True)
    details = {"plans_volume": volume.full_name, "source_notebooks": source_path, "app_principal": principal, "source_notebook_permission": "CAN_READ", "classic_metadata_access": "Published operator evidence; no App cluster grant", "job_permission": "CAN_VIEW via App resource", "execution_enabled": False}
    (LOCAL / "planning-resources.json").write_text(json.dumps(details, indent=2), encoding="utf-8")
    print(json.dumps(details, indent=2))


def service(workspace, context):
    volume = f"/Volumes/{context['catalog']}/{context['schema']}/plans"
    artifacts = f"/Volumes/{context['catalog']}/{context['schema']}/artifacts/discovery/latest.json"
    def report():
        result = workspace.files.download(artifacts)
        with result.contents as content:
            return json.load(content)
    def evidence(cluster_id):
        if cluster_id != context["classic_cluster_id"]:
            raise PermissionError("Unknown classic baseline")
        path = f"/Volumes/{context['catalog']}/{context['schema']}/artifacts/planning_evidence/cluster_{cluster_id}.json"
        result = workspace.files.download(path)
        with result.contents as content:
            return json.load(content)
    return PlanningService(workspace, VolumePlanStore(workspace, volume), report, context["job_id"], evidence)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["provision", "generate", "status", "list"])
    parser.add_argument("--plan-id")
    args = parser.parse_args()
    context = json.loads(CONTEXT.read_text(encoding="utf-8"))
    workspace = client(context)
    if args.action == "provision":
        resources(workspace, context)
        return
    planner = service(workspace, context)
    if args.action == "generate":
        report = planner.report_loader()
        actor = workspace.current_user.me().user_name
        plan = planner.generate(actor, "JOB:" + str(context["job_id"]), report["scan_id"], str(uuid.uuid4()))
        (LOCAL / "latest-plan.json").write_text(json.dumps(plan, indent=2), encoding="utf-8")
    elif args.action == "status":
        plan_id = args.plan_id or json.loads((LOCAL / "latest-plan.json").read_text(encoding="utf-8"))["plan_id"]
        plan = planner.get(plan_id)
        (LOCAL / "latest-plan.json").write_text(json.dumps(plan, indent=2), encoding="utf-8")
    else:
        print(json.dumps([{k: p[k] for k in ("plan_id", "status", "freshness", "created_at", "source_name")} for p in planner.list()], indent=2))
        return
    print(json.dumps({k: plan[k] for k in ("plan_id", "status", "freshness", "source_job_id", "plan_hash", "source_fingerprint", "execution_enabled")}, indent=2))
    print(json.dumps({"tasks": len(plan["tasks"]), "changes": len(plan["changes"]), "findings": len(plan["findings"]), "decision": plan["decision"]}, indent=2))


if __name__ == "__main__":
    main()
