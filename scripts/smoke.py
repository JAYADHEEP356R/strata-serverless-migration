"""Shared deployment context and project ownership guards; no sample provisioning."""
import json
import re
from pathlib import Path
from databricks.sdk import WorkspaceClient

ROOT = Path(__file__).resolve().parents[1]
LOCAL = ROOT / ".local"
CONTEXT = LOCAL / "context.json"
PROJECT = "serverless-migration-challenge-jr"
SCHEMA_PREFIX = "migration_agent_jr_"


def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Invalid catalog/schema identifier")
    return value

def save(data):
    LOCAL.mkdir(exist_ok=True)
    temporary = CONTEXT.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, indent=2), encoding="utf-8")
    temporary.replace(CONTEXT)

def client(context):
    w = WorkspaceClient(profile=context["profile"])
    if w.config.workspace_id:
        # The binary Files API does not add account-domain routing headers.
        workspace_id = str(w.config.workspace_id)
        return WorkspaceClient(profile=context["profile"], custom_headers={
            "X-Databricks-Workspace-Id": workspace_id,
            "X-Databricks-Org-Id": workspace_id,
        })
    return w

def guarded_cluster(w, cluster_id):
    cluster = w.clusters.get(cluster_id)
    if cluster.cluster_name != "serverless-migration-classic-baseline" or (cluster.custom_tags or {}).get("Project") != "serverless-migration-challenge":
        raise PermissionError("Refusing to operate on a cluster outside this project's baseline")
    if cluster.creator_user_name != w.current_user.me().user_name:
        raise PermissionError("Baseline creator does not match the authenticated user")
    return cluster

def guarded_job(w, context):
    job = w.jobs.get(context["job_id"])
    if (job.settings.tags or {}).get("Project") != PROJECT:
        raise PermissionError("Refusing to run a job outside the project")
    for task in job.settings.tasks or []:
        if not task.notebook_task or not task.notebook_task.notebook_path.startswith(context["workspace_root"] + "/"):
            raise PermissionError("Refusing to run notebooks outside the new bundle folder")
        if task.existing_cluster_id and task.existing_cluster_id != context["classic_cluster_id"]:
            raise PermissionError("Refusing to use a different existing cluster")
    if job.settings.schedule or job.settings.continuous or job.settings.trigger:
        raise PermissionError("Smoke job must not have automatic triggers")
    return job
