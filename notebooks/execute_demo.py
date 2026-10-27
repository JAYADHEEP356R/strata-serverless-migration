# Databricks notebook source
"""Run only a confirmed, approved, pinned synthetic demo execution request."""
import json
import re
import sys
from databricks.sdk import WorkspaceClient

for name in ("execution_id", "catalog", "schema", "source_root", "source_job_id", "classic_cluster_id", "runner_job_id"):
    dbutils.widgets.text(name, "")
values = {name: dbutils.widgets.get(name) for name in ("execution_id", "catalog", "schema", "source_root", "source_job_id", "classic_cluster_id", "runner_job_id")}
dbutils.widgets.text("baseline_mode", "legacy_copy")
baseline_mode = dbutils.widgets.get("baseline_mode")
source_root = values["source_root"]
if not source_root.startswith("/Workspace/Users/") or "/.bundle/workspace-migration-agent-jr/" not in source_root or not source_root.endswith("/files/src") or "/../" in source_root:
    raise PermissionError("Invalid bundled executor source")
sys.path.insert(0, source_root)
from migration_agent.demo_runner import execute_demo
from migration_agent.plan_store import VolumePlanStore

if not re.fullmatch(r"[A-Za-z0-9_]+", values["catalog"]) or not re.fullmatch(r"migration_agent_jr_[A-Za-z0-9_]+", values["schema"]):
    raise PermissionError("Invalid project output scope")
workspace = WorkspaceClient()
if workspace.config.workspace_id:
    routing = str(workspace.config.workspace_id)
    workspace = WorkspaceClient(custom_headers={"X-Databricks-Workspace-Id": routing, "X-Databricks-Org-Id": routing})
store = VolumePlanStore(workspace, f"/Volumes/{values['catalog']}/{values['schema']}/plans")
if re.fullmatch(r"validate-[a-f0-9]{32}", values["execution_id"]):
    from migration_agent.post_validation import execute_validation
    result = execute_validation(workspace, store, spark, values["execution_id"][9:], values["runner_job_id"])
    print(json.dumps(result))
    if result["state"] != "PASSED": raise RuntimeError("Post-migration validation failed. Inspect recorded checks.")
    dbutils.notebook.exit(json.dumps(result))

if re.fullmatch(r"plan-[a-f0-9]{32}", values["execution_id"]):
    from migration_agent.migration_center import execute_saved_plan
    result = execute_saved_plan(workspace, store, values["execution_id"][5:], values["runner_job_id"])
    print(json.dumps(result))
    if result["state"] != "SUCCEEDED": raise RuntimeError("Some migration items failed. Inspect per-item progress.")
    dbutils.notebook.exit(json.dumps(result))

if re.fullmatch(r"wave-[a-f0-9]{32}", values["execution_id"]):
    from migration_agent.direct_wave import run_wave
    result = run_wave(workspace, store, values["execution_id"][5:], values["runner_job_id"])
    print(json.dumps(result))
    if result["state"] != "COMPLETED": raise RuntimeError("Wave candidate creation requires review.")
    dbutils.notebook.exit(json.dumps(result))

request = store.read("executions", values["execution_id"])
if not request or request.get("runner_job_id") != values["runner_job_id"]:
    raise PermissionError("Execution request does not match this runner")
if baseline_mode == "existing_source_run" and request.get("baseline_mode") != baseline_mode:
    raise PermissionError("Agent-only execution requires a selected original run; creating a classic copy is disabled")
result = execute_demo(workspace, store, spark, values["execution_id"], values["source_job_id"], values["classic_cluster_id"], values["catalog"], values["schema"])
print(json.dumps({"status": result["status"], "promotion_enabled": False}))
if result["status"] != "PASS":
    raise RuntimeError("Demo migration did not validate. Inspect its recorded execution result; promotion remains blocked.")
dbutils.notebook.exit(json.dumps(result))
