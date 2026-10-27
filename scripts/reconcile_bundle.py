"""Prepare or bind the exact validated candidate to its Asset Bundle; never run or promote it."""
import argparse
import base64
import json
import subprocess
import sys
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from smoke import CONTEXT, LOCAL, client
from plans import service
from migration_agent.reconciliation import inspected_context
from migration_agent.workflow import digest, timestamp
from migration_agent.demo_runner import verify_definition
from databricks.sdk.service.workspace import ExportFormat


def bundle_command(context, directory, args):
    command = ["databricks", "bundle", *args, "--profile", context["profile"], "--target", "validated"]
    result = subprocess.run(command, cwd=directory, text=True, capture_output=True)
    (directory / ("cli-" + args[0] + ".log")).write_text(result.stdout + result.stderr, encoding="utf-8")
    if result.returncode:
        raise RuntimeError("Bundle command failed; inspect " + str(directory / ("cli-" + args[0] + ".log")))
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "bind"])
    parser.add_argument("--plan-id")
    args = parser.parse_args()
    context = json.loads(CONTEXT.read_text(encoding="utf-8"))
    workspace = client(context)
    planner = service(workspace, context)
    planner.workspace_planning = True
    plan_id = args.plan_id or json.loads((LOCAL / "demo-execution-plan.json").read_text(encoding="utf-8"))["plan_id"]
    claim = planner.store.read("execution_claims", plan_id)
    if not claim:
        raise RuntimeError("This plan has no execution")
    execution_id = claim["execution_id"]
    actor = workspace.current_user.me().user_name
    inspected = inspected_context(workspace, planner.store, execution_id, context["job_id"], actor)
    bundle_name = "migration-demo-validated-jr-" + execution_id[:12]
    directory = ROOT / "build" / bundle_name
    directory.mkdir(parents=True, exist_ok=True)
    root_path = "/Workspace/Users/" + actor + "/.bundle/" + bundle_name + "/validated"
    config = {"bundle": {"name": bundle_name}, "sync": {"exclude": ["snapshots/**", "*.log", "README.md"]}, "resources": {"jobs": {"active_demo": inspected["definition"]}}, "targets": {"validated": {"default": True, "mode": "production", "workspace": {"host": workspace.config.host, "root_path": root_path}, "run_as": {"user_name": actor}}}}
    serialized = yaml.safe_dump(config, sort_keys=False)
    config_path = directory / "databricks.yml"
    if config_path.exists() and config_path.read_text(encoding="utf-8") != serialized:
        raise RuntimeError("Prepared bundle differs. Inspect it instead of overwriting an existing binding.")
    config_path.write_text(serialized, encoding="utf-8")
    snapshots = directory / "snapshots"
    snapshots.mkdir(exist_ok=True)
    for task in inspected["definition"]["tasks"]:
        source = workspace.workspace.export(task["notebook_task"]["notebook_path"], format=ExportFormat.SOURCE)
        (snapshots / (task["task_key"] + ".py")).write_bytes(base64.b64decode(source.content))
    readme = """# Validated manual demo candidate

This bundle adopts the already tested candidate by resource binding. Binding does
not deploy configuration and does not change workload execution or schedules.
Migration validation records stay separate from bundle reconciliation.

The definition references the exact tested, isolated workspace notebooks. Source
snapshots are included for review and excluded from sync. This bundle is specific
to this workspace; it does not provision storage or relocate notebooks. The main
project bundle provisions those prerequisites. Future deployment requires review
and new validation if executable configuration or code changes.

No automatic triggers are configured. The original classic job stays available
for rollback. A production schedule cutover and data rollback are not implemented.
"""
    (directory / "README.md").write_text(readme, encoding="utf-8")
    bundle_command(context, directory, ["validate", "--output", "json"])
    if args.action == "bind":
        receipt = planner.store.read("bundle_reconciliations", execution_id) or planner.store.read("promotion_bundles", execution_id)
        if not receipt:
            before = workspace.jobs.get(int(inspected["candidate"]["job_id"])).settings.as_dict()
            bundle_command(context, directory, ["deployment", "bind", "active_demo", inspected["candidate"]["job_id"], "--auto-approve"])
            after = workspace.jobs.get(int(inspected["candidate"]["job_id"]))
            verify_definition(after, inspected["definition"], actor)
            if after.settings.as_dict() != before:
                raise RuntimeError("Candidate settings changed during binding. Review before promotion.")
            state = json.loads(bundle_command(context, directory, ["summary", "--output", "json"]))
            if str(state["resources"]["jobs"]["active_demo"]["id"]) != inspected["candidate"]["job_id"]:
                raise RuntimeError("Bundle did not bind the expected candidate")
            receipt = {"status": "BOUND_NO_DEPLOY", "execution_id": execution_id, "candidate_job_id": inspected["candidate"]["job_id"], "definition_hash": digest(inspected["definition"]), "evidence_hash": inspected["evidence_hash"], "bundle_name": bundle_name, "bundle_target": "validated", "workspace_root": root_path, "local_path": str(directory), "bound_at": timestamp(), "bound_by": actor, "config_hash": digest(config), "scope": "Operator verified binding; no deployment performed. Future bundle changes require review."}
            receipt["receipt_hash"] = digest(receipt)
            planner.store.write_once("bundle_reconciliations", execution_id, receipt)
        (LOCAL / "reconciliation-bundle-receipt.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    print(json.dumps({"bundle": str(directory), "candidate_job_id": inspected["candidate"]["job_id"], "status": "BOUND_NO_DEPLOY" if args.action == "bind" else "PREPARED", "approved": False, "workload_started": False}, indent=2))


if __name__ == "__main__":
    main()
