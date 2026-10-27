"""Read-only discovery agent operations and deployment of its own viewer."""
import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from migration_agent.discovery import collect, PROJECT
from smoke import CONTEXT, LOCAL, client, guarded_job, save


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["assess-local", "scan", "status", "fetch", "app-start", "app-deploy", "app-status", "app-stop"])
    parser.add_argument("--workspace", action="store_true", help="Explicitly expand read-only inspection to visible workspace assets")
    args = parser.parse_args()
    context = json.loads(CONTEXT.read_text(encoding="utf-8"))
    w = client(context)
    if args.action == "assess-local":
        report = collect(w, context["workspace_root"] + "/files", scope="workspace" if args.workspace else "project")
        (LOCAL / "local-assessment.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps({"summary": report["summary"], "coverage": report["coverage"], "errors": report["errors"]}, indent=2))
    elif args.action == "scan":
        job = guarded_job(w, {**context, "job_id": context["scan_job_id"]})
        if (job.settings.tags or {}).get("Purpose") != "discovery-assessment":
            raise PermissionError("Not a project discovery job")
        active = list(w.jobs.list_runs(job_id=context["scan_job_id"], active_only=True))
        if active:
            context["scan_run_id"] = active[0].run_id
            save(context)
            print(json.dumps({"run_id": active[0].run_id, "status": "existing active scan"}))
            return
        run = w.jobs.run_now(context["scan_job_id"], job_parameters={"scope": "workspace" if args.workspace else "project"}).response
        context["scan_run_id"] = run.run_id
        save(context)
        print(json.dumps({"run_id": run.run_id, "job_id": context["scan_job_id"]}))
    elif args.action == "status":
        guarded_job(w, {**context, "job_id": context["scan_job_id"]})
        run = w.jobs.get_run(context["scan_run_id"])
        detail = {"run_id": run.run_id, "run_url": run.run_page_url, "state": run.state.life_cycle_state.value, "result": run.state.result_state.value if run.state.result_state else None, "tasks": []}
        for task in run.tasks or []:
            item = {"task": task.task_key, "state": task.state.life_cycle_state.value, "result": task.state.result_state.value if task.state.result_state else None}
            if item["result"] in {"SUCCESS", "FAILED", "TIMEDOUT"}:
                output = w.jobs.get_run_output(task.run_id)
                if output.error:
                    item["error"] = output.error
                    item["error_trace"] = output.error_trace
                if output.notebook_output and output.notebook_output.result:
                    item["output"] = json.loads(output.notebook_output.result)
            detail["tasks"].append(item)
        (LOCAL / f"scan-run-{run.run_id}.json").write_text(json.dumps(detail, indent=2), encoding="utf-8")
        print(json.dumps(detail, indent=2))
    elif args.action == "fetch":
        path = f"/Volumes/{context['catalog']}/{context['schema']}/artifacts/discovery/latest.json"
        response = w.files.download(path)
        with response.contents as content:
            report = json.load(content)
        (LOCAL / "latest-assessment.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps({"scan_id": report["scan_id"], "summary": report["summary"], "coverage": report["coverage"], "errors": report["errors"]}, indent=2))
    else:
        app = w.apps.get(context["app_name"])
        if app.creator != w.current_user.me().user_name or not (app.description or "").startswith("Read-only migration readiness"):
            raise PermissionError("Refusing to manage an App outside this project")
        if args.action == "app-start":
            w.apps.start(context["app_name"])
            print("Project App start requested; check app-status before app-deploy")
        elif args.action == "app-deploy":
            result = subprocess.run(["databricks", "apps", "deploy", context["app_name"], "--profile", context["profile"], "--source-code-path", context["workspace_root"] + "/files/app", "--mode", "SNAPSHOT", "--no-wait", "--output", "json"], capture_output=True, text=True)
            print(result.stdout)
            if result.stderr:
                print(result.stderr, file=sys.stderr)
            if result.returncode:
                raise RuntimeError("Project App deployment failed")
        elif args.action == "app-stop":
            w.apps.stop(context["app_name"])
            print("Project App stop requested")
        else:
            fields = app.as_dict()
            status = {k: fields.get(k) for k in ("name", "url", "compute_status", "app_status", "active_deployment", "pending_deployment")}
            (LOCAL / "app-status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
            context["app_url"] = app.url
            save(context)
            print(json.dumps(status, indent=2))


if __name__ == "__main__":
    main()
