"""Explicit demo execution requests. No arbitrary workload/job execution API."""
import re
import posixpath
import uuid
from .workflow import PROJECT, WorkflowError, timestamp
from .plan_store import ID
from .experiment_profile import PROFILE as EXPERIMENT_PROFILE, plan_matches as experiment_matches
from .proof_profile import BATCH_PROFILE_ID, batch_plan_matches
from .source_baseline import EXISTING_BASELINE, source_run

RUN_ACKS = {"compute_cost", "demo_outputs", "no_cutover"}


def require_demo(plan, allowed_job_id):
    root = plan.get("scope_root", "")
    if not root.startswith("/Workspace/Users/") or "/.bundle/workspace-migration-agent-jr/" not in root or not root.endswith("/files") or posixpath.normpath(root) != root:
        raise WorkflowError("DEMO_ONLY", "Execution currently supports only the isolated project demo.", 403)
    if plan.get("execution_profile") == EXPERIMENT_PROFILE and experiment_matches(plan):
        return root
    if plan.get("execution_profile") == BATCH_PROFILE_ID and re.fullmatch(r"[0-9]+", str(plan.get("source_job_id", ""))) and batch_plan_matches(plan):
        return root
    if plan.get("source_job_id") != str(allowed_job_id):
        raise WorkflowError("DEMO_ONLY", "This job needs its own supported output-isolation and validation profile.", 403)
    expected = {"prepare": "prepare", "classic": "workload", "serverless": "workload", "validate": "validate"}
    tasks = plan.get("tasks", [])
    if len(tasks) != 4 or {t.get("task_key") for t in tasks} != set(expected):
        raise WorkflowError("DEMO_ONLY", "The selected plan is not the supported four-task demo.", 403)
    for task in tasks:
        path = task.get("source_notebook", "").removesuffix(".py")
        if path != root + "/notebooks/" + expected[task["task_key"]]:
            raise WorkflowError("DEMO_ONLY", "Demo execution cannot use unrelated notebooks.", 403)
    return root


def guarded_runner(workspace, runner_id, root, actor):
    job = workspace.jobs.get(int(runner_id))
    settings = job.settings
    tags = settings.tags or {}
    tasks = settings.tasks or []
    if tags.get("Project") != PROJECT or tags.get("Purpose") != "demo-migration-execution" or (job.creator_user_name or "").casefold() != actor.casefold():
        raise WorkflowError("INVALID_EXECUTOR", "The configured job is not the project demo executor.", 403)
    if len(tasks) != 1 or tasks[0].task_key != "execute_demo" or not tasks[0].notebook_task or tasks[0].notebook_task.notebook_path.removesuffix(".py") != root + "/notebooks/execute_demo" or any(getattr(tasks[0], k, None) for k in ("existing_cluster_id", "new_cluster", "job_cluster_key")) or settings.schedule or settings.trigger or settings.continuous:
        raise WorkflowError("INVALID_EXECUTOR", "Execution must use the dedicated manually triggered serverless runner.", 403)


def evidence_links(workspace, stages):
    config = getattr(workspace, "config", None)
    host = (getattr(config, "host", "") or "").rstrip("/")
    if not re.fullmatch(r"https://[A-Za-z0-9.-]+\.(?:databricks\.com|azuredatabricks\.net)", host):
        return {}
    workspace_id = str(getattr(config, "workspace_id", "") or "")
    root = host + "/" + ("?o=" + workspace_id if workspace_id.isdigit() else "")
    links = {}
    for role in ("baseline", "candidate"):
        binding = stages.get(role) or {}
        job_id, run_id = str(binding.get("job_id", "")), str(binding.get("run_id", ""))
        if job_id.isdigit() and run_id.isdigit():
            links[role + "_job"] = root + "#job/" + job_id
            links[role + "_run"] = root + "#job/" + job_id + "/run/" + run_id
    return links


class ExecutionService:
    def __init__(self, workspace, planner, runner_id, baseline_mode="legacy_copy", output_scope=None):
        self.workspace, self.planner, self.runner_id = workspace, planner, int(runner_id)
        self.store = planner.store
        self.baseline_mode, self.output_scope = baseline_mode, output_scope

    def start(self, actor, plan_id, plan_hash, source_fingerprint, request_id, acknowledgements, baseline_run_id=None):
        if not self.planner.policy(actor)["can_decide"]:
            raise WorkflowError("NOT_AUTHORIZED", "Only the project operator may request a demo execution.", 403)
        if set(acknowledgements) != RUN_ACKS:
            raise WorkflowError("EXECUTION_ACKNOWLEDGEMENTS", "Confirm compute costs, isolated demo outputs, and no automatic cutover.", 400)
        plan = self.planner.get(plan_id)
        root = require_demo(plan, self.planner.allowed_job_id)
        if plan["status"] != "APPROVED" or plan["freshness"] != "CURRENT" or not plan.get("decision") or plan["decision"]["actor"].casefold() != actor.casefold():
            raise WorkflowError("APPROVED_PLAN_REQUIRED", "Review and approve a current demo plan before requesting execution.")
        if plan_hash != plan["plan_hash"] or source_fingerprint != plan["source_fingerprint"]:
            raise WorkflowError("PLAN_CHANGED", "The execution request does not match the reviewed plan.")
        try:
            request_id = str(uuid.UUID(request_id))
        except (ValueError, AttributeError):
            raise WorkflowError("INVALID_REQUEST_ID", "A UUID execution request is required.", 400) from None
        # A new plan must not silently rerun a source version that already validated.
        # Same-plan retries remain idempotent and return their existing execution.
        from .fleet import FleetService
        from .workflow import verified_plan
        history_ids = self.store.list_ids(limit=1001)
        if len(history_ids) >= 1001:
            raise WorkflowError("HISTORY_LIMIT", "Migration history coverage must be resolved before another execution can be authorized.")
        for prior_id in history_ids:
            if prior_id == plan_id:
                continue
            prior = verified_plan(self.store.read("plans", prior_id))
            if prior.get("source_job_id") != plan["source_job_id"] or prior.get("source_fingerprint") != plan["source_fingerprint"] or prior.get("execution_profile") != plan.get("execution_profile"):
                continue
            claim = self.store.read("execution_claims", prior_id)
            if not claim:
                continue
            previous = self.planner.get(prior_id, check_source=False)
            trial = FleetService(self.planner, runner_id=self.runner_id)._trial(previous, claim)
            if trial.get("validation_status") == "PASS":
                raise WorkflowError("ALREADY_VALIDATED", "This job version already has a validated serverless candidate. Open its saved validation instead of migrating it again.")
        guarded_runner(self.workspace, self.runner_id, root, actor)
        baseline = None
        if self.baseline_mode == EXISTING_BASELINE:
            if not self.output_scope:
                raise WorkflowError("BASELINE_SCOPE_REQUIRED", "The project's original-output validation scope is not configured.")
            if plan.get("execution_profile") == EXPERIMENT_PROFILE:
                from .experiment_runner import source_run as experiment_run
                baseline = experiment_run(self.workspace, plan, baseline_run_id)
            else:
                baseline = source_run(self.workspace, plan, baseline_run_id, *self.output_scope)
        execution_id = uuid.uuid5(uuid.NAMESPACE_URL, plan_id + ":" + request_id).hex
        # First request wins for this approved plan. Retries return the same run.
        claim = {"execution_id": execution_id, "plan_id": plan_id}
        if not self.store.write_once("execution_claims", plan_id, claim):
            execution_id = self.store.read("execution_claims", plan_id)["execution_id"]
        record = {"execution_id": execution_id, "plan_id": plan_id, "plan_hash": plan_hash, "source_fingerprint": source_fingerprint, "actor": actor, "requested_at": timestamp(), "acknowledgements": sorted(RUN_ACKS), "profile": plan.get("execution_profile") or "isolated-demo-v1", "runner_job_id": str(self.runner_id), "promotion_enabled": False}
        if baseline:
            record.update(baseline_mode=EXISTING_BASELINE, source_baseline=baseline)
        self.store.write_once("executions", execution_id, record)
        saved = self.store.read("executions", execution_id)
        if saved["plan_id"] != plan_id or saved["plan_hash"] != plan_hash or saved["actor"].casefold() != actor.casefold():
            raise WorkflowError("EXECUTION_CONFLICT", "The existing execution request does not match this plan.")
        if baseline and saved.get("source_baseline") != baseline:
            raise WorkflowError("BASELINE_CONFLICT", "This plan already has an execution tied to another original run.")
        if not self.store.read("execution_launches", execution_id):
            run = self.workspace.jobs.run_now(job_id=self.runner_id, job_parameters={"execution_id": execution_id}, idempotency_token="migration-" + execution_id).response
            self.store.write_once("execution_launches", execution_id, {"run_id": str(run.run_id), "runner_job_id": str(self.runner_id)})
        return self.get(actor, plan_id)

    def get(self, actor, plan_id):
        if not ID.fullmatch(plan_id):
            raise WorkflowError("INVALID_PLAN_ID", "Invalid plan reference.", 400)
        self.planner.get(plan_id, check_source=False)
        claim = self.store.read("execution_claims", plan_id)
        if not claim:
            return {"execution": None, "profile": "isolated-demo-v1", "baseline_mode": self.baseline_mode, "promotion_enabled": False}
        execution_id = claim["execution_id"]
        record = self.store.read("executions", execution_id)
        if not record:
            return {"execution": {"execution_id": execution_id, "state": "REQUESTING", "promotion_enabled": False}}
        stages = {name: self.store.read("execution_" + name, execution_id) for name in ("preflight", "jobs", "baseline", "candidate", "baseline_done", "candidate_done", "results", "progress", "diagnostics")}
        if not stages["jobs"]:
            stages["jobs"] = {role: self.store.read("execution_" + role + "_job", execution_id) for role in ("baseline", "candidate")}
        result = stages["results"]
        state = result["status"] if result else "REQUESTED"
        launch = self.store.read("execution_launches", execution_id)
        if launch and not result:
            run = self.workspace.jobs.get_run(int(launch["run_id"]))
            if run.job_id != self.runner_id:
                raise WorkflowError("INVALID_EXECUTION_RUN", "The execution run is outside the dedicated runner.", 403)
            terminal = run.state.result_state.value if run.state.result_state else None
            state = "NEEDS_REVIEW" if terminal else run.state.life_cycle_state.value
        return {"execution": {**record, "state": state, "stages": stages, "links": evidence_links(self.workspace, stages), "runner_run_id": launch["run_id"] if launch else None, "promotion_enabled": False}}

    def baseline_runs(self, actor, plan_id):
        if not self.planner.policy(actor)["can_decide"]:
            raise WorkflowError("NOT_AUTHORIZED", "Only the project operator may select original runs.", 403)
        plan = self.planner.get(plan_id)
        require_demo(plan, self.planner.allowed_job_id)
        runs = []
        for run in self.workspace.jobs.list_runs(job_id=int(plan["source_job_id"]), limit=20):
            if run.state and run.state.result_state and run.state.result_state.value == "SUCCESS":
                runs.append({"run_id": str(run.run_id), "start_time": run.start_time, "run_url": run.run_page_url})
            if len(runs) >= 10:
                break
        return {"runs": runs, "baseline_mode": self.baseline_mode, "original_job_will_be_run": False}
