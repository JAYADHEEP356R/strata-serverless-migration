"""Reviewed synthetic 10-job profile, with explicit inputs and dependency mapping."""
import re
PROFILE = "isolated-experiment-orders-v1"
PROJECT_TAG = "serverless-migration-experiment-20261026"
GRAPH = {"ingest": [], "aggregate": ["ingest"], "verify": ["aggregate"]}
HASHES = {'aggregate': '413bdbbba986b300a9ed9f2325d954b4b22a40f099eb6cd8aa602064fe1a63f0', 'ingest': '1f9478cb0d5ab8ce46fd863284c1871f0208a706addddab1deea597598466dd6', 'verify': '04fc6d262d5559ee83363c3b07bb5bd82f7cd3865bdb905484ed9a1a3ab6bd98'}
ROOT_SUFFIX = "/.bundle/migration-experiment-10-20261026/experiment/files/notebooks/"

def plan_matches(plan):
    tasks=plan.get("tasks", [])
    return len(tasks)==3 and {t.get("task_key") for t in tasks}==set(GRAPH) and all(
        t.get("language")=="PYTHON" and t.get("source_compute")=="CLASSIC" and t.get("depends_on")==GRAPH[t["task_key"]]
        and t.get("run_if")=="ALL_SUCCESS" and t.get("source_notebook", "").startswith("/Workspace/Users/")
        and t["source_notebook"].removesuffix(".py").endswith(ROOT_SUFFIX+t["task_key"])
        for t in tasks)

def source_supported(source):
    settings=source.get("settings", {})
    tags=settings.get("tags", {})
    tasks=source.get("tasks", [])
    return (source.get("execution_profile")==PROFILE or tags.get("Project")==PROJECT_TAG and tags.get("Experiment")=="classic-libraries-10") and not any(settings.get(k) for k in ("schedule","trigger","continuous")) and plan_matches(source) and all(t.get("source_hash")==HASHES[t["task_key"]] for t in tasks)

def dependencies(libraries):
    """Only pinned, default-index PyPI requirements can be translated automatically."""
    packages={}
    for library in libraries:
        if set(library)!={"pypi"} or set(library["pypi"])!={"package"}:return None
        package=library["pypi"]["package"]
        match=re.fullmatch(r"([A-Za-z0-9][A-Za-z0-9_.-]*)==([A-Za-z0-9][A-Za-z0-9.!+_-]*)",package)
        if not match:return None
        name=re.sub(r"[-_.]+","-",match[1]).lower()
        if name in packages and packages[name]!=package:return None
        packages[name]=package
    return sorted(packages.values())

def parameters(source):
    if source.get("experiment") is not None:
        return dict(source["experiment"])
    settings=source["settings"]
    values=[t["notebook_task"]["base_parameters"] for t in settings["tasks"]]
    params=values[0]
    if any(p!=params for p in values):raise ValueError("Experiment task parameters differ")
    if not re.fullmatch(r"[A-Za-z0-9_]+",params["catalog"]) or not re.fullmatch(r"migration_agent_jr_exp_[A-Za-z0-9_]+",params["schema"]):raise ValueError("Invalid experiment output namespace")
    if params["run_key"]!="{{job.run_id}}" or not 2<=int(params["groups"])<=20:raise ValueError("Invalid experiment parameters")
    if not set(params["libraries"].split(",")) <= {"none","packaging","dateutil","humanize"}:raise ValueError("Unknown experiment library check")
    return {k:params[k] for k in ("catalog","schema","groups","libraries","scenario")}
