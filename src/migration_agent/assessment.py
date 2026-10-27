"""Conservative first-pass findings; rule detection is not runtime certification."""
import ast
import hashlib
import json
import re
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Finding:
    limitation: str
    rule: str
    severity: str
    evidence: str
    recommendation: str
    location: str = "configuration"


@dataclass(frozen=True)
class Assessment:
    source_fingerprint: str
    readiness_score: int
    eligible: bool
    findings: tuple[Finding, ...]
    scope: str = "Static candidate assessment; execution and output validation required"

    def as_dict(self):
        return asdict(self)


def fingerprint(config: dict, source: str) -> str:
    payload = json.dumps({"config": config, "source": source}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def assess(config: dict, source: str, language: str = "PYTHON") -> Assessment:
    findings = []

    def add(limitation, rule, evidence, recommendation, severity="BLOCKER", location="configuration"):
        findings.append(Finding(limitation, rule, severity, evidence, recommendation, location))

    runtime = config.get("spark_version", "")
    if config.get("docker_image"):
        add("L4", "custom-image", "Custom container configured", "Replace required image dependencies with a supported environment.")
    if runtime and ("ml" in runtime.lower() or re.match(r"^(?:[0-9]|1[0-2])\.", runtime)):
        add("L4", "runtime-review", runtime, "Map dependencies and use a supported non-ML environment.")
    if config.get("aws_attributes", {}).get("instance_profile_arn"):
        add("L4", "instance-profile", "Instance profile configured", "Use an approved Unity Catalog storage access path.")
    libraries = config.get("libraries", [])
    for lib in libraries:
        if any(kind in lib for kind in ("egg", "jar", "maven")):
            add("L5", "dependency-review", next(k for k in ("egg", "jar", "maven") if k in lib), "Review task-specific support and explicit dependency replacements.")
    if language.upper() in {"SCALA", "R"} or re.search(r"(?m)^\s*(?:# MAGIC )?%(?:scala|r)\b", source):
        add("L7", "notebook-language", "Scala or R notebook code", "Rewrite the notebook or keep it on classic; evaluate supported JAR tasks separately.")
    for script in config.get("init_scripts", []):
        if "s3" in script or "dbfs" in script:
            add("L3", "init-script-storage", "S3 or DBFS init script", "Retrieve through approved access and translate supported setup into an environment specification.")
        else:
            add("L3", "init-script-review", "Compute-scoped initialization", "Review initialization and translate required dependencies.", "REVIEW")
    if config.get("cluster_log_conf") or config.get("ssh_public_keys"):
        add("L11", "classic-capabilities", "Cluster log delivery or SSH keys configured", "Document serverless logging/access replacements.", "REVIEW")
    if config.get("source_managed"):
        add("L9", "source-reconciliation", "Workload managed by Git or an Asset Bundle", "Reconcile migration changes with the source before promotion.", "WARNING")
    migratable_tasks = config.get("classic_task_count", 0)
    if migratable_tasks > 10:
        add("L2", "agent-task-limit", f"{migratable_tasks} classic tasks", "Use a dependency-preserving independent planner; this is the migration agent's limit, not a serverless Jobs task limit.", "REVIEW")
    code_parts = []
    parsed_calls = []
    sql_cells = []
    # Decode exported Databricks source cells; never execute notebook content.
    for cell_index, cell in enumerate(re.split(r"(?m)^# COMMAND ----------\s*$", source), 1):
        clean = re.sub(r"(?m)^# MAGIC ?", "", cell)
        stripped = clean.lstrip()
        location = f"cell {cell_index}"
        if language.upper() == "SQL" or stripped.startswith("%sql"):
            sql_cells.append((location, re.sub(r"^%sql\s*", "", stripped)))
            continue
        if language.upper() != "PYTHON" or stripped.startswith(("%scala", "%r")):
            continue
        if stripped.startswith("%"):
            add("L6", "notebook-magic-review", "Non-Python notebook magic", "Review environment or external command dependencies.", "REVIEW", location)
            continue
        try:
            tree = ast.parse(clean)
            calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]
            parsed_calls.extend((location, n) for n in calls)
            code_parts.extend(ast.unparse(n.func) for n in calls)
            for call in calls:
                if isinstance(call.func, ast.Attribute) and call.func.attr == "sql" and call.args and isinstance(call.args[0], ast.Constant) and isinstance(call.args[0].value, str):
                    sql_cells.append((f"{location}, line {call.lineno}", call.args[0].value))
        except SyntaxError:
            add("L6", "unparsed-source", "Cell could not be parsed", "Inspect this cell before migration; compatibility is unknown.", "REVIEW", location)
    code = "\n".join(code_parts)
    patterns = {
        "checkpoint": r"\.checkpoint$",
        "global-temp-view": r"\.(?:createGlobalTempView|createOrReplaceGlobalTempView)$",
        "mount-management": r"dbutils\.fs\.(?:mount|unmount|updateMount|refreshMounts)$",
        "catalog-cache-api": r"spark\.catalog\.(?:cacheTable|uncacheTable|clearCache)$",
    }
    for rule, pattern in patterns.items():
        if re.search(pattern, code, re.MULTILINE):
            locations = [f"{loc}, line {call.lineno}" for loc, call in parsed_calls if re.search(pattern, ast.unparse(call.func))]
            add("L6", rule, rule, "Use an explicitly reviewed compatible alternative and validate behavior.", location=", ".join(locations))
    for location, call in parsed_calls:
        expression = ast.unparse(call)
        if isinstance(call.func, ast.Attribute) and call.func.attr in {"start", "toTable"} and ".writeStream" in expression:
            triggers = [n for n in ast.walk(call) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "trigger"]
            supported = any(k.arg in {"availableNow", "once"} and isinstance(k.value, ast.Constant) and k.value.value is True for t in triggers for k in t.keywords)
            if not supported:
                add("L13", "streaming-trigger", "Streaming writer has no explicit supported finite trigger", "Review availableNow or a suitable pipeline mode; validate delivery semantics.", "REVIEW", f"{location}, line {call.lineno}")
        if isinstance(call.func, ast.Attribute) and call.func.attr == "mode" and call.args and isinstance(call.args[0], ast.Constant) and call.args[0].value == "errorifexists":
            add("L6", "connect-save-mode-alias", "errorifexists writer alias", "Use canonical error mode; the tested serverless environment rejects this alias.", "REVIEW", f"{location}, line {call.lineno}")
    for location, sql in sql_cells:
        sql = re.sub(r"--[^\n]*|/\*.*?\*/", " ", sql, flags=re.DOTALL)
        if re.search(r"\b(?:CACHE\s+TABLE|UNCACHE\s+TABLE|REFRESH\s+TABLE|CLEAR\s+CACHE)\b", sql, re.I):
            add("L6", "sql-cache", "Unsupported SQL cache command", "Use a reviewed session-local or persisted-table alternative.", location=location)
        if re.search(r"(?<![\w])CAST\s*\(", sql, re.I):
            add("L13", "ansi-cast-review", "SQL CAST requires ANSI behavior review", "Consider TRY_CAST only if NULL-on-error is intended; test invalid inputs.", "REVIEW", location)
    if config.get("target_environment_version") and int(config["target_environment_version"]) < 5 and re.search(r"\.(?:cache|persist)$", code, re.M):
        add("L6", "cache-environment", "DataFrame caching requires a newer target environment", "Select environment version 5+ and confirm the workspace caching feature is enabled.", "REVIEW")
    penalty = sum(25 if f.severity == "BLOCKER" else 10 if f.severity == "REVIEW" else 3 for f in findings)
    eligible = not any(f.severity in {"BLOCKER", "REVIEW"} for f in findings)
    return Assessment(fingerprint(config, source), max(0, 100 - penalty), eligible, tuple(findings))
