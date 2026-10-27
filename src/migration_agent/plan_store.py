"""Project-only, write-once plan documents and a first-decision-wins journal."""
import io
import json
import re
import threading
from pathlib import Path

from databricks.sdk.errors import AlreadyExists, NotFound, ResourceAlreadyExists

ID = re.compile(r"^[a-f0-9]{32}$")
MAX_RECORD = 2 * 1024 * 1024
EXECUTION_KINDS = {"executions", "execution_claims", "execution_launches", "execution_started", "execution_preflight", "execution_jobs", "execution_baseline_job", "execution_candidate_job", "execution_baseline", "execution_candidate", "execution_baseline_done", "execution_candidate_done", "execution_results"}
EXECUTION_KINDS |= {"execution_progress", "execution_diagnostics", "execution_candidate_bundle"}
EXECUTION_KINDS |= {"promotion_previews", "promotion_requests", "promotion_launches", "promotion_results", "promotion_claims", "promotion_started", "promotion_bundles", "promotion_state", "promotion_latest"}


# Legacy promotion records are retained for historical audit reads.
EXECUTION_KINDS |= {"bundle_reconciliations", "waves", "direct_wave_requests", "direct_wave_launches", "direct_wave_started", "direct_wave_results", "direct_wave_progress", "direct_candidate_claims", "direct_candidate_results"}

EXECUTION_KINDS |= {"migration_definitions", "migration_requests", "migration_launches", "migration_started", "migration_progress", "migration_results", "migration_source_claims", "migration_candidates", "migration_archives"}

EXECUTION_KINDS |= {"validation_requests", "validation_launches", "validation_started", "validation_progress", "validation_results", "validation_candidate_run", "activity_events"}

def record_path(kind, plan_id):
    if kind not in {"plans", "decisions"} | EXECUTION_KINDS or not ID.fullmatch(plan_id):
        raise ValueError("Invalid planning record reference")
    return f"{kind}/{plan_id}.json"


class LocalPlanStore:
    """Explicit local development/test storage; never selected in Databricks."""
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()

    def read(self, kind, plan_id):
        with self.lock:
            path = self.root / record_path(kind, plan_id)
            if not path.exists():
                return None
            if path.stat().st_size > MAX_RECORD:
                raise ValueError("Planning record exceeds limit")
            return json.loads(path.read_text(encoding="utf-8"))

    def write_once(self, kind, plan_id, document):
        data = json.dumps(document, sort_keys=True, ensure_ascii=True)
        if len(data.encode()) > MAX_RECORD:
            raise ValueError("Planning record exceeds limit")
        with self.lock:
            path = self.root / record_path(kind, plan_id)
            path.parent.mkdir(exist_ok=True)
            try:
                with path.open("x", encoding="utf-8") as handle:
                    handle.write(data)
                return True
            except FileExistsError:
                return False

    def list_ids(self, limit=100):
        return self.list_record_ids("plans", limit)

    def list_record_ids(self, kind, limit=100):
        if kind not in {"plans", "waves", "migration_definitions", "validation_requests", "activity_events"}:
            raise ValueError("Only plan/wave collections may be listed")
        directory = self.root / kind
        with self.lock:
            return sorted((p.stem for p in directory.glob("*.json") if ID.fullmatch(p.stem)), reverse=True)[:limit]

    def write_progress(self, execution_id, document):
        self.write_pointer("execution_progress", execution_id, document)

    def write_pointer(self, kind, reference, document):
        if kind not in {"execution_progress", "direct_wave_progress", "migration_progress", "validation_progress"}:
            raise ValueError("Only dedicated progress/state pointers may be updated")
        data = json.dumps(document, sort_keys=True, ensure_ascii=True)
        if len(data.encode()) > MAX_RECORD:
            raise ValueError("Progress record exceeds limit")
        with self.lock:
            path = self.root / record_path(kind, reference)
            path.parent.mkdir(exist_ok=True)
            temporary = path.with_suffix(".tmp")
            temporary.write_text(data, encoding="utf-8")
            temporary.replace(path)


class VolumePlanStore:
    def __init__(self, workspace, volume):
        if not re.fullmatch(r"/Volumes/[A-Za-z0-9_-]+/migration_agent_jr_[A-Za-z0-9_]+/plans/?", volume):
            raise ValueError("Planning writes require the dedicated project plans volume")
        self.workspace = workspace
        self.volume = volume.rstrip("/")

    def read(self, kind, plan_id):
        path = self.volume + "/" + record_path(kind, plan_id)
        try:
            response = self.workspace.files.download(path)
            with response.contents as content:
                data = content.read(MAX_RECORD + 1)
            if len(data) > MAX_RECORD:
                raise ValueError("Planning record exceeds limit")
            return json.loads(data)
        except NotFound:
            return None

    def write_once(self, kind, plan_id, document):
        path = self.volume + "/" + record_path(kind, plan_id)
        data = json.dumps(document, sort_keys=True, ensure_ascii=True).encode()
        if len(data) > MAX_RECORD:
            raise ValueError("Planning record exceeds limit")
        self.workspace.files.create_directory(self.volume + "/" + kind)
        try:
            self.workspace.files.upload(path, io.BytesIO(data), overwrite=False)
            return True
        except (AlreadyExists, ResourceAlreadyExists):
            return False

    def list_ids(self, limit=100):
        return self.list_record_ids("plans", limit)

    def list_record_ids(self, kind, limit=100):
        if kind not in {"plans", "waves", "migration_definitions", "validation_requests", "activity_events"}:
            raise ValueError("Only plan/wave collections may be listed")
        try:
            result = []
            for entry in self.workspace.files.list_directory_contents(self.volume + "/" + kind):
                name = entry.path.rsplit("/", 1)[-1] if entry.path else ""
                if name.endswith(".json") and ID.fullmatch(name[:-5]):
                    result.append(name[:-5])
                if len(result) >= limit:
                    break
            return result
        except NotFound:
            return []

    def write_progress(self, execution_id, document):
        self.write_pointer("execution_progress", execution_id, document)

    def write_pointer(self, kind, reference, document):
        if kind not in {"execution_progress", "direct_wave_progress", "migration_progress", "validation_progress"}:
            raise ValueError("Only dedicated progress/state pointers may be updated")
        path = self.volume + "/" + record_path(kind, reference)
        data = json.dumps(document, sort_keys=True, ensure_ascii=True).encode()
        if len(data) > MAX_RECORD:
            raise ValueError("Progress record exceeds limit")
        self.workspace.files.create_directory(self.volume + "/" + kind)
        self.workspace.files.upload(path, io.BytesIO(data), overwrite=True)
