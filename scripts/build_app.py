"""Build the interface and bundle the exact tested agent rules as native modules."""
import os
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def build():
    npm = shutil.which("npm.cmd" if os.name == "nt" else "npm")
    if not npm:
        raise RuntimeError("Install Node.js and npm before building the application")
    subprocess.run([npm, "run", "build"], cwd=ROOT / "frontend", check=True)
    source = ROOT / "src" / "migration_agent"
    destination = ROOT / "app" / "migration_agent"
    destination.mkdir(exist_ok=True)
    expected_modules = {path.name for path in source.glob("*.py")}
    for stale in destination.glob("*.py"):
        if stale.name not in expected_modules:
            stale.unlink()
    manifest = {"version": "0.3.0", "files": {}}
    for path in sorted(source.glob("*.py")):
        data = path.read_bytes()
        (destination / path.name).write_bytes(data)
        manifest["files"][path.name] = hashlib.sha256(data).hexdigest()
    (destination / "_build_manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2), encoding="utf-8")
    print("Built React and bundled agent modules with source integrity hashes. Synchronize the bundle before deployment.")


if __name__ == "__main__":
    build()
