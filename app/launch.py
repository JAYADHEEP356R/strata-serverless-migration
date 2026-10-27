"""Verify bundled rules and serve the project migration application."""
import hashlib
import json
import os
from pathlib import Path
import uvicorn

if __name__ == "__main__":
    package = Path(__file__).resolve().parent / "migration_agent"
    manifest = json.loads((package / "_build_manifest.json").read_text(encoding="utf-8"))
    if set(manifest["files"]) != {p.name for p in package.glob("*.py")}:
        raise RuntimeError("Bundled agent module set does not match the build manifest")
    for name, expected in manifest["files"].items():
        if Path(name).name != name or not name.endswith(".py") or hashlib.sha256((package / name).read_bytes()).hexdigest() != expected:
            raise RuntimeError("Bundled agent source integrity check failed")
    uvicorn.run("app:app", host=os.environ.get("UVICORN_HOST", "0.0.0.0"),
                port=int(os.environ.get("DATABRICKS_APP_PORT", os.environ.get("UVICORN_PORT", "8000"))),
                log_level="info", proxy_headers=True)
