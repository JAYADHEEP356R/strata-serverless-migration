"""Verify the release payload against its SHA-256 manifest without network access."""
import hashlib,json
from pathlib import Path

root=Path(__file__).resolve().parents[1]
manifest=json.loads((root/'RELEASE_MANIFEST.json').read_text(encoding='utf-8'))
errors=[]
for name,expected in manifest['files'].items():
    p=root/name
    if not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest()!=expected:
        errors.append(name)
if errors:
    raise SystemExit('Changed or missing release files: '+', '.join(errors))
print(str(len(manifest['files']))+' release files verified.')
