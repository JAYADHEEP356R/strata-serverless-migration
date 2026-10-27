# Release verification — 27 October 2026

- Packaged backend/API checks: **18 passed**, using local fakes, not cloud workload runs.
- Databricks CLI `bundle validate`: passed for the `agent` target with placeholder
  catalog/schema/app variables and an authenticated Azure workspace. No deployment was made
  from the submission directory. This does not prove another workspace meets prerequisites.
- Native agent modules match their source files and the App build manifest.
- Bundle includes exactly one App and two operational jobs: discovery and executor.
- Only the discovery/executor notebooks are included. No sample-job definitions,
  sample-workload notebooks, cluster creation, retained data, runs or saved plans are shipped.
- Package text scanned for the known personal workspace IDs, catalog/cluster IDs, user paths,
  and common token/private-key patterns. No matches were found. Synthetic test identities and
  deliberately fake credential-redaction inputs are local test data, not real credentials.
- Build output is precompiled. React/TypeScript source and npm lockfile are included for
  rebuilding; node_modules and environment-specific browser checks are excluded.

The active Azure app and its discovery/candidate creation were exercised during development.
An independently verified clean-workspace installation and a successful cloud output comparison
through the latest flow are not claimed. The submitter must rehearse and record actual results.

`RELEASE_MANIFEST.json` hashes the release payload. Run `python scripts/verify_release.py`
before editing. A mismatch after intentional edits means the manifest needs regeneration;
it is an integrity record, not a cryptographic signature or independent security assessment.
