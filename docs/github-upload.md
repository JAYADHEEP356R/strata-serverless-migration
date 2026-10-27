# Upload the submission to GitHub

Create an **empty private repository**, without an initial README or .gitignore.
Extract the source archive and run these commands from the extracted repository root:

```powershell
git init
git add .
git diff --cached --stat
git commit -m "Submit Strata migration app and Databricks Asset Bundle"
git branch -M main
git remote add origin https://github.com/YOUR_ACCOUNT/YOUR_PRIVATE_REPOSITORY.git
git push -u origin main
git rev-parse HEAD
```

Keep the repository URL and the full final commit SHA for the submission email. Never put
a personal access token in the remote URL. Use your normal GitHub authentication flow.
The repository root must contain `databricks.yml`, `README.md`, `app/`, `src/`, `notebooks/`
and the remaining release files; do not upload only the ZIP as the repository contents.

The supplied .gitignore excludes generated deployment context, state, environments and
common credential files. Still review your own additions before committing. This package
has no .git history, workspace deployment state or retained run data.

The challenge document asks for Admin access for GitHub users `Nivethan2893`, `diggibyte`
and `veerabadrachari-diggibyte`. Follow the organizer's current instructions and your
organization's repository access policy when granting access. No repository is created,
pushed or shared by preparing this archive.

Add the actual demo-video link to your submission. Complete `docs/submission-summary.md`
and put it in the email body. Repository access, video and submission email are not
completed by this package. The source ZIP is **unencrypted** for extraction/GitHub upload;
use AES-256 encryption separately if choosing the challenge's encrypted-ZIP submission route.
