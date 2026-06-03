# Branch Protection

Configure branch protection for `main` to gate merges on reviews and CI.

1) Repository settings → Branches → Add rule

- Branch name pattern: `main`
- Require a pull request before merging: enabled (min 1 approving review)
- Dismiss stale approvals when new commits are pushed: enabled
- Require status checks to pass before merging: enabled
  - Select checks:
    - `CI / dependency_msrv`
    - `CI / check`
    - `CI / wasm_size_report`
- Require branches to be up to date before merging: enabled
- Include administrators: enabled (recommended)
- Restrict who can push to matching branches: enabled (optional; maintainers only)
- Require signed commits: enabled (optional, if your org enforces it)
- Do not allow force pushes and deletions: enabled

Protected Tags

- Settings → Tags → Add protection rule
  - Tag name pattern: `v*`
  - Restrict who can create matching tags: maintainers only
  - Disallow updates (prevents rewriting existing tags)
  - Prefer annotated tags (enforced by release process)

Notes

- Status check names correspond to jobs in `.github/workflows/ci.yml`.
- After saving, GitHub will surface these required checks on each PR into `main`.
- Combine with CODEOWNERS if you want specific reviewers to be auto‑required.
