# Branch Protection

Configure branch protection for `main` to gate merges on reviews and CI.

1) Repository settings → Branches → Add rule

- Branch name pattern: `main`
- Require a pull request before merging: enabled (min 1 approving review)
- Require review from Code Owners: enabled
- Dismiss stale approvals when new commits are pushed: enabled
- Require status checks to pass before merging: enabled
  - Select checks:
    - `CI / dependency_msrv`
    - `CI / check`
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
- Product MSRV and validation run independently. Canic qualification is a
  path-scoped, manually dispatchable workflow; its adapter MSRV, `check` and
  `wasm_size_report` jobs gate composed canister artifact delivery, not product
  library releases. Require those results for affected integration changes or a
  proposed source/dependency pairing; do not configure them as unconditional
  required checks on every core PR. A skipped path-scoped workflow can otherwise
  leave an unrelated PR waiting forever.
- Live inspection on 2026-10-31 through the admin-authorized GitHub CLI found
  `main` unprotected, no repository rulesets and no effective branch rules.
  There was no global `CI / wasm_size_report` requirement to remove, and no
  remote setting was changed. The earlier HTTP 403 applied to the connector's
  administration access. If configuring protection, select the product checks
  above and keep Canic qualification scoped to integration changes.
- After saving, GitHub will surface these required checks on each PR into `main`.
- `.github/CODEOWNERS` requires maintainer review for CI, release, hook, and
  toolchain authority.
- CI runs once for the `main` release commit. The annotated tag does not start
  a duplicate workflow; release artifacts are selected from the main-branch
  run whose commit message begins with `Release `.
