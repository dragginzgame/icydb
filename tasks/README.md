# Repeatable maintenance tasks

Shared Tooling owns this catalog and its reusable instructions. Open this file
from the repository README or run `make tasks`. Task IDs are their filenames
without `.md`; people and agents use the same definitions.

## Catalog

| Task | Question | Suggested cadence |
| --- | --- | --- |
| [cargo-machete](cargo-machete.md) | Which Cargo dependencies appear unused? | Every 3 days |
| [msrv](msrv.md) | Does the advertised minimum really work, and can it be lower? | Every 3 days |
| [rust-toolchain](rust-toolchain.md) | Are development compilers current with stable Rust? | Every 3 days |
| [ci-health](ci-health.md) | Which current CI failures and issues need attention? | Every 3 days |
| [snapshot-drift](snapshot-drift.md) | Are shared snapshots intact and relevant fixes adopted? | Every 3 days |
| [dependency-security](dependency-security.md) | Does the selected dependency graph have new advisories? | Every 3 days |
| [code-audit](code-audit.md) | What concrete correctness or complexity problems exist in one selected area? | Rotate one audit per pass |
| [tooling-duplication](tooling-duplication.md) | Which locally owned tooling could share an existing implementation? | Alternate with code-audit |

The `maintenance` pass runs the first six tasks for the selected repositories,
then alternates the two audit tasks. Bound each audit to one useful area; record
remaining scope rather than claiming a complete estate audit. Cadences are
suggestions until a schedule is enabled. Installing a snapshot enables no timer.

## Run a task

Tell an agent, for example:

> Run task cargo-machete for ic-metrics using shared-tooling/tasks/README.md.

> Run task msrv for canic and icydb; inspect existing CI evidence first.

> Run the maintenance pass for local dragginzgame repositories under
> /home/adam/projects, using Shared Tooling's task catalog. Report findings and
> update the owning issues. Keep repository source unchanged.

Select an explicit repository list or projects directory. For a directory scope,
inventory immediate Git checkouts whose verified GitHub remote owner is
`dragginzgame`, including Shared Tooling; report exclusions and missing roots.
An isolated worktree's parent is not necessarily the projects directory.
Read each repository's `AGENTS.md`, adopted baseline and local command rules.
Use Cargo workspace membership, including independent roots and approved layouts,
rather than assuming all Rust packages live in `crates/`.

## Common run contract

1. Record task/catalog revision, source commits and relevant dirty changes,
   selected repositories, UTC observation time, mode and scope. Pin the method
   for this run. If source changes during inspection, qualify the limitation.
2. Inspect commands and prerequisites before execution. Prefer existing focused
   targets when their effects match this task. Use prepared tools and locked
   dependencies; missing tools, credentials or caches are `BLOCKED`, not an
   implicit installation, fetch, upgrade or a clean result.
3. Central passes keep sibling files unchanged. Source scans and GitHub reads
   can run centrally; compilation and other checks that write in a sibling need
   an invocation explicitly authorizing those validation artifacts there. Use
   matching CI evidence where available, and distinguish it from fresh execution.
   Focused checks do not authorize a broad CI/release gate. Avoid active builds
   and changing inputs; report the conflict and continue independent checks.
4. A check never edits source, manifests, pins or lockfiles, commits, pushes,
   releases, deploys, cleans artifacts or fixes its own findings. Issue work
   follows the [maintenance rules](../rules/agent-maintenance.md): search existing
   issues/PRs, respect active owners, and file or update the owning issue with
   new evidence within `dragginzgame/*`. Issue actions elsewhere on GitHub need
   explicit authorization; otherwise report a prepared proposal for review.
   Avoid duplicate issues and unchanged recurring comments.
5. Preserve command results and failed/incomplete logs. Reuse prior proof only
   when its source, graph, toolchain, scope and assertions still match, linking
   its original run. Freshness tasks must observe current remote state each run;
   an unchanged source tree does not freeze advisories, releases or CI results.
6. Return one row per task/repository with result, source, evidence or log link,
   concrete finding and issue/next action. Use `PASS`, `PASS WITH FINDINGS`,
   `FAIL`, `BLOCKED`, or `N/A` with a reason; a skipped check is never PASS.
   State known failures even if another check is blocked. Keep the summary to
   the most useful changes and decisions, retaining detailed evidence separately.

Task definitions are procedures, not an issue backlog. GitHub issues own active
follow-up. Scheduled reports are immutable run evidence, not another status
ledger. Audit reports follow the [existing audit contract](../audits/README.md);
a central read-only pass returns its report in its run output instead of creating
files in sibling repositories.

## Scheduling and extending

The [local schedule](local-schedule.md) runs the repository-owned
[maintenance prompt](maintenance-prompt.md). It selects paths, cadence and output
location; the task files own the work. A scheduled pass has the same check-only
authority as an interactive pass. Enabling recurring work is explicit.

To add a task, give it one short Markdown file describing its question, inputs,
procedure, expected evidence, limits and completion criteria. Link existing
rules, methods and helpers instead of copying them. Add it to this table and the
governance file list, and check exported links. Add a script only for demonstrated
repeatable mechanics; a task need not become a new Make target or broad gate.
