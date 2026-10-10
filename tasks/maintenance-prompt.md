# Scheduled maintenance pass

Read the selected Shared Tooling checkout's AGENTS.md, DRAGGINZGAME.md and
tasks/README.md first. Run the maintenance pass defined by that catalog for the
explicit projects directory supplied with this prompt. Inventory the eligible
repositories and read their local instructions. Record the catalog/source
identities, dirty inputs and observation time.

This is inspection and owning-repository issue coordination. Keep repository
source, manifests, locks, pins and Git state unchanged, including Shared Tooling.
Use source scans, prepared read-only checks and matching CI evidence. Do not run
write-producing sibling checks, install tools, fetch crate dependencies, change
compiler versions, run broad gates, commit, push, deploy or clean artifacts.
Current GitHub, Rust release and advisory-feed reads are part of these checks.
Write new evidence only under the supplied run directory. Retain failed evidence.

Run the seven routine tasks, including advisory README freshness, then one
bounded audit. README findings never block releases or trigger prose rewrites.
Alternate code-audit and tooling-duplication using the last completed report
when available; start with
tooling-duplication when there is no history. Choose the least recently reviewed
relevant scope from retained reports; do not maintain another work queue.
Work sequentially, avoid active builds, and report unavailable evidence rather
than waiting indefinitely or treating a missing check as success.

Search issues and PRs before filing. Verify the GitHub owner before writing:
create or update owning issues in `dragginzgame/*` for actionable new evidence.
Issue actions in other GitHub repositories require explicit authorization for
the destination and action; otherwise include the prepared proposal in the run
report without posting. Respect other sessions' ownership and avoid unchanged
duplicate comments. Keep proposed source repairs with their owners.

Return a concise report with per-task/repository results and evidence, the most
useful findings and issue links, blockers, audit scope, and remaining unreviewed
scope. Use the common result vocabulary. Do not claim fresh compilation when
only source or existing CI was inspected. Finish within this pass; no polling,
new schedules or self-modification.
