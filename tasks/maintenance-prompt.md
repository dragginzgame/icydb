# Scheduled maintenance pass

Read the selected Shared Tooling checkout's AGENTS.md, DRAGGINZGAME.md and
tasks/README.md first. Run the maintenance pass defined by that catalog for the
explicit projects directory supplied with this prompt. Inventory the eligible
repositories and read their local instructions. Record the catalog/source
identities, dirty inputs and observation time.

This unattended pass is report-only inspection. Keep repository
source, manifests, locks, pins and Git state unchanged, including Shared Tooling.
Use source scans, prepared read-only checks and matching CI evidence. Do not run
write-producing sibling checks, install tools, fetch crate dependencies, change
compiler versions, run broad gates, commit, push, deploy or clean artifacts.
The runner disables command networking and connected apps. Use available local
evidence; mark current remote observations BLOCKED rather than treating cached
results as fresh. Do not request broader permissions or recover credentials.
Write new evidence only under the supplied run directory. Retain failed evidence.

Run the local portions of cargo-machete, msrv, snapshot-drift and readme-freshness,
then one bounded audit. Use source inspection when prepared tools or matching
local evidence are unavailable; mark the unexecuted check explicitly. Review
only local snapshot integrity and local README claims. Do not attempt online
latest-version, CI-health or current-advisory checks. List those deferred remote
observations once for the whole pass, rather than attempting and reporting the
same blocked network operations for every repository. README findings never
block releases or trigger prose rewrites.
Alternate code-audit and tooling-duplication using the last completed report
when available; start with
tooling-duplication when there is no history. Choose the least recently reviewed
relevant scope from the copied prior-report.md when supplied. Other state-root
logs are outside this run's permissions; do not try to read them or maintain
another work queue.
Work sequentially, avoid active builds, and report unavailable evidence rather
than waiting indefinitely or treating a missing check as success.

Prepare concise proposed findings in the report without posting issues, comments
or PRs. An interactive maintainer-reviewed pass searches current issues and PRs
before deciding what to publish. Respect other sessions' ownership and keep
proposed source repairs with their owners. The task catalog's interactive issue
authority does not apply to this unattended report-only invocation.

Return a concise report with per-task/repository results and evidence, the most
useful findings and issue links, blockers, audit scope, and remaining unreviewed
scope. Use the common result vocabulary. Do not claim fresh compilation when
only source or existing CI was inspected. Finish within this pass; no polling,
new schedules or self-modification.
