# Current CI and issue triage

Apply the [common run contract](README.md) and the existing
[CI/issue maintenance procedure](../rules/agent-maintenance.md).

Resolve each selected repository and source commit before querying GitHub.
Inspect all applicable workflows, latest attempts and PR/branch identities;
expand bounded listings before claiming complete coverage. Read failed logs and
separate code defects from missing prerequisites, runner faults and unavailable
evidence. Pending or absent checks are not success.

Review existing issues, discussions and linked PRs for those findings. Check
active ownership before proposing new work. Close an issue only when its actual
completion criteria and consumer adoption are supported by current evidence.
An upstream fix alone does not prove downstream adoption.

Return failing job, tested SHA, run URL, concrete error and smallest next action,
plus a short ordered list of actionable issues. An unchanged known failure needs
no repeated comment; a new source/run or changed diagnosis may warrant one.
Inspection does not dispatch, rerun or cancel workflows or start repairs.

## Queued native jobs and repeated validation

For persistent queues, inspect job details before treating the delay as a code
failure. Record runner labels, assignment, timestamps and step progress. A queued
workflow can already contain running or completed jobs; count jobs rather than
workflow statuses. Check workflow/job concurrency, dependencies and environment
approvals separately from runner availability.

When the selected scope includes the organization, inventory active jobs across
its accessible repositories, paginating runs and jobs and deduplicating run IDs.
Separate queued and running jobs by host, retain representative run links and
the oldest queue timestamps, and record observation time and inaccessible repos.
The inventory is not atomic. Compare observed occupancy with the current
[GitHub Actions limits](https://docs.github.com/en/actions/reference/limits) and
[service status](https://www.githubstatus.com/); distinguish a documented plan
default from a verified custom allowance. Missing admin access is a limitation,
not permission to expand token privileges. Repository-scoped evidence alone
cannot establish organization-wide capacity exhaustion.

Group possible repeated runs by repository, workflow, source SHA and event/ref.
Compare actual job commands, inputs and required checks before calling them
redundant: branch/tag runs can have different gates, PRs can test merge commits,
and manual dispatches can select different inputs. Recommend running equivalent
heavy source checks once while retaining all distinct host, MSRV, product and
tag-identity obligations. Define qualification for supported tag-only releases;
never substitute another source commit's success.

Preserve required qualification of distinct pushed sources. A concurrency key
shared by branch and tag events is not proof of deduplication and may displace
necessary pending work. Route concrete workflow changes to owning issues;
capacity increases are a separate administrator action. Queue inspection does
not authorize cancellation, reruns, runner purchases or settings changes. Stop
after the bounded diagnosis rather than polling an unchanged queue.
