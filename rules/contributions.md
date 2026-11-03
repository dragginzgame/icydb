# Contributions, commits and pull requests

These rules are part of the [engineering baseline](../DRAGGINZGAME.md).
Direct local edits are the default; requested or required review contributions
use a topic branch and PR. Human contributors use their normal repository/fork
permissions. The agent authority below does not restrict that human workflow.

## Trusted pull request sources

Dragginzgame repositories must restrict new pull requests to trusted repository
collaborators using GitHub's `pull_request_creation_policy=collaborators_only`
setting. This admits organization members and approved outside collaborators
with repository write access, as well as authorized installed bots such as
Dependabot. Apply the setting when creating or restoring a repository; archived
repositories remain archived. Preserve existing review and branch rules.

The creation restriction does not revalidate existing PR authors when access is
revoked. Before accepting an existing PR, verify the author's current repository
write access or authorized bot identity; hold contributions whose authorization
cannot be established. A display name, commit email or historical contribution
does not establish current access or authorship of every included commit.

For repositories that adopt an approved contributor agreement, verify executed
acceptance and rights-holder authority before merging covered contributions.
Follow the canonical [CLA and relicensing guidance](https://github.com/dragginzgame/shared-tooling/blob/main/docs/licenses.md#contributor-agreements-and-relicensing).
A PR, DCO sign-off or agent-generated checkbox does not establish a signed CLA;
agents must not sign on another person's or entity's behalf. Publishing an
agreement does not establish acceptance or configure fleet-wide signing checks.

<a id="user-owned-git-and-release-flow"></a>

## Implementation and delivery authority

**Prefer direct edits in the normal authorized checkout when a PR is optional.**
Ordinary fixes end with reviewable working-tree changes, focused checks and
[pending notes](changelogs.md#automatic-next-version-selection). Shared Tooling
also prepares its local `VERSION`; other package versions remain unchanged.
Use a PR when requested or required by the authorized delivery workflow or branch
protections. Optional hosted CI is not a reason to add a PR or approval round.

The [authorization table](#agent-authorization) defines Git and release effects.
Inspect wrappers and Make targets for those effects before running them.
Implementation and validation alone do not authorize staging, commits, release
metadata, tags, pushes or publication. Disposable validation fixtures may exercise
those operations only in their documented test repositories.

Carry established authority forward without repeated permission. During ordinary
implementation, “keep going”, “finish this”, “prepare the release”, a release
number or “ready to push?” does not newly authorize delivery. During delegated
delivery, continuation retains its scope. An exact version-edit request and an
explicit dependency update authorize their named metadata changes, not release
execution. A request to change these rules is not a delivery request.

Preserve unrelated staged/working changes. Never stage or unstage user work to
make checks pass. Do not undo an unexpected release by rewriting history,
deleting tags or changing published versions; report the observed state and
obtain authority for the specific recovery.

## Durable work and checkout location

Use the normal authorized checkout. A topic branch does not isolate sessions
sharing that checkout: coordinate with its active owner before changing branches
or shared inputs. A dirty checkout or failed preflight does not authorize moving
implementation or delivery elsewhere.

- Additional clones, worktrees and contribution directories require approval of
  their exact location, including for an authorized PR. Do not create sibling
  `*-contributions` directories by default.
- Keep lasting changes in the normal checkout or an explicitly approved durable
  checkout outside `/tmp`, `target/` and caches. Disposable validation copies
  and patches must not become the sole copy of unfinished work. An issue records
  deferred work; it does not preserve an existing diff. If no authorized durable
  route is available, ask before creating unique edits elsewhere.
- Before alternate-checkout delivery, disclose its absolute path, branch, source
  commit, remote destination and effect on the normal checkout. Reuse approval
  for that exact path/workflow; a PR checkout does not authorize a separate
  direct-release clone. A final handoff cannot supply prior authorization.
- Afterwards, report the released identity, remote/publication results, remaining
  unique edits and normal checkout state. A remote push does not update local
  files. Return implementation to the normal checkout when authorized and safe,
  or agree where it will continue.
- If unique work is found in disposable storage, report and preserve it through
  an authorized durable route before cleanup. Check tracked/untracked files and
  newer local work; never apply a patch blindly or claim preservation without
  verifying it.

## Agent authorization

The user may expressly delegate a named operation. Preserve approved local
release restrictions; a PR never waives them. The following table describes
explicit delegation, not automatic authority at the end of local development.
Cross-repository work follows the scope rules below.

Treat an explicit request to deliver a PR as authorization to complete its
necessary workflow, including the commits it requires. Do not refuse because agents
cannot commit, ask the maintainer to make those commits, or repeatedly request
permission for already-authorized steps.

| Request | Authorized work |
| --- | --- |
| Fix or implement a change | Make scoped local edits and run relevant checks. |
| Stage the selected change | Stage only its reviewed paths; no commit or release. |
| Commit the change | Stage the selected work and create its local commit; no implicit push. |
| Open or submit a PR for the change | Create/use a topic branch, make scoped commits, push that branch to the intended repository or fork, and open the PR. Use a draft when readiness is incomplete or requested. |
| Update an existing PR with a fix | Commit the scoped fix and push to that PR's branch within the established authorization. |
| Merge a PR or push directly to a named integration branch | Perform the specifically authorized action, subject to repository protections and required checks. |
| Change the repository version to an exact value | Make the named metadata edit and its required consistency updates; no commit, tag, push or release execution. |
| Run a selected standard release command | Execute its documented release effects under the [release contract](../docs/releases.md); package publication and deployment remain separate. |
| Release the scoped changes | Select affected approved groups and appropriate semantic-version increments, prepare scoped commits and execute delivery, including direct integration-branch pushes. Publication is included when the established request includes registry delivery. |
| Ship or deploy the scoped crate changes | Complete Git delivery to the established integration branch and registry publication for the affected groups, including validation and verification, without another push/publication approval. |
| Deploy, without a narrower repository/group or application target | Discover all changed eligible Dragginzgame repositories in the established local projects directory, push their reviewed changes to `origin/main` (or their established integration branch), and release/publish affected eligible crate groups to crates.io in dependency order. |
| Deploy an application to an established environment | Complete its documented build/release/deployment workflow for the named or already established target. Resolve an unknown destination before deployment. |

Carry the repository, base branch, destination and scope already established in
the task forward. Resolve an actual ambiguity before making an external change;
do not ask again merely because PR creation involves a commit and branch push.
A request to change these rules is not itself a request to commit or open a PR.
An explicit limit, such as "draft the description only" or "do not push yet",
narrows the authorized steps.

## Delegated delivery

Interpret requests by their outcome and session context. “Ship these changes”
in a crate-publication task includes release and upload; “how do I deploy?” asks
for guidance. Running only `release-patch` retains its documented Git-only
effects. Readiness, implementation, policy and PR-only requests are not deployment.

- **An unqualified “deploy” covers all changed eligible Dragginzgame repositories
  in the established local projects directory.** Inventory dirty changes, commits
  awaiting push and committed releases awaiting publication; verify origins and
  release state. Include reviewed existing changes, required release preparation,
  direct integration to `origin/main` or the established integration branch,
  tags and crates.io publication for affected publishable groups. Deliver changed
  non-publishable repositories through Git only.
- A named repository/group, application environment or explicit limit narrows
  scope. `toko`, `toko-miner`, DFINITY and other GitHub owners retain their
  exclusions under the [cross-repository rules](#cross-repository-pull-requests).
  Do not scan arbitrary directories. A dependency blocker or “keep going” alone
  does not expand the selected repository set.
- Announce one plan: repositories/groups, checkout paths and branches, remote
  destinations, versions/tags, publication effects and supplier-first order.
  This is informational, not another approval gate. Select routine version
  increments and pass `RELEASE_GROUP` explicitly. Do not bump unaffected groups
  just because a supplier changed or republish an existing version.
- Coordinate stable inputs with active sessions before staging, switching branches
  or preparing releases. An unfinished or changing batch is not publishable just
  because it is dirty. Report concrete blockers and continue independent ready
  repositories. Delivery does not authorize unrelated feature work.
- Complete the selected workflow: scoped commits, required validation, integration,
  version preparation, tags, pushes and publication. Direct integration-branch
  pushes need no separately worded approval. Prefer direct delivery when permitted;
  use a PR only when requested or required. Full delivery includes merging its
  scoped PR once required checks/reviews permit; PR-only and release-command-only
  requests do not. Never self-approve or bypass protections.
- Keep release and publication as separate executable/evidence boundaries without
  asking again between them. Use existing adapters and recovery procedures,
  preserve failed evidence and verify remote state before retrying uncertain effects.
- Ask only for information or authority that changes the outcome: an unknown
  destination, unapproved alternate checkout, genuinely ambiguous scope or
  destructive migration. Ordinary handoff coordination and generic “user-owned”
  wording are not additional maintainer approval gates.
- Preserve explicit restrictions, runtime approval decisions and work outside the
  selected batch. No force pushes, rewritten published history, artifact cleanup,
  unrelated paid effects or new architectural/versioning boundaries are implied.
  Existing checkout rules and release gates remain in force.

## Cross-repository pull requests

Task-relevant sibling inspection is read-only and needs no contribution branch.
Source repair requires authority covering the destination and change; issue
permission alone does not grant it. Fleet release preparation follows
[delegated delivery](#delegated-delivery), not this separate source-contribution
workflow. Prefer direct edits when authorized and a PR is optional.

`dragginzgame/toko` and `dragginzgame/toko-miner` require explicit inclusion.
Repositories outside Dragginzgame require explicit destination/action authority;
DFINITY contributions remain excluded under the
[own-the-solution rule](../DRAGGINZGAME.md#dfinity-dependencies-own-the-solution).
Explicit read-only and no-push instructions remain binding.

- Verify the repository, read its adopted baseline/local overlay, and compare
  current source, issues/PRs and active work before contributing. Do not duplicate
  an ongoing fix or revive an obsolete adoption branch.
- A PR requires an explicit request or the receiving repository's authorized
  delivery workflow. Do not create one just to obtain CI, transport a patch or
  finish ordinary implementation. Routine version/lock/snapshot adoption stays
  with its receiving owner unless explicitly included in the task.
- Use a topic branch in the agreed normal checkout after coordinating with its
  active owner. A PR request does not authorize taking over another session.
  If that checkout is unavailable, follow the
  [checkout-location contract](#durable-work-and-checkout-location) before
  creating another. Never carry another contributor's dirty work into the PR.
- Complete focused checks for changed behavior and affected callers. PR preparation
  does not activate full local suites; an unrun full suite alone does not require
  a draft. Use a draft for incomplete implementation or missing focused evidence.
  Required hosted checks and product restrictions still apply.
- PR-only authority includes scoped commits and branch push, not merge/auto-merge,
  self-approval, direct integration pushes, release versions/tags, publication,
  deployment or cleanup. Separate delivery authority can supply those effects.
- Track actual defects and substantive migrations in owning issues. Report supplier
  release/version waits directly to the user; do not create waiting issues or
  unsolicited adoption PRs. Follow the
  [shared dependency finding rule](engineering-details.md#shared-dependency-findings).

Preserve unique fixes/evidence when recommending retirement: a newer dependency
version does not prove an older PR is superseded. An audit alone does not authorize
PR closure or branch deletion; a policy update does not request fleet adoption PRs.

## Delivery and review

- Review the whole index before committing; stage only explicit reviewed paths.
  Never use blanket `git add .` or `git add -A`. PRs use topic branches; delegated
  direct delivery uses the established integration branch in the normal checkout.
- An authorized commit/PR task may organize its own unpublished commits.
  Rewriting another contributor's or shared history and force pushes need separate
  authority. Ordinary delivery does not waive these restrictions.
- Respect branch protections, required checks/reviews and repository permissions.
  Report actual unmet requirements without inventing optional approval gates.
  Follow the [asynchronous CI policy](../DRAGGINZGAME.md#validation-and-evidence);
  pending optional CI alone does not require holding the turn open.
- Keep each PR scoped to one destination and coherent change. Use its template,
  describing behavior, relevant validation and remaining gaps. Maintain
  [release notes](changelogs.md) without implying release execution.

The standard release runner defaults to an atomic selected-branch/tag push.
Consumers can explicitly adopt `RELEASE_DELIVERY=pr` under the
[release contract](../docs/releases.md#pr-delivery): prepare a release PR, then
validate its exact merged commit and push only its tag. The release request
authorizes that documented delivery, including creating the PR. The runner never
approves or merges it; an agent may merge only with the authority described in
[delegated delivery](#delegated-delivery) or an explicit merge request.
Both policies are authorized release workflows; requested review contributions
use PRs. Do not bypass branch protection.
