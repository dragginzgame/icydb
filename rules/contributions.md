# Contributions, commits and pull requests

These rules are part of the [engineering baseline](../DRAGGINZGAME.md).
Contributions submitted for review use a topic branch and a pull request against
the repository's intended base branch. Ordinary agent implementation leaves
reviewable local changes until delivery is requested. Human contributors may create branches,
commits and PRs using their normal repository or fork permissions; no agent or
special baseline exception is required. Repository reviews, required checks and
branch protections continue to apply.

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

Ordinary implementation includes pending changelog preparation under the
[next-version rules](changelogs.md#automatic-next-version-selection).
Shared Tooling alone also prepares its local `VERSION` automatically.
It does not automatically include staging, commits, package version changes,
tags, pushes or release execution. A request to release, publish, ship or
deploy delegates the corresponding delivery workflow under
[delegated delivery](#delegated-delivery), without requiring the user to choose
every command or release group within the requested repository. Contributions
elsewhere follow the explicitly scoped
[cross-repository PR workflow](#cross-repository-pull-requests).
An ordinary local coding task ends with reviewable working-tree
changes, numbered changelog notes and the appropriate validation. Leave existing
staging untouched; do not stage or unstage the user's work to make checks pass.

In particular, do not run `git add .`, `git add -A`, `git commit` (including
amend), package version-bump helpers, `git tag`, `git push`, `make release-patch`,
`make release-minor`, `make release-major`, `make release-resume` or their
mutating adapters as an automatic completion step. Inspect wrappers and Make
targets for these effects before invoking them. A validation request does not
authorize changing the real checkout's index, release metadata or refs.
Documented isolated fixture effects, where local validation policy permits them,
stay confined to their disposable test repositories; they grant no authority
over the real checkout or remote.

During implementation, "keep going", "finish this", "prepare the release",
"work on 0.4", "new changelog" and "ready to push?" do not newly delegate
release operations. During an already delegated delivery workflow, continuation
retains that authority; do not stop for permission at each group or command.
A release number selects the work/notes and, in Shared Tooling only, pending
`VERSION`; it is not permission to update application/workspace/package versions,
matching release lock entries or generated release metadata. Explicit dependency
updates remain separate from package release-version changes.

Do not change a published version, delete a tag, reset or rewrite history to
undo an unexpected release. Report the observed state and leave recovery to an
explicitly authorized action.

## Durable work and checkout location

Prefer editing the requested local checkout so the user can inspect the actual
working files. Preserve unrelated changes. A dirty checkout or failed release
preflight does not authorize moving implementation or delivery elsewhere.

- Lasting source changes must live in the normal checkout or a disclosed durable
  contribution checkout for an authorized PR. Keep contribution checkouts outside
  `/tmp`, `target/`, caches and other cleanup roots. Disposable clones and patches
  may support validation of already-preserved source; they must never become the
  sole copy of unfinished implementation or the place where follow-up work silently
  continues. A path mentioned only in a final handoff is not prior authorization.
- Use local files for authorized implementation, an authorized PR for contribution,
  or an owning issue for deferred work. An issue describing a change does not
  preserve an existing code diff. If none of those options can retain the work,
  ask the user to approve a durable location before creating unique edits there.
  Do not create an unsolicited PR merely to transport a patch.
- Before using an alternate checkout for delivery, disclose its absolute path,
  branch, source commit, intended remote destination and what will happen to the
  user's normal checkout. Obtain explicit approval unless that exact path and
  workflow are already authorized. Authorization for an isolated PR contribution
  covers its disclosed durable contribution checkout; it does not authorize
  switching to a separate direct-release clone.
- After authorized alternate-checkout delivery, report the released commit and
  version, remote branch/tag/publication results, remaining unique edits and the
  normal checkout's observed state. Never imply that a remote push updated the
  user's local files. Return subsequent implementation to the normal checkout
  when authorized and safe; otherwise obtain agreement on where it will continue.
- If existing work is found only in disposable storage, report that immediately
  and preserve it through an authorized durable route before cleanup. Do not
  overwrite newer local work or apply a patch blindly. Check tracked and untracked
  files, and verify preservation before calling the handoff complete.

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
| Release the scoped changes | Select affected approved groups and appropriate semantic-version increments, prepare scoped commits and execute the authorized delivery path. Direct integration-branch pushes and alternate delivery checkouts require the explicit scope below. Publication is included when the established request includes registry delivery. |
| Ship or deploy the scoped crate changes | Complete the established Git release and registry publication workflow for the affected groups, including authorized integration, validation and verification; observe the direct-push and checkout requirements below. |
| Deploy an application to an established environment | Complete its documented build/release/deployment workflow for the named or already established target. Resolve an unknown destination before deployment. |

Carry the repository, base branch, destination and scope already established in
the task forward. Resolve an actual ambiguity before making an external change;
do not ask again merely because PR creation involves a commit and branch push.
A request to change these rules is not itself a request to commit or open a PR.
An explicit limit, such as "draft the description only" or "do not push yet",
narrows the authorized steps.

## Delegated delivery

Interpret delivery requests by their outcome and session context, not by requiring
exact command names. "Ship these changes" in a crate-publication task authorizes
the release and upload; "how do I deploy?" asks for guidance only. A request to
run only `release-patch` retains that command's Git-only effects. A rules update,
readiness check, implementation task or PR request is not itself delivery authority.
The destination and checkout requirements below constrain every delegated workflow.

- Select affected existing groups, compatible version increments, supplier-first
  order within the authorized repository set, and the established branch/registry
  or environment. Explain
  the selected plan briefly, then execute without asking for routine choices or
  repeating approval. Pass `RELEASE_GROUP` explicitly; leave unaffected groups
  unchanged. Respect any exact version or narrower user-selected scope.
- Complete necessary source preparation, scoped staging/commits, the authorized
  PR or direct integration path, required validation, version preparation, tags,
  pushes and the requested publication/deployment. A full delivery request covers
  merging its own scoped PR when required checks/reviews permit; a PR-only or
  release-command-only request does not. Never self-approve or bypass protections.
- Before executing delivery, state the checkout path and local branch, release
  scope, remote repository/branch, and intended tags and publication/deployment
  effects. Direct pushes to `main`, `master` or another integration branch require
  explicit user authorization for that destination and direct-delivery path.
  Generic "release", "ship" or "deploy" wording and a repository's configured
  `direct` default do not alone authorize that push. An explicit request to run a
  named command whose direct-push effects were already disclosed can authorize
  them. Reuse clear prior approval within its scope; otherwise ask before the
  first remote mutation. Do not substitute an unsolicited PR.
- Keep release and publication as separate executable commands and evidence
  boundaries. One delivery request can authorize both; do not ask again between
  them. Use the existing adapters and recovery flow, retain failed evidence, and
  verify destination/registry state before recovering an uncertain effect.
- Ask only for missing information that changes the authorized outcome, such as
  an unapproved direct-push destination or alternate checkout, an unknown production
  target, ambiguous package scope or destructive migration.
  Generic user-owned/maintainer-operated wording is a default for ordinary work,
  not a second approval requirement after delegation. Explicit target restrictions,
  no-push/no-publication instructions and runtime approval decisions still apply.
- Delivery authority is repository-scoped, including when a dependency needs
  adoption or publication elsewhere. Do not start a supplier's or consumer's
  release preparation, release gates, version transaction, deployment procedure
  or recovery workflow without delivery authority for that repository. Report
  the required supplying version and blocker to its owner. Multi-repository
  delivery requires an explicitly established repository set; a dependency graph,
  supplier-first plan or "keep going" does not establish that set.
- Delivery authority does not extend to other repositories, new release
  boundaries, force pushes, rewritten published history, artifact cleanup or
  unrelated paid effects. Preserve unrelated dirty/staged work and the isolated
  sibling contribution workflow. Report actual blockers; do not weaken release
  gates to complete delivery.

## Cross-repository pull requests

DFINITY crates and repositories are excluded: follow the baseline's
[own-the-solution rule](../DRAGGINZGAME.md#dfinity-dependencies-own-the-solution).

Read-only sibling inspection follows the
[baseline's scope authorization](../DRAGGINZGAME.md#scope-and-authorization);
it is not a source contribution and needs no contribution branch or permission
request. The isolated workflow below applies when making repairs.

The receiving repository's maintainer or active agent owns its dependency and
Shared Tooling snapshot adoption. Do not open unsolicited PRs for version bumps,
lock refreshes, snapshot adoption or propagated supplier changes. New releases,
an audit finding or a blocked downstream build do not authorize those PRs.
Report the supplying version, concrete required source migration and focused
evidence; use an existing owning issue for actionable follow-up. Do not create
waiting issues or PRs merely because another repository is not on the latest version.
Likewise, do not file an issue asking a sibling to publish, tag, release or deploy
an already identified change. Report the required supplier/version and blocker
directly to the user under the [dependency finding rules](engineering-details.md#shared-dependency-findings).
An actual missing capability or code defect remains an owning engineering issue.

Cross-repository source work requires an explicit request covering that
destination and change. A PR additionally requires an explicit PR request or
the receiving repository's already-authorized configured delivery workflow.
Do not create a PR just to obtain CI, transport routine adoption, or finish an
ordinary coding task. Keep current-repository changes reviewable in the working
tree. This replaces the former standing permission for sibling contribution PRs;
issue-posting authority does not grant source or delivery authority.

An explicitly requested PR includes its scoped source edits, required checks,
staging selected files, commits, topic-branch push and PR creation/update.
Carry that authorization forward without asking again for each substep.

- Verify the destination owner, read its adopted baseline and product overlay,
  and search existing issues/PRs before starting. Keep the contribution tied to
  confirmed work in the current task; coordinate with an active owner rather
  than duplicating their fix. Link the owning issue where one exists.
- Use a separate worktree or clone and a dedicated topic branch from the intended
  upstream base, in a disclosed durable location under the
  [checkout contract](#durable-work-and-checkout-location). Never edit, switch
  branches in, stage, reset, clean or commit
  another contributor's normal sibling checkout. Do not carry its dirty changes
  into the contribution. Build and validate in the isolated checkout.
- Complete the destination's focused checks for the changed behavior and affected
  callers, respecting its documented execution restrictions. PR preparation does
  not authorize local full-suite runs; reserve those for release or an explicit
  request under the baseline. Describe evidence and remaining release checks in
  the PR. An unrun full suite alone does not require a draft; use a draft for
  incomplete implementation or missing focused evidence. Required hosted checks
  still apply. Leave review and integration to the owner.
- PR-only permission does not include merging, enabling auto-merge, approving
  your own PR, direct integration-branch pushes, force-pushing, version bumps,
  tags, releases, publication, deployment or artifact cleanup. Those require
  their own explicit authorization.
- Explicit read-only requests, no-push instructions and excluded repositories
  remain binding. `dragginzgame/toko` and `dragginzgame/toko-miner` remain excluded
  unless the maintainer explicitly authorizes work there. Repositories outside
  `dragginzgame` require explicit destination/action authorization.
- An authorized isolated PR permits its necessary contribution commits and
  branch push. It does not relax product safety, validation,
  release or retention requirements. Ordinary work in the current repository
  still leaves its commits and releases to the user.

An inspection-only task reports findings and owning issues; it does not silently
become implementation. Before proposing or updating a contribution, compare its
actual changes with current destination source, open PRs and the active owner's
work. Do not duplicate an ongoing adoption or refresh an obsolete dependency
branch that the receiving owner is already replacing. Preserve unique fixes and
evidence when recommending retirement; a newer version alone does not prove all
of an old PR is superseded. Requesting a policy update does not request fleet-wide
adoption PRs. A PR audit alone does not request PR closure or branch deletion.

## Delivery and review

- Keep unrelated staged and working changes intact. Review the entire index
  before committing and stage only explicit reviewed paths for the authorized
  work; do not use blanket `git add .` or `git add -A`. Use an isolated branch
  or worktree when needed to preserve another contributor's work.
- An authorized commit/PR task may organize its own unpublished task commits.
  Rewriting another contributor's or already-shared history, force-pushing,
  merging and pushing directly to `main`, `master` or another integration/release
  branch require explicit authorization. Opening a PR does not authorize merging it.
- Respect branch protection and required checks; permission to deliver a change
  does not authorize bypassing them. Leave the PR open and report an actual
  unmet requirement. GitHub permissions still determine where a branch can be pushed.
- Follow the [asynchronous CI cadence](../DRAGGINZGAME.md#validation-and-evidence)
  and its independent agent-runtime approval boundary for ordinary PR delivery
  and separately authorized merges. That policy owns pending CI, required versus
  optional reviews, known failures and runtime rejection handling. Report the
  concrete enforced blocker; do not add an independent GitHub approval requirement
  merely because optional hosted qualification is pending.
- Describe the change, validation and remaining gaps in the PR. Use the existing
  repository template where present. Maintain the numbered changelog draft under
  the [changelog rules](changelogs.md); an ordinary contribution does not bump
  package versions, tag, publish or deploy unless those effects were requested.
- Each requested PR covers one destination and coherent change. Coordinate
  cross-repository adoption with the receiving owners and existing actionable
  issues; create isolated PRs only within the explicit authority above. Never
  make direct edits to sibling working checkouts.

The standard release runner defaults to an atomic selected-branch/tag push.
Consumers can explicitly adopt `RELEASE_DELIVERY=pr` under the
[release contract](../docs/releases.md#pr-delivery): prepare a release PR, then
validate its exact merged commit and push only its tag. The release request
authorizes that documented delivery, including creating the PR. The runner never
approves or merges it; an agent may merge only with the authority described in
[delegated delivery](#delegated-delivery) or an explicit merge request.
Both policies are authorized release workflows; requested review contributions
use PRs. Do not bypass branch protection.
