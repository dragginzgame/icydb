# Contributions, commits and pull requests

These rules are part of the [engineering baseline](../DRAGGINZGAME.md).
Ordinary contributions use a topic branch and a pull request against the
repository's intended base branch. Human contributors may create branches,
commits and PRs using their normal repository or fork permissions; no agent or
special baseline exception is required. Repository reviews, required checks and
branch protections continue to apply.

## User-owned Git and release flow

The user owns staging, commits, version changes, tags, pushes and release
execution by default in the current working repository. The standing
[cross-repository PR workflow](#cross-repository-pull-requests) is the scoped
exception for contributions elsewhere. An ordinary local coding task ends with reviewable working-tree
changes, numbered changelog notes and the appropriate validation. Leave existing
staging untouched; do not stage or unstage the user's work to make checks pass.

In particular, do not run `git add .`, `git add -A`, `git commit` (including
amend), version-bump helpers, `git tag`, `git push`, `make release-patch`,
`make release-minor`, `make release-major`, `make release-resume` or their
mutating adapters as an automatic completion step. Inspect wrappers and Make
targets for these effects before invoking them. A validation request does not
authorize changing the real checkout's index, release metadata or refs.
Documented isolated fixture effects, where local validation policy permits them,
stay confined to their disposable test repositories; they grant no authority
over the real checkout or remote.

"Keep going", "finish this", "prepare the release", "work on 0.4", "new
changelog" and "ready to push?" do not delegate Git or release operations.
A release number selects the work/notes; it is not permission to update
`VERSION`, application/workspace/package versions, matching release lock entries
or generated release metadata. Explicit dependency-update authority remains
separate and does not authorize bumping the repository's own release version.

Do not change a published version, delete a tag, reset or rewrite history to
undo an unexpected release. Report the observed state and leave recovery to an
explicitly authorized action.

## Agent authorization

The user may expressly delegate a named operation. Preserve approved local
release restrictions; a PR never waives them. Local contribution restrictions
are subject to the standing cross-repository exception below. The following
table describes explicit delegation, not automatic
authority at the end of local development. Cross-repository PRs also have the
standing authority defined below.

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

Carry the repository, base branch, destination and scope already established in
the task forward. Resolve an actual ambiguity before making an external change;
do not ask again merely because PR creation involves a commit and branch push.
A request to change these rules is not itself a request to commit or open a PR.
An explicit limit, such as "draft the description only" or "do not push yet",
narrows the authorized steps.

## Cross-repository pull requests

Relevant fixes and adoption work in other `dragginzgame/*` repositories should
normally be delivered as PRs. Agents have standing permission to prepare those
contributions without asking for authorization per repository or per step.
This includes scoped source edits, required checks, staging selected files,
commits, topic-branch pushes and creating/updating the PR.

- Verify the destination owner, read its adopted baseline and product overlay,
  and search existing issues/PRs before starting. Keep the contribution tied to
  confirmed work in the current task; coordinate with an active owner rather
  than duplicating their fix. Link the owning issue where one exists.
- Use a separate worktree or clone and a dedicated topic branch from the intended
  upstream base. Never edit, switch branches in, stage, reset, clean or commit
  another contributor's normal sibling checkout. Do not carry its dirty changes
  into the contribution. Build and validate in the isolated checkout.
- Complete the destination's focused checks for the changed behavior and affected
  callers, respecting its documented execution restrictions. PR preparation does
  not authorize local full-suite runs; reserve those for release or an explicit
  request under the baseline. Describe evidence and remaining release checks in
  the PR. An unrun full suite alone does not require a draft; use a draft for
  incomplete implementation or missing focused evidence. Required hosted checks
  still apply. Leave review and integration to the owner.
- Standing permission does not include merging, enabling auto-merge, approving
  your own PR, direct integration-branch pushes, force-pushing, version bumps,
  tags, releases, publication, deployment or artifact cleanup. Those require
  their own explicit authorization.
- Explicit read-only requests, no-push instructions and excluded repositories
  remain binding. `dragginzgame/toko` and `dragginzgame/toko-miner` remain excluded
  unless the maintainer explicitly authorizes work there. Repositories outside
  `dragginzgame` require explicit destination/action authorization.
- This policy replaces blanket sibling-edit or agent-commit prohibitions for
  this isolated PR workflow only. It does not relax product safety, validation,
  release or retention requirements. Ordinary work in the current repository
  still leaves its commits and releases to the user.

An inspection-only task reports findings and owning issues; it does not silently
become implementation. When repair work is authorized, use this standing PR
workflow for relevant sibling fixes instead of requesting permission again.
Requesting a policy update does not itself request fleet-wide adoption PRs.

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
- Describe the change, validation and remaining gaps in the PR. Use the existing
  repository template where present. Maintain the numbered changelog draft under
  the [changelog rules](changelogs.md); an ordinary contribution does not bump
  package versions, tag, publish or deploy unless those effects were requested.
- Each PR covers one destination and coherent change. Coordinate cross-repository
  adoption through linked owning issues and separate isolated PRs under the
  standing authority above; never make direct edits to sibling working checkouts.

The standard release runner defaults to an atomic selected-branch/tag push.
Consumers can explicitly adopt `RELEASE_DELIVERY=pr` under the
[release contract](../docs/releases.md#pr-delivery): prepare a release PR, then
validate its exact merged commit and push only its tag. The release request
authorizes that documented delivery, including creating the PR, but never an
automatic approval or merge. Both policies are separately authorized release
workflows; ordinary contributions still use PRs. Do not bypass branch protection.
