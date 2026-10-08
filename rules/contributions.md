# Contributions, commits and pull requests

These rules are part of the [engineering baseline](../DRAGGINZGAME.md).
Ordinary contributions use a topic branch and a pull request against the
repository's intended base branch. Human contributors may create branches,
commits and PRs using their normal repository or fork permissions; no agent or
special baseline exception is required. Repository reviews, required checks and
branch protections continue to apply.

## Agent authorization

Treat a request to deliver a PR as authorization to complete the necessary
workflow, including the commits it requires. Do not refuse because agents
cannot commit, ask the maintainer to make those commits, or repeatedly request
permission for already-authorized steps.

| Request | Authorized work |
| --- | --- |
| Fix or implement a change | Make scoped local edits and run relevant checks. |
| Commit the change | Stage the selected work and create its local commit; no implicit push. |
| Open or submit a PR for the change | Create/use a topic branch, make scoped commits, push that branch to the intended repository or fork, and open the PR. Use a draft when readiness is incomplete or requested. |
| Update an existing PR with a fix | Commit the scoped fix and push to that PR's branch within the established authorization. |
| Merge a PR or push directly to a named integration branch | Perform the specifically authorized action, subject to repository protections and required checks. |
| Run a selected standard release command | Execute its documented release effects under the [release contract](../docs/releases.md); package publication and deployment remain separate. |

Carry the repository, base branch, destination and scope already established in
the task forward. Resolve an actual ambiguity before making an external change;
do not ask again merely because PR creation involves a commit and branch push.
A request to change these rules is not itself a request to commit or open a PR.
An explicit limit, such as "draft the description only" or "do not push yet",
narrows the authorized steps.

## Delivery and review

- Keep unrelated staged and working changes intact. Review the entire index
  before committing and stage only the authorized work. Use an isolated branch
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
- PR authority covers its named repository and change. It does not authorize
  editing sibling repositories. Coordinate their adoption through issues until
  their own changes are expressly authorized.

The existing standard release runner atomically pushes its selected branch and
release tag. That is a separately authorized release workflow, not a requirement
to push everyday changes directly to the default branch. PR-gated release
automation remains tracked in [#42](https://github.com/dragginzgame/shared-tooling/issues/42);
do not bypass a protected branch to run the current direct-push workflow.
