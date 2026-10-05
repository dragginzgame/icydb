# Common release commands

Every `dragginzgame` repository, including Shared Tooling and repositories without
a publishable package, must provide these Make targets:

| Command | Version change | Example from `0.1.0` |
| --- | --- | --- |
| `make release-patch` | Increment patch | `0.1.1` |
| `make release-minor` | Increment minor; reset patch | `0.2.0` |
| `make release-major` | Increment major; reset minor and patch | `1.0.0` |

The names, version arithmetic, phase order, failure behavior and external effects
are identical across repositories. Each repository supplies its canonical version
source, metadata file set, validation gate, branch and remote. A repository
without package metadata uses its latest finalized changelog version as the
version source. Pre-release versions require a separately selected release plan;
these commands must reject ambiguous or unsupported version inputs.

## Required workflow

1. **Preflight.** Select exactly one release kind, compute and display the current
   and candidate versions, repository, branch, remote and exact effects before
   mutation. Reject conflicting release selections, an existing candidate tag,
   unrelated uncommitted work, missing inputs and concurrent releases. Prepare
   the selected dependency cache before an offline gate without changing the
   lockfile selection. Never infer a deployment destination or credentials.
2. **Validate.** Run the repository's documented complete release gate against
   the selected source and dependencies. Patch, minor and major use the same
   gate. Stop before version mutation if validation fails; retain failure logs
   and build artifacts.
3. **Prepare.** Apply the selected increment to the canonical version and all
   directly owned metadata and lockfile entries, without dependency upgrades.
   Finalize the one current changelog draft as `## [X.Y.Z] - YYYY-MM-DD`, using
   the selected version and UTC release date. Reject a pending heading or
   maintainer-selected version that conflicts with the computed release candidate.
   Verify resulting metadata consistency and run any checks affected by the
   metadata change before staging.
4. **Stage.** Stage the explicit release file set. Do not use an indiscriminate
   `git add -A` or include unrelated work.
5. **Commit and tag.** Create the maintainer-owned release commit with subject
   `Release X.Y.Z` and an annotated `vX.Y.Z` tag on that exact commit. The
   declared release files, candidate version and validation evidence must agree.
6. **Push.** Atomically push only the selected branch and exact release tag to
   the preflight remote. If atomic push is unsupported, fail and retain local
   state. Do not force-push or push unrelated tags.

These commands finish with the branch and tag on GitHub. They do not implicitly
publish packages, create GitHub Release objects, deploy products, bump to another
development version or clean build/evidence directories. Those actions require
their own explicit commands and authority. Keep the finalized release at the
top of the changelog until subsequent work needs a new numbered, undated pending
entry under the [automatic next-version rules](../rules/changelogs.md).

Agents maintain that proposed changelog version during ordinary development.
The proposal does not bump package metadata or execute a release. Select the
matching release kind when invoking the maintainer-owned command; its computed
candidate must agree with the pending heading and the complete batch's
compatibility impact before finalization.

## Artifact retention and exact push scope

Successful push does not authorize cleanup. Retain consumer-owned build outputs,
validation logs, release receipts and recovery plans on success, failure and
retry. Remove automatic post-push cleanup from consumer targets and adapters;
keep consumer workspace cleanup as a separately invoked, explicitly scoped
command. A helper may remove its own disposable scratch files when they contain
no retained artifacts or evidence and are not needed for recovery. A directory
being named `tmp` or `cache` does not establish that it is safe to delete.

The common runner uses exactly this push shape with its saved selections:

```bash
git push --no-follow-tags --atomic "$remote" \
  "HEAD:refs/heads/$branch" "refs/tags/v$candidate:refs/tags/v$candidate"
```

`--no-follow-tags` disables implicit annotated-tag publication, including a
configured `push.followTags`. Both refspecs are explicit: push the selected branch
and this release's tag, without publishing other local tags. `--atomic` requires
the remote to accept both ref updates together or reject both; do not fall back
to separate pushes. An interrupted reply still requires reconciliation against
the saved release identities before retrying.

## Makefile example

All three entry points delegate to the same vendored runner. Keep a harmless
default target and reject multiple release selections before dispatch:

```makefile
.DEFAULT_GOAL := help
RELEASE_REMOTE ?= origin
RELEASE_BRANCH ?= main

.PHONY: help release-patch release-minor release-major release-resume

help:
	@echo "Maintainer releases: release-patch, release-minor, release-major"

ifneq ($(word 2,$(filter release-patch release-minor release-major release-resume,$(MAKECMDGOALS))),)
$(error Select exactly one release target)
endif

release-patch release-minor release-major:
	+@bash scripts/ci/run-release.sh "$(@:release-%=%)" "$(RELEASE_REMOTE)" "$(RELEASE_BRANCH)"

release-resume:
	+@bash scripts/ci/run-release.sh resume "$(VERSION)" "$(RELEASE_REMOTE)" "$(RELEASE_BRANCH)"
```

The runner owns ordering, version selection, Git effects, a directory lock and
intent-before-effect plans in the repository's Git directory. The source SHA,
previous/candidate versions, UTC date, branch and push destination are fixed
across resume. Durable intent begins only after validation succeeds, immediately
before preparation may mutate release metadata. From that point, the plan is
retained on success and failure. A stale lock requires
inspection of its recorded owner before manual removal; never steal an active
release lock.

If preflight or validation fails, correct the inputs and rerun the same normal
release target. There is no new recovery plan to resume; preflight and the complete
validation gate run again against the current source. Consumer-owned failed logs
and build/evidence artifacts remain in place, bound to their original attempt;
adapters must retain them rather than overwrite them or reuse their validation.

An already-retained plan stopped at `preflight` or `validate` also permits a fresh
attempt. The runner checks that the base version and destination still agree and
that no saved release index, preparation file set or local/remote candidate tag
indicates possible preparation effects. It retains that plan unchanged in a unique
`release-state/X.Y.Z.attempt.*/` directory before starting fresh preflight and
validation. Exact resume of an early plan remains bound to its saved source and
also repeats both gates; use the normal target after committing source fixes.

Consumer Make targets provide these adapters:

| Target | Contract |
| --- | --- |
| `release-version` | Print only the canonical `X.Y.Z` version. |
| `release-preflight` | Admit only the declared release metadata as dirty work; reject unrelated staged/unstaged/untracked paths; prepare the selected offline cache. |
| `release-verify` | Run the same complete gate for every release kind. |
| `release-prepare-version` | Apply exactly the saved candidate and finalize notes; preserve dependency selection and verify all directly owned metadata. |
| `release-prepared-check` | Check the candidate and prepared metadata without another bump. |
| `release-files` | Print the explicit relative release paths, each terminated by NUL, and no explanatory output. |
| `release-commit-check` | Admit the exact release index and source-bound validation evidence. |
| `release-committed-check` | Check the release commit and any consumer-owned evidence binding. |
| `release-tagged-check` | Check or record exact tag-bound evidence without another Git effect. |
| `release-push-check` | Check the complete local release and destination before dispatch/reconciliation. |

The runner passes `RELEASE_KIND`, `RELEASE_PREVIOUS`, `RELEASE_VERSION`,
`RELEASE_DATE`, `RELEASE_SOURCE`, `RELEASE_BRANCH` and `RELEASE_REMOTE` as Make
variables. Adapters consume those selections instead of independently choosing a
version or target. Git staging, commit creation, annotated tagging and atomic
push belong only to the common runner. Publishing and deployment stay separate.

## Authority and recovery

The maintainer invokes the one-shot commands. Agents never run them, even when a
push or version change has been authorized, because commits remain
maintainer-owned under the [engineering baseline](../DRAGGINZGAME.md). Agents
may use separate read-only or preparation phases within existing authorization.

Once version preparation has started, the normal targets reject any unfinished
release, including when the bumped metadata would otherwise select another
candidate. If preparation, tagging or a push is interrupted, retain the exact candidate,
source/commit identity, completed phases and evidence. Inspect local and remote
state before `make release-resume VERSION=X.Y.Z` resumes that release. A missing push reply does not prove failure.
Never rerun the increment from the already bumped version, recreate an existing
release commit, overwrite a tag or discard artifacts to obtain a clean retry.

`Release X.Y.Z completed; retained plan: ...` is a success message. The completed
plan is retained evidence and does not block the next release. A later failure
from a separately chained command does not undo that successful branch/tag push.
Package publication requires a consumer-owned publication command and an eligible
package; do not append `make publish` automatically to the standard release flow.

## Adoption and verification

The Makefile pattern specifies a contract; adding this document does not install
helpers or prove consumer adoption. Consumers implement or align their targets,
vendor a clean reviewed Shared Tooling revision with this document,
`scripts/ci/run-release.sh`, `scripts/ci/next-release-version.sh` and any selected
changelog helper in the [governance snapshot](consuming-snapshots.md), and qualify the workflow on their
declared Linux and macOS hosts. Report upstream policy changes separately from
verified consumer adoption.

Snapshot verification establishes the declared files' integrity at the recorded
revision. It does not establish that Make targets invoke the runner or that
consumer adapters comply. During adoption, inspect all release entry points,
adapters and their tests for automatic post-push cleanup, broad tag pushes and
conflicting expectations. Align or retire those paths and update their local
instructions and checks before reporting release-workflow adoption. Report
snapshot integrity, consumer changes and workflow qualification separately;
identify uncommitted adoption edits as working-tree changes.

Use isolated fixtures or command stubs to verify all three increments, identical
phase ordering, stop-on-failure behavior, explicit staging, tag identity, atomic
push scope, artifact retention on success/failure/retry and interruption recovery.
Qualification must not publish a real release
as a side effect of testing. Documentation review or a stub pass is not evidence
of live package publication or a native host release.
