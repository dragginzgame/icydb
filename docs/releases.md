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
without package metadata may use a dedicated version file; otherwise it uses
its latest finalized changelog version. Shared Tooling owns its current local
version in root `VERSION`, displayed by `make version`. Its undated changelog
heading names the next proposed release. Preparation updates both files and
stages them together; neither a version file nor a changelog heading proves
that a tag was pushed. Pre-release versions require a separately selected release plan;
these commands must reject ambiguous or unsupported version inputs.

## Required workflow

1. **Preflight.** Select exactly one release kind, compute and display the current
   and candidate versions, repository, branch, remote and exact effects before
   mutation. Reject conflicting release selections, an existing candidate tag,
   unrelated uncommitted work, missing inputs and concurrent releases. Check
   staged and unstaged paths independently: a working file restored to HEAD can
   still have different staged content. Check pending changelog/candidate
   agreement here, before the validation gate or saved preparation intent. Prepare
   the selected dependency cache before an offline gate without changing the
   lockfile selection. Authorized dependency changes must already have
   [prepared every affected independent lockfile](../rules/cargo-dependencies.md#preparing-authorized-dependency-changes);
   cache fetching stays locked and does not repair stale dependency graphs.
   Never infer a deployment destination or credentials.
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
   Before creating the commit, verify that the entire index contains only
   permitted release changes and matches the prepared metadata.
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

Shared Tooling's own `release-verify` adapter runs `ci` through
`scripts/ci/run-validation-targets.sh`. Failed attempts retain unique raw logs
under the Git directory's `release-state/validation-failures/`, outside tracked
release inputs. Later attempts preserve those logs; `latest.log` is only a
convenience copy. If retention fails, the logger preserves its temporary logs
and reports their location. Consumer adapters must provide equivalent retention
for their actual validation commands, not just simulated fixture evidence.

The common runner uses exactly this push shape with its saved selections:

```bash
git push --no-follow-tags --atomic "$remote" \
  "$push_source:refs/heads/$branch" "refs/tags/v$candidate:refs/tags/v$candidate"
```

`--no-follow-tags` disables implicit annotated-tag publication, including a
configured `push.followTags`. Both refspecs are explicit: push the selected branch
and this release's tag, without publishing other local tags. `--atomic` requires
the remote to accept both ref updates together or reject both; do not fall back
to separate pushes. An interrupted reply still requires reconciliation against
the saved release identities before retrying.
For an ordinary release, `push_source` is HEAD. When recovering an older release,
it is that exact release commit: newer fixes are validated separately before their
next release is pushed. If the remote branch already contains the older release,
preserve its verified tip when atomically publishing a missing tag. Never rewind
the branch. Unknown or diverged remote history stops with a fetch/reconciliation
diagnostic; remote inspection failure does not authorize replay.

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
| `release-preflight` | Check candidate/changelog agreement; admit only declared release metadata as dirty work; inspect staged and unstaged paths separately, reject unrelated untracked paths, and prepare the selected offline cache. |
| `release-verify` | Run the same complete gate for every release kind. |
| `release-prepare-version` | Apply exactly the saved candidate and finalize notes; preserve dependency selection and verify all directly owned metadata. |
| `release-prepared-check` | Check the candidate and prepared metadata without another bump. |
| `release-files` | Print the explicit relative release paths, each terminated by NUL, and no explanatory output. |
| `release-commit-check` | Admit the entire exact release index, ensure it matches prepared metadata, and check source-bound validation evidence. |
| `release-committed-check` | Check `RELEASE_COMMIT` and its consumer-owned evidence binding. |
| `release-tagged-check` | Check or record exact tag-bound evidence for `RELEASE_COMMIT` without another Git effect. |
| `release-push-check` | Check the selected `RELEASE_COMMIT`, tag, evidence and destination before dispatch/reconciliation. |

The runner passes `RELEASE_KIND`, `RELEASE_PREVIOUS`, `RELEASE_VERSION`,
`RELEASE_DATE`, `RELEASE_SOURCE`, `RELEASE_COMMIT`, `RELEASE_BRANCH` and `RELEASE_REMOTE` as Make
variables. Adapters consume those selections instead of independently choosing a
version or target. Git staging, commit creation, annotated tagging and atomic
push belong only to the common runner. Publishing and deployment stay separate.
`RELEASE_COMMIT` is empty until the exact staged release commit exists. Late
checks must read committed metadata and receipts from that selected SHA, rather
than equating it with HEAD. The current maintained adapter code runs the checks;
the runner does not reconstruct an old checkout or substitute new validation for
the older release. Update receipt verifier arguments and their source/tree/tag
bindings together during adoption. A failed consumer check still stops recovery.

## Nested validation and adoption fixtures

Release selections propagate through Make command-line variables, including
`MAKEFLAGS` and `MAKEOVERRIDES`. Preserve them in normal adapters and same-checkout
nested validation. An independently configured fixture owns its own selections:
clear inherited `MAKEFLAGS`, `MFLAGS` and `MAKEOVERRIDES` before its Make calls,
then supply the fixture's intended release variables explicitly.

The validation logger's `VALIDATION_REPOSITORY_ROOT` and
`VALIDATION_RUNNER_SNAPSHOT_PATH` bind its temporary source snapshot. They stop
at that runner's dispatch boundary; dispatched targets retain release selections,
failure-log policy and nesting depth. An independent fixture must establish its
own checkout and log destination, clearing inherited logger checkout/snapshot
identity when it can also run under older snapshots. Do not globally strip
release selections in the release runner to accommodate a fixture.

Qualify adoption fixtures through actual Make release overrides and the actual
logger in a distinct parent checkout, as well as standalone invocation. Use a
cheap parent gate sentinel to catch routing errors without starting a real gate;
verify that nested validation reaches its intended checkout, preserves selected
release identity and retains distinct failed-attempt logs. Keep consumer fixes
outside immutable shared snapshots until adopting a reviewed upstream revision.

## Authority and recovery

The maintainer invokes the one-shot commands. Agents never run them, even when a
push or version change has been authorized, because commits remain
maintainer-owned under the [engineering baseline](../DRAGGINZGAME.md). Agents
may use separate read-only or preparation phases within existing authorization.

Once version preparation may have started, rerun a normal target to
automatically reconcile the unfinished release at its saved version. The runner
selects that intent before computing a new candidate, including when metadata
has already been bumped. It retains the source/commit identity, UTC date,
destination, completed phases and evidence. Uncommitted preparation remains bound
to its saved source and kind. After the release commit exists, newer commits are
allowed only on its direct descendant history, with the exact staged tree, sole
source parent and release subject still verified. A dirty tree, changed release
payload, competing unfinished identities, conflicting tag or destination, or an
occupied lock stops recovery; the existence of a plan alone does not.

An unchanged same-kind retry finishes only the saved release. If HEAD contains
newer committed fixes, or a different increment is explicitly requested after the
release commit exists, the normal command first finishes the older release and
then computes the requested increment from the actual local version. Fresh
preflight and complete validation run against current source before preparation.
For example, an interrupted minor release prepared `0.265.0`; after committing a
callback fix while metadata still reads `0.265.0`, `make release-patch` reconciles
that release, then validates and prepares `0.265.1`. It never recreates the older
commit or tag. A failure in the new gate leaves the older release complete, keeps
both attempts' evidence, and permits the normal target to retry the new attempt.

Recovery checks the saved phase against actual local and remote state. It never
increments the prepared version again, recreates a matching release commit,
overwrites a tag or replays a push whose outcome cannot be established. A missing
push reply does not prove failure: matching remote branch and tag identities
complete that release without another push; a failed remote query stops it.
`make release-resume VERSION=X.Y.Z` remains available for explicit selection of
only that retained release, using the same reconciliation and conflict checks;
it never starts a next increment. Preserve
artifacts throughout recovery; publication and cleanup remain separate.

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
Supplement effect/interruption stubs with real Git index/tree checks at staging
boundaries. Shared Tooling's metadata fixture reuses existing history without
creating commits and exercises the actual adapter's failed-log retention.
