# Common release commands

Every `dragginzgame` repository, including Shared Tooling and repositories without
a publishable package, must provide these Make targets:

| Command | Version change | Example from `0.1.0` |
| --- | --- | --- |
| `make release-patch` | Increment patch | `0.1.1` |
| `make release-minor` | Increment minor; reset patch | `0.2.0` |
| `make release-major` | Increment major; reset minor and patch | `1.0.0` |

The names, version arithmetic and recovery contract are shared across repositories.
Each repository supplies its canonical version source, metadata file set,
validation gate, branch, remote and explicit delivery policy. Direct delivery is
the default; [PR delivery](#pr-delivery) adds review and fresh merged-commit
validation before tagging. A repository
without package metadata may use a dedicated version file; otherwise it uses
its latest finalized changelog version. Shared Tooling owns its current local
version in root `VERSION`, displayed by `make version`. Its undated changelog
heading names the next proposed release. Preparation updates both files and
stages them together; neither a version file nor a changelog heading proves
that a tag was pushed. Pre-release versions require a separately selected release plan;
these commands must reject ambiguous or unsupported version inputs.

## Required workflow

The default `RELEASE_DELIVERY=direct` workflow is:

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
5. **Commit and tag.** Create the explicitly authorized release commit with subject
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
matching release kind when invoking the explicitly authorized command; its computed
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

Direct delivery uses exactly this push shape with its saved selections:

```bash
git push --no-follow-tags --atomic -- "$destination" \
  "$push_source:refs/heads/$branch" "refs/tags/v$candidate:refs/tags/v$candidate"
```

`destination` is the sole push URL captured from the selected remote at entry.
The runner rechecks that selection after validation and before push, rejecting
changed or additional URLs. Observation and dispatch both use the captured URL,
so a later remote-name change cannot redirect the push.

URL-form push does not refresh named remote-tracking refs. After direct delivery
or completed resume verifies the exact remote tag and branch history, the runner
refreshes the selected branch's matching configured upstream from that observation.
Git derives the mapping, including custom fetch refspecs; fetch and push destinations
must agree. The conditional local update preserves newer/divergent or concurrently
changed tracking values. It prepares a Git ref transaction, checks that the ref is
direct while Git holds its lock, and only then commits the observation. A concurrent
symbolic replacement is preserved even when it resolves to the captured old commit;
the runner never dereferences or overwrites it. Other upstreams remain untouched.
This optional refresh uses core Perl IPC and Git's `update-ref --stdin` transaction
protocol. Failed preparation or type inspection aborts the optional update. Local
refresh failures report a fetch remedy without repeating commit, tag or push;
they do not undo confirmed delivery.

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

After `release-push-check`, both delivery policies recheck the committed payload
and require the selected annotated tag object to remain unchanged, including its
annotation. Direct delivery also checks the index independently of working files.
A completed direct resume verifies the local tag, exact remote tag object and
that the observed branch still contains the release commit. A known descendant
tip is valid; missing tags, conflicting history or unavailable observations stop
completion without recreating or pushing anything. Fetch unknown branch history
and reconcile actual conflicts before retrying; retain the completed plan and
earlier evidence.

## Makefile example

Use the optional `make/release.mk` include to keep standard command
routing and conflicting-goal admission at their shared owner:

```makefile
.DEFAULT_GOAL := help
RELEASE_REMOTE ?= origin
RELEASE_BRANCH ?= main
export RELEASE_DELIVERY ?= direct

include make/release.mk

.PHONY: help

help:
	@echo "Maintainer releases: release-patch, release-minor, release-major"

```

The include preserves the default goal and supplies `release-resume VERSION=X.Y.Z`.
Set `SHARED_TOOLING_ROOT` before including it for a nested reviewed snapshot.
Keep delivery admission and metadata adapters local. Attach existing admission
prerequisites to the four entrypoints and export cache-preparation selections
with target-specific variables; do not replace the shared recipes. For example:

```makefile
release-patch release-minor release-major release-resume: release-delivery-check
release-patch release-minor release-major release-resume: export MY_RELEASE_CACHE_PREPARE := 1
```

Preserve any consumer post-run reconciliation when adopting; a prerequisites-only
conversion does not reproduce an operation that previously followed the runner.
The include does not select PR delivery, add validation or metadata adapters,
publish packages, or clean artifacts. Keep those contracts explicit. Include
`make/release.mk` in the extra inputs to `check-release-commands.sh` when testing
an adopting Makefile with the runner at `scripts/ci/run-release.sh`. That smoke
checker substitutes the root runner; nested snapshots need a caller fixture
substituting their selected runner path, as the shared include tests do.
Direct runner invocation remains supported for specialized
entrypoints with independently qualified behavior.

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
| `release-merged-preflight` | Required for PR delivery: check finalized candidate metadata and prepare the exact merged checkout's dependencies/cache before repeating the complete gate. Do not bump or require the previous version. |
| `release-prepare-version` | Apply exactly the saved candidate and finalize notes; preserve dependency selection and verify all directly owned metadata. |
| `release-prepared-check` | Check the candidate and prepared metadata without another bump. |
| `release-files` | Print the explicit relative release paths, each terminated by NUL, and no explanatory output. |
| `release-commit-check` | Admit the entire exact release index, ensure it matches prepared metadata, and check source-bound validation evidence. |
| `release-committed-check` | Check `RELEASE_COMMIT` and its consumer-owned evidence binding. |
| `release-tagged-check` | Check or record exact tag-bound evidence for `RELEASE_COMMIT` without another Git effect. |
| `release-push-check` | Check the selected `RELEASE_COMMIT`, tag, evidence and destination before dispatch/reconciliation. |

### Dependency preparation before compiled adapters

Standard release entry points include locked cache preparation; maintainers
should not need a separate manual fetch after every selected dependency update.
Document the affected workspace graphs and exact preparation commands in the
consumer's release guide. An explicit request for that documented release
includes this network preparation, but never dependency upgrades or lock repairs.
Cargo's explicit offline environment/configuration settings remain authoritative.
Ordinary validation and standalone adapter commands remain offline.
Do not hard-code `--offline` onto the authorized preparation step or export an
offline setting across both preparation and validation. `--locked` preserves
the selected dependency graph while allowing missing inputs to be downloaded.
The same separation applies to authorized deployment preparation. See the
[Cargo network policy](../rules/cargo-dependencies.md#cargo-network-policy):
an expressly requested dependency update may resolve online before qualification,
whereas an ordinary release/deployment fetches its existing lockfile and does not
upgrade dependencies. Offline local-version synchronization and test fixtures
are separate from dependency updates that need new registry inputs.

If an adapter must compile before it can read the version or run preflight,
prepare its selected cache before compilation. Keep this within the runner's
normal adapter dispatch: select unfinished intent first, and never add a fetch
prerequisite that runs against possibly interrupted metadata before recovery
selection. A consumer can pass an internal preparation selection from its
release entry points to its launcher; do not infer permission from an adapter
name or enable it globally for ordinary checks.
Remove that internal selection before dispatching the compiled adapter so its
validation children cannot inherit online preparation permission.

For example, a Cargo launcher selected by an authorized release uses
`cargo +TOOLCHAIN fetch --locked`, followed by `cargo run --locked --offline`.
Its ordinary invocation uses `cargo +TOOLCHAIN fetch --locked --offline` before
the same offline compilation. Retain Cargo's original diagnostics and status
on failure, name the consumer's explicit fetch command, and stop before further
release effects. Do not switch an explicitly offline caller online on failure.

Interrupted manifest/lock writes may require a coherent scratch workspace for
compilation. Prepare that same scratch graph with the copied selected lock;
do not fetch against inconsistent tracked metadata or change its dependency
selection. Later qualification remains offline and bound to the selected source,
inputs and receipts. A locked fetch does not qualify the source or reconcile
release effects.

Qualify missing-cache preparation, explicit offline refusal and network failure
before compilation, unchanged locks, and interrupted metadata recovery through
the consumer's focused launcher fixture. Native support and adoption by other
consumers remain separately evidenced. Shared Tooling owns this convention;
consumer launchers own their workspace/toolchain and recovery inputs.

### Source admission diagnostics

When refusing uncommitted source, report the staged, unstaged and untracked
paths that violate the consumer's policy. Quote unusual path bytes and report
all observed violations. A failed Git observation must preserve its error and
be distinguished from a successfully observed dirty checkout. Never repair,
stage or commit a file or lockfile merely to make admission succeed.

Shell adapters can delegate to
[`check-release-source.sh`](../scripts/ci/check-release-source.sh) from the
checkout root. With no arguments it requires clean source. Repeat
`--allow RELATIVE-PATH` for exact consumer-owned release metadata exceptions;
these are literal paths, not globs. Shared Tooling allows `VERSION` and
`CHANGELOG.md`. The helper separately observes index, worktree and untracked
paths without writing the index, and rejects observation failures before
reporting admission. Existing native-language owners may implement the same
contract using their existing Git observation rather than adding a wrapper.

The calling adapter owns phase context. An initial preflight refusal should say
validation and version preparation have not started for that attempt. A later
prepared, committed or resumed check must describe its actual phase instead;
the reusable helper makes no blanket claim that validation never ran. Add the
helper explicitly when refreshing a shell adapter's snapshot. Qualify clean,
lock-only, hidden staged, unstaged, untracked and unusual-name cases, Git errors,
and unchanged file/index bytes; consumer release metadata allowances stay local.

The runner passes `RELEASE_KIND`, `RELEASE_PREVIOUS`, `RELEASE_VERSION`,
`RELEASE_DATE`, `RELEASE_SOURCE`, `RELEASE_COMMIT`, `RELEASE_BRANCH`, `RELEASE_REMOTE`,
`RELEASE_DELIVERY`, `RELEASE_PREPARATION_SOURCE` and `RELEASE_PREPARED_COMMIT` as Make
variables. Adapters consume those selections instead of independently choosing a
version or target. Git staging, commit creation, annotated tagging and atomic
push belong only to the common runner. Publishing and deployment stay separate.
`RELEASE_COMMIT` is empty until the exact staged release commit exists. Late
checks must read committed metadata and receipts from that selected SHA, rather
than equating it with HEAD. The current maintained adapter code runs the checks;
direct delivery does not reconstruct an old checkout or substitute new validation
for the older release. PR delivery has the separate merged checkout and fresh
evidence contract below. Update receipt verifier arguments and their source/tree/tag
bindings together during adoption. A failed consumer check still stops recovery.

## PR delivery

Repositories requiring review before release can explicitly select
`export RELEASE_DELIVERY ?= pr` in their Makefile after adopting the adapters and
fixtures below. All three standard targets retain the same interface. An explicit
`make release-patch RELEASE_DELIVERY=pr` also selects this policy; use the same
selection on retries. Never switch policies to work around branch protection.

The first invocation must start on the selected base branch at its published
commit. It runs preflight and complete validation, saves intent, switches to
`release/vX.Y.Z`, and uses the common metadata/staging/commit engine. It pushes
only that prepared branch and opens one same-repository PR into the selected base.
The runner never approves, merges or bypasses required checks. An open PR returns
status **75** (GNU Make reports a failed recipe/status 2), with its URL and retry
instructions. This is pending review, not release completion: a chained
`&& make publish` cannot proceed.

After the separately authorized merge, rerun the same normal release target.
Merge, squash and rebase merges are accepted when GitHub's recorded merged commit
is in the selected remote base history and its tree exactly equals the saved
prepared tree. A merge that changes the payload, including unrelated base changes,
requires reconciliation; fresh validation does not authorize silently expanding
the prepared release. The runner retains a detached checkout of the admitted
commit, runs `release-merged-preflight`, then repeats the **complete**
`release-verify` gate. Only successful fresh validation and consumer evidence
checks permit an annotated tag on that merged SHA and its atomic tag-only push.
The base branch is changed by the reviewed merge, never by the runner's push.

Both pushes retain `--no-follow-tags --atomic` and the captured destination:

```bash
git push --no-follow-tags --atomic -- "$destination" "$prepared_commit:refs/heads/$pr_branch"
git push --no-follow-tags --atomic -- "$destination" "refs/tags/v$candidate:refs/tags/v$candidate"
```

These are separate phases around review, not an atomic branch/tag pair. No
package publication, GitHub Release object or cleanup is implicit. No branch
deletion or checkout reset is implicit either. Finish the saved release first;
then deliberately return to and synchronize the base before requesting another
release. PR recovery never starts a follow-up increment in the same invocation.

### PR adapters and retained state

Adoption requires authenticated `gh`, jq, a Git version supporting `switch`,
`worktree` and `fetch --no-write-fetch-head`, and the sourced
`scripts/ci/release-pr.sh` beside the runner. PR lookup uses `gh api --paginate`
and jq to read every response page; it does not require the newer GitHub CLI
`--slurp` option. Failed, incomplete or malformed queries stop reconciliation
before another external effect. The initial implementation accepts
explicit github.com HTTPS/SSH push URLs and same-repository PRs; fork heads and
GitHub Enterprise destinations are outside this contract. Run from an ordinary
checkout without inherited Git directory, worktree, index or object-store overrides.
PR attempts share the common Git directory's release lock across linked worktrees.

Before merging, `RELEASE_SOURCE` identifies the original validated source,
`RELEASE_COMMIT` identifies the prepared commit once it exists, and
`RELEASE_PREPARATION_SOURCE` retains the original source. In the merged checkout,
both `RELEASE_SOURCE` and `RELEASE_COMMIT` identify the exact merged commit;
`RELEASE_PREPARED_COMMIT` identifies the original PR commit. Update receipt creation
and verification together: the earlier source's validation cannot qualify the
merged source, even when their prepared trees match. Adapters run from the
retained merged checkout and must prepare its tools, dependencies and evidence
paths explicitly. Preserve all earlier logs and build artifacts.
The ordinary `release-preflight` runs initially on the base and again immediately
before metadata preparation on the release branch; branch checks must admit those
explicit PR phases. The merged preflight admits finalized candidate metadata and
detached HEAD instead of applying the previous-version preparation checks.

PR plans use `release-pr-plan-1` and a required `X.Y.Z.plan.pr.json` sidecar for
repository, prepared commit, PR number, creation intent and merged commit. The
common Git directory retains these alongside API requests/responses and
`release-state/X.Y.Z.merged/`. Do not delete the plan, sidecar, local release branch
or merged checkout to unblock a retry. A changed/dirty retained checkout stops
tagging; retain the evidence and reconcile the actual change. An interrupted or
failed merged validation reruns its preflight and complete gate at the saved SHA.

Before repeating an external effect, the runner observes exact branch, PR and tag
identities. A lost successful push/create reply is recovered without another
effect when the saved identity is observable, including an already-merged PR
whose remote head was automatically deleted. A failed remote query never permits
replay. An explicit GitHub creation rejection (HTTP 401/403/404/422) permits retry
after correcting access/inputs. A lost or server-error creation response with no
observable PR remains uncertain: inspect the retained request/response and GitHub
state. Only after establishing that no creation occurred may an operator explicitly
clear the sidecar's `attempted` flag while preserving the remaining identity and
evidence. Never clear it merely because a query returned no results.

Qualify `scripts/ci/test-release-pr.sh` alongside the direct runner fixture. It
uses real isolated Git histories/bare destinations and substituted GitHub API
responses to cover review, merge/squash/rebase, fresh evidence, conflicts and lost
replies. Consumers still need actual adapter/receipt tests and native host
qualification. A fixture pass does not prove real GitHub permissions, branch
protection or live PR delivery.

## Nested validation and adoption fixtures

Release selections propagate through Make command-line variables, including
`MAKEFLAGS` and `MAKEOVERRIDES`. Preserve them in normal adapters and same-checkout
nested validation. The release runner, validation logger and formatting hook use
`scripts/ci/check-make-execution.sh` to reject inherited ignore-errors, dry-run,
question, touch and version-only modes before dispatch. An isolated Make probe
must execute a harmless failing recipe and report its failure; it loads no consumer
Makefile. Ordinary release variables and jobserver settings remain inherited by
the actual targets. Rerun without the rejected mode. Consumer
recipes must still propagate failures and execute their declared gate.

An independently configured fixture owns its own selections:
clear inherited `MAKEFLAGS`, `MFLAGS`, `MAKEOVERRIDES`, `GNUMAKEFLAGS` and
`MAKEFILES` before its Make calls,
then supply the fixture's intended release variables explicitly, including
`RELEASE_DELIVERY`. Direct fixtures must not inherit an enclosing PR selection.

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

### Fixture ownership

Consumer adoption runs the canonical `scripts/ci/test-release-runner.sh` suite.
It simulates repository and release effects; its native Git delegate accepts only
`hash-object --stdin`, without object writes. Any attempted real Git operation
fails the suite even if a negative case consumes its immediate failure status.

`scripts/ci/test-release-tracking.sh` separately owns real-Git tracking and lock
races in disposable repositories, including commits, tags and local bare pushes.
The complete Shared Tooling portable suite runs both entrypoints on Linux and
both macOS hosts. Consumers whose fixture authority excludes those effects can
select the simulation suite without vendoring or invoking the native suite.
The PR and metadata owner fixtures also use real disposable Git histories; this
split does not make the entire portable suite simulation-only.

Keep consumer tests for their own contracts, using this ownership map before
deleting duplicate scenarios or extracting test support:

| Assertion | Canonical owner | Consumer obligation |
| --- | --- | --- |
| Phase order, all increments, restart before preparation, saved-version recovery | Shared runner fixture | One actual Make-to-adapter wiring/recovery case. |
| Lost commit/tag/push replies, destination changes, exact atomic refspecs, locks | Shared runner fixture | Do not copy the runner's fake Git machine solely to repeat these cases. |
| Allowed release files, metadata changes, independent locks/package sets | Consumer adapter fixture | Use actual selections and verify unrelated staged/unstaged input refusal. |
| Receipt identity, selected older `RELEASE_COMMIT`, tag/evidence binding | Consumer adapter fixture | Preserve negative identity and payload tests across recovery. |
| Failed preparation restoration and product-specific side effects | Consumer adapter fixture | Prove its own transaction and retained evidence. |
| Publication eligibility and registry behavior | Consumer publication fixture | Keep separate from the branch/tag release proof. |

For IC Backup and IC Blob Storage, map local assertions to these obligations
before removing runner-only scenarios. Their receipt and package/lock checks
remain local, even when both suites simulate Git. Extract common effect support
only if the remaining adapter cases demonstrate that need. Raw fixture LOC is
not a deletion target. Existing adoption work is tracked in
[IC Backup #18](https://github.com/dragginzgame/ic-backup/issues/18) and
[IC Blob Storage #22](https://github.com/dragginzgame/ic-blob-storage/issues/22).

### Authorized release effects

The maintainer may invoke a one-shot command or explicitly ask an agent to run
the selected release for the identified repository and destination. That request
covers the documented complete gate, version preparation, release commit, tag
and selected delivery effects. Direct delivery includes the atomic branch/tag
push; PR delivery includes its branch/PR, fresh validation after an independently
authorized merge, and exact tag push. Permission to fix code, create a commit, open a PR,
push a topic branch or change a version alone is not permission to run this flow.
Agents may use separate read-only or preparation phases within existing scope.

Ordinary contributions follow the [PR rules](../rules/contributions.md). The
runner defaults to direct delivery and supports explicit PR delivery under
the contract above ([#42](https://github.com/dragginzgame/shared-tooling/issues/42)).
Neither policy bypasses branch protection or required reviews. Publication,
deployment and cleanup remain separate effects.

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

An unchanged same-kind retry finishes only the saved release. In direct delivery, if HEAD contains
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

## Shared changelog finalization

Release adapters reuse `scripts/ci/finalize-release-changelog.awk` for candidate
selection and heading rewriting instead of implementing another changelog parser:

```bash
awk -v version="$RELEASE_VERSION" -v previous="$RELEASE_PREVIOUS" \
  -v date="$RELEASE_DATE" \
  -f scripts/ci/finalize-release-changelog.awk CHANGELOG.md > "$candidate"
```

The adapter validates these identities from the saved release intent and owns
the temporary candidate path. Check the command's status before using its
output; never redirect directly over the input. Preflight can discard a
successful candidate without modifying the changelog. Preparation installs it
inside the consumer's existing metadata transaction, preserving backups,
rollback, receipts and exact prepared-payload checks.

Pass the saved previous version even after package metadata has been bumped.
Undated numbered sections at or below that version remain history. One pending
numbered section must agree with the target; competing candidates or a target
already dated differently are conflicts. The helper also understands the older
`Draft` input, but maintained notes follow the numbered-draft rules. With no
draft it creates the selected heading; it does not invent release-note content.

Horizontal spaces and tabs in headings are normalized only for classification;
an already-dated target with extra separator whitespace cannot become a second
release entry. Retained content keeps its original bytes, including a historical
body without a terminal newline. A draft moved from EOF gains only the separator
needed before the following history. The helper reads the document as one awk
record using regular-expression `RS`, supported by the system awks on the host
matrix; it does not depend on GNU awk's `RT` extension.

The optional `-v allow_finalized=1` admits exactly one already-finalized target
at the top with the same date and no pending candidate. Select it only where
the adapter's recovery contract admits that exact state; it does not establish
source, payload or publication identity. The helper is not a general historical
ledger linter or an empty-note gate. Before deleting local selectors, review
their extra checks separately: preserve independent corruption/identity checks,
and reconcile prose requirements with the [changelog rules](../rules/changelogs.md).
Do not silently change a consumer's refusal or recovery behavior during adoption.

## Adoption and verification

The Makefile pattern specifies a contract; adding this document does not install
helpers or prove consumer adoption. Consumers implement or align their targets,
vendor a clean reviewed Shared Tooling revision with this document,
`scripts/ci/run-release.sh`, `scripts/ci/next-release-version.sh` and any selected
changelog helper, together with `scripts/ci/check-make-execution.sh`, in the
[governance snapshot](consuming-snapshots.md), and qualify the workflow on their
declared Linux and macOS hosts. PR adopters also include `scripts/ci/release-pr.sh`,
add `release-merged-preflight`, and qualify fresh merged-source receipts and their
complete gate from the retained checkout. Report upstream policy changes separately from
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
