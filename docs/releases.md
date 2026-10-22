# Common release commands
<!-- Shared companions: docs/cargo-publication.md -->

Every `dragginzgame` repository, including Shared Tooling and repositories without
a publishable package, must provide these Make targets:

| Command | Version change | Example from `0.1.0` |
| --- | --- | --- |
| `make release-patch` | Increment patch | `0.1.1` |
| `make release-minor` | Increment minor; reset patch | `0.2.0` |
| `make release-major` | Increment major; reset minor and patch | `1.0.0` |

These are user-operated commands. Preparing code or notes for a named release
does not authorize an agent to invoke them, their recovery target or mutating
adapters, or to perform their staging, commit, version or tag effects manually.
Follow the [user-owned Git and release boundary](../rules/contributions.md#user-owned-git-and-release-flow)
and any approved local maintainer-only restriction. Only an express delegation
of execution permits an agent to run a release within that policy.

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

Shared's metadata adapter replaces `VERSION` last. Failed preparation retains
the original files and rendered candidates under the reported Git-owned scratch
directory, including after a partial replacement. Retry uses the existing release
reconciliation; it does not delete earlier evidence or automatically restore files.
Successful attempts remove only their own scratch directory.

## Required workflow

The default `RELEASE_DELIVERY=direct` workflow is:

1. **Preflight.** Select exactly one release kind, compute and display the current
   and candidate versions, repository, branch, remote and exact effects before
   mutation. Reject conflicting release selections, an existing candidate tag,
   unrelated uncommitted work, missing inputs and concurrent releases. Check
   staged and unstaged paths independently: a working file restored to HEAD can
   still have different staged content. Check pending changelog/candidate
   agreement here, before the validation gate or saved preparation intent. Prepare
   the selected dependency cache and required executable tools through their
   existing setup/check targets before an offline gate without changing the
   lockfile selection. Authorized dependency changes must already have
   [prepared every affected independent lockfile](../rules/cargo-dependencies.md#preparing-authorized-dependency-changes);
   cache fetching stays locked and does not repair stale dependency graphs.
   Never infer a deployment destination or credentials.
2. **Validate.** Run the repository's documented complete release gate against
   the selected source and dependencies. Patch, minor and major use the same
   gate. Rust consumers run their existing Clippy gate before expensive builds,
   tests and packaging, with failure stopping later stages even under parallel
   Make. The same ordering applies to Rust deployment entrypoints under the
   [shared validation policy](../DRAGGINZGAME.md#validation-and-evidence).
   Stop before version mutation if validation fails; retain failure logs
   and build artifacts. Coordinated Rust crates with unchanged versions may use
   [local archive qualification](../rules/cargo-dependencies.md#qualifying-coordinated-package-archives-before-a-version-bump)
   when registry verification selects an older published dependency. This does
   not move version preparation earlier or replace publication admission.
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
Assess that impact at the released package's own boundary under the
[package-impact rules](../rules/changelogs.md#classify-the-released-packages-own-impact).
An upstream tooling hard cut does not by itself select `release-minor` for a
consumer library; compatible internal tooling adoption uses patch notes.

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

Use the [consumer Make entrypoint](make-interface.md) to keep standard command
routing and conflicting-goal admission at their shared owner (`make/release.mk`):

```makefile
.DEFAULT_GOAL := help
RELEASE_REMOTE ?= origin
RELEASE_BRANCH ?= main
export RELEASE_DELIVERY ?= direct

include make/consumer.mk

.PHONY: help

help:
	@echo "Maintainer releases: release-patch, release-minor, release-major"

```

Non-Rust repositories set `SHARED_TOOLING_RUST := 0` before the include.
The include preserves the default goal and supplies `release-resume VERSION=X.Y.Z`.
Its adjacent `make/execution.mk` companion rejects Make ignore-errors and
non-executing modes before recipes run, using the existing execution probe.
Keep both the Make companion and `scripts/ci/check-make-execution.sh` selected.
Admission resolves its probe relative to the selected include, independently of
`SHARED_TOOLING_ROOT`; a missing companion refuses without consulting another
snapshot. It probes the running GNU Make executable (`MAKE_COMMAND`), preserving
the invocation's execution flags. A consumer's recursive `MAKE` command may carry
arguments such as `-f Makefile -f overrides.mk`; those files and arguments never
enter the isolated admission probe. Recursive invocations qualify their own modes.
The probe checks `MAKEFLAGS` and Make's retained `MFLAGS` independently, so clearing
or replacing `MAKEFLAGS` cannot hide an unsafe outer invocation. Leave `MFLAGS`
generated by GNU Make: command-line and Makefile assignments to it are refused
because they erase that independent evidence. Safe `MAKEFLAGS` selections,
parallel execution and ordinary release variables remain supported.
Set `SHARED_TOOLING_ROOT` for runtime runner routing to a nested reviewed snapshot;
consumer target-specific overrides still apply to recipes.
Keep supported delivery choices and metadata adapters local. A consumer that
supports a subset of `direct`/`pr` sets `RELEASE_DELIVERY_POLICY` before the include
to one regular data file, with one supported mode per line. Make admission and
standalone adapters share `check-release-delivery.sh`; see the
[policy contract](make-interface.md).
The canonical hook roster includes PR delivery's `release-merged-preflight`.
Attach additional product admission prerequisites to the four entrypoints and export cache-preparation selections
with target-specific variables; do not replace the shared recipes. For example:

```makefile
release-patch release-minor release-major release-resume: local-release-preflight
release-patch release-minor release-major release-resume: export MY_RELEASE_CACHE_PREPARE := 1
```

Preserve any consumer post-run reconciliation when adopting; a prerequisites-only
conversion does not reproduce an operation that previously followed the runner.
The include does not select PR delivery, add validation or metadata adapters,
publish packages, or clean artifacts. Keep those contracts explicit. Include
`make/release.mk` (or `make/consumer.mk` for the complete common interface)
as a root input to `check-release-commands.sh`; its declared companion closure
supplies the execution probe, delivery checker and other dependencies when testing
an adopting Makefile with the runner at `scripts/ci/run-release.sh`. That smoke
checker binds `SHARED_TOOLING_ROOT` to its disposable checkout, overriding parent
environment and ordinary Makefile assignments. It substitutes the root runner;
nested snapshots need a caller fixture
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

### Source changes after saved preparation intent

Normal direct release targets automatically restart a saved `prepare` attempt
when HEAD is a newer descendant commit and the index, worktree and untracked
source inventory are clean. The saved index tree must be empty, no preparation
file set (`PLAN.files`, including symlinks) may exist, and the recorded branch,
remote and exact push destination must still agree. Current canonical metadata
must read either the saved previous version or the saved candidate. When it reads
the candidate, a committed bump alone is insufficient: its local annotated tag
must lie between the saved source and current HEAD, the exact tag object must
match the saved destination, and the observed destination branch must contain
that tagged commit. Missing or conflicting tags, failed remote observations and
unavailable/divergent branch history stop before archiving the plan or running
another gate. Keep the saved plan and reconcile the intended release; do not
remove its evidence or invent a tag to bypass the refusal. This checks Git
delivery, not registry publication.

The runner archives the original plan unchanged under
`release-state/X.Y.Z.attempt.*/`, while holding the release lock, then reads the
**current** canonical version and applies the **requested** increment. It repeats
preflight and the complete validation gate against current HEAD before saving
another preparation intent. Earlier logs, receipts and build artifacts remain
bound to their original attempt; adapters must preserve them on revalidation.
The archived intent is superseded local preparation, not proof of successful
release, delivery or publication.

For example, after `0.6.3 -> 0.6.4` preparation fails and its metadata is committed,
`make release-patch` refuses to select `0.6.5` while delivery of `0.6.4` is
unconfirmed. It preserves the `0.6.4` plan and all earlier evidence. If `0.6.4`
has separately been tagged and delivered with the identities above, the retry
can archive that stale preparation, validate current source and prepare `0.6.5`.
If committed metadata still reads `0.6.3`, it validates and prepares `0.6.4`
instead. Patch, minor and major use this same admission; a different requested
increment does not bypass the delivery check.

This boundary depends on the consumer adapter contract: preparation changes
**local release metadata only**. It may retain local backups, journals and
receipts, but must not stage, commit, create tags, publish, deploy or dispatch
other external effects. The newer clean commit records the disposition of those
metadata changes; the runner does not claim preparation never ran or discard
consumer transaction evidence. Consumers must reconcile any outstanding local
transaction in preflight before permitting fresh preparation.

At unchanged HEAD, interrupted preparation still resumes its exact saved version.
`make release-resume VERSION=X.Y.Z` also retains exact-source recovery and never
selects another version. Changed source during a running gate/preparation, dirty
or divergent history, conflicting identities, staging evidence and occupied
locks still stop the command. Plans stopped at `stage` or later retain exact
payload and effect reconciliation. PR delivery retains its review-branch,
worktree and external-effect recovery; it does not use this direct-only restart.
Ordinary agent repair does not authorize executing a real release or recovery.

### Consumer adapters

Consumer Make targets provide these adapters:

| Target | Contract |
| --- | --- |
| `release-version` | Print only the canonical `X.Y.Z` version. |
| `release-preflight` | Check candidate/changelog agreement; admit only declared release metadata as dirty work; inspect staged and unstaged paths separately, reject unrelated untracked paths, and prepare the selected cache and executable tools. |
| `release-verify` | Run the same complete gate for every release kind, with offline tool admission before expensive checks. |
| `release-merged-preflight` | Required for PR delivery: check finalized candidate metadata and prepare the exact merged checkout's dependencies/cache and selected executable tools before repeating the complete gate. Do not bump or require the previous version. |
| `release-prepare-version` | Apply exactly the saved candidate to local metadata and finalize notes; preserve dependency selection and verify all directly owned metadata. No staging, commits, tags or external effects. |
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
Authorized development and validation also include separate locked source-cache
preparation under the Cargo network policy below. Deliberately offline gates and
standalone adapter commands retain their offline contract; a missing cache does
not enable an implicit online retry inside them.
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

### Selected executable tools before validation

`cargo fetch --locked` prepares package sources, not installed CLI executables.
After admitting a coherent source/candidate and preparing its locked graph,
`release-preflight` invokes the consumer's existing narrowly selected tool setup
target, followed by its offline admission target. Document these commands locally;
the standard release request includes their preparation effects. Reuse valid
installations and preserve older selections and failed evidence. Keep explicit
offline settings authoritative and propagate preparation failures before the
expensive gate or version mutation.

Keep this ordering in the consumer adapter. Do not add an unconditional fleet-wide
`make install-tools`, another selection catalog or new installer to the shared
runner. Do not run setup as an entrypoint prerequisite before saved-release
reconciliation, against partially written manifests/locks, or replay it blindly
after preparation, commit or tag effects. PR merged preflight prepares the admitted
merged checkout's exact selections before its fresh validation.

The complete validation gate and standalone qualification commands also run their
offline tool checks before dependent builds/tests, even when release preparation
was bypassed. Enforce sequencing under parallel Make with ordered recipes or real
dependency edges; unordered sibling prerequisites do not establish order. Ordinary
tool checks never install or download tools. A caller adds its own explicit setup
command to the shared installer's selection diagnostic, preserving the failed status.

The consumer adapter also selects the admitted executable path for its children.
Standard `make release-*` commands must work without a per-shell tool-path export.
An installation in one process cannot export a variable into the calling shell;
adding a setup target alone does not wire its result into the release adapter.
Capture a successful offline check before dispatching the helper, preserving a
nonzero status even if the check printed a plausible path. For saved-release
checks, select against the admitted saved source or retained original lock,
not partially prepared live metadata. Keep installation in fresh preflight after
source/recovery admission, never in a rendering or recovery check.

For adoption, prove an installed version N / selected N+1 prepares N+1 before
builds, reuses it on retry and leaves locks, older installations and server bytes
unchanged. Cover standalone early refusal without installation/build dispatch,
explicit offline and network failures, changed selections, invalid receipts,
installation locks and saved-release recovery. Qualify the consumer's actual
ordering under parallel Make and on its supported native hosts. Shared contract
and installer coverage do not establish consumer adoption; coordinate remaining
adoption through [#96](https://github.com/dragginzgame/shared-tooling/issues/96).

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
Before preparation, the repository lookup must return one JSON object whose
complete `full_name` exactly matches the captured push destination. Trailing
newlines and malformed response shapes are rejected before shell extraction.
Each raw lookup response is retained separately under `release-state/`, including
failed observations followed by a successful retry.

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
saved identities and merged API identities must match their complete JSON
strings; trailing newlines are rejected before shell extraction or state writes.
Malformed input remains retained for diagnosis rather than silently normalized.
The common Git directory retains these alongside API requests/responses and
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

Shared Tooling owns the canonical `scripts/ci/test-release-runner.sh` regression
suite. Consumers qualify their actual Make/metadata/logger wiring, source admission
and evidence custody against the reviewed snapshot; they need not rerun the
producer's exhaustive implementation suites in every ordinary CI run. The owner
suite simulates repository and release effects; its native Git delegate accepts only
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
| Shared publication-helper source, tag, destination and registry admission | Shared publication fixture, including real local Git | Keep actual adapter refusal before upload; do not duplicate the generic eligibility matrix. |
| Consumer publication selection and upload recovery | Consumer publication fixture | Prove exact package/registry arguments, upload status and refusal to repeat an already-present version; keep separate from branch/tag release proof. |

For IC Backup and IC Blob Storage, map local assertions to these obligations
before removing runner-only scenarios. Their receipt and package/lock checks
remain local, even when both suites simulate Git. Extract common effect support
only if the remaining adapter cases demonstrate that need. Raw fixture LOC is
not a deletion target. Existing adoption work is tracked in
[IC Backup #18](https://github.com/dragginzgame/ic-backup/issues/18) and
[IC Blob Storage #22](https://github.com/dragginzgame/ic-blob-storage/issues/22).

### Authorized release effects

The maintainer owns execution. Where local policy permits delegation, they may
explicitly ask an agent to run the selected release for the identified repository
and destination. Naming a version, preparing a release, completing validation or
asking for push readiness is not that delegation. An explicit execution request
covers the documented complete gate, version preparation, release commit, tag
and selected delivery effects. Direct delivery includes the atomic branch/tag
push; PR delivery includes its branch/PR, fresh validation after an independently
authorized merge, and exact tag push. Permission to fix code, create a commit, open a PR,
push a topic branch or change a version alone is not permission to run this flow.
Agents may inspect release state and prepare source, dependencies and changelog
notes within existing scope. Mutating version preparation, staging and saved
release recovery require their own explicit execution authority; calling a
helper directly does not bypass the boundary.

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
to its saved source and kind at unchanged HEAD. A newer clean descendant commit
can supersede direct `prepare` intent under
[the restart contract](#source-changes-after-saved-preparation-intent), with current
metadata selection and fresh complete validation. A committed candidate cannot
be incremented again until its Git delivery is confirmed under that contract.
After the release commit exists,
newer commits are
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
For a crates.io package using the workspace's stable version, consumers may reuse
the [read-only publication admission helper](verification-helpers.md#read-only-cargo-publication-admission)
while keeping upload authority, package policy and recovery local.

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
already dated differently are conflicts. The selected numbered candidate must
be the first release heading; `Draft` and misplaced candidates are rejected.
Both `## [X.Y.Z]` and `## [X.Y.Z] - Unreleased` are accepted pending spellings.
Release preparation writes `## [X.Y.Z] - YYYY-MM-DD` using the saved UTC release
date before the release commit, tag and push; no manual heading edit is needed
for the standard flow. If preparing a release manually, replace `Unreleased`
with that date before committing/tagging/pushing the release. Unsupported
suffixes are refused with the source line and accepted selected heading before
any candidate output is emitted; conflicting dates and duplicate or misplaced
candidates remain separate diagnostic causes.
With no candidate, the first release heading must identify the saved previous
version; the helper then inserts the selected heading without inventing content.

Horizontal spaces and tabs in headings are normalized only for classification;
an already-dated target with extra separator whitespace cannot become a second
release entry. Retained content keeps its original bytes, including a historical
body without a terminal newline. Candidates are never moved from history.
The helper reads the document as one awk
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

For a caller-admitted Cargo upload or dry run, the optional
[publication attempt helper](cargo-publication.md) records one exact selection
and command. It does not replace consumer registry/provenance admission or
partial-upload recovery, and release execution never invokes it implicitly.

The Makefile pattern specifies a contract; adding this document does not install
helpers or prove consumer adoption. Consumers implement or align their targets,
vendor a clean reviewed Shared Tooling revision with this document,
`scripts/ci/run-release.sh`, `scripts/ci/validate-release-inputs.pl`, its
`StableVersion.pm` companion and any selected
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

## Cargo release candidate rendering

IC Build owns manifest and lockfile candidate rendering. The calling consumer
adapter selects its released `ic-build-release-metadata` executable through the
consumer's offline admission interface. Before dispatching Shared's low-level
Cargo helper, it supplies `CARGO_RELEASE_METADATA_EDITOR` as that absolute
executable path and `CARGO_RELEASE_METADATA_MAX_BYTES` as a positive per-document
byte allowance. The low-level helper does not install or discover tools through
PATH or sibling checkouts. Standard release entrypoints perform the consumer's
admitted preflight setup and path selection themselves; they must not require
the maintainer to export these variables manually.

For direct low-level invocation, explicitly prepare the pinned published package,
then capture its offline-admitted selection before running the helper:

```sh
bash scripts/dev/install-rust-tools.sh --package ic-build --version 0.18.0 \
  --bin ic-build-release-metadata --profile debug || exit "$?"
CARGO_RELEASE_METADATA_EDITOR="$(bash scripts/dev/install-rust-tools.sh \
  --package ic-build --version 0.18.0 --bin ic-build-release-metadata --profile debug --check)" || exit "$?"
export CARGO_RELEASE_METADATA_EDITOR
export CARGO_RELEASE_METADATA_MAX_BYTES=8388608
```

The byte allowance above is a fixture example; consumers choose a limit for their
own documents. Shared's Cargo adapter and saved-source context fixtures require
these same explicit selections. Common tool setup does not install this optional
release tool unless the consumer includes it in its local extensions. The
published package requires Rust 1.91 or later.

Direct callers of the retired `render-cargo-workspace-version.pl` use the editor's
`--max-input-bytes LIMIT manifest SOURCE PREVIOUS CANDIDATE [WORKSPACE_INPUT_JSON]`
operation. Callers of `rewrite-local-lock-versions.pl` use
`--max-input-bytes LIMIT lock LOCKFILE PREVIOUS CANDIDATE PACKAGE...`.
Saved-source callers retain the admitted context JSON, including `source_root`;
source/transaction custody and independent candidate checks stay with the caller.
The Shared Cargo adapter delegates these operations using the selections above.

The command uses TOML-owned literal spans and complete context admission. It
preserves unrelated bytes, source-qualified lock references, saved-source
identities and simple local requirement precision. Shared Tooling retains source
admission, originals, changelog preparation, independent locked/offline Cargo
graph checks and restoration on renderer or graph failure. Keep saved-byte
comparison separate from parsed semantic equality at its owning boundary.

## Coordinated Cargo metadata adapter

`scripts/ci/cargo-release-metadata.sh prepare|check PRIMARY_PACKAGE` owns
the reusable Cargo-only metadata flow. Adopt it as a snapshot root; its version
reader, changelog finalizer and preparation transaction are required companions.
It requires root `Cargo.toml`, `Cargo.lock`, `CHANGELOG.md`, workspace-inherited package versions
and no source-less packages outside the workspace. It admits actual workspace
and lock identities before mutation; independently versioned packages need their
consumer-owned adapter. Existing adapters are not replaced by adoption alone.

Preparation delegates installation and restoration to
`release-metadata-transaction.sh`. Every candidate finishes rendering before any
live file is installed. Failed attempts retain `original/` and `candidate/`
directories under the printed preparation path.

The caller supplies `RELEASE_PREVIOUS`, `RELEASE_VERSION`, `RELEASE_DATE`, prepared
Cargo, Perl, jq, Mike Farah yq and the explicitly selected IC Build editor and
byte allowance above. Preparation uses the locked offline graph and the editor.
It independently parses the candidate before installation, preserves manifest layout, file modes and external lock selections,
restores all three original files on failure and retains failure custody. It does
not run Cargo-edit or Cargo-sort; consumer ordering gates remain separate.
Catchable INT/TERM interruptions restore the same originals and preserve their
exit status. Unsupported inputs refuse before installation.
The `check` operation uses `RELEASE_COMMIT` when selected, exporting that exact
committed source instead of reading newer working metadata. No version selection,
fetch, tool installation, Git staging/ref mutation or publication is implicit.
Consumer preflight and late source/index admission remain outside this helper.

When `check` or `check-graph` dispatches Cargo metadata, failures retain its stdout
(including an empty JSON file), streamed stderr, separate Cargo/logger statuses,
the selected physical root and copies of its root `Cargo.toml`/`Cargo.lock`.
A Cargo failure takes priority over a simultaneous logger failure; a logger
failure also refuses an otherwise successful check. Root input copies do not freeze member manifests or Cargo
configuration in a live workspace. A selected committed-source check additionally
retains its exported tree. Neither path retries Cargo or repairs the lockfile.

IC Metrics adopts the corrected helper from committed Shared Tooling 0.11.1
through normal snapshot distribution, retaining its local policy in a thin adapter.
Other Cargo consumers must retain their own file set, recovery and metadata policy
when qualifying this adapter; it is not a fleet-wide automatic replacement.

### Pure release input admission

`scripts/ci/validate-release-inputs.pl version X.Y.Z` accepts canonical stable
versions with decimal u64 components and prints those three components separated
by tabs. It compares large values as strings, preserving integers above 2^53.
`date YYYY-MM-DD` validates the Gregorian date (years 0001 through 9999) and
prints its exact spelling. Both refuse extra whitespace, CR/LF and malformed
values, perform no file IO and select no release effects. Consumer adapters can
use this same grammar at each saved-input/receipt boundary.

`next-version X.Y.Z patch|minor|major` calculates the next version with exact
decimal arithmetic and the same u64 component ceiling. Only the incremented
component needs headroom; lower components reset to zero for minor/major bumps.
Overflow, invalid inputs and unsupported increments produce no candidate.
The public `next-release-version.sh X.Y.Z patch|minor|major` entrypoint delegates
to this calculation and returns exit 2 for invalid inputs; existing adapters do
not need an invocation change.
The runner admits generated and retained dates before dispatching release
callbacks. Consumers still own metadata, selected identity, receipts and replay.

## Reusing metadata preparation and evidence custody

Consumers with custom metadata rendering can source
`scripts/ci/release-metadata-transaction.sh` and call:

```bash
release_metadata_prepare "$state/$RELEASE_VERSION.prepare.XXXXXX" \
  render_metadata verify_metadata Cargo.toml Cargo.lock CHANGELOG.md
```

`render_metadata ORIGINAL_ROOT CANDIDATE_ROOT` writes each selected candidate
under the supplied root. `verify_metadata` validates the installed working payload.
Callbacks must explicitly propagate every failed observation, or execute an
independent script with its own strict shell settings; do not rely on a caller's
`errexit` when a function is invoked in a conditional context. The helper admits
all original files before rendering and all candidate files before installation.
Nested relative names and spaces are supported; noncanonical paths, symlink
traversal, non-regular files and duplicate selections refuse. Existing destination
modes are retained.

The helper owns backup, installation and caught-failure restoration only. It
preserves originals/candidates on failure, including failed restoration, and
prints their location before preparation. It does not promise multi-file atomicity
or crash durability. An uncatchable interruption retains evidence for the
consumer's existing reconciliation; this helper never decides whether to resume,
rerender or retry. Existing pending releases must finish with their owning adapter
before incompatible receipt formats are adopted. No receipt migration is implied.

The consumer retains selected files, exact source/commit qualification, metadata
semantics, gates, version and delivery decisions. It must hold exclusive source
and state custody, supply a trusted temporary parent outside selected files, and
keep callbacks within those contracts. A successful verifier must not change the
rendered payload. The helper does not execute Git/Cargo or publication effects.

`scripts/ci/release-receipts.sh` supplies atomic single-receipt write/check and
`release_receipt_archive TEMPLATE FILE...`. Archive admits the complete old set
before copying, preserves it before append/replacement, and retains partial
archives on failure. Missing entries are permitted; links, non-regular entries
and duplicate basenames refuse. Receipt schemas and multi-file relationships
remain consumer policy. These sourceable helpers leave caller traps intact;
preparation installs its restoration traps only inside its own subshell.

Host's adoption retains its original validation receipt format and reconstruction
of expected release bytes from the selected source. Testkit and Backup have
additional metadata contracts; they must qualify their own callbacks and recovery
fixtures before replacing their local preparation mechanics.
