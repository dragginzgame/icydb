# Consuming Shared Tooling Snapshots

CI and release behavior must not depend on a sibling checkout, a moving Git
branch, or network availability. Consumers vendor a reviewed file set and
record its exact Shared Tooling source revision.

Directory paths must not contain LF or CR characters. Snapshot refresh and
verification reject them in both supplied and resolved physical paths, including
ancestor directories and symlink destinations. They never trim a forbidden name
into another checkout. Rename an affected operational directory explicitly before
using these commands; existing artifacts are not renamed or deleted automatically.
Spaces and ordinary physical aliases remain supported. Snapshot-relative file
and manifest paths also reject LF, CR and tab characters.

## Initial snapshot

Run the refresh helper from a clean Shared Tooling checkout:

```bash
/path/to/shared-tooling/scripts/distribution/refresh-consumer.sh \
  --consumer /path/to/consumer \
  --file scripts/ci/verify-file-checksum.sh \
  --file scripts/ci/verify-shared-tooling-snapshot.sh \
  --file scripts/ci/check-make-execution.sh \
  --file scripts/ci/run-validation-targets.sh
```

The helper validates every source path, exports regular files and their
executable modes from the selected Git revision at the same relative paths,
and writes `.shared-tooling.snapshot` in the consumer. Ignored or otherwise
uncommitted files cannot enter the snapshot, and concurrent working-tree edits
cannot change the exported bytes. The manifest
records format version `1`, source remote, source commit, and the SHA-256 digest
and executable state of every vendored file.

Refresh also writes a `# version<TAB>X.Y.Z` annotation from `VERSION` at that
exact source commit. The committed file must contain a canonical stable version;
the exporter never reads its display version from mutable working-tree bytes or
changes the consumer's own `VERSION`. This descriptive annotation keeps the
existing v1 integrity records intact and is understood by the current verifier
and fleet report. A snapshot without it displays `unrecorded`; its next refresh
records the selected source version. Malformed or duplicate version annotations
are rejected by the current verifier and refresh helper.

The commit remains the exact source identity: several commits can carry the same
version, and an annotation does not prove a release tag or publication. Local
verification checks files against the reviewed manifest, not the truth of its
source claims against GitHub. The annotation is not a second version selection
to edit independently. Review the manifest with its snapshot refresh; no Cargo
dependency or duplicate Cargo metadata is needed.

Use `--manifest <relative-path>` to choose a different manifest location. Its
parent directories are created before consumer files are replaced. Pass the
same option to subsequent refreshes and drift verification.

File paths are intentionally identical in source and consumer. A repository
that needs a different path or behavior owns an adapter rather than a patched
shared copy.

Refresh checks every declared destination before replacing any file. It refuses
to overwrite consumer edits, deletions, or unrelated untracked/ignored files.
Unrelated dirty paths remain allowed. A destination already matching the selected
source bytes and executable state is safe to retry, including after an interrupted refresh. Only
declare paths owned by the shared snapshot; the consumer's `AGENTS.md` remains local.
The manifest is reviewed configuration: an intentional file-set edit is read as
input and replaced with the resulting manifest, rather than rejected as dirty work.

## Refresh

After the initial snapshot, omit `--file`. The existing manifest owns the file
set:

```bash
/path/to/shared-tooling/scripts/distribution/refresh-consumer.sh \
  --consumer /path/to/consumer
```

Review the resulting consumer diff normally. Refresh never deletes a file and
does not commit, stage, or push changes.

A previous export need not be committed before refreshing it again. An unchanged
file may advance when its bytes and executable mode match both the previous
manifest and that manifest's exact source commit. The old commit must be available
in the selected source checkout; refresh never fetches it or trusts edited hashes
without that proof. A selected file that would change must have no staged changes;
preserve or reconcile those index changes before retrying. Files already matching
the new source keep the existing interrupted-refresh behavior.

Preparation captures consumer file identities, bytes, modes, selected index
entries and the manifest, then rechecks them before replacement. Changed paths,
parents redirected through symlinks, index conflicts and concurrent manifest edits
are refused. These checks do not lock out editors or make the whole file set
atomic; stop other edits/validation of selected paths during refresh. A late
conflict can leave an incomplete refresh, with the same recovery procedure below.

The source remote must continue to match the manifest exactly. Switching
between SSH, HTTPS, or a fork is an explicit provenance change and requires a
reviewed manifest recreation.

Refresh stages files in a temporary directory under the consumer so the final
manifest replacement stays on the same filesystem. Consumers should ignore
`.shared-tooling-refresh.*` in case an ungraceful process termination prevents
normal cleanup.

Refresh replaces files individually and then publishes the manifest; it is not
an atomic replacement of the whole file set. If interrupted, stop consumer
validation, inspect the partial diff, and refresh again from the same reviewed
source revision. Verify the completed snapshot before resuming validation.

To extend an existing selection, repeat `--add-file` for each reviewed addition:

```bash
/path/to/shared-tooling/scripts/distribution/refresh-consumer.sh \
  --consumer /path/to/consumer \
  --add-file scripts/ci/archive-evidence.sh
```

An already selected addition is harmless. Use the same `--manifest` for a custom
location. Existing source, provenance and destination checks still apply, and
all additions are admitted before any destination is replaced. Ordinary refresh
preserves the selection. Removing records remains an explicit reviewed manifest
edit; refresh never deletes the corresponding consumer files.

Selected committed files can declare unconditional shared dependencies on their
second line as `# Shared companions: relative/path another/path`. Refresh checks
those declarations in the exported blobs, including companions' own declarations,
and refuses an incomplete selection with the missing path before replacing files.
Add the named companions explicitly; refresh never silently expands the selection.
These declarations belong to the selected source revision. Older files without
them retain integrity checks but provide no dependency-completeness guarantee.
Conditional features and consumer configuration still need adoption review.

The optional `scripts/ci/test-installers.sh` fixture exercises all five CI
installer wrappers: actionlint, gitleaks, ShellCheck, yq and sccache. Selecting
that fixture requires all five wrappers, their shared `install-ci-tool.sh` engine
and the checksum verifier, even if production CI uses only some of them. Add
that complete set explicitly or omit the optional fixture and its callers;
selecting an individual production installer does not require the whole suite.

Reusable Cargo, formatter, evidence, registry, release-PR and tool-command tests
also declare their required helpers. Review fixture dependencies when selecting
tests, not only production entrypoints. The PR fixture requires the PR helper
even though direct release delivery does not. Caller-owned pin files and optional
production modes stay explicit inputs rather than unconditional companions.

Run `test-snapshot-distribution.sh` from Shared Tooling for upstream exporter and
governance qualification. Consumers should normally omit that integration fixture
and its invocation, retaining actual snapshot verification and local adoption
checks rather than importing its growing upstream test roster.

The validation-runner fixture is different: `test-validation-target-runner.sh`
invokes `scripts/ci/test-release-metadata.sh` in nested Make/release contexts.
Consumers retaining this fixture own that metadata-test entrypoint. Keep an
existing qualified adapter to the consumer's own release tests, as IC Backup
does. Do not import Shared Tooling's release metadata adapter or root Makefile
to satisfy it; without a local metadata-test adapter, run this integration check
upstream and retain focused local validation-runner checks.

The release runner requires `scripts/ci/next-release-version.sh`. The runner,
validation logger and formatting hook also require
`scripts/ci/check-make-execution.sh`. Include it when adding or refreshing any of
those entrypoints; existing manifests need that explicit file-set addition.

PR release delivery additionally requires `scripts/ci/release-pr.sh` beside
`run-release.sh`, the updated `docs/releases.md` and explicit
`RELEASE_DELIVERY=pr` selection. Add `release-merged-preflight` and qualify the
complete gate and receipt bindings in the retained merged checkout before
selecting it. Vendor `scripts/ci/test-release-pr.sh` when adopting the PR fixture
or the complete portable suite. Direct delivery remains the default; refreshing
the runner alone does not adopt PR delivery. See the
[PR release contract](releases.md#pr-delivery).

The IC installer now shares matrix admission through `scripts/ci/ic-tool-pins.awk`.
Add that file explicitly before refreshing `scripts/dev/install-ic-tools.sh`;
refresh never widens the selected file set automatically. Adopting 0.2.0 also
requires the [PocketIC ownership handoff](ic-tools.md#pocketic-ownership-handoff):
remove retired helper selections and update consumer pins and callers together.

Logger adoption can replace local batch concatenation with the runner's announced
unique combined failure file and `latest-combined.log`. Its existing `latest.log`
still names only the last failed target. Move consumer readers to the combined
path before removing a local aggregator; preserve target selection, storage-root
selection and any product-owned presentation in the caller.

Fleet reports and their regression tests are optional consumer selections;
normally run them centrally from Shared Tooling. Consumers intentionally running
the tooling LOC regression vendor `scripts/ci/test-cloc-tooling.sh`,
`scripts/dev/cloc-tooling.pl` and `scripts/ci/verify-file-checksum.sh`, in addition
to their normal snapshot verifier. That test runs against adopted working-tree
bytes before a consumer commit and does not need the distribution helper.
`test-cloc-tooling-distribution.sh` remains upstream-only: it qualifies actual
committed exporter/verifier integration and the consumer fixture's independence.
`test-cloc-fixture-contexts.sh` is the upstream admission check for the reusable
LOC fixtures under enclosing Git/Cargo configuration, including sibling-report
checks with trailing-slash and aliased temporary roots. Refresh
`test-cloc-siblings.sh` to receive its physical-path correction; counts, partial
totals, error handling and retained failures keep their existing contracts.

When refreshing `install-actionlint.sh`, `install-shellcheck.sh`, `install-gitleaks.sh`,
`install-sccache.sh` or `install-yq.sh`, also declare `scripts/ci/install-ci-tool.sh`
and the existing `scripts/ci/verify-file-checksum.sh`. These entry points share that
implementation; refreshing only an entry point leaves an incomplete installation.
Pins and command arguments remain consumer-owned and unchanged.

Cargo inheritance adoption refreshes `check-dependency-pins.sh` and
`dependency-pins.jq` together, then adds `--cargo-inheritance` to the consumer's
CI/release invocation. The workspace-version reader is independently available as
`scripts/ci/read-cargo-workspace-version.sh`. Include
`docs/verification-helpers.md` for their dependencies and boundary contracts.

## Drift verification

Consumers that vendor the verifier and checksum helper can check their snapshot
without the Shared Tooling checkout or network access:

```bash
bash scripts/ci/verify-shared-tooling-snapshot.sh
```

The verifier fails when a declared file is absent, symlinked, has different
content, or changes executable state. It hashes files directly through the host's
`sha256sum` or `shasum`, without executing the inspected snapshot's checksum helper
or any other declared file. This independent bootstrap is deliberate: shared
checksum code cannot establish its own integrity. Invoke a trusted copy of the
verifier; this is local integrity checking, not signed provenance or protection
against replacement of the verifier and manifest themselves. The source commit
and normal review establish provenance.

## Shared baseline and local instructions

[`DRAGGINZGAME.md`](../DRAGGINZGAME.md) is the reusable engineering baseline.
Vendor it with its linked guides so the complete rule set remains readable
offline. Keep the consumer's `AGENTS.md` as its entry point and local overlay;
do not copy Shared Tooling's `AGENTS.md` over it.

For a new governance snapshot, use the canonical
[governance file list](../scripts/distribution/governance-files.txt), including
linked guides and required helpers. Add selected tools explicitly to the command:

```bash
shared_tooling=/path/to/shared-tooling
files=()
while IFS= read -r file; do
  files+=(--file "$file")
done < "$shared_tooling/scripts/distribution/governance-files.txt"
bash "$shared_tooling/scripts/distribution/refresh-consumer.sh" \
  --consumer /path/to/consumer "${files[@]}"
```

The distribution fixture exports this list and checks its Markdown links inside
the isolated consumer. The source checkout's link check alone cannot establish
that exported documentation is complete. Keep the list current when adding guides.
It is an initial selection, not hidden inheritance or automatic manifest widening.

For an existing snapshot, add missing paths with the reviewed `--add-file`
procedure above, including all shared rule files. Include
`docs/releases.md`, the release runner and its version/changelog helpers when
adopting the release command contract. The local `AGENTS.md` must direct contributors to
`DRAGGINZGAME.md`, identify `.shared-tooling.snapshot` as its source record, and
state local product contracts, commands and approved exceptions. Resolve local
conflicts before claiming adoption. Do not edit a vendored shared document in
place or attribute dirty upstream bytes to a committed revision.

When adopting PR contribution authority, add `rules/contributions.md` explicitly
and refresh the baseline, changelog/maintenance rules, release guidance and
reviewable-changes guide together. Remove blanket agent-commit prohibitions from
the consumer's `AGENTS.md`, `CLAUDE.md` and other active overlays so an authorized
PR can include commits and a topic-branch push. Retain required reviews/checks,
branch protections and separate merge/release authority. Historical audit reports
remain evidence of their reviewed revision. Test the instruction interpretation
against a scoped fix, an explicit PR request and a release request; reading or
adopting these rules does not itself authorize Git writes or a release. Policy
adoption requires no native builds. Existing pinned snapshots do not update
automatically; preserve their bytes until a reviewed refresh is authorized.

When adopting the local-repair and owning-repository issue workflow, refresh
`DRAGGINZGAME.md` and `rules/agent-maintenance.md` together from the reviewed
commit. Remove equivalent local instructions after checking their obligations;
retain approved scoped exceptions and consumer release boundaries. Walk through
an authorized local repair (apply in the working tree and run focused checks)
and an upstream finding (search, confirm issue authority for the GitHub owner,
report evidence in the authorized owning issue, then adopt the committed
correction). Standing issue permission covers only `dragginzgame/*`; other
GitHub destinations require explicit authorization. Remove broader local issue
permissions when adopting this rule, and refresh scheduled prompts that repeat
them. Reporting an issue neither applies the upstream fix nor verifies consumer
adoption; broad validation authority stays unchanged.

Consumers using the maintenance rule's exact-commit CI inspection command also
refresh `scripts/dev/gh-ci.sh` from that reviewed revision. It remains a read-only
interactive helper using the consumer checkout and authenticated GitHub CLI.
When adopting the repeatable task commands, include the `tasks/` catalog,
definitions, prompt and schedule guidance plus linked companions from the
governance file list. Shared Tooling coordinates central runs; consumers retain
their focused commands and product scope. Copying those files never installs a
timer, activates an agent, grants sibling edit authority or requires Codex/systemd
on every consumer host. Enable one local scheduler separately when requested.
The portable-suite prerequisite check is Shared Tooling's own test setup, not a
required consumer gate. If vendoring the complete portable suite, include its
new `scripts/ci/check-portable-prerequisites.sh`, `scripts/ci/test-portable-prerequisites.sh`
and `scripts/ci/test-gh-ci.sh` dependencies along with the existing suite inputs.

A revision-bound baseline reference remains an allowed alternative under the
baseline. It must identify the exact source revision and document; a branch URL
or moving sibling path does not establish which rules were reviewed. Snapshot
integrity checks detect changes to declared files; they do not prove that a
consumer has adopted the newest policy or resolved its local instruction conflicts.
Rust consumers also add `.githooks/pre-commit`,
`scripts/dev/install-git-hooks.sh` and `scripts/ci/check-make-execution.sh` to the
declared file set, align their formatting targets with the
[hook contract](../rules/git-hooks.md), and enable the hook through
`make install-hooks`. Refresh preserves executable modes but does not activate
hooks or replace Git configuration. Review existing local hooks before declaring
their paths for replacement; preserve and reconcile their obligations.
Release adoption also requires aligning the consumer's entry points, adapters,
instructions and checks with the [release contract](releases.md), including
artifact retention and the exact atomic branch/tag push. A passing snapshot
check alone does not verify those behaviors. Explicit PR adopters instead qualify
the exact branch push, review boundary, merged-source validation and tag-only push.
The direct release runner's consumer fixture, `scripts/ci/test-release-runner.sh`,
simulates Git effects. Real-Git tracking qualification is independently selected
as `scripts/ci/test-release-tracking.sh`; the owner portable suite always runs it.
Select fixtures according to their documented effects and local authority rather
than editing vendored tests or invoking an unauthorized broader suite.
Pinning adoption also requires the checker and its jq module, prepared Git/jq/yq
tools (and Cargo for Rust workspaces), a CI/release invocation, and consumer-owned
qualification for locked builds and external inputs. Consumers may also vendor
`scripts/ci/install-yq.sh` with `scripts/ci/install-ci-tool.sh` and the checksum
helper; choose and record their own reviewed version and platform digests.
Resolve existing exceptions under the
[pinning policy](../rules/dependency-pinning.md) before claiming adoption.

## Audit method adoption

Include the complete `audits/` file set in the governance list when adopting this
baseline, so its method links work offline. The snapshot's reviewed source revision identifies
the common contract; do not read methods from a moving sibling checkout or copy
uncommitted documents and attribute them to an older revision.

For each selected existing local audit, map its questions to the shared method
and retain product-specific obligations in a local overlay under the consumer's
existing audit directory. Link to the vendored `audits/` definition and record
local source roots, authority boundaries, focused checks, allowed measurement
methods and report destination. Resolve conflicts with the common contract,
including automatic fixes, unconditional broad gates and composite scores.

Replace duplicated generic prose only after the snapshot is installed. Update
local catalogs, entrypoints and any method fingerprints in the same batch.
Where a frozen method is needed to reproduce earlier reports, preserve its old
identity as historical and ineligible for new runs; do not label historical
scores comparable to the new method. Keep domain-specific audits and executed
reports in their existing repository. This does not migrate report trees,
introduce shared runtime measurement tooling, or retire distinct safety proofs.

Verify snapshot integrity, local links, obligation coverage and method selection
by walking a representative existing report against the new method and overlay.
Record this as an adoption review, not a newly executed product audit. Script or
product behavior changes need their own relevant validation and host evidence;
documentation adoption alone does not call for full CI or native builds.

## Local IC tool adoption

After the new files are committed and reviewed, add `ci/ic-tools.tsv`,
`scripts/dev/install-ic-tools.sh`, `scripts/ci/ic-tool-pins.awk`,
`scripts/ci/verify-evidence-checksums.sh` and
`scripts/ci/verify-file-checksum.sh` to the snapshot, with `docs/ic-tools.md`.
Also include `make/tools.mk`, `scripts/dev/install-host-tools.sh`,
`scripts/dev/cloc.sh`, `ci/tool-versions.env` and `docs/local-setup.md` for the
common commands and pinned host setup. Adopt the complete
[required tool inventory](local-setup.md#required-tool-inventory), including
ripgrep and cloc pins, and add `/.tools/` to the consumer's ignore rules. Remove
the consumer's duplicate setup/check/LOC recipes and include the shared commands
once in its root Makefile:

```makefile
include make/tools.mk
```

That include supplies `install-tools`, `tools-check`, `install-host-tools`,
`host-tools-check`, `install-ic-tools`, `ic-tools-check` and `cloc`, plus the
checkout-local PATH. It preserves the consumer's default Make goal; including it
does not trigger installation. Future changes to these recipes and tool
selections arrive with the reviewed snapshot rather than another copied recipe.
Existing checkouts need an explicit snapshot refresh and installation to receive
new files and executables; they never execute a mutable sibling checkout.

For the shared Cargo-installed set, also vendor `scripts/dev/install-rust-tools.sh`
and the three `SHARED_TOOLING_CARGO_SORT_VERSION`, `SHARED_TOOLING_CARGO_SORT_DERIVES_VERSION`
and `SHARED_TOOLING_CANDID_EXTRACTOR_VERSION` pins in the selected versions file.
The include provides `install-rust-tools` and `rust-tools-check`; Rust consumers
attach these to their aggregate commands as shown in
[Rust setup](local-setup.md#rust-development-tools). Retire their duplicate
Cargo-tool install recipes and version constants after qualified adoption.
Consumer-selected registry binaries/examples use this same installer and its
`scripts/ci/verify-file-checksum.sh` companion, with prepared host tools and an
explicit local selection; see [Cargo tool setup](local-setup.md#consumer-selected-cargo-tools).
Retire synthetic Cargo resolvers only after qualifying the selected package and
profile. Source-checkout builds and application executable overrides stay local.

Defaults use scripts and pins at the checkout root. For a snapshot stored below
that root, set `SHARED_TOOLING_ROOT` to its reviewed local directory before the
include, and include its `make/tools.mk`. `HOST_TOOL_VERSIONS` and `IC_TOOL_PINS`
select consumer-owned pin exceptions; their defaults remain the checkout's
`ci/tool-versions.env` and `ci/ic-tools.tsv`. Installations always target the
consumer checkout. Include `make/tools.mk` in any isolated Makefile export,
including the extra inputs to `check-release-commands.sh`.

Two optional includes centralize further command wiring without owning product
policy:

- `make/release.mk` supplies the standard release entrypoints
  and conflicting-goal rejection. Select its declared runner companions and
  retain the consumer's delivery admission, cache preparation, metadata and
  validation adapters. See the [release example](releases.md#makefile-example).
- `make/rust-format.mk`, included after `make/tools.mk`,
  supplies the simple root-workspace formatting commands. Select both includes,
  `scripts/ci/check-format-tools.sh`, `scripts/ci/run-formatting.sh` and the
  reviewed pin file. The wrapper reports one success line and retains failure
  diagnostics behind a two-line summary. Include it in isolated hook fixtures.
  Refresh the failure collector to retain `formatting.*` logs in CI. Keep richer
  workspace/frontend recipes local under the [formatting rules](../rules/git-hooks.md).

Both includes require the adjacent `make/execution.mk` and its execution-probe
companion. Export these explicitly so unsupported Make modes cannot turn a
failed command into success.

Replace equivalent local recipes only after testing the actual caller and hook.
These files must come from the recorded snapshot, never a live sibling include.
The optional `test-make-format.sh` fixture declares its own companions; the
complete `test-git-hooks.sh` fixture also requires the new formatting include.
Explicitly extend a consumer's file selection before refreshing that fixture.

`make cloc` reports the consumer's root Cargo workspace and requires its prepared
Rust toolchain. Shared Tooling itself selects `CLOC_REPORT` and `CLOC_ROOT` before
the include to summarize sibling workspaces; consumers normally use the defaults.
Installing the raw cloc executable alone does not add these Make commands.
The optional `make cloc-tooling` scans sibling CI and tooling, including non-Rust repositories;
`CLOC_PARENT` selects its parent directory. It needs Git, cloc and core Perl
modules, with no Cargo dependency or consumer command execution. Run it from
Shared Tooling by default. A consumer that intentionally needs its own command
can explicitly select `scripts/dev/cloc-tooling.pl`; existing selections continue
to work. Without that file the target explains where to run or adopt the report,
and never invokes another checkout implicitly.

The report includes each recorded `version@revision` and complete declared-file
integrity, with separate identities for multiple bundles. Missing versions remain
`unrecorded`; missing, modified or symlinked files and mode differences show
`DRIFT`, including declared documents outside the LOC scope. `OK` means declared
hashes and modes match, not that provenance or consumer behavior was qualified.
The JSON output retains full revisions and each manifest's drifted paths.

To retire unnecessary fleet copies, review their local callers first, then remove
the report/test files and their snapshot records together with Make/CI/help
references. Candidates are `cloc-tooling.pl`, `cloc-siblings.sh` and their
dedicated tests; keep `cloc.sh` for the consumer's own workspace. Preserve
checksum/verifier helpers used by other selected tools, local integration checks
and historical evidence. Snapshot refresh never prunes files automatically.
This is a consumer-owned selection change, not permission for another repository
to edit its files. Fleet reports and their regression coverage remain maintained
and tested in Shared Tooling.

Review pins against existing qualified versions before activation. A local
exception uses its own explicitly selected matrix outside the snapshot; remove
superseded pin ownership rather than maintaining two independent selections.
Update local setup/CI callers to use `.tools/host/bin` and `.tools/ic/bin`, plus
`.tools/rust/bin` when adopting the Cargo-installed set; CI
must invoke the same Make installation/check targets and put these paths on its
own PATH after explicit setup. Document the shell export for direct interactive commands;
installation and Make exports do not change the user's terminal PATH. Remove
separate package-manager jq/yq/ripgrep/cloc selections and preserve one owner
for each tool pin.
The existing single-yq installer remains available to consumers that need only
that standalone parser; it accepts caller-owned pins rather than defining them.
Document the [system bootstrap prerequisites](local-setup.md#bootstrap-prerequisites)
and retain product-specific
alignment checks. Retire duplicated installer bodies once callers have moved;
do not patch vendored files. Keep package-manager setup for unrelated tools.

Qualify explicit installation and offline checking on the consumer's declared
native hosts, plus relevant product checks for changed tool selections. Do not
claim deployment or PocketIC client/server compatibility from `--version` alone.
Snapshot integrity and consumer adoption remain separate from upstream fixtures.

The evidence archiver can be adopted independently as
`scripts/ci/archive-evidence.sh`; see its
[selection and retention contract](verification-helpers.md#evidence-archives).
Vendoring `.github/actions/retain-failure-evidence/action.yml` requires that helper
at its canonical relative path. Downloaded artifacts from this action contain
`evidence.tar.gz`; extract it before inspecting the retained files. Consumers
with their own evidence layouts keep their collector and call the helper directly.

The evidence-manifest helper can also be adopted independently with the existing
checksum verifier. The nonempty Cargo test helper and exact release-tag checker
have no shared helper dependencies. Consumers keep test arguments, manifest
selection and publication/release authority in their adapters.

The read-only runner disk checker has no shared-script dependencies. Add
`scripts/ci/check-runner-disk-space.sh` and `docs/verification-helpers.md` to the
reviewed selection. Callers supply an existing filesystem path, minimum MiB,
label and any diagnostic paths; move local callers before removing their old
capacity parser. Keep thresholds, diagnostic policy and any separately authorized
image cleanup local. See the [capacity contract](verification-helpers.md#runner-disk-capacity).

The documentation-link, release-command and crates.io observation helpers also
have no shared helper dependencies. Follow their explicit input and failure
contracts in [verification helpers](verification-helpers.md), include that
document and each selected helper in the snapshot, and move callers before
deleting duplicated code. Keep product-specific checks and publication policy
local. Adoption must wait for a reviewed committed source revision; local
upstream tests do not establish that consumers have refreshed their snapshots.

Exact registry metadata uses the existing `check-crates-io-version.sh` selection
with `--metadata NEW-DIRECTORY`; no additional shared script is required. Prepare
jq and curl 8.4.0+ for that mode. Retain each attempt's directory and handle all
three statuses before removing local transport/parsing. The consumer still owns
payload verification, yanked-version acceptance and publication reconciliation.

File-digest generation uses the checksum verifier's additive `--print` interface,
so its existing snapshot file set is sufficient. Refresh that verifier before
changing local hash callers. Advisory database preparation is independently
available as `scripts/ci/prepare-rustsec-db.sh` with no shared-script dependencies;
include it and `docs/verification-helpers.md` when adopting it. Replace only
database acquisition/isolation, retain consumer audit policy and failure evidence,
and pass `--no-fetch` when auditing the prepared database so its identity stays
bound to the recorded commit.
