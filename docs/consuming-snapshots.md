# Consuming Shared Tooling Snapshots

CI and release behavior must not depend on a sibling checkout, a moving Git
branch, or network availability. Consumers vendor a reviewed file set and
record its exact Shared Tooling source revision.

## Entrypoint selection in 0.7.0

Select the commands and documents the consumer uses with `--entrypoint`; the
exporter derives their transitive shared companions from the selected commit.
The checksum and snapshot verifiers are always included. There is no separate
bundle registry or dependency download during refresh.

This replaces the `--file` / `--add-file` CLI and manually completed file roster.
On first adoption, pass the complete intended entrypoint set with the new
exporter, including locally used helpers, tests and governance documents. Existing
payload records remain ownership evidence; they are never guessed to be roots.
Review `--preview` before applying. As of 0.8.0, omitted previous payloads are
removed automatically after unchanged-source proof; see the retirement rules below.
Consumers can keep every existing payload by explicitly selecting all of them,
then narrow roots in a later reviewed adoption.

The same v1 manifest retains exact payload hashes, modes, source and revision.
`# entrypoint` annotations record selection intent separately from generated
`file` records. Integrity-only readers can still inspect historical manifests;
refresh without recorded entrypoints requires explicit selection. Never hand-edit
payload records to remove ownership evidence, or use an older exporter after
adopting entrypoint selection. The current verifier validates any recorded roots
against the payload roster; byte integrity does not prove that local callers were
all selected or that runtime/host qualification passed.

Directory paths must not contain LF or CR characters. Snapshot refresh and
verification reject them in both supplied and resolved physical paths, including
ancestor directories and symlink destinations. They never trim a forbidden name
into another checkout. Rename an affected operational directory explicitly before
using these commands; existing artifacts are not renamed or deleted automatically.
Spaces and ordinary physical aliases remain supported. Snapshot-relative file
and manifest paths also reject LF, CR and tab characters.

## Shared host and IC storage adoption in 0.5.0

Refresh both host/IC installers and their new `scripts/dev/tool-store.sh`
companion together with `scripts/ci/select-tool-evidence.sh` and the failure
evidence action. If you select the installer fixtures, add their
`scripts/ci/test-tool-store.sh` companion too. Preview the refresh to identify
the complete required selection before applying it.

Run `make install-host-tools`, `make install-ic-tools`, then their offline
checks. Existing command/PATH entries remain `.tools/host/bin` and
`.tools/ic/bin`; these now resolve to shared bundles. The previous physical
checkout-local bundles remain retained but no longer pass active installation
checks. Use one explicit store setting for setup, checks and failure collection;
see [storage configuration and retention](local-setup.md#shared-executable-storage).
Consumers must not copy payloads or invent their own cache/link manager. Rust
tool installation and PocketIC ownership are unchanged in this batch.

This storage-admission change requires a Shared Tooling minor release. A consumer
library adopting it still classifies its own release by its published contract;
setup changes alone do not require a library minor release.

## Initial snapshot

Run the refresh helper from a clean Shared Tooling checkout:

```bash
/path/to/shared-tooling/scripts/distribution/refresh-consumer.sh \
  --consumer /path/to/consumer \
  --entrypoint make/consumer.mk
```

The helper validates every source path, exports regular files and their
executable modes from the selected Git revision at the same relative paths,
and writes `.shared-tooling.snapshot` in the consumer. Ignored or otherwise
uncommitted files cannot enter the snapshot, and concurrent working-tree edits
cannot change the exported bytes. The manifest
records format version `1`, source remote, source commit, and the SHA-256 digest
and executable state of every vendored file.
Records use exact tab-separated fields: extra, empty or trailing fields are
malformed. Refresh, verification and the tooling inventory reject them; literal
spaces within a valid relative filename remain significant and supported.

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
The manifest may contain an uncommitted prior export. Preserve its ownership
records and change selection through the entrypoint options; refresh publishes
the resulting manifest after reconciling its payloads.

## Refresh

After adoption, omit selection options to reuse the recorded entrypoints. Their
companion closure is resolved again from the new selected revision:

```bash
/path/to/shared-tooling/scripts/distribution/refresh-consumer.sh \
  --consumer /path/to/consumer
```

Review the resulting consumer diff normally. Refresh automatically deletes
unchanged obsolete payloads under the retirement rules below. It does not commit,
stage, push, install tools, publish, deploy or clean build/evidence artifacts.

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

To extend an existing selection, repeat `--add-entrypoint` for each reviewed root:

```bash
/path/to/shared-tooling/scripts/distribution/refresh-consumer.sh \
  --consumer /path/to/consumer \
  --add-entrypoint scripts/ci/archive-evidence.sh
```

An already selected addition is harmless. Use the same `--manifest` for a custom
location. Existing source, provenance and destination checks still apply, and
all additions are admitted before any destination is replaced. Ordinary refresh
preserves the roots. Pass a complete replacement `--entrypoint` set to narrow or
replace them; supplying roots is a replacement, not an addition.

Selected committed files can declare unconditional shared dependencies on their
second line as `# Shared companions: relative/path another/path`. Refresh checks
those declarations in the exported blobs, including companions' own declarations,
and includes their complete closure. Missing source payloads refuse before any
consumer replacement. Review additions in preview; no inspected code is executed
to discover dependencies.
These declarations belong to the selected source revision. Older files without
them retain integrity checks but provide no dependency-completeness guarantee.
Conditional features and consumer configuration still need adoption review.

### Preview before refreshing

Add `--preview` to the same initial or refresh command to inspect the complete
proposed selection without writing in the consumer, including its Git index:

```bash
/path/to/shared-tooling/scripts/distribution/refresh-consumer.sh \
  --preview --consumer /path/to/consumer
```

The preview uses external temporary scratch and the same clean, committed source
and destination admission checks as refresh. A `TMPDIR` inside either checkout
(including through an alias) is rejected before creating scratch files.
It prints source version/revision,
the manifest path, and tab-separated `ADD`, `UPDATE`, `KEEP` or `REMOVE` rows.
The second column distinguishes `selected` roots from `required` companions.
Removal rows use `obsolete` for unchanged prior payloads outside the new selection.
Companion discovery follows committed declarations
transitively, including cycles, and reports the complete payload in one pass.
Changes include executable-mode differences. Local conflicts still refuse.

Exit 0 means the proposed refresh is admitted, including any listed removals.
Admission failures return nonzero without a completed preview. Review the entire
diff, then rerun without `--preview` and with the same entrypoint selection.
This is an observation, not saved authorization or validation: apply rechecks
the actual source and consumer.

### Retiring obsolete payloads

As of 0.8.0, refresh retires unchanged obsolete payloads automatically; the
`--retire-file` option is removed. Update refresh commands to omit it. When a
selected root no longer needs a companion, or roots are deliberately replaced,
preview lists previous payloads outside the new closure. Applying removes those
files without a separate approval step. Files still required by any root remain;
files absent from the prior ownership roster are never retirement candidates.
Before removing anything, refresh verifies the prior manifest's digest and mode
against its available committed source, then checks actual consumer bytes, mode,
index and path identity. Local edits, staged changes, redirected paths and forged
ownership records refuse; a clean consumer Git status alone is insufficient.

Before refreshing, check local callers and other snapshot manifests.
The exporter derives ownership only from the selected manifest; it does not scan
other bundles or infer consumer-owned dependencies. Keep a still-used file as an
entrypoint, or coordinate its callers and other bundles before removing it.

Only those exact files are removed. Parent directories, unowned files, installed
tool generations, logs, build artifacts and other repositories are not swept.
The manifest is published last. If interrupted, retain the old manifest, inspect
the partial diff and rerun against the same source with the same selections.
Already absent owned retirements and already installed exact payloads are safe
to reconcile. Concurrent editors are not locked out: stop other modifications
and validation during refresh, and verify before resuming. Repeating a completed
refresh is harmless: removed paths are no longer in its ownership roster.
No retirement journal or cleanup daemon is added.

The optional `scripts/ci/test-installers.sh` fixture exercises all five CI
installer wrappers: actionlint, gitleaks, ShellCheck, yq and sccache. Selecting
that fixture requires all five wrappers, their shared `install-ci-tool.sh` engine
and the checksum verifier, even if production CI uses only some of them. The
exporter includes that closure; omit the optional fixture and its callers if unwanted.
Selecting an individual production installer does not require the whole suite.

Reusable Cargo, formatter, evidence, registry, release-PR and tool-command tests
also declare their required helpers. Review fixture dependencies when selecting
tests, not only production entrypoints. The PR fixture requires the PR helper
even though direct release delivery does not. Caller-owned pin files and optional
production modes stay explicit inputs rather than unconditional companions.

Run `test-snapshot-distribution.sh` from Shared Tooling for upstream exporter and
governance qualification. Consumers should normally omit that integration fixture
and its invocation, retaining actual snapshot verification and local adoption
checks rather than importing its growing upstream test roster.
If a consumer retains it, the optional companion-pair catalog checks only
entrypoints present in that checkout; omitted fleet reporters do not become
dependencies. Present entries still undergo derived-companion checks.
The remaining exporter, focused-fixture and governance integration inputs must
be available. Shared Tooling runs the complete catalog; a reduced consumer run
reports omissions and does not qualify the omitted upstream cases.

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

Keep the local entry point organised around four things: the adopted baseline,
local safeguards and approved exceptions, development/delivery commands, and
links to detailed guidance selected by the task. The snapshot manifest owns its
revision; avoid repeating that identity or a history of past adoptions throughout
the overlay. An explicit revision-bound policy reference still needs its exact
revision when no local policy snapshot exists.

Reference common contribution, changelog, dependency and setup rules instead of
maintaining parallel copies. Keep critical effect/recovery restrictions visible.
Move lengthy product contracts to their existing documentation owner, or a linked
local document when no suitable owner exists, and say when contributors must read
them. Moving a requirement does not make it optional. Preserve unique constraints
and historical evidence; remove only demonstrated duplication or obsolete policy.
Do not impose a line quota or erase approved layout, validation, release or asset
exceptions merely to make overlays look identical. Check local entry points and
their linked governance for contradictory authority before declaring alignment.

For a new governance snapshot, use the canonical
[governance file list](../scripts/distribution/governance-files.txt), including
linked guides and required helpers. Add selected tools explicitly to the command:

```bash
shared_tooling=/path/to/shared-tooling
files=()
while IFS= read -r file; do
  files+=(--entrypoint "$file")
done < "$shared_tooling/scripts/distribution/governance-files.txt"
bash "$shared_tooling/scripts/distribution/refresh-consumer.sh" \
  --consumer /path/to/consumer "${files[@]}"
```

The distribution fixture exports this list and checks its Markdown links inside
the isolated consumer. The source checkout's link check alone cannot establish
that exported documentation is complete. Keep the list current when adding guides.
It is an initial selection, not hidden inheritance or automatic manifest widening.

For an existing snapshot with recorded roots, add paths with `--add-entrypoint`
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
After delivering the adoption, apply that rule's issue reconciliation: record
the exact consumer commit, Shared revision and required validation evidence in
the consumer's owning issue, close it when its criteria are met, and update the
linked Shared tracker. Consumers still awaiting required CI remain open. This is
part of the agent's adoption/maintenance pass, not a GitHub auto-close-on-push
workflow or authorization to dispatch CI or start a schedule.

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
`scripts/dev/tool-store.sh`, `scripts/ci/verify-evidence-checksums.sh` and
`scripts/ci/verify-file-checksum.sh` to the snapshot, with `docs/ic-tools.md`.
Also include `make/tools.mk`, `make/execution.mk`,
`scripts/ci/check-make-execution.sh`, `scripts/dev/install-host-tools.sh`,
`scripts/dev/install-rust-tools.sh`, `scripts/dev/cargo-tool-build.sh`,
`scripts/dev/cloc.sh`, `ci/tool-versions.env`
and `docs/local-setup.md` for the
common commands and pinned host setup. Adopt the complete
[required tool inventory](local-setup.md#required-tool-inventory), including
ripgrep and cloc pins, and add `/.tools/` to the consumer's ignore rules. Remove
the consumer's duplicate setup/check/LOC recipes and include the shared commands
once in its root Makefile:

```makefile
include make/tools.mk
```

That include supplies `install-tools`, `tools-check`, `install-host-tools`,
`host-tools-check`, `install-ic-tools`, `ic-tools-check`, `install-rust-tools`,
`rust-tools-check` and `cloc`, plus the
checkout-local PATH. It preserves the consumer's default Make goal; including it
does not trigger installation. Future changes to these recipes and tool
selections arrive with the reviewed snapshot rather than another copied recipe.
Existing checkouts need an explicit snapshot refresh and installation to receive
new files and executables; they never execute a mutable sibling checkout.

For the required shared Cargo-installed set, include the three
`SHARED_TOOLING_CARGO_SORT_VERSION`, `SHARED_TOOLING_CARGO_SORT_DERIVES_VERSION`
and `SHARED_TOOLING_CANDID_EXTRACTOR_VERSION` pins in the selected versions file.
The aggregates always include Rust tools, even in non-Rust repositories;
prepare a declared Rust/Cargo toolchain first as described in
[Rust setup](local-setup.md#rust-development-tools). Retire duplicate
Cargo-tool install recipes and version constants after qualified adoption.
Consumer-selected registry binaries/examples use this same installer and its
`scripts/ci/verify-file-checksum.sh` companion, with prepared host tools and an
explicit local selection; see [Cargo tool setup](local-setup.md#consumer-selected-cargo-tools).
Retire synthetic Cargo resolvers only after qualifying the selected package and
profile. Source-checkout builds and application executable overrides stay local.

### Rust installation adoption in 0.4.0

This is a breaking installation contract for Shared Tooling. Consumer libraries
whose published contracts remain unchanged should use compatible patch notes
for adoption, even though contributors must reinstall tools and change PATH.
Do not mirror Shared's minor bump. Assess independent product changes under the
[package-impact rules](../rules/changelogs.md#classify-the-released-packages-own-impact).

For the 0.4 Rust installation hard cut, also follow the
[current slot and PATH contract](local-setup.md#rust-development-tools): refresh
`scripts/dev/install-rust-tools.sh`, `make/tools.mk`, `.githooks/pre-commit`,
`scripts/ci/check-formatting-hooks.sh` and the retained-evidence action together
where selected, along with their declared companions and applicable fixtures.
Change consumer-owned CI, shell exports and documentation from `.tools/rust/bin`
to `.tools/rust/common/bin`. Run explicit `make install-rust-tools` followed by
`make rust-tools-check`, then qualify formatting and complete tool setup on the
supported native hosts. Standalone Rust setup now needs prepared shared host
tools; `make install-tools` already orders these prerequisites. Retain old
installations and failed evidence; do not add fallback paths or copy old binaries
into slots. Selected product CLI paths remain unchanged, and Testkit still owns
PocketIC. Remove duplicated installation recipes only after their callers use
the shared owner. Upstream acceptance and consumer adoption remain distinct.

### Shared Cargo storage adoption in 0.6.0

Refresh the Cargo installer, `scripts/dev/cargo-tool-build.sh`, shared store,
evidence selector/action and their declared companions together. Preserve
`.tools/rust/common/bin` on PATH. Selected CLI adapters must use the installer’s
returned path, not reconstruct the old version-only slot name.

Prepare the consumer's selected Rust compiler, then run `make install-rust-tools`
and `make rust-tools-check`, plus explicit setup/check for selected product CLIs.
Tool builds now use the [standard environment](local-setup.md#standard-cargo-tool-build-environment)
instead of inheriting application flags, wrappers or custom Cargo configuration.
Both offline checks and subsequent setup require the managed source cache.
Old local slots, common sets, failed attempts and shared generations stay intact;
adoption never deletes or silently reuses the old physical installations.

Use the same shared-store setting in setup/checks and evidence collection. The
selector now owns both local Rust evidence and referenced shared generations;
custom collectors must remove duplicate `.tools/rust` selections when adopting
that output. The common action uploads safe local evidence if store admission
fails, records incomplete collection, then fails after upload. Qualify same-host
two-checkout reuse and failure retention before claiming consumer adoption.
Library consumers classify this contributor tooling change at their own API
boundary; it does not itself require a library minor release.

### Complete toolset adoption in 0.3.0

Refresh the host installer, `make/tools.mk`, Cargo installer/checksum companion,
complete pin catalogs, baseline and setup guidance together from a reviewed
committed revision. Include updated selected fixtures and evidence-collector
companions when adopted; the selector calls the complete host check too.
The current contract has no parser-only host mode or optional ripgrep/cloc flags.
Remove `--with-ripgrep`/`--with-cloc` from direct callers, including custom CI and
evidence checks. Retain historical documents as historical evidence.

Every aggregate now includes the three Cargo tools after host and IC setup,
including in non-Rust repositories. Remove redundant Rust aggregate prerequisites.
Move additional product setup/check prerequisites into `LOCAL_TOOL_INSTALL_TARGETS`
and `LOCAL_TOOL_CHECK_TARGETS` in matching order, as shown in
[local setup](local-setup.md#required-tool-inventory). Keep existing owner targets,
version/profile selections, overrides and admitted release-preflight boundaries.
Do not make a product extension call the aggregate that invokes it. Ordinary
offline checks must not invoke setup.

Prepare the declared Rust toolchain first, then explicitly run `make install-tools`
and `make tools-check` as separate commands. A complete matching host set can be
reused; an incomplete set needs explicit setup. Preserve every earlier bundle,
receipt and failed artifact. This cut changes the common roster and invocation
contract, not the retained Cargo installation layout. Qualify setup reuse,
missing-tool refusal, failure retention and product extensions under parallel
Make on supported native hosts before claiming consumer adoption.

The compatible 0.3.1 setup improvement adds read-only `--preflight` calls to the
existing IC and Rust installers before the aggregate starts any installation.
Refresh `make/tools.mk`, both installers and the improved host installer together.
Consumer setup fixtures that substitute installer commands must admit these
non-mutating calls separately from simulated installation failures. Ordinary
`tools-check` and narrow setup commands keep their existing offline/setup roles.

The compatible 0.3.2 correction preserves Cargo's jobserver descriptors through
shared Rust setup/check, LOC metadata and formatting recipes. Refresh
`make/tools.mk` and `make/rust-format.mk` with `make/execution.mk` and its
`scripts/ci/check-make-execution.sh` companion, including in isolated Makefile
exports. The execution guard now applies even when only `make/tools.mk` is
included: unsafe Make modes must fail before any installer runs. Consumer-owned
Cargo recipes still need their own descriptor handoff and execution admission;
this shared correction does not change those recipes automatically.

Refresh selected shared fixtures too. Their cleanup now requires explicit
completion as well as a successful exit, preserving evidence and failing on
early exits, including Bash 3.2 nounset errors. Native consumer qualification
remains separate from upstream source acceptance.

Defaults use scripts and pins at the checkout root. For a snapshot stored below
that root, set `SHARED_TOOLING_ROOT` to its reviewed local directory before the
include, and include its `make/consumer.mk`. `HOST_TOOL_VERSIONS` and `IC_TOOL_PINS`
select consumer-owned pin exceptions; their defaults remain the checkout's
`ci/tool-versions.env` and `ci/ic-tools.tsv`. Command selections target the
consumer checkout; host/IC payloads use the [shared per-user store](local-setup.md#shared-executable-storage).
The [consumer Make interface](make-interface.md) selects the setup, release and
Rust formatting modules and supplies hook installation and snapshot checking.
Select its declared companion closure, keeping product admission, metadata,
validation and custom formatter adapters local. Non-Rust consumers explicitly
set `SHARED_TOOLING_RUST := 0`. Existing focused module includes remain supported;
do not include them again alongside `consumer.mk`.

Export every parsed include in isolated Make/hook fixtures, including the extra
inputs to `check-release-commands.sh`. Keep `make/execution.mk` and its probe so
unsupported Make modes cannot turn failures into success. The complete
`test-git-hooks.sh` fixture now exercises `make/consumer.mk`; extend its selection
explicitly before refreshing. `test-make-consumer.sh` tests common routing with
substitute format/release commands; native consumer qualification remains separate.
Refresh the failure collector to retain `formatting.*` logs in CI. Replace local
recipes only after testing their actual callers and hook, following the
[adoption checklist](make-interface.md#adoption).

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

To retire unnecessary fleet copies, review their local callers first, update
Make/CI/help references, then select the remaining entrypoints and refresh through
the exporter. Preserve ownership records until
that refresh completes. Candidates are `cloc-tooling.pl`, `cloc-siblings.sh` and their
dedicated tests; keep `cloc.sh` for the consumer's own workspace. Preserve
checksum/verifier helpers used by other selected tools, local integration checks
and historical evidence. Automatic retirement is limited to verified unchanged
payloads outside the selected closure.
This is a consumer-owned selection change, not permission for another repository
to edit its files. Fleet reports and their regression coverage remain maintained
and tested in Shared Tooling.

Review pins against existing qualified versions before activation. A local
exception uses its own explicitly selected matrix outside the snapshot; remove
superseded pin ownership rather than maintaining two independent selections.
Update local setup/CI callers to use `.tools/host/bin` and `.tools/ic/bin`, plus
`.tools/rust/common/bin` when adopting the Cargo-installed set; CI
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
