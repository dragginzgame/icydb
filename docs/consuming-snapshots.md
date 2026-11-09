# Consuming Toko Tooling Snapshots
<!-- Shared companions: docs/toko-tooling.md docs/rust-toolchains.md scripts/distribution/maintenance-files.txt scripts/distribution/governance-files.txt docs/ic-tools.md docs/verification-helpers.md docs/local-setup.md docs/make-interface.md tasks/snapshot-drift.md rules/dependency-pinning.md rules/changelogs.md -->

CI and release behavior must not depend on a sibling checkout, a moving Git
branch, or network availability. Consumers vendor a reviewed file set and
record its exact Toko Tooling source revision.

## Small baseline snapshots

`TOKO-TOOLING.md` is a standalone entrypoint. Its detailed policy and procedure
links point to an immutable reviewed revision rather than pulling every linked
guide, audit and maintenance task into a product snapshot. This changes document
selection, not the Cargo pin, receipt, installer or policy authority contract.
Executable entrypoints still carry their required executable companions.

Choose the baseline, `make/consumer.mk` and the hook as needed. For offline policy
access, additionally select the required guide entrypoints or the complete list
in `scripts/distribution/governance-files.txt`. That list intentionally remains
the comprehensive option; selecting all of it does not produce a minimal bundle.
Keep required references available before a task; a small installation is not
permission to skip the detailed rules. The crate retains the full documents.

During an ordinary refresh, preview the selected roots. Previously derived,
unchanged files that are no longer required retire through the exporter. Explicitly
selected roots remain selected: use the complete `--entrypoint` list to remove
unneeded audit/task roots, preserving guides used by local commands or offline
workflows. Never edit receipt records or delete snapshot files by hand. No
immediate refresh is required for an intact consumer installation.

Policy adoption uses the checklist in `TOKO-TOOLING.md`: binding rules,
consumer-owned choices, explicitly permitted defaults, and scoped approved
exceptions. Record unresolved conflicts before claiming adoption; tooling can
be installed and verified independently. This checklist clarifies existing
authority and does not create blanket exceptions for legacy product practices.

Maintainers should batch changes to selection, receipt and installer contracts
at a planned minor boundary and document the concrete need and migration once.
Do not introduce another migration merely to refresh terminology or policy prose.
Consumers adopt for a needed correction or capability; an older intact snapshot
alone is not an adoption defect or a reason to block a product release.

The baseline's detailed-reference revision is a policy selection, not a moving
"latest" pointer. Keep it unchanged for unrelated script releases. When revising
the detailed rules themselves, review and update that reference deliberately;
do not claim a newer policy is adopted merely because its files are packaged.

## Baseline filename migration in 0.37.0

This is a hard cut to the Toko Tooling name, including `TOKO-TOOLING.md`,
`scripts/dev/toko-tooling.sh`, `.toko-tooling.snapshot`, Make targets prefixed
`toko-tooling-`, environment variables prefixed `TOKO_TOOLING_`, and
`package.metadata.toko-tooling.loc`. There are no command or filename aliases.
Existing reviewed snapshots remain usable until deliberate adoption.

Update the exact Cargo pin and Cargo.lock together. Bootstrap with the installer
from the reviewed new crate, rather than assuming an older installed script can
run the new contract. Preserve the existing receipt: pass its current relative
path with `--manifest` during migration. Select the complete entrypoint set with
repeated `--entrypoint`, replacing renamed paths and preserving other roots.
The exporter authenticates the prior crate and retires unchanged owned files;
never edit receipt hashes or delete modified files to bypass admission. After
refresh, move the receipt to `.toko-tooling.snapshot` and update consumer-owned
Make callers, CI, environment settings, instruction links and LOC metadata in
one adoption batch. Use an explicit manifest setting until that move is complete.

For Git provenance changes, use `refresh-consumer.sh --previous-source` with a
checkout retaining the recorded origin and revision. The new source must contain
that prior revision in its ancestry. Git-to-crate migration uses the installer's
`--source` argument for the same explicit proof. Redirects alone are not identity
proof. Preview first and retain the old receipt until admission succeeds.

### Adoption choices and prerequisites

- Keep the exact root Cargo pin inherited by every real member, Cargo.lock and
  installed-byte verification. A single carrier or metadata-only pin is not an
  alternative selection contract.
- Before hook activation, inspect tracked symlinks. The formatting hook requires
  regular tracked files, even for internal links permitted by the general source
  policy. For agent instructions, use a regular `AGENTS.md` and a regular
  `CLAUDE.md` containing `@AGENTS.md`. Commit removal of a replaced local hook
  before refreshing: the exporter will not overwrite staged deletions.
- Prepare jq, Mike Farah yq v4, Perl with JSON::PP and Digest::SHA, and Cargo.
  Archive refresh also needs Archive::Tar and IO::Uncompress::Gunzip. Installation
  checks these prerequisites before extracting or changing the snapshot.
- Run `bash scripts/dev/toko-tooling.sh update --preview` or
  `make toko-tooling-preview` to inspect without changing consumer files or tools.
  Preview downloads authenticated archives into external temporary scratch.
  Use `--snapshot-only` or `make toko-tooling-update TOKO_TOOLING_INSTALL_TOOLS=0`
  to apply files without installing tools; prepare tools explicitly before checks
  or workflows that require them. Normal update still installs and checks tools.
- Select executable entrypoints for the runtime closure. Governance guides are
  explicit optional entrypoints, not companions of runtime scripts. Adopt the
  baseline either as the standalone `TOKO-TOOLING.md` entrypoint or
  through a reviewed revision-bound link. The crate still contains the full source
  bundle; a smaller consumer snapshot does not change that distribution.
- Common engineering rules, integrity and execution admission remain mandatory.
  Product architecture, limits, supported targets and local commands belong in
  the consumer overlay. Conflicts such as existing Python tooling need a scoped
  maintainer-approved exception with its reason; they are not implicit defaults.
  Review conflicts before claiming baseline adoption. Snapshot installation and
  policy adoption are distinct results.

The current adoption contract remains exact Cargo selection plus authenticated
export and offline admission. Future incompatible adoption changes require a
minor release and an explicit migration guide; this pre-1.0 contract provides no
deprecation aliases. Verify snapshot integrity, local links and affected callers
before claiming a completed upgrade.

## Explicit IC tool selection in 0.26.0

After this release is reviewed and available, refresh through the canonical
exporter and update IC callers together. Do not export an uncommitted producer
working tree or remove catalog rows to express a subset.

- Declare the needed names in `IC_TOOLS` before including `make/consumer.mk`.
  For example, an optimizer-only job selects `IC_TOOLS := wasm-opt`. Products
  that need the complete set explicitly name `didc ic-wasm icp quill wasm-opt`.
  Host/Cargo-only consumers need no IC selection.
- Add repeated `--tool NAME` arguments to direct installer/check/preflight calls.
  Setup and checks must use the same set. The complete catalog remains validated;
  pins, authenticated archives and Binaryen runtime libraries retain their owners.
- Compact collection uses the caller's declared selection, never one inferred
  from installed files. Pass the action's `ic-tools` input, or the new final
  selection argument to direct compact-selector and compact-collector calls. Full
  collection retains its existing interface. An empty
  list retains full IC evidence. IC version reports require both pins and a
  quoted space-separated selection after the common host/Rust catalog.
- Explicitly prepare the selected tools before offline admission. Old bundles,
  failed candidates, archives and release evidence remain retained. Qualify
  actual consumer routing and applicable native hosts; producer fixtures alone
  do not qualify product builds or deployment.

## Dependency-role adoption in 0.24.0

Completing adoption includes the consumer-owned changes below; copying a new
snapshot alone does not classify dependencies or install a deployment gate.
For registry installation, use [Cargo selection and installation](#cargo-selection-and-installation)
below. The following Git export procedure remains available for historical
provenance, review and snapshot reorganization.

Select a released, reviewed source revision and refresh through the canonical
exporter. Include `rules/cargo-dependencies.md` and `rules/git-hooks.md` in the
governance selection. If the consumer uses selected-source manifest ordering,
also select `scripts/ci/cargo-release-metadata.sh` and its derived companions.
No step depends on a local draft, sibling working tree or `/tmp` proposal.

1. Identify the actual production packages and supported target/feature
   combinations from the consumer's build and deployment entrypoints. Review
   member normal, build, development and target-specific dependency tables.
   Split the root dependency catalog into two sections with the
   [canonical comment headers](../rules/cargo-dependencies.md#dependency-roles-and-deployment-checks):
   `# Production dependencies` and `# Tests and tooling only`. Put each header
   once above its section, with a blank line between sections; remove repeated
   role annotations after individual dependencies. Entries shared by production
   and tests belong in production; the catalog remains one owner. These headers
   belong only in `[workspace.dependencies]`, never in `[workspace]`, `members`
   or other tables. Remove role headers added outside the dependency catalog.
2. The shared formatting defaults preserve those sections with grouped sorting.
   Remove local `FORMAT_WRITE_COMMAND`/`FORMAT_CHECK_COMMAND` overrides that only
   repeat root-workspace Cargo sort and rustfmt. Keep complete paired adapters
   for additional work such as derive sorting, nested workspaces or frontend
   formatting; retain grouped sorting in those adapters.
   Use the same grouped policy in independent/nested-workspace formatters and
   all existing release adapter calls. Change `--manifest-order` to
   `--manifest-order-grouped` in current, commit-preparation and committed-source
   checks together. Both remain non-mutating; hooks continue to use `make fmt`.
3. Verify snapshot integrity and run the consumer's focused formatting/hook and
   release-adapter qualification. Show that a formatter rerun preserves groups,
   that check mode rejects unsorted entries within a group, and that a committed
   release check reads its selected source. Preserve dependency selections,
   lockfiles, release versions and unrelated work during adoption.
4. Review the production graph with the actual build target/features before
   deployment. Reuse the consumer's existing dependency-policy check to enforce
   any test-only exclusions, including transitive paths. If that enforcement is
   missing, record the remaining consumer work explicitly: Shared's sorter and
   metadata helper do not enforce role classifications. A passing snapshot or format
   check must not be reported as automated deployment admission.

Report baseline/snapshot adoption, formatting activation and deployment
enforcement separately. To request the complete change, say: "Adopt Toko
Tooling with two comment-headed dependency sections and matching formatting and
release checks; qualify the deployment dependency boundary and report any
remaining enforcement gaps." Registry publication remains a separate fact;
neither role labels nor cached build success establishes it.

## Cargo selection and installation

Starting with 0.36, Cargo owns tooling selection so `cargo update -v` reports
newer available releases. Declare the exact version once in root `Cargo.toml`:

```toml
[workspace.dependencies]
toko-tooling = "=0.36.0"
```

Every real workspace package, including a root package and private/test packages,
must inherit it unconditionally:

```toml
[dev-dependencies]
toko-tooling.workspace = true

[package.metadata.cargo-machete]
ignored = ["toko-tooling"]
```

Merge the cargo-machete exemption with existing entries: the dependency supplies
installed source tooling, not imported Rust APIs. Do not add an artificial Rust
import. Do not create a package to hold this dependency. A standalone package
without `[workspace]` declares `toko-tooling = "=0.36.0"` directly in its root
`[dev-dependencies]`, with the same exemption. The producer `toko-tooling` itself
is exempt from consuming its own package; it validates the packaged payload.

"Every manifest" means maintained workspace packages, not vendored dependencies,
build output or intentionally isolated fixture manifests. Independent maintained
workspaces apply the same rule at their own root. Non-Cargo repositories do not
create synthetic packages for notifications; they can use the authenticated
snapshot exporter directly, but have no `cargo update` visibility or Cargo-based
admission. The standard `toko-tooling-*` selection commands require a real
Cargo workspace/package.

An exact pin allows `cargo update -v` to show a newer available version without
silently adopting it. After deliberately editing the root pin:

```bash
cargo update -p toko-tooling # update Cargo.lock; leave unrelated selections alone
make toko-tooling-update  # install the selected authenticated crate and tools
make toko-tooling-check   # offline member/pin/lock/snapshot admission
```

`make toko-tooling-install` also installs the existing Cargo.lock selection;
`make toko-tooling-status` reports selected and installed identities offline.
Neither install nor update edits manifests or Cargo.lock. Commit the root pin,
member declarations, Cargo.lock and `.toko-tooling.snapshot` together. Cargo.lock
supplies the exact crates.io version and archive SHA-256; no separate tooling lock
or metadata version is used. Installation downloads the actual scripts, Make
interfaces, rules, pins, licenses and documentation from crates.io, authenticates
the archive, and preserves consumer-owned edits and recorded entrypoints.

Offline admission checks every workspace member, not just default-members, for
one inherited dev-dependency at the root pin. It rejects missing declarations,
local version overrides, normal/build dependencies, aliases, target-specific
selection and non-registry source overrides. Cargo.lock must contain exactly one
matching crates.io entry, and the installed receipt's version/checksum and owned
files must agree. Deleting a member cannot remove the dependency from remaining
members; deleting the last declaration fails admission. A changed Cargo selection
never silently installs tools. Common CI/release entrypoints require admission;
custom deployment adapters must invoke it before effects when bypassing them.

### Migration from Cargo dependencies

This migration replaces both the 0.34–0.35 metadata/lock scheme and the earlier
selection-only package approach:

1. Add the root exact dependency and inherited dev-dependency to every maintained
   workspace package, with the cargo-machete exemption above. Remove old
   `workspace.metadata.toko-tooling` / `package.metadata.toko-tooling` tables and
   `toko-tooling.lock`. Remove renamed, normal, build or independently versioned
   tooling dependencies; retain one inherited dev-dependency per package.
2. Remove packages created solely to hold tooling selection and their workspace,
   default-member, release-group, CI and documentation references. Preserve
   packages with real tools, tests or qualification code. No new helper package,
   `crates/Cargo.toml` or Rust import is needed.
3. Update Cargo.lock with Cargo. Bootstrap with the 0.36 installer from a reviewed
   producer checkout or an authenticated published 0.36+ crate extraction, since
   the old installer still expects metadata and a separate lock:

   ```bash
   bash /home/adam/projects/toko-tooling/scripts/dev/toko-tooling.sh update \
     --consumer "$PWD"
   ```

   The selected version must already be published. Payload bytes still come from
   crates.io. Historical Git receipts also need `--source` pointing at the old
   checkout for ownership proof; registry receipts use their prior authenticated
   archive. For first adoption, pass `--entrypoint make/consumer.mk
   --entrypoint TOKO-TOOLING.md` and other required roots, and include
   `make/consumer.mk` from the consumer Makefile before setup.
4. Run `make toko-tooling-check`, focused caller checks and existing membership/
   release-group validation. Replace obsolete metadata/lock and helper-package
   instructions in consumer-owned guides. Never hand-edit snapshot-owned files
   or receipts. Do not migrate consumers to an unpublished producer release.

Prepare the project's compiler/native prerequisites and host parsers first.
Installation requires Cargo, jq, Mike Farah yq, Perl Archive::Tar and
IO::Uncompress::Gunzip, then runs `make install-tools` and `make tools-check`.
Failure leaves evidence and reports incomplete adoption. Cargo builds themselves
never install tooling. Source review lives at
`https://github.com/dragginzgame/toko-tooling`.

## Upgrade impact

Adopt a snapshot for an applicable fix, a needed capability or an explicitly
requested refresh. An intact older snapshot does not trigger adoption on its own.
Batch optional refreshes with requested maintenance or a natural release boundary;
do not refresh every sibling after each Shared release. Required fixes retain
their own urgency and actual caller qualification.

Run `scripts/distribution/refresh-consumer.sh --impact --source /reviewed/shared-checkout
--consumer /consumer` before adopting a clean committed Shared revision. This
implies `--preview`; it never publishes or retires consumer files. The source
checkout's HEAD is the target revision unless `--revision FULL_COMMIT` is supplied.
Custom manifests and entrypoints use the existing exporter options.

Use `--revision` to export a reviewed commit already present in the local object
database while leaving unrelated source work in place. Payloads, modes, companion
closure and display version come only from that commit. Full commit IDs are
required; branches, tags, abbreviated IDs and unavailable objects refuse. Git
replacement refs cannot substitute another tree. No fetch, stash, branch switch,
index update or source checkout is performed.

The executing exporter and its version reader and integrity helpers must match their own committed
HEAD in bytes and executable mode. Dirty execution files refuse before helpers
run or consumer files change; unrelated dirty source files are allowed. Admitted
helpers run from private committed copies, independently of the payload revision.
A newer committed exporter can therefore export older payloads. Preview/impact
use the same admission. Consumer conflict, ownership and concurrent-change checks
still apply, and a changed source origin refuses before publication. Concurrent
source working edits do not change the pinned payload. After refresh, the admitted
verifier independently compares the manifest and files against the selected commit.

Producer validation can select `test-snapshot-distribution.sh --case revision`,
`--case core` or `--case transactions`. The full entrypoint retains all cases and
the broader exported-consumer fixtures.

The normal ADD/UPDATE/KEEP/REMOVE roster is followed by `AFFECTS` records mapping
changed payloads to current entrypoints through their transitive companions.
Shared integrity helpers affect every entrypoint. Cycles terminate. Retirements
remain separately listed; the report does not reconstruct historical caller
coverage or infer consumer behavior from filenames. `COMMIT` records give
path-filtered history from the recorded base to the selected target. A first
adoption or missing/divergent ancestry is explicit rather than guessed.

An older intact snapshot is not corruption. Review applicable changes and the
consumer's actual callers, then adopt normally and run focused consumer checks.
Use the central tooling inventory for fleet revision/integrity reporting; this
preview adds the impact of a particular proposed adoption, not another tracker.

## Consolidating existing snapshots

Use the explicit `--takeover-snapshot ROOT MANIFEST` option on
`scripts/distribution/refresh-consumer.sh`. ROOT is relative to the consumer
(`.` means its root); MANIFEST is relative to that snapshot root. The consumer
must still be a Git checkout root. For example:

```sh
/path/to/toko-tooling/scripts/distribution/refresh-consumer.sh --preview \
  --consumer /path/to/consumer \
  --takeover-snapshot . .supplement.snapshot \
  --takeover-snapshot helpers .toko-tooling.snapshot
```

Review the preview, then repeat without `--preview`. Without an explicit complete
`--entrypoint` set, existing and transferred entrypoints are unioned. Every
transferred record is proved against its recorded source commit, hash and mode;
edited, forged, staged, aliased or unavailable inputs refuse before publication.
The recorded revision must itself be a commit object. Tree or annotated-tag
object IDs and failed Git type observations cannot establish that identity.
The root must match the actual prior layout; it is never inferred from the
manifest's directory. The source remote must match the destination owner.

Selected canonical payloads are exported normally. Unneeded old copies retire
under the same unchanged-source proof as ordinary refresh; transferred receipts
retire only after the new manifest and payloads verify. Unrelated files, index
entries and tool/evidence storage remain untouched. No ownership records are
hand-edited. Subsequent refreshes need only the consolidated manifest. An
interruption can leave some retired copies absent or some prior receipts still
present: retain the preview and old receipts, inspect the new manifest's integrity,
and retry with the remaining receipts. This one-off ownership operation is not a
transactional release or a tool-store cleanup mechanism.

## Common catalog ownership in 0.10.0

Common installers now bring their reviewed pin catalogs as required snapshot
companions. Make, standalone setup, formatting, freshness checks and the evidence
action use the executing snapshot's defaults. Consumer-local catalog discovery
and implicit host-to-Rust override inheritance are removed. See
[catalog selection](local-setup.md#catalog-selection) for explicit exceptions.

Before adoption, compare existing host/Cargo versions and IC versions/digests
against the selected source commit. Qualify differences or retain an explicitly
selected consumer exception; do not silently replace a product requirement.
Review direct script calls, CI cache inputs, formatter adapters and evidence
action inputs as well as Make. Product and PocketIC selections stay local.
Compiler selectors are consumer-owned files implementing the
[shared support policy](../rules/cargo-dependencies.md#minimum-supported-rust-version-msrv);
snapshot export does not update them automatically.

For root-layout snapshots, existing consumer-owned `ci/tool-versions.env` or
`ci/ic-tools.tsv` may occupy the new payload paths. Move any still-needed local
selections to a distinct exception/workflow file and update their callers before
refresh. The exporter must retain its normal ownership and collision refusals;
never force an overwrite or hand-edit snapshot ownership. With a nested snapshot,
remove obsolete consumer copies only after all callers use the new defaults or
explicit exceptions. Do not delete catalogs still used by local CI tools.

Preview the full entrypoint closure, adopt the reviewed commit, explicitly run
`make install-tools` and `make tools-check`, then qualify formatting and the
consumer gate on its supported native hosts. Existing installations, failed logs
and release plans remain retained. This is a Toko Tooling breaking selection
change; a library consumer assesses its own published API impact independently.

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
`file` records. Integrity-only readers can still inspect historical manifests.
New exports require both version and entrypoint annotations, checked before
installation and during final verification with `--require-annotations`.
Refresh without recorded entrypoints requires explicit selection. Never hand-edit
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

Select both host/IC installers, `scripts/ci/select-tool-evidence.sh` and the
failure evidence action. The exporter derives `scripts/dev/tool-store.sh` and
the other declared companions, including `scripts/ci/test-tool-store.sh` when
installer fixtures are selected. Review that complete closure in the preview.

Run `make install-host-tools`, `make install-ic-tools`, then their offline
checks. Existing command/PATH entries remain `.tools/host/bin` and
`.tools/ic/bin`; these now resolve to shared bundles. The previous physical
checkout-local bundles remain retained but no longer pass active installation
checks. Use one explicit store setting for setup, checks and failure collection;
see [storage configuration and retention](local-setup.md#shared-executable-storage).
Consumers must not copy payloads or invent their own cache/link manager. Rust
tool installation and PocketIC ownership are unchanged in this batch.

This storage-admission change requires a Toko Tooling minor release. A consumer
library adopting it still classifies its own release by its published contract;
setup changes alone do not require a library minor release.

## Initial snapshot

Run the refresh helper from a clean Toko Tooling checkout:

```bash
/path/to/toko-tooling/scripts/distribution/refresh-consumer.sh \
  --consumer /path/to/consumer \
  --entrypoint make/consumer.mk
```

The helper validates every source path, exports regular files and their
executable modes from the selected Git revision at the same relative paths,
and writes `.toko-tooling.snapshot` in the consumer. Ignored or otherwise
uncommitted files cannot enter the snapshot, and concurrent working-tree edits
cannot change the exported bytes. The manifest
records format version `1`, source remote, source commit, and the SHA-256 digest
and executable state of every vendored file.
Records use exact tab-separated fields: extra, empty or trailing fields are
malformed. Refresh, verification and the tooling inventory reject them; literal
spaces within a valid relative filename remain significant and supported.

As of 0.9.0, those commands share the verifier's manifest reader. Refresh the
reporter and verifier together from the same reviewed revision; the verifier is
already included in every snapshot. Existing v1 manifests need no conversion.
For metadata inspection without payload verification, use:

```bash
bash scripts/ci/verify-toko-tooling-snapshot.sh --read-manifest /path/to/snapshot
```

This emits admitted tab-separated records in their original order after the
complete manifest passes syntax, duplicate and entrypoint-membership checks.
It preserves field bytes, omits ordinary comments and blank lines, rejects NUL
bytes and emits no records on invalid input. It does not require payloads to be
present. Success establishes syntax only: ordinary verification still checks
payload hashes/modes and required integrity tools, refresh proves prior ownership
against committed source, and fleet reporting checks source attribution and drift.
Each caller uses its own reviewed reader, never a helper from an inspected repo.

Refresh also writes a `# version<TAB>X.Y.Z` annotation from `[package].version` in `Cargo.toml` at that
exact source commit. The committed file must contain a canonical stable version;
the exporter never reads its display version from mutable working-tree bytes or
changes the consumer's own package version. This descriptive annotation keeps the
existing v1 integrity records intact and is understood by the current verifier
and fleet report. A snapshot without it displays `unrecorded`; its next refresh
records the selected source version. Malformed or duplicate version annotations
are rejected by the current verifier and refresh helper.

The commit remains the exact source identity: several commits can carry the same
version, and an annotation does not prove a release tag or publication. Local
verification checks files against the reviewed manifest, not the truth of its
source claims against GitHub. The annotation is not a second version selection
to edit independently. Cargo selects the tooling dependency; the snapshot records
installed bytes. The offline check requires both identities to agree.

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
/path/to/toko-tooling/scripts/distribution/refresh-consumer.sh \
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
manifest replacement stays on the same filesystem. Each payload is copied to a
temporary file beside its destination, rechecked against concurrent edits and
atomically renamed into place. An interrupted copy leaves the destination intact,
including when a selected subtree is on a different filesystem. Consumers should
ignore `.toko-tooling-refresh.*` and `.toko-tooling-payload.*` in case an
ungraceful process termination prevents normal cleanup.

Refresh replaces files individually and then publishes the manifest; it is not
an atomic replacement of the whole file set. If interrupted, stop consumer
validation, inspect the partial diff, and refresh again from the same reviewed
source revision. Verify the completed snapshot before resuming validation.

To extend an existing selection, repeat `--add-entrypoint` for each reviewed root:

```bash
/path/to/toko-tooling/scripts/distribution/refresh-consumer.sh \
  --consumer /path/to/consumer \
  --add-entrypoint scripts/ci/archive-evidence.sh
```

An already selected addition is harmless. Use the same `--manifest` for a custom
location. Existing source, provenance and destination checks still apply, and
all additions are admitted before any destination is replaced. Ordinary refresh
preserves the roots. Pass a complete replacement `--entrypoint` set to narrow or
replace them; supplying roots is a replacement, not an addition.

Selected committed files can declare unconditional shared dependencies on their
second line as `# Shared companions: relative/path another/path`, or for Markdown
as `<!-- Shared companions: relative/path another/path -->`. Markdown declarations
keep required guides with reduced task exports without selecting optional commands.
Refresh checks those declarations in the exported blobs, including companions' own declarations,
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
/path/to/toko-tooling/scripts/distribution/refresh-consumer.sh \
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
tools through `install-ci-tool.sh`. Selecting the fixture requires that command
and the checksum verifier, even if production CI uses only some of the tools. The
exporter includes that closure; omit the optional fixture and its callers if unwanted.
Selecting the production installer does not require the fixture.

Reusable Cargo, formatter, evidence, registry, release-PR and tool-command tests
also declare their required helpers. Review fixture dependencies when selecting
tests, not only production entrypoints. The PR fixture requires the PR helper
even though direct release delivery does not. Caller-owned pin files and optional
production modes stay explicit inputs rather than unconditional companions.
Fixtures using `scripts/ci/fixture-lifecycle.sh` include it through their companion
declarations. It removes successful fixture directories and preserves failures,
including premature exits; production entrypoints do not acquire this test helper.
Callers resolve the created fixture directory to an absolute physical path before
registering their trap or changing working directory; clear `CDPATH` for that
resolution. They own the directory, completion assertion and diagnostic label.
The helper reports cleanup failure even when invoked from a conditional, and
retains the original nonzero status if reporting fails. Sourcing it has no effects.
The release runner also includes `scripts/ci/check-release-tag.sh` so direct and
PR release paths use the same annotated-tag and exact-commit checks.

Run `test-snapshot-distribution.sh` from Toko Tooling for upstream exporter and
governance qualification. Consumers should normally omit that integration fixture
and its invocation, retaining actual snapshot verification and local adoption
checks rather than importing its growing upstream test roster.
If a consumer retains it, the optional companion-pair catalog checks only
entrypoints present in that checkout; omitted fleet reporters do not become
dependencies. Present entries still undergo derived-companion checks.
The remaining exporter, focused-fixture and governance integration inputs must
be available. Toko Tooling runs the complete catalog; a reduced consumer run
reports omissions and does not qualify the omitted upstream cases.

The validation-runner fixture is different: `test-validation-target-runner.sh`
invokes `scripts/ci/test-release-metadata.sh` in nested Make/release contexts.
Consumers retaining this fixture own that metadata-test entrypoint. Keep an
existing qualified adapter to the consumer's own release tests, as IC Backup
does. Do not import Toko Tooling's release metadata adapter or root Makefile
to satisfy it; without a local metadata-test adapter, run this integration check
upstream and retain focused local validation-runner checks.

The release runner requires `scripts/ci/validate-release-inputs.pl` and its
`StableVersion.pm` companion for admission and exact increment calculation. The runner,
validation logger and formatting hook also require
`scripts/ci/check-make-execution.sh`. The exporter derives these companions when
adding or refreshing those entrypoints; review the additions in the preview.
The public `next-release-version.sh X.Y.Z patch|minor|major` command delegates to
that same calculation, retaining exit 2 for invalid inputs. Existing direct callers
can keep their command and select it as an export root.

PR release delivery additionally requires `scripts/ci/release-pr.sh` beside
`run-release.sh`, the updated `docs/releases.md` and explicit
`RELEASE_DELIVERY=pr` selection. Add `release-merged-preflight` and qualify the
complete gate and receipt bindings in the retained merged checkout before
selecting it. Vendor `scripts/ci/test-release-pr.sh` when adopting the PR fixture
or the complete portable suite. Direct delivery remains the default; refreshing
the runner alone does not adopt PR delivery. See the
[PR release contract](releases.md#pr-delivery).

The catalog reader now derives `scripts/ci/StableVersion.pm` for canonical version
grammar. Refresh its complete declared closure, including manually copied fixture
payloads; IC callers carry that module too.

The IC installer shares matrix admission through `scripts/ci/ic-tool-pins.awk`.
Selecting `scripts/dev/install-ic-tools.sh` derives that companion automatically;
pin catalogs and product callers remain consumer-owned. The AWK backend now always
emits complete validated records; replace direct version-only or validation-only
uses with `tool-catalog-records.pl ic CATALOG [TOOL]` and explicitly discard stdout
when only admission is needed. Its former `tool` and `records` output selectors
are retired. Changelog preflight now derives `validate-release-inputs.pl` and its
companions and shares the runner's u64 version and Gregorian date admission.
Adopting 0.2.0 also
requires the [PocketIC ownership handoff](ic-tools.md#pocketic-ownership-handoff):
remove retired helper selections and update consumer pins and callers together.

Logger adoption can replace local batch concatenation with the runner's announced
unique combined failure file and `latest-combined.log`. Its existing `latest.log`
still names only the last failed target. Move consumer readers to the combined
path before removing a local aggregator; preserve target selection, storage-root
selection and any product-owned presentation in the caller.

Tooling adoption is reviewed through the [snapshot-drift task](../tasks/snapshot-drift.md),
using the maintained snapshot verifier. The retired tooling LOC reporter and its
two dedicated fixtures are no longer selectable entrypoints. Remove their local
callers and selections during an authorized refresh; retain the verifier and
checksum helpers required by other entrypoints.
`test-cloc-fixture-contexts.sh` is the upstream admission check for the reusable
LOC fixtures under enclosing Git/Cargo configuration, including sibling-report
checks with trailing-slash and aliased temporary roots. Refresh
`test-cloc-siblings.sh` to receive its physical-path correction; counts, partial
totals, error handling and retained failures keep their existing contracts.

Select `scripts/ci/install-ci-tool.sh` as the production entrypoint; the exporter
derives `scripts/ci/verify-file-checksum.sh`. Pins and installation arguments
remain consumer-owned. See the 0.11 adoption instructions below.

Cargo inheritance adoption selects `check-dependency-pins.sh`, whose declared
companions include `dependency-pins.jq`, then adds `--cargo-inheritance` to the
consumer's CI/release invocation. The workspace-version reader is independently available as
`scripts/ci/read-cargo-workspace-version.sh`. Include
`docs/verification-helpers.md` for their dependencies and boundary contracts.

## Drift verification

Consumers that vendor the verifier and checksum helper can check their snapshot
without the Toko Tooling checkout or network access:

```bash
bash scripts/ci/verify-toko-tooling-snapshot.sh
```

The verifier fails when a declared file is absent, symlinked, has different
content, or changes executable state. It hashes files directly through the host's
`sha256sum` or `shasum`, without executing the inspected snapshot's checksum helper
or any other declared file. This independent bootstrap is deliberate: shared
checksum code cannot establish its own integrity. Invoke a trusted copy of the
verifier; this is local integrity checking, not signed provenance or protection
against replacement of the verifier and manifest themselves. The source commit
and normal review establish provenance.

At adoption, independently select the reviewed full commit from the trusted
Toko Tooling repository. Do not take that trust decision from the consumer's
manifest or its `source` URL. Use a trusted verifier to compare every declared
payload's bytes and executable mode against that commit:

```bash
/trusted/toko-tooling/scripts/ci/verify-toko-tooling-snapshot.sh \
  --consumer /path/to/consumer --source /trusted/toko-tooling \
  --revision FULL_REVIEWED_COMMIT_ID
```

Both options are required together. This reads existing Git objects without
fetching, executing payloads or consulting CI; dirty source files do not supply
the evidence. A locally rewritten payload and matching manifest digest can pass
ordinary integrity checking but fail this source comparison. A changed manifest
revision also fails against the independently selected commit. The caller still
owns trust in the source checkout, verifier and selected commit. This does not
prove signatures, completeness of the consumer's selected entrypoints, product
behavior or producer CI qualification. Review the selection/companion closure
through the exporter and consumer diff; ordinary builds retain offline checks.

## Shared baseline and local instructions

[`TOKO-TOOLING.md`](../TOKO-TOOLING.md) is the reusable engineering baseline.
Vendor it with its linked guides so the complete rule set remains readable
offline. Keep the consumer's `AGENTS.md` as its entry point and local overlay;
do not copy Toko Tooling's `AGENTS.md` over it.

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
toko_tooling=/path/to/toko-tooling
files=()
while IFS= read -r file; do
  files+=(--entrypoint "$file")
done < "$toko_tooling/scripts/distribution/governance-files.txt"
bash "$toko_tooling/scripts/distribution/refresh-consumer.sh" \
  --consumer /path/to/consumer "${files[@]}"
```

The distribution fixture exports this list and checks its Markdown links inside
the isolated consumer. The source checkout's link check alone cannot establish
that exported documentation is complete. Keep the list current when adding guides.
It is an initial root selection. Subsequent refreshes derive script companions,
but do not implicitly adopt new governance roots from a changed list.

For an existing snapshot with recorded roots, add paths with `--add-entrypoint`
procedure above, including all shared rule files. Include
`docs/releases.md`, the release runner and its version/changelog helpers when
adopting the release command contract. The local `AGENTS.md` must direct contributors to
`TOKO-TOOLING.md`, identify `.toko-tooling.snapshot` as its source record, and
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

When adopting asynchronous hosted CI, refresh `TOKO-TOOLING.md`,
`rules/contributions.md`, `docs/supported-hosts.md` and
`docs/principles/reviewable-changes.md` from the reviewed commit.
Remove local instructions that require waiting for every integration/native job
before ordinary PR delivery or an otherwise authorized merge. Preserve focused
checks, review, enforced branch requirements, test coverage and separate
release/publication/deployment acceptance. Pending evidence must retain its
source/run links and be reported as pending; known failures need review and an
owning follow-up. Do not add a polling service or change branch protections as
part of this policy adoption.

Consumers pinned to an older snapshot keep those reviewed bytes intact. Adopt
a reviewed revision containing this rule, or record a maintainer-approved scoped
cadence exception until that refresh; a moving producer checkout is not adopted
policy. An explicit synchronous qualification request still applies. This rule
does not supply merge authority or relax an issue's stated acceptance criteria.

When adopting the local-repair and owning-repository issue workflow, refresh
`TOKO-TOOLING.md` and `rules/agent-maintenance.md` together from the reviewed
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
the consumer's owning issue and close it when its criteria are met. Update a
linked Shared issue only for its own remaining work; completed producer issues
stay closed while adoption proceeds in consumer issues. Retire redundant
coordination trackers through the maintenance procedure, preserving owner links.
Consumers still awaiting required CI remain open. This is
part of the agent's adoption/maintenance pass, not a GitHub auto-close-on-push
workflow or authorization to dispatch CI or start a schedule.

Consumers using the maintenance rule's exact-commit CI inspection command also
refresh `scripts/dev/gh-ci.sh` from that reviewed revision. It remains a read-only
interactive helper using the consumer checkout and authenticated GitHub CLI.
When adopting the repeatable task commands, include the `tasks/` catalog,
definitions, prompt and schedule guidance from the governance file list. Toko
Tooling coordinates central runs; consumers retain their focused commands and
product scope. The operational runner and systemd units are optional: their
selection roots are listed in
[`scripts/distribution/maintenance-files.txt`](../scripts/distribution/maintenance-files.txt).
A host that needs copied operational files may add those roots explicitly with
`--add-entrypoint`; their normal owner is the selected Toko Tooling checkout.
Existing snapshots retain recorded roots on ordinary refresh. To stop carrying
these files, pass the complete retained `--entrypoint` selection without the
optional roots. Canonical retirement refuses modified payloads; preserve local
work and resolve it before retrying. Never hand-edit a snapshot or delete its
owned files to bypass that check. Catalog/guidance adoption does not select
operational files, install a timer, activate an agent, grant sibling edit authority
or require Codex/systemd on every consumer host. Enable one local scheduler
separately when requested.
The portable-suite prerequisite check is Toko Tooling's own test setup, not a
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
The owner regression fixture, `scripts/ci/test-release-runner.sh`, simulates Git
effects. Consumers retain their actual integration checks and tool/snapshot
admission rather than routinely rerunning Shared's implementation regressions. Real-Git tracking qualification is independently selected
as `scripts/ci/test-release-tracking.sh`; the owner portable suite always runs it.
Select fixtures according to their documented effects and local authority rather
than editing vendored tests or invoking an unauthorized broader suite.
Pinning adoption also requires the checker and its jq module, prepared Git/jq/yq
tools (and Cargo for Rust workspaces), a CI/release invocation, and consumer-owned
qualification for locked builds and external inputs. Prepare YQ through the
common host bundle with `make install-host-tools` and `make host-tools-check`;
its reviewed version and platform digests live in the selected tool catalog.
Resolve existing exceptions under the
[pinning policy](../rules/dependency-pinning.md) before claiming adoption.

## Consumer qualification selection

Toko Tooling CI qualifies the shared implementation. A consumer qualifies how
its selected snapshot is wired into its own repository. Installing a snapshot
does not require running every exported upstream test in routine consumer CI or
adding the complete producer suite to its release gate.

Inspect the selected fixture's nested dispatch before running it. A single
script may still execute hundreds of unrelated cases. Prefer a named case or
bounded caller check, and add independent case selection when changing an
oversized fixture. Keep its full entrypoint for producer release qualification.
When narrow selection is unavailable, report the specific gap rather than
silently substituting the complete upstream suite during ordinary development.
Development completion requires the affected proof, not release readiness for
every host or automatic follow-up adoption in another workspace.

During adoption, inventory prerequisites for **every target in the actual release
gate**, including nested/native and browser steps. Keep consumer-owned
system/compiler bootstrap before shared or selected Cargo-tool preparation;
post-common install extensions cannot supply those earlier prerequisites.
Wire explicit setup and read-only admission for the full inventory, and qualify
the real adapter's order and failure stopping using
[script-boundary substitutes](make-interface.md#hosted-preparation-and-disposable-fixtures).
Keep [native fingerprint rejection and re-preparation](local-setup.md#native-bootstrap-before-cargo-tools)
intact; neither a generic tool check nor a substituted ordering fixture proves
complete native/product acceptance.

| Proof | Owner and selection |
| --- | --- |
| Snapshot bytes, modes and declared roots | Consumer: verify every maintained bundle. Integrity alone does not establish caller behavior. |
| Shared parser, installer and helper implementation cases | Toko Tooling: keep the general regression matrix beside its owner. Select relevant cases during a changed-contract adoption or when they supply a missing consumer proof. |
| Actual Make, hook, release adapter and CI wiring | Consumer: exercise its real callers, selected inputs, status propagation and applicable recovery/evidence boundaries. |
| Installed tools and host behavior | Consumer: retain prepared-tool admission and applicable native platform qualification for its own selected catalogs and workflows. |
| Product correctness and release acceptance | Consumer: retain its product tests and release obligations. A passing upstream job does not qualify them. |

Before removing a test invocation, identify what it proves and where that proof
will remain. For example, a generic maintenance-task or LOC parser matrix usually
belongs upstream; a test that checks the consumer's actual Make target or input
selection belongs with that consumer. A generic release-runner fixture may still
be useful when it exercises a consumer-specific invocation contract not covered
by its adapter tests. Classify behavior, not filenames or the number of tests.
Keep justified upstream fixtures with a short explanation at their caller.

For a roster reduction, check the affected consumer callers and failure paths,
verify snapshot integrity, and retain the required native acceptance. Measure
gate duration before and after on comparable source/tool/host inputs if claiming
a speed-up. Syntax checks, snapshot hashes and upstream CI each provide different
evidence; none alone replaces a consumer integration test. Do not remove product
or recovery tests merely because their helpers are now shared.

Revisit the selection when adopted helper bytes or contracts, caller wiring,
tool catalogs or supported-host inputs change. Reuse evidence only for unchanged
inputs under the [validation policy](../TOKO-TOOLING.md#validation-and-evidence).
Keep evidence and consumer-specific exceptions with their existing owners; no
new scheduler or parallel test inventory is required. This guidance does not
itself rewrite existing CI/release targets or authorize broad local execution.

Only retire an exported test entrypoint after checking all consumer callers.
Use the canonical refresh preview to inspect its companion closure and proposed
retirements; do not delete snapshot files or edit their manifest records by hand.
Shared fixtures that remain dependencies of another selected root stay exported.

## Audit method adoption

Audit methods are optional export roots, not an automatic baseline dependency.
Select `audits/README.md` and its companions when offline audit procedures are
needed; otherwise use the baseline's revision-bound method links for an explicitly
requested audit. The snapshot's reviewed source revision identifies the common
contract; do not read methods from a moving sibling checkout or copy uncommitted
documents and attribute them to an older revision.

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

For the required shared Cargo-installed set, include
`TOKO_TOOLING_CARGO_SORT_VERSION` and `TOKO_TOOLING_CARGO_SORT_DERIVES_VERSION`
in the selected versions file. Candid extraction callers own an explicit selected
Cargo-tool pin, install/check target and executable PATH; qualify those before
adopting this snapshot. The common set no longer selects the extractor.
Canic's [explicit setup contribution](https://github.com/dragginzgame/canic/pull/597)
must be integrated before that consumer adopts the narrower common set.
The aggregates always include Rust tools, even in non-Rust repositories;
prepare a declared Rust/Cargo toolchain first as described in
[Rust setup](local-setup.md#rust-development-tools). Retire duplicate
Cargo-tool install recipes and version constants after qualified adoption.
Consumer-selected registry binaries/examples use this same installer and its
derived companions, with prepared host tools and an
explicit local selection; see [Cargo tool setup](local-setup.md#consumer-selected-cargo-tools).
Retire synthetic Cargo resolvers only after qualifying the selected package and
profile. Source-checkout builds and application executable overrides stay local.

### Rust installation adoption in 0.4.0

This is a breaking installation contract for Toko Tooling. Consumer libraries
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

Select `make/tools.mk`, the baseline and setup guidance from a reviewed committed
revision; the Make entrypoint derives the common installers and their companions.
Adopt the common snapshot catalogs (or explicit qualified exceptions) and refresh
selected fixtures and evidence collectors together; the selector calls the complete host check too.
The current contract has no parser-only host mode or optional ripgrep/cloc flags.
Remove `--with-ripgrep`/`--with-cloc` from direct callers, including custom CI and
evidence checks. Retain historical documents as historical evidence.

Every aggregate includes the common Cargo formatter tools after host setup,
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

Defaults use scripts and pins from the reviewed snapshot. For a snapshot stored below
that root, set `TOKO_TOOLING_ROOT` to its reviewed local directory before the
include, and include its `make/consumer.mk`. `HOST_TOOL_VERSIONS`,
`RUST_TOOL_VERSIONS` and `IC_TOOL_PINS` independently select consumer-owned pin
exceptions; their defaults use the snapshot's `ci/tool-versions.env` and
`ci/ic-tools.tsv`. Command selections target the
consumer checkout; host/IC payloads use the [shared per-user store](local-setup.md#shared-executable-storage).
The [consumer Make interface](make-interface.md) selects the setup, release and
Rust formatting modules and supplies hook installation and snapshot checking.
Select `make/consumer.mk` and review its derived closure, keeping product admission,
metadata, validation and custom formatter adapters local. Non-Rust consumers explicitly
set `TOKO_TOOLING_RUST := 0`. Existing focused module includes remain supported;
do not include them again alongside `consumer.mk`.

Export every parsed include in isolated Make/hook fixtures, including the extra
roots passed to `check-release-commands.sh`. That checker now copies their complete
declared closure through `prepare-script-fixture.sh` before replacing the runner;
refresh partial selections to include that closure and the preparer's companions.
Keep `make/execution.mk` and its probe so
unsupported Make modes cannot turn failures into success. The complete
`test-git-hooks.sh` fixture exercises `make/consumer.mk`; the exporter includes
that declared dependency automatically. `test-make-consumer.sh` tests common routing
with substitute format/release commands; native consumer qualification remains separate.
Refresh the failure collector to retain `formatting.*` logs in CI. Standard Rust
formatting now uses the declared `run-rust-formatting.pl` companion for visible
per-pass timings and exact-input reuse; refresh its complete export/fixture closure.
Replace workspace/derive/order-only adapters with `FORMAT_RUST_ARGS` under the
[formatting interface](make-interface.md#custom-formatting-without-replacement-recipes),
retaining the actual root/extra workspace order and manifest policy. Opaque
frontend/custom commands remain paired and uncached. Replace local
recipes only after testing their actual callers and hook, following the
[adoption checklist](make-interface.md#adoption).

`make cloc` reports the consumer's root Cargo workspace and requires its prepared
Rust toolchain. Toko Tooling itself selects `CLOC_REPORT` and `CLOC_ROOT` before
the include to summarize sibling workspaces; consumers normally use the defaults.
Installing the raw cloc executable alone does not add these Make commands.
Use the [tooling audit tasks](https://github.com/dragginzgame/toko-tooling/blob/08cd17240fd60dfd309e35eb131e73323e883e6f/docs/fleet-tools.md#tooling-adoption-audit) for snapshot
integrity, applicable upstream changes and actual consumer wiring. The former
`cloc-tooling` command is retired; Rust LOC remains available through `cloc.sh`.

To retire unnecessary fleet copies, review their local callers first, update
Make/CI/help references, then select the remaining entrypoints and refresh through
the exporter. Preserve ownership records until
that refresh completes. Candidates are `cloc-tooling.pl`, the retired
`cloc-siblings.sh` entrypoint and fleet-only tests; keep the unified `cloc.sh`
for workspace and parent-directory reports. Preserve
checksum/verifier helpers used by other selected tools, local integration checks
and historical evidence. Automatic retirement is limited to verified unchanged
payloads outside the selected closure.
This is a consumer-owned selection change, not permission for another repository
to edit its files. Snapshot verification and its regression coverage remain maintained
and tested in Toko Tooling.

Review pins against existing qualified versions before activation. A local
exception uses its own explicitly selected matrix outside the snapshot; remove
superseded pin ownership rather than maintaining two independent selections.
Update local setup/CI callers to use `.tools/host/bin` and
`.tools/rust/common/bin`, plus `.tools/ic/bin` for explicitly selected IC tools; CI
must invoke the same Make installation/check targets and put these paths on its
own PATH after explicit setup. Document the shell export for direct interactive commands;
installation and Make exports do not change the user's terminal PATH. Remove
separate package-manager jq/yq/ripgrep/cloc selections and preserve one owner
for each tool pin.
YQ belongs to the common host bundle; the standalone CI installer no longer
accepts `yq`.
Document the [system bootstrap prerequisites](local-setup.md#bootstrap-prerequisites)
and retain product-specific
alignment checks. Retire duplicated installer bodies once callers have moved;
do not patch vendored files. Keep package-manager setup for unrelated tools.

Qualify explicit installation and offline checking on the consumer's declared
native hosts, plus relevant product checks for changed tool selections. Do not
claim deployment or PocketIC client/server compatibility from `--version` alone.
Snapshot integrity and consumer adoption remain separate from upstream fixtures.

The Rust setup action is an independent entrypoint:
`.github/actions/prepare-rust-toolchains/action.yml`. Its declared companion is
`scripts/dev/prepare-rust-toolchain.sh`; the canonical exporter includes that
helper automatically when selecting or refreshing the action.
Select `docs/rust-toolchains.md` too for its [caller contract](rust-toolchains.md).

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

Registry observation accepts `check-crates-io-version.sh CRATE VERSION` for
HTTP-status-only presence checks. Reconciliation callers select `--metadata
NEW-DIRECTORY CRATE VERSION`, prepare jq and curl 8.4.0+, and retain each attempt's
directory. Both contracts require handling all three statuses before removing
local transport/parsing. The consumer still owns
payload verification, yanked-version acceptance and publication reconciliation.

File-digest generation uses the checksum verifier's additive `--print` interface,
so its existing snapshot file set is sufficient. Refresh that verifier before
changing local hash callers. Advisory database preparation is independently
available as `scripts/ci/prepare-rustsec-db.sh` with no shared-script dependencies;
include it and `docs/verification-helpers.md` when adopting it. Replace only
database acquisition/isolation, retain consumer audit policy and failure evidence,
and pass `--no-fetch` when auditing the prepared database so its identity stays
bound to the recorded commit.

## CI installer command in 0.11.0

Replace `install-actionlint.sh`, `install-gitleaks.sh`, `install-shellcheck.sh`,
and `install-sccache.sh` calls with `install-ci-tool.sh TOOL`, where
TOOL is respectively `actionlint`, `gitleaks`, `shellcheck` or `sccache`.
Keep existing `--version`, `--sha256` and `--install-dir` arguments and local pins.
There are no forwarding aliases in 0.11.

The former `install-yq.sh` and `install-ci-tool.sh yq` routes are retired. Use
`make install-host-tools` and offline `make host-tools-check`, with yq selected by
the reviewed host catalog. Retire the standalone installer call and its duplicate
pin only after callers use `.tools/host/bin/yq`; retain explicit qualified host
catalog exceptions through `HOST_TOOL_VERSIONS`.

As of 0.16.0, every invocation must include `--install-dir`; an environment-only
destination or the former home-directory default is no longer accepted. Update
callers before refreshing the command and its selected installer fixture.

Before refresh, replace each recorded wrapper entrypoint with
`scripts/ci/install-ci-tool.sh` in the reviewed selection. Pass the complete new
root set with repeated `--entrypoint` arguments; `--add-entrypoint` alone retains
the old roots. Use the normal preview and owned-file retirement checks to remove
snapshot-owned wrappers; preserve modified or consumer-owned files for review.
Refresh `test-installers.sh` too if selected, then qualify the same installer
failure cases and each declared native host. No storage layout or tool upgrade
is part of this command change.


## Cargo dependency boundaries

Adopt `scripts/ci/check-cargo-dependency-boundaries.sh` as a snapshot entrypoint
and run it with `--consumer /absolute/repository/path`. The checker uses locked,
offline Cargo metadata; prepare the selected toolchain and lockfile first. It
requires Perl with JSON::PP. Policy lives in `ci/cargo-dependency-boundaries.json`:

```json
[
  {"dependency": "ic-stable-structures", "allowed_packages": ["ic-storage"]}
]
```

Each rule restricts a canonical dependency package name to the listed workspace
packages. An empty allowed list bans that dependency everywhere in the workspace.
Renames do not bypass the rule. Normal, build, development and target-specific
direct dependencies are included. Transitive dependencies are not restricted.
Malformed rules, repeated dependency rules or allowed packages, and unknown
fields fail closed before Cargo runs. The selected consumer must be the workspace
root with a regular `Cargo.toml`; failed or incomplete Cargo output and mismatched
workspace identity refuse before any success report. This opt-in policy is
separate from version pinning; consumers choose their own ownership rules.


### CBOR boundary coverage

With `--external-ic`, the Cargo checker includes the centrally maintained CBOR
inventory as a snapshot companion; local allowed-package lists cannot relax it.
Pair this with `check-candid-boundary.pl --cbor ROOT POLICY SOURCE...` and the
browser SDK boundary checker, which also rejects direct CBOR packages/imports.
Do not copy the package list into each consumer or replay Shared's generic tests.

Run Cargo admission separately for every maintained independent Cargo workspace,
including fixtures and generators; a parent workspace's metadata does not cover
excluded packages. Select browser and generated source roots explicitly. An
alternate `manifest.toml` template is not admitted by Cargo metadata: retire an
obsolete executable fixture, or check its actual materialized Cargo workspace
through the owning fixture workflow before compilation. Review the template and
its materialization as inputs; a root-only pass does not cover them. Do not rename
or materialize consumer manifests inside the shared checker.

Platform implementation roots are producers, not browser consumer roots. Rust
source exceptions use exact file/crate entries with reasons and review conditions;
Cargo/browser consumer checks have no generic bypass flag. A new exceptional
package owner needs an explicit reviewed shared-policy change, not a permissive
consumer allowlist. Source masking/literal imports are lexical evidence only;
dynamic imports, generated aliases, copied parsers and byte compatibility retain
explicit review/consumer qualification obligations. No automatic release gate or
fleet adoption is activated by exporting these helpers.
