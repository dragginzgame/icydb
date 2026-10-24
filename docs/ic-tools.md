# Common local IC tools

Every repository can provide the same local executable names through a reviewed
Shared Tooling snapshot. Run from the checkout root:

For the complete developer setup including jq and yq, use
[local setup](local-setup.md) and `make install-tools`. The targets below prepare
only the IC executables.

```bash
make install-ic-tools IC_TOOLS='ic-wasm wasm-opt'
make ic-tools-check IC_TOOLS='ic-wasm wasm-opt'
export PATH="$PWD/.tools/ic/bin:$PATH"
wasm-opt --version
```

Installation is explicit and downloads the selected official release assets.
The check is offline: it verifies installed file hashes and reported versions.
Ordinary tests, builds and release validation must not invoke the installer
implicitly. Neither command starts a network, configures identities, submits
Quill messages or performs deployment/publication effects.

## Selected set

Products declare `IC_TOOLS` before including `make/consumer.mk`, for example
`IC_TOOLS := ic-wasm wasm-opt`. Both setup and offline checking require the same
nonempty selection. Direct calls require repeated `--tool NAME` arguments.
Unknown or empty selections refuse before setup; order and duplicate requests
do not change the selected set. Unselected tools are neither downloaded nor run.
When IC setup is registered in the common aggregate, its selection is admitted
before common host/Cargo preparation, so a missing declaration fails early.

The `didc` executable serves required Candid interfaces and their compatibility
checks. Its presence in this bundle does not select an application encoding;
follow the [IC Wire preference](../DRAGGINZGAME.md#ic-application-encoding) for
controlled messages. IC Wire's generated codecs do not replace Candid IDL tools
where an external interface requires them.

The reviewed default pins live only in [ci/ic-tools.tsv](../ci/ic-tools.tsv).
Each row specifies executable, exact
version, native host and archive SHA-256. The digests were read from the official
GitHub release asset metadata on 2026-10-06; the Binaryen 133 archive digests
were checked against downloaded bytes for all three host mappings on 2026-10-10.
ICP CLI 1.7.0 digests were reviewed against official release-asset metadata on
2026-10-17; all other selections are unchanged. Native installer/check results
remain source-bound qualification, separate from asset availability
([#268 — Adopt ICP CLI 1.7.0](https://github.com/dragginzgame/shared-tooling/issues/268)).

| Executable | Default version | Official release |
| --- | --- | --- |
| `quill` | 0.5.4 | [Quill](https://github.com/dfinity/quill/releases/tag/v0.5.4) |
| `icp` | 1.7.0 | [ICP CLI](https://github.com/dfinity/icp-cli/releases/tag/v1.7.0) |
| `didc` | 0.6.2 | [Candid tools](https://github.com/dfinity/candid/releases/tag/didc-v0.6.2) |
| `ic-wasm` | 0.11.1 | [ic-wasm](https://github.com/dfinity/ic-wasm/releases/tag/0.11.1) |
| `wasm-opt` | 133 | [Binaryen](https://github.com/WebAssembly/binaryen/releases/tag/version_133) |

The complete set has native assets for Linux x86-64, macOS Intel and macOS
Apple Silicon. Unsupported hosts fail before downloading; Linux ARM64 is not
silently given Quill's ARM32 asset or an emulated executable. dfx is excluded
from the standard toolset. Availability of an asset does not prove native qualification.

Prerequisites are Bash 3.2+, curl, tar with xz/gzip support, Perl, and either
`sha256sum` or `shasum`. `make` exposes the standard targets. No Rust compilation,
sudo, package-manager bootstrap or shell-profile editing is involved.
Explicit downloads use the common [bounded transient retry policy](local-setup.md#pinned-local-tool-sets).

## Installation and retention

`scripts/dev/install-ic-tools.sh` defaults to its containing checkout and
`ci/ic-tools.tsv`. `--consumer <checkout>` selects another explicit destination;
`--pins <file>` selects a reviewed alternative matrix. Missing or duplicate rows,
inconsistent per-host versions and malformed checksums fail before downloads.
The installer enforces that admission through
`scripts/ci/ic-tool-pins.awk`; include it in the reviewed snapshot.

The installer checks each archive hash before extraction or execution, then
checks the exact version token. It retains Binaryen's runtime library beside
`bin/`, including the library required by native macOS `wasm-opt`.

A complete candidate is built in the [shared per-user store](local-setup.md#shared-executable-storage),
with a lightweight `.tools/ic-set.<unique>` reference in the initiating checkout.
Installed file hashes, host, the canonical `tools` selection, complete input pins
and selected downloaded inputs remain in the
shared bundle. Only after all
tools pass does an atomic symlink replacement select `.tools/ic`. Existing
global tools and shell profiles are untouched. Valid repeated setup is offline.
Failed or interrupted attempts retain their candidate directory and leave the
prior selected set intact; retries create a fresh candidate. Previous sets and
downloads are not automatically deleted. Cleanup is an explicit local action.

Installation and verification admit the complete matrix, including a final row
without a newline, then select only requested tools. The exact pin-file bytes
remain retained as provenance. Reuse compares requested tools' validated
tool/version/host/archive-checksum records across all supported hosts.
Comments, row order and changes to unselected valid records do not change that selection
or trigger downloads. The installed pins remain the exact installation input;
neither the caller's pins nor installed receipts are rewritten during reuse.
Every changed selected record requires explicit setup, and offline reuse still verifies
the host, installed file hashes, pinned archives, extracted executable/library
bytes and executable versions. A changed binary and edited local receipt cannot
authorize reuse. Changes to the caller's
pin file during installation still prevent activation of that candidate.

Setup and offline checks admit the literal `.tools/ic` link target. Malformed
managed names, including trailing newlines, are refused before tool execution
or downloads; the existing link and bundles remain intact.

A shared selection lock and a checkout lock prevent competing installers. After an abrupt kill, inspect
`.tools/.ic-tools.lock/owner` and confirm the process has stopped before removing
that exact stale lock; the store slot also records `install.lock/owner`.
Never steal an active installation lock. A checkout activation lock alone does
not prevent checking its previous complete set. A held shared-slot lock does:
offline checks refuse with the exact lock path until its owner finishes, even
when the checkout already references a verified generation in that slot.

The installed hash receipt detects accidental byte changes; it is not a signed
attestation against an attacker who can rewrite the checkout and receipts.
The reviewed archive hashes and snapshot provenance establish admitted inputs.
The native CI matrix exercises installation, archive integrity, version execution
and offline reuse on the three declared hosts. Semantic optimizer qualification
is owned by [IC Build](https://github.com/dragginzgame/ic-build/pull/62): its native
matrix runs real `-O3`, `-Os` and `-Oz` transforms through retained artifact
transactions, checks repaired cache reuse and executes the transferred Wasm oracle.
Run its local `scripts/ci/qualify-wasm-opt.sh` with an explicitly admitted optimizer
and a new absolute evidence directory. The former Shared qualification command
is removed; this does not replace product canister/deployment acceptance.
Product build and deployment qualification stay with consumers;
PocketIC compatibility and server admission belong to Testkit.

## Consumer adoption

### PocketIC ownership handoff

IC Testkit owns PocketIC-specific release selection, asset checksums, explicit
provisioning, offline admission and server lifecycle in the
[IC Tooling workspace](https://github.com/dragginzgame/ic-tooling/tree/main/crates/ic-testkit).
File current capability/adoption work in IC Tooling. The earlier published 0.25.4 CLI
setup/check and managed-launch contract passed all three native hosts in
[owner CI](https://github.com/dragginzgame/ic-testkit/actions/runs/37901828971).
The historical [Testkit #38](https://github.com/dragginzgame/ic-testkit/issues/38) and
[Shared #76](https://github.com/dragginzgame/shared-tooling/issues/76) retain the
original handoff evidence. Consumers select a qualified
Testkit package; they do not duplicate its server catalog or version policy.

Shared 0.2.0 removes PocketIC from the required IC matrix and removes
`check-pocketic-alignment.sh`, `check-pocketic-binary.sh` and their dedicated
fixture. The shared Make targets retain their names and now prepare/check the
five-tool bundle only. This is a breaking adoption boundary:

1. Select a published, qualified Testkit CLI with setup/check support, using the
   consumer's reviewed dependency and executable selection. Add explicit Testkit
   setup to developer/CI preparation; offline validation uses its check command.
2. Update Make/CI/test/release callers that select `.tools/ic/bin/pocket-ic` or
   compare client and server versions. Obtain the admitted absolute server path
   from Testkit, or use its managed run contract. Keep product test topology,
   credentials, endpoints and evidence destinations with the consumer.
3. Remove PocketIC rows from consumer-owned IC matrices and remove the retired
   checkers/fixture from snapshot selections and callers. Refresh the installer,
   matrix validator, guides and canonical pins together from a reviewed release.
   Snapshot refresh does not prune retired selections automatically; do not patch
   vendored implementations or introduce a second server installer.
4. Run explicit `make install-ic-tools`, then `make ic-tools-check`. An old
   six-tool bundle fails the new offline check; only explicit setup activates
   a new five-tool bundle. Old bundles, pins, receipts and failed candidates
   remain unchanged. No automatic cleanup or conversion occurs.
5. Qualify consumer setup, offline check and actual product startup on Linux and
   both supported macOS hosts before retiring its former route. Until adoption
   is ready, the consumer keeps its existing complete reviewed snapshot.

For an already prepared, consumer-selected `ic-testkit-server` executable:

```bash
ic-testkit-server setup --directory "$PWD/.tools/testkit-server"
server="$(ic-testkit-server check --directory "$PWD/.tools/testkit-server")" || exit 1
export POCKET_IC_BIN="$server"
```

Only setup downloads. Check prints the admitted executable path and works
offline; tests and builds must not implicitly run setup. Do not select binaries
by globbing caches. Existing retained bundles are evidence, not the active
Testkit selection.

### Identity storage and local resets

Keep identity/key stores outside every directory removed by a consumer's reset
or fresh-deploy commands. If a consumer selects `ICP_HOME`, it must not sit
under disposable network or build state such as `.icp`, `.canic` or `target`.
The consumer owns its cleanup path inventory and backup/recovery procedure;
verify preservation with non-secret sentinel files when testing reset helpers.

A dedicated repository-local home such as `.icp-local-home` is one option,
provided it is ignored by Git, backed up appropriately and excluded from all
cleanup paths. An existing persistent home can also satisfy the rule. Scope
custom home selection to the consumer's CLI wrapper rather than a global shell
export that changes identity selection in sibling repositories. Common tool
installation does not move identity stores, create keys or change that selection.

### Snapshot and pin selection

Binaryen 133 changes optimization at `-O3` and `-Os` and above; see its
[upstream changes](https://github.com/WebAssembly/binaryen/blob/version_133/CHANGELOG.md).
Adopting its pins requires explicit `make install-ic-tools` before offline
checks, with previous installations and failed candidates retained. Qualify
actual consumer Wasm bundles and startup on the supported native hosts before
switching a product's optimizer selection. In particular, IcyDB's separate
post-link optimizer byte checks remain consumer-owned: a common pin update
does not qualify or replace that policy. A consumer with outstanding product
qualification may keep its reviewed local matrix outside the shared snapshot.

Follow [snapshot adoption](consuming-snapshots.md#local-ic-tool-adoption). Use one
authoritative pin matrix for this set; remove duplicate version/checksum selections
from old setup files after their callers move. The offline IC check prints the
verified absolute bin directory for these five tools. PocketIC callers follow
the Testkit handoff above.

Consumers can select an explicitly reviewed local matrix when an existing
qualification requires different versions. Keep the common executable names
and installation/check behavior, document the reason, and qualify the selected
versions before switching. Keep overrides outside the immutable shared snapshot.
Do not copy version constants into a second catalog or upgrade tools implicitly
as part of baseline adoption.

This script owns executable provisioning. Build and Candid extraction orchestration
belong to IC Build; host primitives and IC interpretation belong to the Host
crates. Both now live in IC Tooling. See the [capability boundary](https://github.com/dragginzgame/shared-tooling/blob/main/README.md#capability-ownership).
Consumer orchestration, credentials, destinations and measurement policy remain local.
