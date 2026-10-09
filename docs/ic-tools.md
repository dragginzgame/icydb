# Common local IC tools

Every repository can provide the same local executable names through a reviewed
Shared Tooling snapshot. Run from the checkout root:

For the complete developer setup including jq and yq, use
[local setup](local-setup.md) and `make install-tools`. The targets below prepare
only the IC executables.

```bash
make install-ic-tools
make ic-tools-check
export PATH="$PWD/.tools/ic/bin:$PATH"
quill --version
```

Installation is explicit and downloads the selected official release assets.
The check is offline: it verifies installed file hashes and reported versions.
Ordinary tests, builds and release validation must not invoke the installer
implicitly. Neither command starts a network, configures identities, submits
Quill messages or performs deployment/publication effects.

## Selected set

The reviewed default pins live only in [ci/ic-tools.tsv](../ci/ic-tools.tsv).
Each row specifies executable, exact
version, native host and archive SHA-256. The digests were read from the official
GitHub release asset metadata on 2026-10-06.

| Executable | Default version | Official release |
| --- | --- | --- |
| `quill` | 0.5.4 | [Quill](https://github.com/dfinity/quill/releases/tag/v0.5.4) |
| `icp` | 1.6.0 | [ICP CLI](https://github.com/dfinity/icp-cli/releases/tag/v1.6.0) |
| `didc` | 0.6.2 | [Candid tools](https://github.com/dfinity/candid/releases/tag/didc-v0.6.2) |
| `ic-wasm` | 0.11.1 | [ic-wasm](https://github.com/dfinity/ic-wasm/releases/tag/0.11.1) |
| `wasm-opt` | 132 | [Binaryen](https://github.com/WebAssembly/binaryen/releases/tag/version_132) |

The complete set has native assets for Linux x86-64, macOS Intel and macOS
Apple Silicon. Unsupported hosts fail before downloading; Linux ARM64 is not
silently given Quill's ARM32 asset or an emulated executable. dfx is excluded
from the standard toolset. Availability of an asset does not prove native qualification.

Prerequisites are Bash 3.2+, curl, tar with xz/gzip support, Perl, and either
`sha256sum` or `shasum`. `make` exposes the standard targets. No Rust compilation,
sudo, package-manager bootstrap or shell-profile editing is involved.

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

A complete candidate is built under `.tools/ic-set.<unique>/`. Installed file
hashes, host, selected pins and downloaded inputs remain there. Only after all
tools pass does an atomic symlink replacement select `.tools/ic`. Existing
global tools and shell profiles are untouched. Valid repeated setup is offline.
Failed or interrupted attempts retain their candidate directory and leave the
prior selected set intact; retries create a fresh candidate. Previous sets and
downloads are not automatically deleted. Cleanup is an explicit local action.

Installation and verification consume every validated row, including a final row
without a newline. The exact pin-file bytes remain retained as provenance.
Reuse compares the complete validated tool/version/host/archive-checksum records
across all supported hosts. Comments and row order do not change that selection
or trigger downloads. The installed pins remain the exact installation input;
neither the caller's pins nor installed receipts are rewritten during reuse.
Every changed record requires explicit setup, and offline reuse still verifies
the host, installed file hashes and executable versions. Changes to the caller's
pin file during installation still prevent activation of that candidate.

Setup and offline checks admit the literal `.tools/ic` link target. Malformed
managed names, including trailing newlines, are refused before tool execution
or downloads; the existing link and bundles remain intact.

A directory lock prevents competing installers. After an abrupt kill, inspect
`.tools/.ic-tools.lock/owner` and confirm the process has stopped before removing
that exact stale lock. Never steal an active installation lock. The offline
check can inspect the previous complete set during installation.

The installed hash receipt detects accidental byte changes; it is not a signed
attestation against an attacker who can rewrite the checkout and receipts.
The reviewed archive hashes and snapshot provenance establish admitted inputs.
The native CI matrix exercises installation and version execution on the three
declared hosts. Product build and deployment qualification stay with consumers;
PocketIC compatibility and server admission belong to Testkit.

## Consumer adoption

### PocketIC ownership handoff

IC Testkit owns PocketIC-specific release selection, asset checksums, explicit
provisioning, offline admission and server lifecycle. Its published 0.25.4 CLI
setup/check and managed-launch contract passed all three native hosts in
[owner CI](https://github.com/dragginzgame/ic-testkit/actions/runs/37901828971).
Follow [Testkit #38](https://github.com/dragginzgame/ic-testkit/issues/38) and
[Shared #76](https://github.com/dragginzgame/shared-tooling/issues/76) for
owner qualification and consumer coordination. Consumers select a qualified
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

This script owns provisioning only. The four packages in
[IC Host Tooling](https://github.com/dragginzgame/ic-host-tooling) own their library
contracts: `ic-host-artifacts` owns artifact streams and inspection; `ic-host-fs`
owns bounded filesystem reads, publication and locks; `ic-host-process` owns
admitted executable execution; and `ic-host-tools` owns Candid extraction and IC
response decoding. Consumer orchestration, credentials, destinations and
measurement policy remain local.
