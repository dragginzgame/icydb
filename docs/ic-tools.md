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
Existing consumer versions are retained for the five previously used tools;
Quill is added from its official release. Each row specifies executable, exact
version, native host and archive SHA-256. The digests were read from the official
GitHub release asset metadata on 2026-10-06.

| Executable | Default version | Official release |
| --- | --- | --- |
| `quill` | 0.5.4 | [Quill](https://github.com/dfinity/quill/releases/tag/v0.5.4) |
| `icp` | 1.6.0 | [ICP CLI](https://github.com/dfinity/icp-cli/releases/tag/v1.6.0) |
| `didc` | 0.6.2 | [Candid tools](https://github.com/dfinity/candid/releases/tag/didc-v0.6.2) |
| `ic-wasm` | 0.11.1 | [ic-wasm](https://github.com/dfinity/ic-wasm/releases/tag/0.11.1) |
| `pocket-ic` | 16.1.0 | [PocketIC](https://github.com/dfinity/pocketic/releases/tag/16.1.0) |
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
The installer and optional alignment checker share that admission through
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
Native CI verifies installation and version execution on the three declared
hosts; product build, PocketIC compatibility and deployment qualification stay
with their consumers.

## Consumer adoption

### PocketIC ownership handoff

IC Testkit is the intended owner of PocketIC-specific release selection, asset
checksums, provisioning, offline admission and server lifecycle. Track its
published setup/check contract in
[Testkit #38](https://github.com/dragginzgame/ic-testkit/issues/38) and the Shared
Tooling removal in [#76](https://github.com/dragginzgame/shared-tooling/issues/76).
Shared Tooling can supply generic download/checksum/archive mechanics; application
repositories should not acquire another independent PocketIC version policy.

The currently supported installer still requires the complete six-tool matrix
above. Preserve that functioning setup until the replacement is published and
qualified on Linux and both macOS architectures. Adoption must trace callers of
`make install-tools`, `make ic-tools-check`, the alignment/binary helpers and
`.tools/ic/bin/pocket-ic`, as well as each consumer's selected snapshot and pins.
Retiring the row and PocketIC-specific branches changes the setup contract and
requires a separately coordinated release, with explicit Testkit setup, offline
checks and product startup verified before removing the old route. Preserve
existing bundles, receipts and failed-install evidence; tests must not silently
download a replacement, and consumers must not patch their vendored snapshots.

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
from old setup files after their callers move. Product adapters may read that
matrix for an explicitly qualified PocketIC client/server pairing. The shared
[alignment and binary checks](verification-helpers.md#pocketic-alignment-and-external-binaries)
cover exact version equality and external byte admission separately.
Use the local bin path in Make/CI commands. The offline check already prints the
verified absolute bin directory; obtain the server path without a cache glob:

```bash
verified_bin="$(bash scripts/dev/install-ic-tools.sh --consumer "$PWD" --pins ci/ic-tools.tsv --check)" || exit 1
export POCKET_IC_BIN="$verified_bin/pocket-ic"
```

This verifies the complete selected bundle before printing its directory, with
diagnostics on stderr. It does not install anything. Do not depend on the user's
global PATH or glob crate download caches to select a server.

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
