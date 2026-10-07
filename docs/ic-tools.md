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
| `pocket-ic` | 16.0.0 | [PocketIC](https://github.com/dfinity/pocketic/releases/tag/16.0.0) |
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

Follow [snapshot adoption](consuming-snapshots.md#local-ic-tool-adoption). Use one
authoritative pin matrix for this set; remove duplicate version/checksum selections
from old setup files after their callers move. Product adapters may read that
matrix for version alignment, such as PocketIC client/server compatibility.
Use the local bin path in Make/CI commands and set `POCKET_IC_BIN` to
`$(CURDIR)/.tools/ic/bin/pocket-ic` where required; do not depend on the user's
global PATH choosing the right binary.

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
