# Installing And Local Development

This document is for contributors and maintainers setting up this repository
locally. The README stays focused on what IcyDB is and how to use it from a
canister.

The repository does not provide a bootstrap target that installs OS packages,
Rust, or user-level release binaries. Install prerequisites explicitly with the
tools you trust for your machine.

## System Prerequisites

On Ubuntu, install the normal build and script dependencies with your package
manager:

```bash
build-essential cmake curl wget gzip libssl-dev pkg-config ripgrep python3 python-is-python3
```

Canister development and wasm inspection also need:

```bash
binaryen wabt jq
```

Local Make targets do not install OS packages and do not run `sudo`.

## Rust

Use the Rust toolchain pinned by the workspace:

```bash
rustup toolchain install 1.95.0
rustup target add wasm32-unknown-unknown
```

Then fetch locked dependencies and check the local prerequisite surface:

```bash
make update-dev
```

## ICP And Canister Tools

Local ICP workflows require the current Canic ICP tools with `icp` on `PATH`.
Install those tools through the Canic ICP distribution you normally use.

Optional canister-operation utilities should be installed explicitly when you
need them:

- `didc` from DFINITY Candid releases.
- `idl2json` and `yaml2candid` from DFINITY idl2json releases.
- `quill` from DFINITY Quill releases.

Cargo-installed wasm helper tools can be installed with:

```bash
make install-canister-deps
```

That target installs the pinned wasm target plus `candid-extractor`, `ic-wasm`,
and `twiggy`.

## Common Commands

```bash
make check      # type-check workspace
make clippy     # lint with warnings denied
make test       # unit + integration tests
make fmt        # format workspace
make build      # release build
```

Install the local CLI binary:

```bash
make install
```

Inspect the generated-endpoint config that local CLI commands use:

```bash
icydb config show
icydb config show --environment demo
icydb config check --environment demo
```

Create or replace a local `icydb.toml` for a canister when setting up a new
demo or test canister:

```bash
icydb config init --canister demo_rpg --all
icydb config init --canister demo_rpg --all --force
```

`config init` writes at the visible workspace root by default. Pass
`--start-dir <path>` when running from a canister subdirectory or from outside
the workspace. Readonly SQL is enabled by default; pass `--no-readonly` only for
canisters that should not expose `__icydb_query`.

Opt into repository git hooks:

```bash
make install-hooks
```

Hooks are optional. When installed, the pre-commit hook formats and stages
tracked formatting changes, and the pre-push hook runs invariant checks plus
clippy.

## PocketIC Tests

Some tests need a PocketIC server binary. The helper checks in this order:

1. `POCKET_IC_BIN`, when it points at an executable.
2. A cached binary for the pinned `pocket-ic` crate version.
3. A GitHub release download, only when `ICYDB_ALLOW_POCKET_IC_DOWNLOAD=1`.

Use a trusted local binary when you have one:

```bash
POCKET_IC_BIN=/path/to/pocket-ic make test
```

Or explicitly allow the helper to download the pinned release:

```bash
ICYDB_ALLOW_POCKET_IC_DOWNLOAD=1 make test
```

Set `POCKET_IC_SERVER_SHA256` when you have a trusted digest and want checksum
verification for the provided, cached, or downloaded executable.

## Wasm Reports

Build and summarize wasm sizes:

```bash
make wasm-size-report
make wasm-size-report SIZE_REPORT_ARGS="--profile wasm-release --canister minimal"
make wasm-size-report SIZE_REPORT_ARGS="--sql-variants both"
```

Build Twiggy-backed wasm audit reports:

```bash
make wasm-audit-report
make wasm-audit-report AUDIT_REPORT_ARGS="--profile wasm-release --canister minimal"
make wasm-audit-report AUDIT_REPORT_ARGS="--date 2026-05-16 --skip-build"
```

Raw non-gzipped `.wasm` bytes are the primary optimization signal. Gzip output
is useful secondary context for transport.

## Troubleshooting

### `make update-dev` says Python is missing

Install `python3` and a `python` alias through your system package manager. On
Ubuntu, `python3` plus `python-is-python3` provides the expected shape.

### `make update-dev` says `rg` is missing

Install `ripgrep` with your system package manager.

### `make test` cannot find PocketIC

Set `POCKET_IC_BIN=/path/to/pocket-ic`, or run with
`ICYDB_ALLOW_POCKET_IC_DOWNLOAD=1` to allow the pinned GitHub release download.

### PocketIC checksum verification fails

The executable bytes do not match `POCKET_IC_SERVER_SHA256`. Recheck the digest
source, remove the cached binary if it is stale, or point `POCKET_IC_BIN` at the
trusted executable you intended to use.

### Local SQL demo cannot find a canister

Confirm the local ICP environment is running and inspect canister IDs:

```bash
cargo run -q -p icydb-cli -- canister list --environment demo
```

Then confirm the local generated-endpoint config and pass the SQL target
explicitly:

```bash
cargo run -q -p icydb-cli -- config show --environment demo
cargo run -q -p icydb-cli -- sql --environment demo --canister demo_rpg
```

If `config show` reports a missing or disabled surface, update `icydb.toml`,
then rebuild and deploy or refresh the canister so the generated methods match
the config.

### `icydb canister refresh` looks destructive

It is destructive to the selected ICP canister state: the command resets that
canister's local install and clears its stable memory. It does not wipe host
disk contents.

### Publishing crates

Publishing is manual maintainer work through `cargo publish`. There is no repo
Make target or script that reads crates.io credentials.
