# Installing And Local Development

This document is for contributors and maintainers setting up this repository
locally. The README stays focused on what IcyDB is and how to use it from a
canister.

The repository does not provide a bootstrap target that installs OS packages or
Rust. Install system prerequisites explicitly with the tools you trust for your
machine. The maintainer `make update-dev` target does update user-level Rust,
Cargo, and ICP development tooling.

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

Then update the local maintainer tooling surface:

```bash
make update-dev
```

Formatting and lint-oriented Make targets expect the Cargo helper binaries used
by the repository:

```bash
cargo install cargo-sort cargo-sort-derives --locked
```

## ICP And Canister Tools

Local ICP workflows require the current Canic ICP tools with `icp` on `PATH`.
`make update-dev` installs or updates `@icp-sdk/icp-cli` and
`@icp-sdk/ic-wasm` under `$HOME/.local` through npm.

Optional canister-operation utilities should be installed explicitly when you
need them:

- `didc` from DFINITY Candid releases.
- `idl2json` and `yaml2candid` from DFINITY idl2json releases.
- `quill` from DFINITY Quill releases.

Cargo-installed wasm helper tools can be installed with:

```bash
make install-canister-deps
```

That target installs the pinned Rust toolchain, the wasm target,
`candid-extractor`, `ic-wasm`, and `twiggy`.

## Common Commands

```bash
make check      # type-check workspace
make clippy     # lint with warnings denied
make test       # unit + integration tests
make fmt        # format workspace
make build      # release workspace build; requires a clean worktree
```

## Generated Endpoint Config

Local canisters load generated endpoint switches from `icydb.toml` through
`icydb-config-build`. Generated canister glue uses fixed `__icydb_*`
Rust/export names, and the CLI checks the config before calling endpoint
families.

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

Example generated endpoint config:

```toml
[canisters.demo_rpg.sql]
readonly = true
ddl = true
fixtures = true

[canisters.demo_rpg.metrics]
enabled = true
reset = true

[canisters.demo_rpg.snapshot]
enabled = true

[canisters.demo_rpg.schema]
enabled = true
```

Current generated surfaces:

- `__icydb_query` for controller-gated read SQL
- `__icydb_ddl` for supported accepted-catalog SQL DDL
- `__icydb_fixtures_reset` and `__icydb_fixtures_load` for local fixture flows
- `__icydb_snapshot` for storage inventory and stable allocation metadata
- `__icydb_schema` and `__icydb_schema_check` for accepted schema diagnostics
- `__icydb_metrics` and `__icydb_metrics_reset` for runtime metrics

Fixture loading calls a plain non-exported user hook when present:

```rust
fn icydb_fixtures_load() -> Result<(), icydb::Error> {
    Ok(())
}
```

## Local SQL Demo

The repository includes a demo RPG canister with SQL-visible `character` and
`grid` entities. `character` has a scalar primary key; `grid` uses a composite
`(x, y)` primary key.

```bash
scripts/dev/sql-start-demo
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "SELECT name, charisma FROM character ORDER BY charisma DESC LIMIT 5"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "SELECT x, y, terrain FROM grid ORDER BY danger_level DESC LIMIT 5"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "DESCRIBE character"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "SHOW TABLES"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "CREATE INDEX IF NOT EXISTS character_renown_idx ON character (renown)"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "DROP INDEX IF EXISTS character_renown_idx ON character"
```

`sql` keeps an explicit `--canister/-c` flag because it also accepts trailing
SQL text. Target-style commands such as `snapshot`, `schema show`,
`schema check`, `metrics`, and `canister refresh` take the canister as a
required positional argument.

All canister-targeting commands default the ICP environment to `demo`, or use
`ICP_ENVIRONMENT` when it is set:

```bash
cargo run -q -p icydb-cli -- canister list
cargo run -q -p icydb-cli -- canister list --environment test
```

`icydb sql` only queries the current canister state. It does not create or load
demo data automatically. Use `canister refresh` for the destructive local reset
flow for the selected ICP canister; it clears that canister's stable memory,
then calls `__icydb_fixtures_load` when the fixture endpoint is configured.

## CLI Command Shapes

```bash
icydb config init --canister demo_rpg --all
icydb config show --environment demo
icydb config check --environment demo

icydb sql --canister demo_rpg --sql "SELECT COUNT(*) FROM character"
icydb sql -e test -c demo_rpg --sql "SHOW TABLES"

icydb canister list
icydb canister deploy demo_rpg
icydb canister refresh demo_rpg
icydb canister upgrade demo_rpg
icydb canister status demo_rpg

icydb snapshot demo_rpg
icydb schema show demo_rpg
icydb schema check demo_rpg
icydb metrics demo_rpg
icydb metrics demo_rpg --window-start-ms <timestamp>
icydb metrics demo_rpg --reset
```

Opt into repository git hooks:

```bash
make install-hooks
```

Hooks are optional. When installed, the pre-commit hook formats and stages
tracked formatting changes, and the pre-push hook runs invariant checks plus
clippy.

## IC Testkit Tests

Some integration tests need the IC testkit server binary. The helper checks in
this order:

1. `POCKET_IC_BIN`, when it points at an executable.
2. A cached binary for the pinned `pocket-ic` crate version.
3. A GitHub release download.

Use a trusted local binary when you have one:

```bash
POCKET_IC_BIN=/path/to/pocket-ic make test
```

Or let `make test` download the pinned release into the repo cache when it is
missing:

```bash
make test
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

### `make fmt` or `make check` cannot find `cargo sort`

Install the repository's formatting helper binaries:

```bash
cargo install cargo-sort cargo-sort-derives --locked
```

### `make test` cannot find the IC testkit runner

Set `POCKET_IC_BIN=/path/to/pocket-ic`, or run with
`ICYDB_ALLOW_POCKET_IC_DOWNLOAD=1` to allow the pinned GitHub release download.

### IC testkit runner checksum verification fails

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
