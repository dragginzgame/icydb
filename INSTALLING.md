# Installing IcyDB

This document covers installing IcyDB in downstream canisters first, then the
maintainer-only workstation setup for this repository.

## Downstream Canisters

Pin IcyDB by tag in the canister crate:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.183.28" }
```

The default crate feature set is typed/fluent-only. Enable SQL explicitly when
the canister uses session/library SQL APIs or generated SQL endpoints:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.183.28", features = ["sql"] }
```

The public `icydb` crate path supports Rust `1.88.0` and newer. Repository
maintenance uses the newer internal toolchain listed below.

Generated endpoint build scripts should depend on `icydb-config` with the same
tag as `icydb` and call `icydb_config::build_configured_canister!()`.

## Generated Endpoint Config

Local canisters load generated endpoint switches from `icydb.toml` through
`icydb-config`. Generated canister glue uses fixed `__icydb_*`
Rust/export names, and the CLI checks the config before calling endpoint
families.

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

Readonly SQL is a generated controller-gated admin surface, not a generated
public read endpoint. Do not expose `icydb_query` or a thin wrapper around it
to arbitrary callers. Caller-facing reads should use ordinary typed/fluent
execution so the default bounded read-admission gate applies after the endpoint
has performed caller authorization. See
[docs/contracts/READ_ADMISSION.md](docs/contracts/READ_ADMISSION.md).
Hand-written public read endpoint migration recipes and templates for pages,
complete small sets, and exact aggregates are in
[docs/guides/read-intent.md](docs/guides/read-intent.md).

Example generated endpoint config:

```toml
[canisters.demo_rpg.sql]
readonly = true
ddl = true
fixtures = true

[canisters.demo_rpg.sql.introspection]
local = true
ic = false

[canisters.demo_rpg.metrics]
local = "extended"
ic = "simple"

[canisters.demo_rpg.snapshot]
enabled = true

[canisters.demo_rpg.schema]
enabled = true
```

Current generated surfaces:

- `__icydb_query` for controller-gated read SQL
  - `EXPLAIN`, `DESCRIBE`, and `SHOW` follow
    `[canisters.<name>.sql.introspection]`; defaults are `local = true` and
    `ic = false`
- `__icydb_ddl` for supported accepted-catalog SQL DDL
- `__icydb_fixtures_reset` and `__icydb_fixtures_load` for local fixture flows
- `__icydb_snapshot` for storage inventory and stable allocation metadata
- `__icydb_schema` and `__icydb_schema_check` for accepted schema diagnostics
- `__icydb_metrics` and `__icydb_metrics_reset` for default runtime metrics
- `__icydb_metrics_extended` when the target metrics mode is `extended`

Fixture loading calls a plain non-exported user hook when present:

```rust
fn icydb_fixtures_load() -> Result<(), icydb::Error> {
    Ok(())
}
```

## Local CLI

Install the local CLI binary from this repository:

```bash
make install
```

Inspect the generated-endpoint config that local CLI commands use:

```bash
icydb config show
icydb config show --environment demo
icydb config check --environment demo
```

## Maintainer Workstation Setup

This section is for maintaining this repository. It is not required for ordinary
downstream canister dependency installation.

The repository provides local maintainer targets for Ubuntu-like hosts with
`apt-get`. `make install-dev` is the initial workstation bootstrap: it installs
system packages, Rust, Cargo helper tools, ICP tooling, and repository hooks.
`make update-dev` refreshes user-local Rust, Cargo, actionlint, and npm-backed
ICP tooling without installing system packages, then runs the maintainer update
checks.

### System Prerequisites

On Ubuntu, `make install-dev` installs the normal build and script dependencies:

```bash
build-essential cmake curl wget gzip libssl-dev pkg-config ripgrep nodejs npm
```

Canister development and wasm inspection also need:

```bash
bubblewrap binaryen wabt jq
```

On other operating systems, install those packages manually before using the
developer targets.

### Rust

`make install-dev` installs rustup when missing, then installs the Rust channel
declared in `rust-toolchain.toml`:

```bash
rustup toolchain install --target wasm32-unknown-unknown
```

After initial setup, update the local maintainer tooling surface with:

```bash
make update-dev
```

Formatting and lint-oriented Make targets expect the Cargo helper binaries used
by the repository:

```bash
cargo install cargo-sort cargo-sort-derives --locked
```

### ICP And Canister Tools

Local ICP workflows require the current Canic ICP tools with `icp` on `PATH`.
Both `make install-dev` and `make update-dev` install or update
`@icp-sdk/icp-cli` and `@icp-sdk/ic-wasm` under `$HOME/.local` through npm.

Optional canister-operation utilities should be installed explicitly when you
need them:

- `didc` from DFINITY Candid releases.
- `idl2json` and `yaml2candid` from DFINITY idl2json releases.
- `quill` from DFINITY Quill releases.

Install local developer dependencies and repository hooks with:

```bash
make install-dev
```

That target installs apt-backed system prerequisites when `apt-get` is present,
configures repository git hooks, and installs the pinned Rust toolchain, the
wasm target, standard Cargo helper tools, `candid-extractor`, `ic-wasm`,
`twiggy`, and npm-backed ICP CLI tools.

`make update-dev` does not run `apt-get` or `sudo`; install missing system
packages manually or re-run `make install-dev` when the host package surface
needs to change.

### Common Commands

```bash
make check      # type-check workspace
make clippy     # lint with warnings denied
make test       # unit + integration tests
make fmt        # format workspace
make build      # release workspace build; requires a clean worktree
```

### SQL Evidence Commands

Run the compact native generated and bundled-SQLite comparisons without the
live canister boundary with:

```bash
cargo test --locked -p icydb-core --no-default-features --features sql db::session::tests::sqlite_reference
cargo test --locked -p icydb-core --no-default-features --features sql db::session::tests::mutation_reference
cargo test --locked -p icydb-testing-integration --test sql_correctness
```

Run the generated live-canister SQL boundary separately with:

```bash
make test-sql-canister-matrix
```

The complete Tier C native profile is a scheduled eight-shard lane. Run one
exact shard locally with:

```bash
make test-sql-tier-c-shard TIER_C_SHARD=0
```

Run all shard indexes from `0` through `7` into the same
`TIER_C_ARTIFACT_DIR`, then require their exact clean merge with:

```bash
make test-sql-tier-c-merge
```

When a generated SELECT or mutation case fails, the shard first writes its
bounded minimized replay under `failures/failure.<blake3>.json`, then writes a
red receipt referencing that exact identity, and finally fails the command.
Keep the artifact directory when diagnosing a red shard. Merge reopens every
referenced failure artifact and rejects scenario or content-identity drift.
Reproduce one retained minimized failure, including its exact typed signature
and provider outcomes, with:

```bash
make test-sql-tier-c-replay TIER_C_FAILURE_ARTIFACT=/path/to/failure.HEX_DIGEST.json
```

The replay command passes only while the minimized failure reproduces exactly.
It fails when the defect no longer reproduces or its typed signature or outcomes
have drifted.

The merge does not execute missing scenarios or reconstruct missing receipts.
It writes both the exact merged receipt and a strict coverage-distribution
artifact recomputed from the same typed native catalog; mixed mutation sequences
contribute every statement and mutation family they actually contain.

Scheduled performance evidence is a separate workflow. Run all eight P1 and
scale shards before the P1 merge, use its exact candidate artifact for all eight
P2 shards, then merge P2:

```bash
make test-sql-perf-p1-shard P1_SHARD=0
make test-sql-perf-scale-shard SCALE_SHARD=0
make test-sql-perf-p1-merge
make test-sql-perf-p2-shard P2_SHARD=0
make test-sql-perf-p2-merge
make test-sql-perf-instrumentation
```

Replace `0` with every shard index through `7` before each merge. Compare a
reviewed baseline only after exact current P2 and scale reports exist:

```bash
make test-sql-perf-baseline P2_BASELINE_PATH=... SCALE_BASELINE_PATH=...
```

Performance artifacts and verdicts cannot satisfy correctness obligations, and
correctness success cannot substitute for missing performance evidence.

## Local SQL Demo

The repository includes a demo RPG canister with SQL-visible `character` and
`grid` entities. `character` has a scalar primary key; `grid` uses a composite
`(x, y)` primary key.

```bash
icydb canister refresh -e demo demo_rpg
icydb sql -e demo -c demo_rpg --sql "SHOW ENTITIES"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "SELECT name, charisma FROM character ORDER BY charisma DESC LIMIT 5"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "SELECT x, y, terrain FROM grid ORDER BY danger_level DESC LIMIT 5"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "DESCRIBE character"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "SHOW ENTITIES"
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
icydb sql -e test -c demo_rpg --sql "SHOW ENTITIES"

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

Some integration tests need the PocketIC server binary. `ic-testkit` resolves
the binary for `make test` in this order:

1. `POCKET_IC_BIN`, when it points at an executable.
2. A cached binary for the pinned `pocket-ic` crate version under `.cache`.
3. A pinned GitHub release download through `ic-testkit`.

Use a trusted local binary when you have one:

```bash
POCKET_IC_BIN=/path/to/pocket-ic make test
```

Or let `make test` allow `ic-testkit` to download the pinned release into the
repo cache when it is missing:

```bash
make test
```

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

### `make install-dev` cannot install system packages

On non-apt systems, install the packages listed in System Prerequisites with
your platform package manager, then re-run `make install-dev`.

### `make fmt` or `make check` cannot find `cargo sort`

Install the repository's formatting helper binaries:

```bash
cargo install cargo-sort cargo-sort-derives --locked
```

### `make test` cannot find the IC testkit runner

Set `POCKET_IC_BIN=/path/to/pocket-ic`, or run `make test` so the repository
test target opts into `ic-testkit`'s pinned GitHub release download.

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
