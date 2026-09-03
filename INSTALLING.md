# Installing IcyDB

This document covers installing IcyDB in downstream canisters first, then the
maintainer-only workstation setup for this repository.

## Downstream Canisters

Pin IcyDB by tag in the canister crate:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.237.3" }
```

The default crate feature set provides structural writes and accepted-schema
runtime support. Enable SQL when the canister uses typed queries,
session/library SQL APIs, or generated SQL endpoints:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.237.3", features = ["sql"] }
```

Schema-authoring crates use `icydb-model` with the same tag:

```toml
[dependencies]
icydb-model = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.237.3" }
```

The runtime `icydb` facade does not re-export model declaration macros.

The public runtime `icydb` crate path supports Rust `1.88.0` and newer.
Its library dependency path, including `icydb-model` and
`icydb-model-macros`, retains the same floor. Other workspace-only packages
may use the workspace Rust `1.96.0` floor. Repository maintenance uses the
pinned Rust `1.97.1` toolchain listed below.

Generated endpoint build scripts should depend on `icydb` with the same tag and
call `icydb::build::build_canister!(SchemaCanister)`.

## Explicit Endpoint Declarations

`icydb::start!()` installs private runtime wiring and never creates a public
Candid method. Declare each maintained public IcyDB method explicitly in the
canister source:

```rust
icydb::start!();

icydb::endpoints! {
    #[cfg(feature = "local-sql-query")]
    icydb_sql_query(introspection = true);
    icydb_ddl;
    icydb_update(admission = primary_key_only);
    icydb_metrics(authorization = public);
    icydb_metrics_reset;
    icydb_schema(authorization = controller);
}
```

One declaration creates exactly one fixed method. A declaration whose required
Cargo capability is absent fails compilation; compiling a capability without a
declaration exports nothing. Use canister-owned Cargo features for local/test
declarations and omit those features from production builds.

For example, keep development capabilities and their declarations behind the
same canister-owned features:

```toml
[features]
default = []
local-sql-query = ["icydb/sql"]
test-admin-api = ["icydb/sql"]
```

```rust
#[cfg(feature = "test-admin-api")]
fn load_fixtures() -> Result<(), icydb::Error> {
    Ok(())
}

icydb::start!();

icydb::endpoints! {
    #[cfg(feature = "local-sql-query")]
    icydb_sql_query(introspection = true);
    #[cfg(feature = "test-admin-api")]
    icydb_fixtures_reset;
    #[cfg(feature = "test-admin-api")]
    icydb_fixtures_load(handler = load_fixtures);
}
```

Production builds omit both features, so neither the methods nor capability
code enabled only by those features, such as SQL introspection, is present in
that Wasm. No IcyDB TOML file or target environment variable participates in
endpoint selection.

Readonly SQL is a generated controller-gated admin surface, not a generated
public read endpoint. Do not expose `icydb_query` or a thin wrapper around it
to arbitrary callers. Caller-facing reads should use ordinary typed
execution so the default bounded read-admission gate applies after the endpoint
has performed caller authorization. See
[docs/contracts/READ_ADMISSION.md](docs/contracts/READ_ADMISSION.md).
Hand-written public read endpoint guidance is in
[docs/guides/read-intent.md](docs/guides/read-intent.md).

Current generated endpoint surfaces:

- `icydb_query` for controller-gated read SQL
  - `introspection = true` admits `EXPLAIN`, `DESCRIBE`, and `SHOW`; these are
    included by the `icydb/sql` capability
- `icydb_ddl` for supported accepted-catalog SQL DDL
- `icydb_update` with declared `primary_key_only` or `bounded_deterministic`
  admission; both policies are controller-gated and narrower than the
  session/library mutation surface
- `icydb_fixtures_reset` and `icydb_fixtures_load` for local fixture flows
- `icydb_snapshot` for storage inventory and stable allocation metadata
- `icydb_schema` for accepted schema descriptions
- `icydb_metrics` and `icydb_metrics_reset` for the optional on-canister
  entity hit/instruction report

Fixture loading calls the explicitly named plain non-exported user hook:

```rust
fn load_fixtures() -> Result<(), icydb::Error> {
    Ok(())
}
```

## Local CLI

Install the local CLI binary from this repository:

```bash
make install
```

The CLI calls fixed method names on the deployed canister. If a declaration is
absent, the replica's ordinary method-not-found response is authoritative.

For a canister that enables the optional `migration` capability, see
[Schema Migrations](docs/guides/schema-migrations.md) for the explicit
version-1 adoption, adjacent deployment, bounded run/resume, and abort flow.

## Maintainer Workstation Setup

This section is for maintaining this repository. It is not required for ordinary
downstream canister dependency installation.

The repository provides local maintainer targets for Ubuntu-like hosts with
`apt-get`. `make install-dev` is the initial workstation bootstrap: it installs
system packages, Rust, Cargo helper tools, ICP tooling, and repository hooks.
`make update-dev` refreshes user-local Rust, Cargo, actionlint, and npm-backed
ICP tooling without installing system packages, ensures the repository's
formatting hook is installed, then runs the maintainer update checks.

### System Prerequisites

On Ubuntu, `make install-dev` installs the normal build and script dependencies:

```bash
build-essential cmake curl wget gzip libssl-dev pkg-config ripgrep shellcheck nodejs npm
```

Canister development and wasm inspection also need:

```bash
bubblewrap binaryen wabt jq
```

On other operating systems, install those packages manually before using the
developer targets.

Both `make install-dev` and `make update-dev` use the shared `make install-gh`
path to ensure the GitHub CLI is available. It installs the apt-backed `gh`
package only when the command is missing; on non-apt systems it reports the
required manual action.

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

Install local developer dependencies with:

```bash
make install-dev
```

That target installs apt-backed system prerequisites when `apt-get` is present,
the pinned Rust toolchain, the wasm target, standard Cargo helper tools,
`candid-extractor`, `ic-wasm`, `twiggy`, and npm-backed ICP CLI tools.

`make update-dev` does not run `apt-get` or `sudo`; install missing system
packages manually or re-run `make install-dev` when the host package surface
needs to change.

### Common Commands

```bash
make validate-fast # formatting, workflows, shell, invariants, and type checks
make check         # type-check workspace
make clippy        # lint with warnings denied
make test          # complete unit and integration test boundary
make validate      # complete formatting, invariant, feature, lint, and test gate
make fmt           # format workspace
make install-hooks # install the formatting-only pre-commit hook
make build         # release workspace build
```

For an integration change, use the focused feedback target before the complete
gate. It runs the named case first and, if that passes, its complete test
binary:

```bash
make test-integration-feedback \
  TEST_TARGET=sql_canister \
  TEST_NAME=exact_test_name
```

`make validate-fast` intentionally omits executable tests and the
feature-specific Clippy lanes. It is an iteration preflight, not a substitute
for `make validate`.

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

Run the CI-equivalent required Tier B lane on Linux x86-64 with the exact
PocketIC release pinned by `Cargo.lock`:

```bash
POCKET_IC_BIN="$(bash scripts/ci/install-pocketic.sh)" make ci-sql-tier-b
```

Tier B starts one runner-owned PocketIC server, connects the parallel fixture
pool to that server, runs the complete `sql_canister` binary and the focused
0.237 performance regressions, then reports elapsed time and peak server
resources. The runner stops the server on success, failure, or termination.

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

Validate all three artifacts in a downloaded Tier D bundle before starting a
measurement run:

```bash
make test-sql-perf-baseline-contract PERF_BASELINE_DIR=/path/to/tier-d-bundle
```

The scheduled workflow runs this check before building its shared Wasm or
launching shards. A current-format failure is a hard cut: capture and review a
fresh three-run calibration cohort, then update
`ICYDB_SQL_PERF_BASELINE_RUN_ID` to the selected current run. Do not translate
or inject fields into an older artifact.

After three initial-calibration workflow bundles exist, validate them together
and produce the bounded review projection with:

```bash
make test-sql-perf-calibration-review \
  PERF_CALIBRATION_RUN_1_DIR=/path/to/run-1 \
  PERF_CALIBRATION_RUN_2_DIR=/path/to/run-2 \
  PERF_CALIBRATION_RUN_3_DIR=/path/to/run-3
```

Each directory must contain that run's merged P2, scale, and instrumentation
artifacts. The reviewer requires exact ordinals `1`, `2`, and `3` from one
cohort and one clean measured subject. It reports cross-run envelopes and
recurring top-20 promotion candidates but does not choose thresholds, edit the
focused set, or bless a baseline.

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
`metrics`, and `canister refresh` take the canister as a
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
then calls `icydb_fixtures_load` and skips loading when the method is absent.

## CLI Command Shapes

```bash
icydb sql --canister demo_rpg --sql "SELECT COUNT(*) FROM character"
icydb sql -e test -c demo_rpg --sql "SHOW ENTITIES"

icydb canister list
icydb canister deploy demo_rpg
icydb canister refresh demo_rpg
icydb canister upgrade demo_rpg
icydb canister status demo_rpg

icydb snapshot demo_rpg
icydb schema show demo_rpg
icydb metrics demo_rpg
icydb metrics demo_rpg --reset
```

### Git Formatting Hook

`make install-dev` and `make update-dev` configure the repository's sole Git
hook. To install it without changing any other developer tooling, run:

```bash
make install-hooks
```

The pre-commit hook runs `make fmt`, covering Cargo manifests, derive ordering,
and Rust code. When formatting changes a file, the hook aborts and lists the
affected paths so you can review and re-stage them. It never runs `git add`,
tests, Clippy, builds, PocketIC, or release validation. It also rejects
partially staged Rust and Cargo-manifest paths rather than risk committing an
unformatted staged snapshot.

`git commit --no-verify` remains an explicit bypass, and `git push` performs no
repository validation. `make validate` retains the non-mutating `fmt-check`
gate for release readiness and other hook bypasses.

## IC Testkit Tests

Some integration tests need the PocketIC server binary. `ic-testkit` resolves
the binary in this order:

1. `POCKET_IC_BIN`, when it points at an executable.
2. A cached binary for the pinned `pocket-ic` crate version under `.cache`.
3. A pinned GitHub release download through `ic-testkit`, but only when
   `IC_TESTKIT_ALLOW_POCKET_IC_DOWNLOAD=1` explicitly permits it.

Use a trusted local binary when you have one:

```bash
POCKET_IC_BIN=/path/to/pocket-ic make test
```

Or explicitly allow `ic-testkit` to download the pinned release into the repo
cache when it is missing:

```bash
IC_TESTKIT_ALLOW_POCKET_IC_DOWNLOAD=1 make test
```

CI Tier B does not rely on a test-process download. On Linux x86-64,
`scripts/ci/install-pocketic.sh` resolves the exact locked version, validates
its reported version, and prints its cached executable path. `ci-sql-tier-b`
then requires that path through `POCKET_IC_BIN` and owns one shared server for
the complete lane.

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

Set `POCKET_IC_BIN=/path/to/pocket-ic`, or explicitly opt into the pinned
download with `IC_TESTKIT_ALLOW_POCKET_IC_DOWNLOAD=1 make test`. For the
CI-equivalent Linux x86-64 Tier B lane, let `scripts/ci/install-pocketic.sh`
resolve and validate the exact locked binary as shown above.

### Local SQL demo cannot find a canister

Confirm the local ICP environment is running and inspect canister IDs:

```bash
cargo run -q -p icydb-cli -- canister list --environment demo
```

Then pass the deployed SQL target explicitly:

```bash
cargo run -q -p icydb-cli -- sql --environment demo --canister demo_rpg
```

If the replica reports a missing method, add the matching source declaration
and required Cargo feature, then rebuild and deploy or refresh the canister.

### `icydb canister refresh` looks destructive

It is destructive to the selected ICP canister state: the command resets that
canister's local install and clears its stable memory. It does not wipe host
disk contents.

### Publishing crates

Publishing is manual maintainer work through `cargo publish`. There is no repo
Make target or script that reads crates.io credentials.
