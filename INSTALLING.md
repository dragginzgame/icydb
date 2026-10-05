# Installing IcyDB

This document covers installing IcyDB in downstream canisters first, then the
maintainer-only workstation setup for this repository.

## Downstream Canisters

Copy the runtime and build dependency declarations from
[README's release-pinned example](README.md#add-icydb). README is the sole
release-pin owner and is updated by release tooling.

The default crate feature set provides accepted-schema runtime support plus
structural, typed, and dynamic reads and writes. Enable `sql` only when the
canister uses session/library SQL APIs or generated SQL endpoints:

Add `features = ["sql"]` to the runtime dependency when needed. The other
optional features are `metrics` and `migration`.

Runtime-enabled crates normally author models through `icydb::model`, which
re-exports the model declaration surface. Schema-only tooling that deliberately
omits the runtime may depend on `icydb-model` directly.
Use the same release tag for every IcyDB package.

The public runtime `icydb` crate path supports Rust `1.88.0` and newer.
Its library dependency path, including `icydb-model` and
`icydb-model-macros`, retains the same floor. Other workspace-only packages
may use the workspace Rust `1.96.0` floor. Repository maintenance uses the
pinned Rust `1.99.0` toolchain listed below.

Generated endpoint build scripts should depend on `icydb` with the same tag and
call `icydb::build::build_canister!(SchemaCanister)`.
The [compiled single-package example](testing/model-facade-only/) owns the
complete host/runtime setup; [schema authoring](docs/guides/schema-authoring.md)
explains how to adapt it.

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

Readonly SQL is controller-gated by default. An explicit
`authorization = guard(path)` declaration replaces controller authority with
one synchronous application decision; anonymous callers still reject. Do not
expose unrestricted SQL to arbitrary callers. Caller-facing reads should use ordinary typed
execution so the default bounded read-admission gate applies after the endpoint
has performed caller authorization. See
[docs/contracts/READ_ADMISSION.md](docs/contracts/READ_ADMISSION.md).
Hand-written public read endpoint guidance is in
[docs/guides/read-intent.md](docs/guides/read-intent.md).

Current generated endpoint surfaces:

- `icydb_query` for controller- or application-guarded read SQL
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

One-shot `icydb sql --sql "..."` and trailing SQL commands print successful
results to stdout and exit zero. Rejected SQL responses, transport failures and
invalid replies report diagnostics on stderr and exit nonzero, so shell command
chains stop on failure. The interactive shell reports a failed statement and
continues accepting SQL.

For a canister that enables the optional `migration` capability, see
[Schema Migrations](docs/guides/schema-migrations.md) for the explicit
version-1 adoption, adjacent deployment, bounded run/resume, and abort flow.

## Maintainer Workstation Setup

This section is for maintaining this repository. It is not required for ordinary
downstream canister dependency installation.

macOS host support is required; the
[host qualification matrix](docs/governance/shared-tooling.md#host-qualification)
records the current native-validation gaps. Setup selects apt packages on Linux
and Homebrew packages on macOS; installer branches do not qualify native builds,
tests or deployment workflows.

Install [rustup](https://rustup.rs) before using these targets. On macOS, also
install Xcode Command Line Tools and [Homebrew](https://brew.sh).
`make install-dev` installs system packages, the repository's Rust toolchain,
Cargo helper tools, ICP tooling, and repository hooks.
`make update-dev` refreshes user-local Rust, Cargo, actionlint, and npm-backed
ICP tooling (apart from installing `gh` if missing), ensures the repository's
formatting hook is installed, and leaves repository dependencies unchanged.
Dependency upgrades and security audits are separate maintainer actions: review
any intentional `cargo update` diff, then audit the selected graph with
`cargo audit`. Neither action runs automatically during workstation setup.

### System Prerequisites

On Ubuntu, `make install-dev` installs the normal build and script dependencies:

```bash
build-essential cmake curl wget gzip libssl-dev pkg-config perl ripgrep shellcheck nodejs npm cloc
```

Canister development and wasm inspection also need:

```bash
bubblewrap wabt jq
```

On macOS, setup installs these Homebrew formulas:

```bash
cmake curl openssl@3 pkg-config perl ripgrep shellcheck node wabt jq cloc make
```

Binaryen is installed separately from the official pinned release, using the
archive and executable digests in
[the optimizer pin table](scripts/ci/wasm-optimizer-checksums.tsv). Linux x86-64
and macOS ARM64/x86-64 assets are admitted. An update reports whether a newer
release exists; it does not change the qualified optimizer version.

Both `make install-dev` and `make update-dev` use the shared `make install-gh`
path to ensure the GitHub CLI is available. It installs the apt-backed `gh`
package, or the macOS Homebrew formula, only when the command is missing.

Actionlint installation uses the version and verified platform digests in
[scripts/ci/actionlint-checksums.tsv](scripts/ci/actionlint-checksums.tsv).
`make install-dev` and `make update-dev` use the same pin as CI and workflow
lint. To change the tool version, update that reviewed pin file from the official
release checksum list; no version-only environment override bypasses verification.

### Rust

Both setup targets require rustup and install the Rust channel declared in
`rust-toolchain.toml`, including rustfmt, Clippy and the Wasm target:

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
source ci/tool-versions.env
cargo install cargo-sort --version "$SHARED_TOOLING_CARGO_SORT_VERSION" --locked
cargo install cargo-sort-derives --version "$ICYDB_CARGO_SORT_DERIVES_VERSION" --locked
```

Setup also installs `cargo-edit`, `cargo-get`, and `cargo-watch` for release
helpers and `make test-watch`. General analysis tools are optional; install
`cargo-audit`, `cargo-bloat`, `cargo-deny`, `cargo-expand`, `cargo-machete`,
`cargo-llvm-lines`, or `cargo-tarpaulin` explicitly when needed. For example:

```bash
cargo install cargo-audit --locked
```

### ICP And Canister Tools

Local ICP workflows require the ICP SDK CLI with `icp` on `PATH`.
Both `make install-dev` and `make update-dev` install or update
`@icp-sdk/icp-cli` and `@icp-sdk/ic-wasm` under `$HOME/.local` through npm.
Keep `$HOME/.local/bin` before `$HOME/.cargo/bin` on your shell's `PATH` so
the maintained npm `ic-wasm` is selected if an older Cargo copy remains installed.
Workstation setup uses that ordering itself. Add both directories to your shell's
`PATH`; setup cannot change its parent shell's environment.

Optional canister-operation utilities should be installed explicitly when you
need them:

- `didc` from DFINITY Candid releases.
- `idl2json` and `yaml2candid` from DFINITY idl2json releases.
- `quill` from DFINITY Quill releases.

Install local developer dependencies with:

```bash
make install-dev
```

That target installs host-specific system prerequisites,
the pinned Rust toolchain, the wasm target, standard Cargo helper tools,
`candid-extractor`, `ic-wasm`, `twiggy`, and npm-backed ICP CLI tools.

`make update-dev` may install the GitHub CLI if it is missing. Other
missing system packages require manual installation or `make install-dev`.

### Common Commands

```bash
make validate-fast # formatting, workflows, shell, invariants, and type checks
make check         # type-check workspace
make clippy        # lint with warnings denied
make test          # complete unit and integration test boundary
make test-documentation # focused compiled examples, capability lists, and codec evidence
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

The [documentation maintenance policy](docs/governance/documentation.md) names
the source owners and explains the distinction between structural link checks
and executable behavioral evidence.

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
pool to that server, and runs the complete `sql_canister` and `sql_perf_audit`
binaries. The runner stops the server on success, failure, or termination.

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

The shared pre-commit hook runs `make fmt` in an isolated copy of the index,
covering Cargo manifests, derive ordering and Rust code. It refreshes only fully
staged selected files, preserving unselected working edits. Partial staging
rejects before formatting; formatter failure leaves the real files and index
unchanged. Installation refuses to replace existing hook authority. The hook
does not run tests, Clippy, builds, PocketIC or release validation.

The current local `../ic-metrics` dependency is absent from the isolated index
copy, so Cargo metadata prevents staged Rust formatting in this checkout. Local
development wiring is retained; hook activation and real staged formatting remain
unqualified until that prerequisite is resolved. See
[dependency adoption](https://github.com/dragginzgame/icydb/issues/298).

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

On macOS, ensure Xcode Command Line Tools and Homebrew are installed. On other
non-apt systems, the bootstrap has no package mapping; install prerequisites
manually and use `make update-dev` to install user-local tools.

### `make fmt` or `make check` cannot find `cargo sort`

Install the repository's formatting helper binaries:

```bash
source ci/tool-versions.env
cargo install cargo-sort --version "$SHARED_TOOLING_CARGO_SORT_VERSION" --locked
cargo install cargo-sort-derives --version "$ICYDB_CARGO_SORT_DERIVES_VERSION" --locked
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
