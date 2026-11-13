# Native qualification

This independent Cargo workspace owns fifteen unpublished packages: native
schema fixtures, model/compiler consumers, codec checks, Wasm helpers, the source
scanner, startup polling qualification and canister build/report tools. Its
lockfile selects its own suppliers; the product workspace never depends on this
workspace.

Select individual packages with `--manifest-path qualification/Cargo.toml` when
running Cargo from the repository root. `make fmt` covers both native workspaces.
Configured CI retains qualification lint, feature checks and tests alongside the
product gates. Release groups use Toko's native multi-lock projection to align
product catalog references without releasing private qualification packages.

The `icydb-testing-startup` package composes IcyDB's dependency-free readiness
predicate with Testkit's bounded polling driver. Its simulator test remains
explicitly selected; normal qualification gates compile it without starting a
server. Prepare the selected Testkit CLI and server explicitly, then admit the
server for the focused test:

```sh
testkit_cli="$(bash vendor/toko-tooling/scripts/dev/install-rust-tools.sh --consumer "$PWD" \
  --package ic-testkit --lockfile "$PWD/qualification/Cargo.lock" \
  --bin ic-testkit-server --profile debug --features cli)"
"$testkit_cli" setup --directory "$PWD/.tools/ic-testkit-server-native"
TESTKIT_ADMITTED_SERVER="$("$testkit_cli" check \
  --directory "$PWD/.tools/ic-testkit-server-native")"
POCKET_IC_BIN="$TESTKIT_ADMITTED_SERVER" cargo test --locked \
  --manifest-path qualification/Cargo.toml -p icydb-testing-startup \
  --test startup_polling -- --ignored
```

Testkit owns server setup, admission, startup and teardown. These commands run
from the repository root and select this workspace's lockfile; the older Canic
setup selection remains independent.
PocketIC's exact thiserror 2.0.18 requirement stays in this native graph while
the product lock independently selects 2.0.21. This is the explicit integration
constraint isolated by [#634](https://github.com/dragginzgame/icydb/issues/634);
revisit that selection when the supported Testkit/PocketIC cohort removes it.

Core's SQL generator and SQLite reference backend remain in
[`crates/test-support/`](../crates/test-support/) under the approved dev-only
exception; they need private executor access. The independent
[Canic workspace](../testing/canic-adapter/README.md) retains its older graph and
approved package locations. See the
[migration record](../docs/design/0.308-package-layout/status.md) for the mapping
and qualification limits.
