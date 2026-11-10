# Native qualification

This independent Cargo workspace owns fourteen unpublished packages: native
schema fixtures, model/compiler consumers, codec checks, Wasm helpers, the source
scanner and canister build/report tools. Its lockfile selects its own suppliers;
the product workspace never depends on this workspace.

Select individual packages with `--manifest-path qualification/Cargo.toml` when
running Cargo from the repository root. `make fmt` covers both native workspaces.
Configured CI retains qualification lint, feature checks and tests alongside the
product gates. Release groups use Toko's native multi-lock projection to align
product catalog references without releasing private qualification packages.

Core's SQL generator and SQLite reference backend remain in
[`crates/test-support/`](../crates/test-support/) under the approved dev-only
exception; they need private executor access. The independent
[Canic workspace](../testing/canic-adapter/README.md) retains its older graph and
approved package locations. See the
[migration record](../docs/design/0.308-package-layout/status.md) for the mapping
and qualification limits.
