# Generated logical-memory upgrade fixture

Test-only actor: never deploy it with application data. It exposes raw control
frame restoration solely to test the existing pending-commit upgrade barrier.
There is no production fault-injection API or alternate memory implementation.

Two builds share one permanent namespace and surviving store. The
`omit-retiring-store` build removes the other store and its entity. Normal typed
writes produce journal debt; the generated startup driver folds it. The fixture
pauses IC timer delivery so the test controls when that driver runs, without
changing its work or budgets.

The pending-marker case captures the actual bytes left behind by normal commit
clearing, then restores their frame after journal folding. It must first recover
successfully with the unchanged actor. Only then is it used to test rejection of
store removal. Frame manipulation is bounded and belongs only to this fixture;
the marker payload is never synthesized by a second codec.

## Running the focused upgrade tests

From the repository root, with the usual Cargo environment configured:

```sh
export CARGO_HOME="$PWD/.cache/cargo/icydb"
export CARGO_TARGET_DIR="$PWD/target/icydb"
mkdir -p target/logical-memory-l3
cargo build --locked --offline -p icydb-testing-logical-memory --target wasm32-unknown-unknown
ic-wasm "$CARGO_TARGET_DIR/wasm32-unknown-unknown/debug/icydb_testing_logical_memory.wasm" -o target/logical-memory-l3/full.wasm shrink
cargo build --locked --offline -p icydb-testing-logical-memory --target wasm32-unknown-unknown --features omit-retiring-store
ic-wasm "$CARGO_TARGET_DIR/wasm32-unknown-unknown/debug/icydb_testing_logical_memory.wasm" -o target/logical-memory-l3/omitted.wasm shrink
export ICYDB_LOGICAL_MEMORY_FULL_WASM="$PWD/target/logical-memory-l3/full.wasm"
export ICYDB_LOGICAL_MEMORY_OMITTED_WASM="$PWD/target/logical-memory-l3/omitted.wasm"
export POCKET_IC_BIN="$PWD/.cache/pocket-ic-server-16.0.0/pocket-ic"
cargo test --locked --offline -p icydb-testing-integration --test logical_memory -- --ignored --test-threads=1
```

Use an installed PocketIC 16 server if it is not at that cache location. These
tests need a local server port. They are explicitly ignored by ordinary test
runs because they require both separately built Wasm artifacts; a skipped run is
not upgrade qualification.

Coverage: empty-journal retirement preserves surviving rows and slots; only the
omitted journal remains declared for inspection; retired database identities
reject reintroduction; journal debt and valid pending markers reject removal
without publishing a new database control record. Both rejected-removal cases
recover when the original actor is restored. Allocation commitment and database
publication are separate boundaries: these tests do not assert ledger rollback.
