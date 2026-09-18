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

The integration target builds both feature variants through the shared retained
Cargo and post-link pipeline. It reads each exact artifact while its build owner
is alive, then shares the owned Wasm bytes across the three tests. No manual
artifact paths or separately prepared Wasms are required.

From the repository root, with the usual Cargo environment configured:

```sh
export CARGO_HOME="$PWD/.cache/cargo/icydb"
export CARGO_TARGET_DIR="$PWD/target/icydb"
export TMPDIR="$PWD/.cache"
export POCKET_IC_BIN="$PWD/.cache/pocket-ic-server-16.0.0/pocket-ic"
cargo test --locked -p icydb-testing-integration --test logical_memory
```

Use an installed PocketIC 16 server if it is not at that cache location. These
tests need a local server port and run in ordinary workspace/release validation;
none is ignored. The focused target above runs just this upgrade family.

Coverage: empty-journal retirement preserves surviving rows and slots; only the
omitted journal remains declared for inspection; retired database identities
reject reintroduction; journal debt and valid pending markers reject removal
without publishing a new database control record. Both rejected-removal cases
recover when the original actor is restored. Allocation commitment and database
publication are separate boundaries: these tests do not assert ledger rollback.
