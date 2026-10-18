# IcyDB build and report support

This unpublished package owns maintained canister build selection, artifact
retention, post-link optimization, interface policy and Wasm measurement reports.
It stays in the root workspace and lockfile. IC Build and Host own generic build,
cache, process and file operations; IcyDB keeps product policy and report formats.

The three command targets are `build_fixture_canister`, `write_wasm_size_report`
and `check_wasm_entity_scale`. Application builds, Make adapters and CI reports
select this package directly. The maintained measurement subjects have one owner
in `src/wasm_measurement_subjects.tsv`.

IC Testkit owns PocketIC installation, admission and server/instance lifecycle.
[`testing/integration`](../integration/) supplies IcyDB scenarios and drives
startup/recovery through that owner. The harness consumes this build library;
this package has no Testkit/PocketIC or SQLite-reference dependency. There are
no forwarding APIs at the retired integration build paths.

Tracking: [IcyDB #361](https://github.com/dragginzgame/icydb/issues/361).

The host-only `wire_bindings` example describes the twelve maintained native
endpoint contracts and emits metadata and exact Rust frames. IC Wire's shared
`typescript::generate_registry` renders browser types and method objects; IcyDB
retains reflection, identities, resource limits and native scalar constraints.
Its reflection dependencies are development-only; it does not change runtime
payloads or library dependencies. See [browser qualification](../wire-browser/README.md).

Rust and browser generation use the same registry IC Wire 0.4 package. The
`typescript` feature is selected only by this package's development dependencies;
its runtime source does not enter application or library graphs. CI prepares the
locked graph with ordinary `cargo fetch --locked`, with no private Wire pin or
repository-specific credentials.

Artifacts expose native IC Wire identities in `icydb:wire` custom sections.
Inspection requires their complete method/mode roster to agree with application
Wasm exports, excluding only the reserved timer dispatcher. Staging publishes
compiler and deployable Wasm files; reports and CI use those retained artifacts.
There is no DID generation or interface-extractor prerequisite.
