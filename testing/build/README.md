# IcyDB build and report support

This unpublished package owns maintained canister build selection, artifact
retention, post-link optimization, interface policy and Wasm measurement reports.
It stays in the root workspace and lockfile. IC Build and Host own generic build,
cache, process and file operations; IcyDB keeps product policy and report formats.

The three command targets are `build_fixture_canister`, `write_wasm_size_report`
and `check_wasm_entity_scale`. Application builds, Make adapters and CI reports
select this package directly. The maintained measurement subjects have one owner
in `src/wasm_measurement_subjects.tsv`.

PocketIC installation, startup and integration scenario contracts remain in
[`testing/integration`](../integration/). That harness consumes this library;
this package has no Testkit/PocketIC or SQLite-reference dependency. There are
no forwarding APIs at the retired integration build paths.

Tracking: [IcyDB #361](https://github.com/dragginzgame/icydb/issues/361).

The host-only `wire_bindings` example describes the twelve maintained native
endpoint contracts and emits browser types, metadata and exact Rust frames.
Its reflection dependencies are development-only; it does not change runtime
payloads or library dependencies. See [browser qualification](../wire-browser/README.md).

Rust contracts use registry IC Wire 0.3. Browser generation separately pins the
TypeScript runtime from [Wire PR #1](https://github.com/dragginzgame/ic-wire/pull/1)
through the development-only `ic-wire-browser` dependency until that API is
published. This runtime source does not enter application or library graphs.

Artifacts expose native IC Wire identities in `icydb:wire` custom sections.
Inspection requires their complete method/mode roster to agree with application
Wasm exports, excluding only the reserved timer dispatcher. Staging publishes
compiler and deployable Wasm files; reports and CI use those retained artifacts.
There is no DID generation or interface-extractor prerequisite.
