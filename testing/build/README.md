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
