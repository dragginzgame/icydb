# Entity metric lookup evidence

This report records the initial qualification of the borrowed entity-path
lookup for [#300](https://github.com/dragginzgame/icydb/issues/300), originally
prepared under the compatible 0.265.1 notes. It retains one map and the existing
counter, reset, report and inclusive-span contracts.

The lookup is present in published IcyDB 0.267.1 at
`cb8cefca1d68c20025398eae6dfab18a2eb45883`. The 2026-10-08 closeout confirms
the source and ten current metrics-state tests; the measurements below remain
bound to the original fixture and dependency identities, not the current graph.

The source baseline is `20a9aa7d9802cf73ad17ed9444fc06a2c37e2909`. Final
`state.rs` SHA-256 is
`32945e458d872c1f27110e7eaa156b9641aeadda4a290fa7d279ff7ac52fda1b`.
The preserved root lock SHA-256 is
`2b292946d6f3a2283dc10d05863a360b6fdaf2d271c82572f72edad02f022db9`;
it selects registry ic-metrics 0.1.6. Concurrent publication, canister and
shared-tooling changes were preserved and are outside this qualification.

## Actual IC observations

An isolated fixture mechanically extracts the before/after recorder bodies,
retaining the counter fields and map. It omits other ambient state, database
runtime, spans and journal. This is recorder-body evidence, not a full database
build or measurement of all report work. Rust 1.99 builds size-optimized (`z`),
fat-LTO Wasm with one codegen unit, stripped symbols and aborting panics.
Fixture dependency versions, sources and checksums match the current root lock.

PocketIC 16.0.0 executes on one application subnet. Server SHA-256:
`69e324bdb68d32d878b7a9504b1379f08f8d1921272bacb065b0fabb3d0f3792`.
Each scenario uses three fresh canisters, two preseeded zero keys, then 10,000
repeated observations or 1,000 new paths. Table cardinality, counts and totals
are checked. Each artifact/scenario has identical observations across its three
runs. Instructions bracket the loop; actual cycles are balance differences
across the whole update, excluding installation.

| Scenario | Before instructions | After instructions | Before cycles | After cycles |
| --- | ---: | ---: | ---: | ---: |
| Repeated measured path | 9,630,238 | 3,990,238 | 16,642,400 | 11,002,514 |
| Repeated scoped path | 9,590,238 | 3,980,238 | 16,602,400 | 10,992,514 |
| New paths | 8,821,517 | 15,070,739 | 16,082,960 | 22,332,296 |

Repeated path work drops about 59% in this fixture. New paths cost about 71%
more instructions because the failed borrowed lookup is followed by an owned
insertion lookup. Stable paths repeated over time are the intended benefit;
no universal gain or mainnet cycle saving is claimed. Raw fixture Wasm grows
from 296,447 to 296,654 bytes, a 207-byte increase. No whole-IcyDB size reduction
has been demonstrated.

Baseline Wasm SHA-256:
`82b72e13a9fa9f6afc6e27b8160ff7a0a9e81d52dfdd73248a8adf9b3c51d738`.
Candidate Wasm SHA-256:
`034fb20bbec1cccd191dd318c0875474f210a103120832c60601bbb5bd01e7af`.
Fixture lock SHA-256:
`e41ffb4e46ec0cb47f718fb2d139918442d39cb5ca9a4fc6ca1f74b4cc15cb40`.
The fixture, original extracted bodies, locks, Wasm and build logs remain in
`target/evidence/metrics-improvements/`. The host harness and execution log
remain in `/home/adam/projects/ic-metrics/target/evidence/consumer-improvements/`.
The measurement log SHA-256 is
`faad79d996912b913f53b952618523ce12dcee15d771546e8891b66e17551f0e`.
An initial mismatched metrics fixture selection was refused before building;
the root dependency graph was not changed.

## Focused checks

`make fmt` passed before these locked offline checks using this repository's
`target/`:

- `cargo clippy -p icydb-core --lib --tests --features metrics --locked --offline -- -D warnings`
- `cargo clippy -p icydb-core --lib --features metrics --target wasm32-unknown-unknown --locked --offline -- -D warnings`
- `cargo test -p icydb-core --lib --features metrics --locked --offline metrics::state::tests::`

All ten selected state tests pass, including mixed paths, zero, saturation,
maxima and reset. Logs remain in the root harness's owned evidence directory.
These are focused Linux results. Rust 1.96 minimum qualification was not run
because that toolchain is not installed. No broad test/CI gate, macOS execution,
commit, push, release or retained-artifact cleanup ran for this batch.
