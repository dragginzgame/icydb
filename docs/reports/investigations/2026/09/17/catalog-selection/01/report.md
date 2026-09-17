# Shared accepted catalog selection — IC qualification

Date: 2026-09-17. Baseline: `7012352f13ec78bbc03b7224fb5f7fbb81d058a9`
(published 0.257.20). Candidate: shared structured snapshot handoff.
Rust 1.98.1; identical locked dependencies, `wasm-release` profile and PocketIC
16.0.0 server. No native timing measurements.

## Result

Selection plus snapshot acquisition uses 49.6–62.7% fewer instructions on cold
entity-selection misses, and 72.7–99.9% fewer on warm selections. The immutable
accepted snapshot is shared instead of repeatedly decoded and fingerprinted.
All 54 samples (nine fixtures, three repeats, two artifacts) pass; repeats agree
exactly. Every invocation also checks equal cold/warm snapshots and unchanged
normalized fingerprint identity outside the measured intervals.

| Schema (total fields) | Cold instructions, before → after | Warm instructions, before → after |
| --- | ---: | ---: |
| 2 scalar | 297,250 → 149,403 | 184,577 → 50,472 |
| 17 scalar | 1,552,915 → 615,583 | 880,813 → 50,682 |
| 65 scalar | 11,359,720 → 4,294,063 | 6,389,990 → 51,354 |
| 255 scalar | 107,673,481 → 40,136,601 | 60,095,350 → 54,014 |
| 255 scalar, long field names | 114,641,982 → 43,532,026 | 63,453,771 → 54,014 |
| 2, depth-10 composite reference | 296,673 → 149,333 | 184,576 → 50,472 |
| 2, nested record leaf | 324,301 → 161,290 | 199,325 → 50,487 |

Raw probe Wasm: **564,991 → 564,371 bytes (−620, −0.11%)**.
Defined functions: **1,499 → 1,500 (+1)**. This is an isolated reachable-owner
probe, not a production actor size measurement. The extra compiled function
does not indicate a second selection flow; the three construction sites now
share one constructor. Full-query and end-to-end startup instruction deltas
are unmeasured; these percentages must not be presented as whole-query gains.

## Scope and retention

The probe calls the real `SchemaStore::current_accepted_catalog_selection`
against a heap store. Fixture construction, accepted-candidate publication and
initial verified-bundle loading happen before measurement. Cold includes the
first entity selection, its fingerprint and cache insertion; warm includes
current-root lookup and snapshot acquisition. Both retain the returned owners.
The CSV also records four warm selection/drop pairs. Composite references stay
compact here: this does **not** measure expanded `SchemaInfo` construction.

The decoded bundle remains retained as before. Its selection cache now retains
one structured entity snapshot instead of encoded entity bytes; inspection and
row authority share that snapshot. This removes their separate decoded copies,
but selection-only workloads may retain more heap than the old byte-only cache.
Exact heap deltas are unmeasured. No new cache, budget lifecycle, configuration,
persisted representation or generated-model fallback is added.

Focused regressions cover shared ownership through inspection and row authority,
fingerprint parity with persisted records (including version normalization),
detached snapshot lifetime, root replacement and live/canonical journal isolation.
Persisted decoding, validation and canonical recovery selection remain intact.

## Reproduction and evidence

- [Baseline samples](baseline.csv), [candidate samples](candidate.csv),
  [runner](runner.rs.txt), [measured function](probe-run.rs.txt).
- In separate baseline/candidate source copies, install the existing
  [schema fixture probe](../../../16/schema-preparation/01/probe.rs.txt) as
  `schema::info::qualification`, replacing `retained` and `run` with the measured
  function here. Keep its IC query export and fixture builder. Replace the
  unused `RetainedBytes` import with `db::{schema::SchemaStore,
  integrity::DatabaseIncarnationId}`. Candidate changes only the acquisition
  expression from `selected.decode_verified().unwrap()` to `selected.snapshot()`.
- Compile each SQL-free `icydb-core` as a `cdylib` for
  `wasm32-unknown-unknown` using the workspace `wasm-release` profile and locked,
  offline dependencies. Copy each artifact as `probe.wasm` into its own directory.
  Compile the runner against the workspace PocketIC dependency and pass that
  directory. The runner checks three-repeat equality; `wasm-objdump -h` supplies
  defined function counts, and file length supplies raw Wasm bytes.

Both isolated PocketIC instances were released; shared networks were untouched.
The evidence is not a bound on database-wide cold construction. Existing
SORT-01 and broader write/replay-accounting deferrals remain unchanged.
