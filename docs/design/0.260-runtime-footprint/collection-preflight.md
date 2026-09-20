# C1 — Collection consumer preflight

Scope: the user authorised this investigation ahead of W1/W2 on 2026-09-19.
This records the source preflight for [C1](0.260-status.md), not an instruction
to add a streaming subsystem. The later
[matched correction and final disposition](../../reports/investigations/2026/09/19/collection-materialization/02/report.md)
supersedes the preliminary candidate assumptions below: the measured named-list
fixture uses CatalogValue, whereas the retained correction is by-kind.

## Subject and result

Inspected runtime: HEAD `4698cecb8dbba6786079b759b43494b020559997`, with only
0.260 planning documentation dirty at entry. Cargo still declares 0.259.6;
no version changes were made. Lock SHA-256:
`8024973c209ab9244ac24687f3a8d42e772f0c3c4454d7a5f57aeaaa0c34c658`.
Toolchain pin: Rust 1.98.1. No compared Wasm artifacts have been built.

**Confirmed opportunity, not a measured win:** list `Contains`, `IsEmpty` and
`IsNotEmpty` can return a boolean without returning the collection. Their
structural path currently materializes the selected slot. Full-row output or
projection of that same list still requires ownership; repeated predicates
can reuse its cache. These consumers must be distinguished in measurements.

## Existing owners and constraints

| Boundary | Evidence and implication |
| --- | --- |
| Predicate | `db/predicate/runtime/mod.rs`: structural comparisons use `with_compare_operands_structural`; non-scalar emptiness uses `eval_required_value_slot`. `runtime/compare/value.rs` performs membership over an owned `Value::List`. `IN` has a literal list on the other side; it is not evidence of persisted-list allocation. |
| Slot cache | `db/data/persisted_row/reader/structural_slot_reader.rs`: `required_cached_value` decodes once and caches the selected value. Untouched fields are already lazy. Do not add another collection cache. |
| Decode authority | `db/data/persisted_row/contract.rs`: the accepted field selects by-kind or canonical recursive decoding. Plain by-kind lists use `structural_field/accepted.rs`, not the generic value-storage list TODO. |
| Canonical admission | `persisted_row/canonical.rs` decodes the canonical value and admits it through the accepted persistence contract. `schema/enum_catalog/admission.rs` checks recursive kinds and budgets, including strict set/map ordering. A wire-only skip is not equivalent. |
| Existing walkers | `structural_field/binary.rs` walks complete by-kind lists/maps; `value_storage/canonical.rs` owns canonical boundaries including enum payloads. These are not interchangeable visitor contracts. `value_storage/walk.rs` itself returns owned vectors, not a general non-owning visitor. |
| Maps | Maintained map predicates reject at query admission; collection-emptiness session tests prove this for stored empty and populated maps. Do not widen query semantics merely to obtain a map benchmark. |
| Relations | By-kind relation-list decoding omits null items (`accepted.rs::decode_accepted_list_bytes`). Raw wire length cannot replace logical emptiness. |
| Projection | Existing borrowed scalar views and nested-path projection already avoid some ownership. Full owned outputs are not redundant allocations. |

Paths in the table are relative to `crates/icydb-core/src/`, except the predicate
subpaths, which continue under `db/predicate/`. No generated model is proposed
as runtime authority.

## Maintained semantic proof

The existing canonical-materialization tests cover accepted-kind, size,
truncation, trailing-byte, null and reread boundaries. Existing session tests
exercise stored text/list/set emptiness and reject map predicates.

Two new tests in
[`canonical_materialization.rs`](../../../crates/icydb-core/src/db/data/persisted_row/reader/structural_slot_reader/tests/canonical_materialization.rs)
exercise the actual `PredicateProgram` and accepted structural slot reader:

- Known independent results for early/late/absent membership and empty/null
  lists, with a fresh reader for each predicate.
- A matching first item followed by a wrong kind, over-limit blob, truncated
  item or trailing bytes must still produce typed corruption. Emptiness and
  non-emptiness must also reject those payloads.

These tests protect semantic behavior, not a particular allocation/cache shape.
They do not prove planner admission for every possible collection element type.

## Smallest next experiment

Use the existing SQL-test actor's test-only workload and retained integration
build owner; do not create another actor/cache/runner. Freeze one accepted
scalar-list schema and a query projecting only the ID. Measure early, late and
absent matches, empty/null cases where admitted, and a scalar control; distinguish
rejected rows from selected rows and separately returning the list itself.
Keep seed, startup and request transport outside any local query instruction
measurement, and label full-message cycles separately if collected.

Any candidate must preserve both accepted decode contracts and reuse comparison
semantics. Measuring only the old generic decoder would not qualify the real
consumer. A canonical streaming path needs an owner-level admission design;
do not duplicate recursive kind checks inside the predicate evaluator.

No runtime candidate is retained yet. Raw Wasm, cycles, instructions and
allocation deltas are **unmeasured**; no speedup or reduced complexity is claimed.
C1 remains in progress. A no-build disposition remains valid if the required
admission work is disproportionate to the measured workload.

Follow-up: the [current-path IC baseline](../../reports/investigations/2026/09/19/collection-materialization/01/report.md)
now supplies raw actor size and query instructions/full-message cycles for
54 calls. It confirms a growing collection-read cost, not an allocation-only
attribution or a retained runtime improvement. The measurements and exact
fixture source inputs belong to that report; the validation below describes
this earlier preflight only.

## Validation

### Candidate decision

The nullable by-kind boundary used `value_storage_bytes_are_null` to validate
the whole generic envelope before accepted decoding/validation. Replace that
walk with exact null-sentinel recognition; every other payload still passes
its accepted codec. This also fixes valid Float32/Float64 list rejection by
the mismatched preliminary grammar. Both callers remain in the existing
`persisted_row/contract.rs` owner; ownership is unchanged.

Final qualification passed 18 focused core tests and all 54 measured query
messages. Raw actor Wasm is 252 bytes smaller, but instructions and cycles
are unchanged: the fixture's named list selects CatalogValue and bypasses
this guard. Retain the correction without claiming query acceleration.
No broader streaming path is built; accepted canonical admission complexity
and unmeasured allocation attribution do not justify that expansion here.
See run 02 for exact inputs, limits and the completed C1 disposition. Earlier
preflight and run 01 results below remain historical context.

### Preflight results

Focused final-source validation: **12 passed, 0 failed, 0 ignored**, with
2,810 unrelated tests filtered out. Ran `icydb-core --lib --features sql`,
locked/offline, selecting `canonical_materialization`, `collection_emptiness`
and `accepted_kind_codec` through the test harness. The preceding unmodified
test baseline passed 10 tests. Both builds emitted the same 65 existing
dead-code warnings in this feature selection, predominantly migration owners;
this is not a warning-free or clippy qualification.

Formatting, whitespace checks and 253 local documentation links passed. Full
suites, clippy and PocketIC measurements have not run in this preflight. No
local networks were started or stopped. Production behavior and state-space
are unchanged. This handoff touches seven files beyond the prior planning
handoff, approximately +225 net lines (110 test lines, the rest documentation).
It adds no production branch, cache, decoder, execution route or API.
