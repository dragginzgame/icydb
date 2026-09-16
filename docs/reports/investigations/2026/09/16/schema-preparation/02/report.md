# Shared accepted-schema preparation handoff

## Result

The production cache-to-preparation handoff now shares the immutable schema
already owned by EntityAuthority. It no longer clones the complete SchemaInfo.
No cache identity, admission mode, retained graph, budget or execution route was
added. Expression-index reconstruction remains tied to the accepted snapshot
and catalog. Schema replacement publishes a different root; detached preparation
keeps its original view and does not bypass current-authority checks.

Matched IC measurements cover the real handoff helper and temporary-owner drop.
Both versions retain the source authority throughout the interval. These are
metadata microcanister measurements, **not whole-query or production-actor deltas**.

| Fixture (data fields, plus one primary key) | Baseline one handoff | Shared one handoff | Baseline four handoffs | Shared four handoffs |
| --- | ---: | ---: | ---: | ---: |
| 16 scalar | 16,707 | 278 | 66,363 | 647 |
| 64 scalar | 57,891 | 278 | 231,099 | 647 |
| 254 scalar | 220,911 | 278 | 883,179 | 647 |
| 16 direct expanded, depth 8 | 3,559,232 | 278 | 13,592,767 | 647 |
| 64 direct expanded, depth 10 | 56,948,461 | 278 | 217,494,030 | 647 |
| 254 direct expanded, depth 10 | 226,004,498 | 278 | 863,142,618 | 647 |
| 16 records, depth 8, width 16 | 370,636 | 278 | 1,482,079 | 647 |
| 8 records, depth 10, width 16 | 186,892 | 278 | 747,103 | 647 |

Values are IC instructions. All 114 samples (19 fixtures × 3 repeats × 2 versions)
completed with exact within-version repeat parity and matching fixture metadata.
Counter overhead is included; the four-call interval is not four single-call
intervals. Earlier construction/clone samples have different allocator history;
use this matched pair for the handoff delta.

The fixtures have no indexes, isolating schema ownership transfer. This is not
a claim that all handoffs cost 278 instructions: expression-index detection
still examines accepted index metadata, and incomplete views still reconstruct.

| Isolated artifact | Raw Wasm bytes | Defined functions |
| --- | ---: | ---: |
| Baseline | 486,119 | 1,247 |
| Shared handoff | 483,854 | 1,243 |
| Delta | -2,265 | -4 |

Raw-Wasm hashes:
- Baseline: e5271f873f7e8c3ab7ba0c3224b9f9170360b79ff1264a518ed6902739c7b1af
- Shared: 33a03e2fe922dda780148c582caa6af7f72f21b924147089d5579d253240be78

Production-actor Wasm and whole-query cycles/instructions are unmeasured.

## Reproduction

Use the fixtures/imports/query export in [the earlier probe](../01/probe.rs.txt),
replacing only its run function with [probe-run.rs.txt](probe-run.rs.txt).
The synthetic fingerprints/root identify an isolated metadata owner, not a
session publication experiment. Candidate bundle admission, row-contract/schema
construction and owner creation occur outside every measured interval.

Install beneath schema::info in an isolated source copy. In session::query::cache
add the following probe-only bridge and re-export it through query and session
with pub(in crate::db) visibility (none of this wiring is in production):

```rust
pub(in crate::db) fn qualification_handoff(
    authority: &EntityAuthority,
    snapshot: &AcceptedSchemaSnapshot,
) {
    drop(std::hint::black_box(
        schema_info_for_plan_cache_authority(authority, snapshot).unwrap(),
    ));
}
```

Use the [earlier runner](../01/runner.rs.txt), expecting four u64 results and
header handoff,four_handoffs,bundle_bytes,total_fields after the fixture columns.
Both builds use Rust 1.98.1, no default features and the same wasm-release profile
(opt-level z, fat LTO, one codegen unit, panic abort). Build icydb-core as cdylib
for wasm32-unknown-unknown. PocketIC server 16.0.0 executes three query calls per
fixture. [samples.csv](samples.csv) retains both versions' complete output.

Baseline is the pre-handoff worktree, including the earlier expansion-admission
slice, based on 38d4067a04a2dc11421fc91fa06fa077d9732840. Candidate replaces only
executor/authority/entity.rs, query/intent/{model,query}.rs, query/plan/pipeline.rs
and session/query/cache.rs with this handoff implementation. Both share identical
probe fixtures, bridge and runner. Existing shared networks were not changed;
the isolated PocketIC instances were released.

## Boundaries and follow-up

Focused regression coverage exercises shared schema identity, root replacement,
detached view lifetime, missing expression-index reconstruction, warm cache
identity/accounting, cursor authority, and typed explain output/lifecycle.
No per-query accounting was added: the temporary owner shares metadata already
held by authority and existing cache retention still traverses that authority.

Cold construction and cumulative accepted-metadata expansion remain open.
This change removes a repeated copy; it does not make the initial schema cheap,
cap total metadata, or close R1/R4. SORT-01 and broader write/replay accounting
remain accepted deferrals. Full release validation remains user-owned.
