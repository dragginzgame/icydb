# Batch Preparation Cost Audit

## Verdict And Scope

**PASS WITH FINDINGS.** Prioritize avoiding the owned accepted-schema bundle
copy during typed-binding validation. Do not start a general batch-preparation
cache on this evidence. No production optimization was implemented, and no
performance improvement is claimed.

The requested investigation follows the batch correctness audit and result-boundary
tests on the current 0.254 line. It measures repeated layout/decode construction
and typed-binding validation through existing batch entrypoints. Atomicity,
Identity, current authority, result admission and recovery remain unchanged.
This is not a whole-system audit or authorization for another minor line.

Source: `4b11bf85f6c848e8944de7ef51572affa819d704`, including the pending
test-only boundary slice. The original worktree was preserved. Probe code lived
only in `/tmp/icydb-batch-audit.bw3k0s`, a copy of the working source files.
Cargo.lock SHA-256:
`20c9d8e610f00067d753f7fb4cda5a27843d8a7b49aca3d463230e37327a446e`.

## Findings

### MEDIUM: Typed validation copies the entire store schema per binding

Owner: `crates/icydb-core/src/db/session/write.rs`,
`typed_entity_binding_matches_catalog`, and
`crates/icydb-core/src/db/schema/store.rs`, `current_accepted_schema_bundle`.

The general typed batch validates its anchor binding, then every item: 1,025
validations for 1,024 writes. Each check obtains an owned copy of the complete
accepted store bundle. The underlying verified bundle is already cached; this
is repeated copying, not repeated wire decoding. Field-source checks also
allocate their temporary source keys.

With the small two-entity native catalog, validation accounts for 35.8% of
general homogeneous typed execution time and 36.4–37.1% of mixed typed time at
1,024 rows. Its cumulative allocation requests account for 6,061,850 and
6,018,330 bytes respectively. These figures include all work in the validation
function, not just the bundle clone. Source inspection implies sensitivity to
unrelated catalog size; a wide-catalog measurement has not established its magnitude.

Disposition: recommend a narrow borrowed-bundle validation change before any
binding memo or batch-context cache. Keep every per-item exact binding check.
The existing `current_accepted_schema_authority_ref` offers a borrowed authority,
but it is not a drop-in substitute for the owned accessor: preserve the latter's
per-call `validate_constraint_validation_job_closure` check, including warm-cache
calls. Preserve incarnation, revision, fingerprint, generation, entity-source,
field-ID and slot checks, and release the borrow before writes.

No new mode, public API, persistent state, long-lived authority or cache key is
needed for this direction. Its net benefit still needs matched native/IC A/B
measurement and raw Wasm verification before landing. Include wide catalogs and
late stale/mismatched bindings in that proof. Do not interpret the measured
validation share as the amount that can all be eliminated.

### LOW: Repeated layout construction is real, but not the first target

Owners: `AcceptedRowLayoutRuntimeContract::from_accepted_schema`,
`AcceptedRowDecodeContract::from_runtime_contract`, and the shared session batch
engine. General batches build layouts in lowering, row preparation and result
construction. Commit preflight already reuses contexts per entity/fingerprint.

At 1,024 homogeneous structural inserts, the probe counted 3,073 layout builds
and 1,025 decode-contract constructions. Together they accounted for 4.9–5.2%
of instrumented host time and 941,607 cumulative requested bytes. In the
journaled nested-relation fixture at 128 rows, they accounted for 0.88–0.93%
of time and 622,739 requested bytes. Derived contract clones elsewhere are not
included in these constructor measurements.

Disposition: defer a general context cache. These constructors enforce accepted
layout completeness; reducing call count alone does not justify changing their
ownership/lifetimes or removing protective validation. Reconsider with a
demonstrated workload bottleneck after the narrower typed-copy work is measured.

The existing same-entity typed terminal already performs one binding validation
and hoists frontend layout preparation. Prefer it where its homogeneous result
surface fits; it is not an automatic substitute for mixed-entity batch handles.

## Native Evidence

Rust 1.97.1, repository test profile (`opt-level=1`), all features. Temporary
thread-local allocation counters and timed guards surrounded the three named
constructors/validators. Each case used fresh thread-local storage and one warm-up
write through its terminal before measurement. Requests and initial bindings
were constructed outside the interval; terminal execution and result destruction
were inside. Generated facade builder `push`, binding issuance and final generated
row decoding were not measured.

Five scalar shapes ran at 1, 8, 128 and 1,024 rows, twice each. The catalog had
two entities, with two Nat64 fields on the first and one on the second. Mixed
cases alternated entities in one store. The relation fixture ran at 1, 8 and
128 rows, twice each, with journaled Identity inserts and three nested collection
edges referencing four pre-existing targets. Every measured batch succeeded and
returned the expected item/affected-row count: 46 measured batches in total.

| Shape | Rows | Host ms/batch, two samples | Allocation requests | Requested bytes |
| --- | ---: | ---: | ---: | ---: |
| General structural, one entity | 1,024 | 6.14–6.60 | 92,843 | 8,843,430 |
| General structural, two entities | 1,024 | 5.89–6.53 | 89,297 | 8,057,655 |
| General typed, one entity | 1,024 | 9.46–10.04 | 130,769 | 14,905,346 |
| General typed, two entities | 1,024 | 9.167–9.168 | 126,199 | 14,076,051 |
| Same-entity typed terminal | 1,024 | 5.84–6.24 | 80,595 | 8,543,176 |
| Nested-relation structural | 128 | 19.87–19.88 | 384,133 | 20,657,430 |

Allocation counts/bytes were identical across the two samples. Bytes count
cumulative allocation, zeroed-allocation and reallocation requests, not live
heap, allocator overhead or Wasm memory. Timing includes instrumentation and
scheduling noise; these are small diagnostic samples, not production latency
claims. Different terminals also produce different result envelopes. Their
timing difference does not isolate binding validation alone.

## IC Baseline And Wasm

The maintained `nested_relation_direct` actor ran unchanged, using production
features, SQL disabled, Candid export disabled, `wasm-release`, and the canonical
Binaryen 132 post-link pipeline. Native test instrumentation is unreachable in
this build. PocketIC 16.0.0 ran two independent fresh installations per row count.
Each installation warmed one target/source row, inserted measured target and
replacement-target batches, then inserted, updated and deleted the measured
source rows. Startup/journal convergence was delivered between calls.

The existing calibration method measures request construction plus the complete
batch call with local performance counter 1; Candid endpoint decoding and
subsequent convergence callbacks are outside that interval. This is a separate
stable-storage baseline, not an IC attribution of the native typed finding.

| Rows | Target insert instructions | Source insert instructions | Source update instructions | Source delete instructions |
| --- | ---: | ---: | ---: | ---: |
| 1 | 2,670,898 | 3,495,699 | 3,639,697 | 3,724,290 |
| 8 | 4,207,641 | 7,348,446 | 9,573,181 | 6,423,877 |
| 128 | 31,953,521 | 99,903,267 | 183,800,805 | 86,587,428 |
| 1,024 | 262,155,158 | 979,599,366 | 1,949,200,979 | 891,555,844 |

Both samples produced identical instruction counts; all 40 measured phases
succeeded, including replacement-target insertion. Growing stable trees and
relation/index work are part of these measurements; do not infer linear scaling
or an optimized batch-size policy from four points.

Compiler Wasm: **2,374,616 raw bytes**. Final deployable Wasm: **2,075,549 raw
bytes**, 5,340 defined functions; deterministic gzip: 820,852 bytes. Final SHA-256:
`fef2d726adfb8aff6ef435409bdfcaf6b23251414b0c8ac0cb85770b4fde1522`.
This is one baseline, not an optimization delta or a release-over-release gate.

## Validation, Limits And Handoff

- Passed two temporary native measurement tests and one focused IC measurement
  test, plus 42 focused maintained batch/result-boundary/recovery tests. The
  maintained tests ran in the copied native build with measurement disabled.
- Temporary native fixture compile errors (imports, count types, patch helper
  and allocator lint allowance) were corrected before obtaining these results.
  No production failure was found in the selected checks.
- The isolated PocketIC server was started for this measurement and stopped by
  the wrapper afterward. No local ICP network was changed.
- No production candidate, wide-catalog benchmark, typed IC attribution/A/B,
  or optimization Wasm delta was produced. Full suites remain user-owned;
  clippy was not rerun for a documentation-only repository change.
- Temporary sources/logs and the Wasm remain under
  `/tmp/icydb-batch-audit.bw3k0s`; the tables above retain the material evidence.
- Repository complexity delta: one audit report, no production/test source
  edits from this turn, zero new behavior axes, unchanged execution flow.
  Pending boundary-test changes and release/design metadata were left intact.

Next proposed outcome: remove avoidable owned bundle copies from the existing
typed validation owner, preserving its complete admission contract, then measure
that candidate before deciding whether any broader reuse is justified. This
audit does not promote the deferred batch-progress feature or change the
insert-only response-limit policy.
