# Typed catalogue reads — shared accepted bindings

## Result and bounded disposition

Retain shared immutable facade bindings. In this representative nested
catalogue fixture, ordinary 128-row reads use **16.1–16.3% fewer query
instructions** and **13.1% fewer explicit-update cycles**. Raw Wasm falls
**744 bytes**. All selected fields, IDs, order and continuation results match
independent host expectations. No application code changes are required.

This completes R1 of the authorised 0.260 plan. R2 (selected-field ergonomics)
has not started. W1 still requires the deferred Canic-owned fixture extension.
This is not a measurement of Toko Miner's deployment or its remaining
instruction-cap headroom; no production ceiling is proposed.

## Existing owner and trade-off

`crates/icydb/src/db/session/write.rs::TypedEntityBinding` previously owned a
`DynamicTypedEntityBinding` directly. Output projections cloned this accepted
mapping. Generated field decoding calls `take_row_value`, which checks that
the row and adapter carry equal bindings before consuming a value. Previously
that repeated full equality over owned strings, field mappings and named-type
metadata for each field in each row.

The facade now holds `Arc<DynamicTypedEntityBinding>`. Clone-derived bindings
share one immutable mapping; equality can short-circuit on pointer identity.
Independently issued bindings still use content equality. The existing
generated adapter, accepted schema, current-binding validation, row projection
and executor remain the owners. No pointer-identity-only acceptance rule or
cache is introduced. Current authority is still checked at execution.

Arc adds one shared allocation per issued binding and reference counting;
it replaces repeated deep copies/comparisons and preserves existing native
Send/Sync traits. Rc would not preserve those traits. The mixed typed-write
batch handoff uses `Arc::unwrap_or_clone` to give its existing core owner an
owned mapping: uniquely owned mappings move; shared ones clone by value.
No mutation semantics, persisted format, public API or configuration changes.
This does not eliminate full-row decoding or expensive unused nested payloads.

## Comparable input set and artifacts

HEAD: `4698cecb8dbba6786079b759b43494b020559997`; Cargo remains 0.259.6.
The subject is HEAD plus prior C1 dirty work and the frozen R1 schema/actor/host
fixture, not HEAD alone. [sources.txt](sources.txt) records lock and relevant
dirty actor inputs for the baseline and candidate. Only the facade binding
owner differs between the paired builds; fixture hashes were checked again
after the candidate. Other dirty source changes are the prior C1 core tests;
they are not actor inputs. Design/changelog changes do not enter actor builds.

Lock SHA-256:
`8024973c209ab9244ac24687f3a8d42e772f0c3c4454d7a5f57aeaaa0c34c658`.
Rust 1.98.1 (`48a229cea`), Binaryen 132, PocketIC 16.0.0. Both builds use the
existing retained `canister_test_sql` owner, defaults off, explicit features
`candid-export,local-sql-query,test-admin-api`, LocalTest, SQL/Candid enabled,
`wasm-release` (z, fat LTO, one codegen unit, abort, stripped) and existing
-Oz post-link flow. The typed workload does not parse or execute SQL.
Artifact retention covers byte reads; both sizes install clones of the exact
module bytes from the corresponding build. No second cache/lock owner.

| Artifact | Raw bytes | SHA-256 |
| --- | ---: | --- |
| Baseline | 4,302,295 | `e17148f7337f72361f4369215c9b4fef077d3053913abc9e3afa5f0b512f04dc` |
| Shared binding | 4,301,551 | `7a010bdf53e76bd75449c69e1f8b9d7dad6fc96ae764bd5cb3a7761308977593` |

Exact retained paths and per-message measurements are in
[baseline-messages.txt](baseline-messages.txt) and
[candidate-messages.txt](candidate-messages.txt).

| Metric | Baseline | Candidate | Delta |
| --- | ---: | ---: | ---: |
| Raw Wasm bytes | 4,302,295 | 4,301,551 | -744 |
| Defined functions | 10,194 | 10,191 | -3 |
| Code-section payload bytes | 4,066,085 | 4,065,344 | -741 |
| Data-section payload bytes | 220,499 | 220,499 | 0 |

The earlier C1 actor did not contain the R1 catalogue fixture. Comparing its
size with this actor would conflate fixture reachability with runtime change.
Gzip, heap allocation totals and peak memory are unmeasured.

## Exact workload and attribution

New `SqlTestCatalogItem` has an indexed key, ID, name, description, capacity
and optional nested placement (768-byte shape, four point records, asset,
boolean). Seed 16 or 128 deterministic rows in separate fresh instances.
Every third placement is absent. Host expectations check all fields, not just
rendered equality, counts or checksums.

The ordinary typed query selects complete rows ordered by key with limit 257
and follows the returned live continuation. The existing public envelope caps
each page at 100 rows: 16 rows take one page, 128 take two. A four-page loop
and 128-row cap fail if the fixture cannot finish. No reset or cursor forgery.
The staged probe splits the same existing binding/page/generated-adapter
owners to localise instructions; it is test-only, not a production executor.
It creates the binding per page just as the ordinary typed surface does.

| Ordinary query | Instructions before → after | Change | Update cycles before → after | Change |
| --- | ---: | ---: | ---: | ---: |
| 16 rows, observation 0 | 7,681,441 → 6,396,319 | -16.73% | 16,769,025 → 15,487,270 | -7.64% |
| 16 rows, observation 1 | 7,848,678 → 6,572,007 | -16.27% | 16,957,139 → 15,680,953 | -7.53% |
| 128 rows, observation 0 | 62,012,603 → 51,924,646 | -16.27% | 76,858,075 → 66,773,170 | -13.12% |
| 128 rows, observation 1 | 61,612,618 → 51,705,409 | -16.08% | 76,880,387 → 66,806,407 | -13.10% |

The staged 128-row adapter interval falls from 15,488,772 to 5,401,565
instructions for observation 0, and from 15,481,437 to 5,398,150 for observation
1 (both -65.13%). Page-stage instructions vary modestly in both directions;
the evidence locates the large saving in typed conversion, not storage I/O.
The 16-row staged adapter reduction is 66.2% approximately. These intervals
include generated nested conversion and result-vector construction; they are
not allocation-only counters. Independent staged/ordinary requests need not
have identical counters or cache state.

Instruction counter type 1 surrounds query construction through accumulated
typed row results. Request scope/database opening and transport shaping are
outside it. Stage counters exclude surrounding loop/counter overhead and do
not sum exactly to the outer counter. Cycle balance differences cover the
broader explicit update, including shaping/serialization and message costs.
Install, startup, seed, convergence and pre-call ticks are excluded. Each
seed chunk has one second of virtual-time advancement and 64 drain ticks;
each measured call has 64 preceding ticks without advancing virtual time.
Co-scheduled work is not separately attributed. Observation 1 is a repeated
call, not a universal isolated warm-cache measurement. No native/wall-clock
metric or inference of encoded bytes from stable pages is used.

## Correctness, validation and exclusions

Each seed verifies clone-derived and separately issued equivalent bindings
decode successfully. A different-entity binding must return typed StaleBinding
before consuming a field, after which the correct binding still decodes the
row. These checks are outside measured messages. Existing focused core tests
cover stale/foreign incarnation bindings, current authority after renames,
late batch rejection, atomicity, field contracts and moved nested values.
They exercise core batch semantics, not an isolated facade mixed-batch cost.

Final focused results:

- Catalogue integration: one test passed, none failed/ignored, 100 filtered;
  eight measured messages for each paired build, all exact values checked.
- Facade typed-output selection: five passed, including Send/Sync assertion.
- Core typed-adapter selection with SQL/migration: 16 passed.
- Facade-only native generated-driver read/write selection: one passed.
- Focused facade/actor/integration Clippy with warnings denied passed.
  `make clippy` also passed after correcting the seed helper's divisibility
  spelling when Clippy reported it. Formatting/whitespace checks passed.

Two preliminary fixture probes failed before freezing the paired input set:
one exceeded pending-journal capacity by seeding 128 writes in one message;
one incorrectly required 128 rows in a single public page. Seed writes now
use 16-row chunks with ordinary convergence, and reads follow continuation.
No runtime limit was weakened. Their partial measurements are excluded.
The valid paired baseline/candidate runs both passed. Full repository suites,
production/deployment qualification and publication remain user-owned.

Local lifecycle: the four probes started local PocketIC through the existing
harness and created/dropped two disposable instances each (eight total).
No deployed network, downstream source, dependency, Cargo version or release
state was changed. No commits or pushes.

Complexity: six source files for R1, approximately +416 net lines: one existing
runtime file (+5 production, +7 unit-test lines) and five fixture files
(+404, including two new test-only modules). Design/status, active notes and
this evidence accompany that source change. The runtime flow stays the same
with simpler ownership/reuse; measurement support is the bulk of added code.
No new runtime behavior axis, legacy path, cache or public report type.
