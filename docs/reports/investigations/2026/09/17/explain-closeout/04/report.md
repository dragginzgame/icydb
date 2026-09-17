# Upstream feedback: selected-authority reuse and handoff

2026-09-17 · active 0.257.22 candidate on published `355bca1f7`.
The user authorized working through ICYDB-036/021/033/034 together. Canic and
Toko Miner remain read-only; versions, dependency pins and installed tools are
unchanged. This is not publication or downstream adoption evidence.

## Disposition

| Item | IcyDB outcome | Remaining boundary |
| --- | --- | --- |
| ICYDB-036 | The approved pagination repair still passes the point/range/large-membership matrix, including range traversal without explain warm-up. | Publish, adopt and qualify Toko Miner's recipe, NFT-preview and Metrics continuation queries. No application deduplication workaround. |
| ICYDB-021 | Reuse exact selected schema authority while borrowing typed-binding metadata; paired IC instructions decrease and reports match. | Four warm equality/sort samples still miss the unchanged SQL-relative gate; an explicit cost decision remains. No new cache or further optimization is implied. |
| ICYDB-033 | Existing Canic audit owner and evidence reviewed; a bounded [Canic task](canic-handoff.md) is ready to queue. | Matched composed entity/query/write attribution, then application qualification or explicit narrowing. No current Toko Miner byte saving is established. |
| ICYDB-034 | Generated-field producer, Candid fact transport and CLI resolution checks pass. Installed global CLI is still 0.223.0. | Update the CLI alongside the published runtime and preserve facts through application wrappers; qualify an actual invalid fixture against exact accepted metadata. |

## Implementation and safety

Typed binding issuance and terminal validation already select a current
accepted catalog. They now borrow its exact store-scoped bundle through the
existing schema store instead of immediately rereading both durable root slots.
Execution's authority predicate and the borrower share one selection helper.
Root publication invalidates the decoded cache; a cache miss reloads the durable
selection rather than treating valid authority as absent.

The original selection owner still validates live identity-state closure, and
the borrower still checks constraint-validation-job closure. These records can
change without replacing the accepted root. Database incarnation, field/source
IDs, slots, layout, revision and fingerprint checks remain. Issuance and later
execution remain separate checks: query builders may outlive their schema.

No new cache, state, mode, budget, public API or persisted format is introduced.
The measured repeated-root work justifies extending the existing owner; merely
sharing mapping strings would not address it. Production delta: three Rust
files, **29 net implementation lines and 132 test lines**. Selection logic is
consolidated, with one narrow borrowed-metadata entry point; no second validator.

Tests additionally cover equal revisions/fingerprints from a different store,
root replacement, cache eviction, missing live identity state and changed jobs.
Review caught that the first prototype treated cache eviction as missing
authority; that prototype is not the final implementation. The final shared
selector preserves ordinary cold-cache reload. No test was weakened.

## Matched IC instructions

Final before/after actors have the same input queries and report strings, with
no added production instrumentation. Each endpoint runs cold/warm/warm in one
request; these are local instruction intervals, not whole-call charged cycles.

| Query | Cold planning before → after | First warm planning before → after | First warm planning + render before → after |
| --- | ---: | ---: | ---: |
| Primary-key equality | 2,402,995 → 2,283,159 | 660,963 → 541,771 | 891,477 → 772,285 |
| Scan/sort | 2,392,991 → 2,273,155 | 618,160 → 499,186 | 888,383 → 769,409 |
| Grouped COUNT | 2,368,178 → 2,248,342 | 630,331 → 511,315 | 951,768 → 832,752 |

All nine paired samples pass; [complete measurements](samples.txt) include the
second warm calls. Cold planning falls 4.99–5.06%; warm planning falls
18.03–19.31%, or 12.50–13.45% including rendering. Rendering instructions are
unchanged. These one-empty-entity
actor results do not establish populated-read or Toko Miner endpoint savings.
Charged cycles and final whole-endpoint instruction counts remain unmeasured.

Final typed raw Wasm: **2,510,157 → 2,510,810 bytes (+653)**; defined functions
remain **6,411**. The entire byte increase is code-section payload; data-section
payload stays 163,144 bytes. No gzip-based decision is used.

The final maintained SQL-relative gate ran once and **fails four of nine
samples**, down from eight in receipt 01: warm equality and warm sort still
miss. All cold and grouped samples pass the existing comparison. All reports
match typed/SQL/mixed, and the mixed actor's retained SQL read returns zero rows.
The gate compares typed planning against SQL's render-inclusive total; passing
it does not establish render-inclusive cost parity. For example, first warm
equality costs 772,285 typed instructions including rendering versus SQL's
448,024. No threshold or previous cost acceptance was changed.

| Final matched actor | Raw Wasm bytes | Defined functions |
| --- | ---: | ---: |
| Typed | 2,510,810 | 6,411 |
| SQL explain | 3,383,838 | 8,511 |
| Mixed | 3,407,631 | 8,595 |

The SQL-free typed actor is 873,028 raw bytes smaller than matched SQL. These
are maintained audit shapes, not Toko Miner or populated-query measurements.

## Qualification and provenance

Focused native checks: 52 schema-store, 16 typed-adapter, 64 cardinality/query,
71 session-ordering, one generated-identity frontend/fact test, one corrupt
control-frame test, the 16 typed-adapter tests without default features,
39 CLI diagnostic tests and one facade/Candid identity test. These are 261
successful executions, not necessarily distinct tests. Two native timing tests
remain ignored by policy. No full repository suite or application CI ran.

The initial focused Clippy run found an expanded lifecycle test over the local
line-count limit. Its single-fixture justification is explicit; required
`make clippy` and repository invariants pass. Formatting and whitespace checks
pass. Temporary host-probe wiring was removed; the [probe source](probe.rs.txt)
is evidence only, not a new production or maintained test mode.
Seven disposable PocketIC fixtures were created and released across prototype,
final paired, and final matched tests. Shared local networks were untouched.

Reproduction: package `canister_audit_one_entity_typed_query`, Rust 1.98.1,
locked/offline dependencies, `wasm-release`, `wasm32-unknown-unknown`, no default
features. Typed feature `typed-explain-measurement`; SQL feature
`sql-explain-measurement`; mixed features `sql,typed-explain-measurement`.
Binaryen 132 uses `-Oz --enable-bulk-memory --enable-sign-ext
--enable-nontrapping-float-to-int --one-caller-inline-max-function-size=0`.
Cargo.lock SHA-256:
`2861ffbeb525446769bda64557a70e263e750ebfb547b2a6d4e3323f9d3d9562`.

Baseline typed SHA-256:
`f8581ce124529dd63ec84e08febd66163c46f876b5e8f6cd6960de4795d02d03`.
Final typed SHA-256:
`93a108cc3ea477a7f3d67bd5b667b4073ba65914742547f08a71a3e74598081b`.
Final SQL and mixed SHA-256, respectively:
`d16fdc4f1f812d73ea8dcaecbf01c1b292d0788a9d3c37f2c544b8c18166f856`;
`477f42e789a0594fc0a05187c2aee26dad21ad651a23d786a9a1b06bfa22c0bc`.
Logs, source hashes and artifacts: `target/upstream-feedback-25722/`.
The baseline includes receipt 02's pagination repair and is byte-identical to
receipt 01's typed actor. Prototype measurements are retained separately and
do not stand in for the final build.

To reproduce the paired check, temporarily wire `probe.rs.txt` as
`selected_authority_probe` beside the maintained `typed_explain_measurement`
target. Set `ICYDB_SELECTED_AUTHORITY_DIR` to the directory containing
`before.wasm` and `after.wasm`, and select only
`selected_authority_preserves_reports_and_reduces_instructions`.

Publication and downstream qualification are not automated here. For ICYDB-036,
adopt the published repair and prove complete, ordered termination over three
or more pages using the application's actual continuation owners. For ICYDB-034,
use the matching CLI and the recipe in the [diagnostic guide](../../../../../../../guides/diagnostics.md#rejected-generated-fields);
retain numeric facts and refuse name resolution from mismatched schema metadata.
Neither step requires a new IcyDB API. ICYDB-033 stays a separate, explicitly
bounded measurement task, not a reason to postpone the correctness repair.

Handoff footprint: eleven files, approximately 400 net added lines including
tests, measurements and the Canic prompt. Runtime state-space is unchanged.
The original feature remains delivered, but this report does not close 0.257's
outstanding cost decision or silently start 0.258.
