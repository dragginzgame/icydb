# Journal recovery closeout investigation

Investigation and subsequently authorized implementation after the two schema
lifecycle cleanups, 2026-11-11. This records the reasoning and qualification for
the two bounded reductions; it is not a separate issue queue or authorization
for a broader recovery redesign.

## 1. Borrow temporary cardinality coalescing inputs

Owner: [recovery](../../../crates/icydb-core/src/db/commit/recovery/mod.rs),
`collect_folded_cardinality_changes`.

The function receives an immutable slice of prepared operations. It clones row
keys into `final_rows` and index keys plus optional index values into
`final_indexes`. Both maps are consumed within the same call; their inputs outlive
them. The maps preserve last-write-wins coalescing before comparing canonical
state and producing owned count deltas. They need ordering and coalescing, not
independent ownership of their keys or values.

Candidate: retain both BTreeMaps but borrow row/index keys and optional index
values from the prepared operations. Preserve store-handle checks, checkpoint
inclusion, last mutation wins, prefix witness validation, overflow checks and
zero-delta removal. Do not replace these maps with direct per-operation counting:
multiple index mutations can address the same key, so that would change semantics.

Expected complexity benefit: three clone sites disappear without adding a type,
cache, helper or lifetime parameter to the public interface. Code-line reduction
may be neutral; allocation/IC/Wasm effects are unmeasured. Qualification needs
repeated-key put/delete combinations, Building row/index checkpoints, Ready
counts, source invalidation and corrupted optional evidence. Trace the maintained
fold/cardinality tests before selecting the exact gate.

## 2. Use admitted publication position in schema candidate extraction

Owner: the same recovery file, `journal_batch_schema_candidate`, used by live
schema checkpoint recovery and journal envelope validation.

It scans every record, tracks an optional candidate, checks publication position
and uniqueness, and rejects a row-delete suffix. However,
[JournalBatch admission](../../../crates/icydb-core/src/db/journal/codec/mod.rs)
already requires a publication to be first and unique, and rejects non-schema
suffixes. Both the checked constructor and persisted decoder enter
`from_validated_records`, which runs that batch-shape validator. Batch fields
are private, records are exposed only immutably, and the test constructor also
delegates to checked construction.

Candidate: inspect `batch.records().first()` directly, return None for a
non-publication batch, and decode/validate the one publication. Retain recovery's
expected-store comparison, decoded candidate/root revision checks and source
binding checks. Retain candidate decoding: the batch retains entity tags, not
the complete candidate required for publication. Do not add a cached full
candidate solely to remove decoding.

Expected complexity benefit: remove one full-record scan, mutable optional state
and repeated publication-shape dispatch. No new representation or helper is
needed. Net code delta remains unmeasured until implementation. Qualification:
constructor and raw-decoder rejection of late/duplicate/mixed publications,
no-publication batches, store mismatch, source binding mismatch, live metadata
checkpoint replay, stale fold refusal and sibling index fingerprint checks.
Existing journal publication tests and recovery `schema_snapshot` tests cover
parts of that contract; check raw-decoder shape coverage explicitly.

## Boundaries retained

- Individual `JournalRecord` constructors and batch admission both validate
  payloads, but record enum variants can be built directly. Removing the batch
  check would trust an unproven input. A new trusted-record wrapper is not yet
  justified merely to eliminate repeated validation.
- Raw header inspection supports bounded admission before payload decode. It
  cannot replace checksum, record or complete batch-shape validation.
- Recovery validates candidate semantics before moving that same candidate into
  application. Its existing preparation/application separation protects atomic
  effects; do not collapse it to save similar-looking branches.
- Online retirement owns prepared keys across preparation/application and checks
  exact overlay positions. Its ownership differs from the call-local cardinality
  maps; the same borrowing proposal does not automatically apply there.

## Implementation outcome

Both candidates are implemented. The cardinality maps retain ordered,
last-write-wins coalescing while borrowing their inputs. Schema extraction reads
the first record and retains store/revision/source-binding validation. Neither
changes persisted bytes or accepted schema authority. Broader recovery, journal
storage and reverse-relation algorithms remain outside this inspected boundary.

Delta: **-18 non-test, +38 test code lines (net +20)**. Borrowed map inputs are
line-count neutral; the schema extraction removes 18 production code lines. The
new raw-decoder test authenticates malformed publication sequences, proving
rejection reaches admission rather than the checksum gate. Complexity decreases
by three cloning sites, a full-batch scan, mutable optional state and repeated
record-kind dispatch. No helper, cache, type or persisted representation is added.

Audit totals: **-274 non-test, +80 test, net -194 code lines**. IC cycles,
instructions and uncompressed Wasm total/code-section effects remain unmeasured.

Strict Core lib/tests Clippy with SQL/migration/metrics and 87 focused tests pass:
72 journal tests, nine schema-recovery tests, four Building-cardinality fold tests
and two Ready/source-drift tests. This includes the new authenticated raw-byte
regression and existing replay/fold contradiction tests. Formatting, documentation
and diff checks pass. Broad workspace/release gates remain skipped user-owned
validation. No package versions, dependencies, persisted formats or delivery
boundaries change in this cleanup.
