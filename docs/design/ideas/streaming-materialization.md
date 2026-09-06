# Nested Collection Materialization

Status: unpromoted measurement question; no implementation authority

The general streaming/key-first execution work is already covered by the
[0.222 design](../archive/0.222-streaming-execution-and-key-first-operators/0.222-design.md).
Do not reopen that programme or add another stream abstraction from this note.

## Remaining Question

Recursive list and map decoding in
`crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs`
validates bounded wire input and allocates the final owned `Value` collection.
Those allocations implement maintained behavior, not obsolete codecs.

Measure whether a real collection-predicate workload discards enough of that
owned data to justify delaying materialization. Existing scalar borrowed access
and nested-path projection already avoid materializing untouched values.

The ongoing [0.255 owned-value handoff work](../0.255-owned-value-handoff/0.255-status.md)
removes copies at admission, constraint evaluation, full-row output, and direct
projection, and converges canonical materialization on the decoder. Its tracker
also owns the authorized nested-relation root borrowing work. These outcomes
must form the baseline for a future investigation; do not reuse pre-0.255 copy
costs as evidence for a streaming layer or assume the line is released.

The remaining question is whether a consumer can avoid constructing the final
owned collection after those ownership fixes. Wire length is not necessarily
logical cardinality: relation-list decoding can omit null items. Early predicate
success must still preserve required whole-value corruption checks.

## Promotion Gate

- Identify a consumer that can finish without owning the complete collection.
- Freeze a baseline incorporating the relevant completed 0.255 outcomes and
  identify any outstanding overlap using its current tracker.
- Compare the maintained path with the smallest change at its existing owner.
- Preserve accepted-kind validation, recursive-depth and collection bounds,
  typed corruption failures, and deterministic map semantics. Skipping output
  must not skip required validation.
- Measure requested allocation bytes separately from peak/live memory, plus
  instructions, raw Wasm, defined functions and implementation complexity.
- If consumers require an owned public `Value`, retain the existing allocation.

No borrowed collection API, persisted format, execution route or release line
is selected by this idea.
