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

Measure whether a real filtering or projection workload discards enough of
that owned data to justify delaying materialization. Existing scalar borrowed
access and the 0.252 nested-path work are the starting point, not missing
capabilities to recreate.

## Promotion Gate

- Identify a consumer that can finish without owning the complete collection.
- Compare the maintained path with the smallest change at its existing owner.
- Preserve accepted-kind validation, recursive-depth and collection bounds,
  typed corruption failures, and deterministic map semantics. Skipping output
  must not skip required validation.
- Measure requested allocation bytes separately from peak/live memory, plus
  instructions, raw Wasm, defined functions and implementation complexity.
- If consumers require an owned public `Value`, retain the existing allocation.

No borrowed collection API, persisted format, execution route or release line
is selected by this idea.
