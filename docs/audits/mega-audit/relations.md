# Relation preparation closeout investigation

Investigation and authorized implementation after the recovery cleanup,
2026-11-11. This records one bounded preparation reduction, not an independent
task queue or a redesign of relation execution.

## Implemented outcome: remove redundant preparation representations

Owner: [reverse relation compilation](../../../crates/icydb-core/src/db/relation/reverse_index/mod.rs),
`compile_accepted_relation_binding`, and
[tuple validation](../../../crates/icydb-core/src/db/relation/mod.rs),
`accepted_relation_tuple_edge_descriptor`.

### Build component specifications once

Direct tuple compilation first collects `(slot, AcceptedFieldDecodeContract)`
pairs in `local_fields`. After validating the tuple's kinds, it converts those
same pairs into a second `Vec<AcceptedRelationLocalComponentSpec>`. Neither
conversion changes the identity, slot or borrowed field contract. The second
vector exists only to call `try_from_component_specs`.

Populate the existing component-spec type during the first fallible field lookup
and use it for scalar selection, tuple-kind projection and final owned component
construction. This removes one temporary vector and conversion pass without a
new representation. Retain the initial complete field-resolution pass and current
validation order: moving allocation of owned components ahead of tuple validation
would be a different change. Preserve the existing empty-component error behavior.

### Remove the descriptor that only returns its input

`AcceptedRelationTupleEdgeDescriptor` contains only the target contract passed
into its constructor. Its only method moves that contract back out. Both callers
immediately unwrap it into ordinary relation construction; no downstream API
requires the descriptor as a validated proof type.

Replace it with a tuple-component validator borrowing the target contract and
returning `Result<(), InternalError>`. Callers retain and then move their existing
target contract. Preserve arity comparison, ordered kind comparison, supported
primary-key-kind validation and diagnostic enrichment. Do not introduce a generic
iterator framework merely to remove the separate kind-fact slice.

Expected complexity reduction: one temporary vector/conversion pass, one private
wrapper type and its move-out method. Actual test/non-test code deltas remain
unmeasured until implementation. No public interface, durable format or runtime
fallback changes. IC cycles/instructions and uncompressed Wasm total/code-section
impacts remain unmeasured.

## Qualification boundaries

- Scalar and ordered composite local fields, empty input, arity/type mismatches,
  accepted target identity and unchanged diagnostic identity.
- Direct optional list/set relations and nested paths, including bounded
  traversal, nullable terminals and unsupported kind refusal.
- Tuple reverse-key encoding, delete validation, source-slot ordering and
  schema collection-relation admission/enforcement.

Existing relation tests cover many of these cases. Add a direct compilation
regression only where existing callers do not exercise a changed boundary; do
not retain retired wrappers solely for test convenience.

## Similar-looking code to retain

- Scalar leaf decoding, tuple slot decoding and nested traversal have different
  representations and budgets. Their target-key dispatch already has one owner;
  merging them into a generic traversal is not justified by this inspection.
- `AcceptedNestedRelationSource` clones an `AcceptedValueCatalogHandle`, which
  shares enum/composite catalogues through Arc. It is not a deep catalogue copy.
  Retaining that authority keeps compiled relation state independent of temporary
  borrowed row contracts.
- Target filtering currently occurs after relation compilation. Moving it earlier
  would suppress errors in filtered-out accepted edges; that is a semantic change,
  not automatically an optimization.
- The one-call `accepted_relations_from_edges` wrapper is trivial indirection,
  but removing it alone is not a substantive landing slice. It does not establish
  a duplicate relation engine or justify a broad helper cleanup.

## Closeout

Both preparation reductions are implemented. The existing specification type is
populated during fallible lookup and reused for scalar selection, kind projection
and final construction. Tuple validation now borrows its target and returns unit;
callers retain ownership. Field lookup and validation order are unchanged.

Actual delta: **-10 non-test / 0 test code lines**. One temporary vector and
conversion pass, one wrapper type and its move-out method are removed; no new
representation or abstraction is added. Audit totals are **-284 non-test,
+80 test, net -204 code lines**. Existing behavioral tests are retained.

Strict Core lib/tests Clippy with SQL/migration/metrics and 50 focused tests pass:
36 relation tests, ten nested write/recovery tests, two collection-relation tests
and two mixed-batch relation tests. Formatting, documentation and diff checks
pass. The initial offline lint attempt lacked incoming locked Runtime 0.21.2;
locked online validation fetched Runtime 0.21.2, Types 0.19.4 and Storage 0.73.4
without selecting upgrades. These incoming dependency edits are preserved.
Broad workspace/release gates remain skipped user-owned validation. IC cycles,
instructions and uncompressed Wasm total/code-section effects are unmeasured.
