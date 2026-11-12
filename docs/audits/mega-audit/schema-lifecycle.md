# Schema lifecycle follow-on audit

Investigation and authorized implementation after the grouped DISTINCT cleanup. Scope:
initial proposal lowering, existing/generated reconciliation, new-entity assembly,
startup reconciliation observation and cardinality rebuild ownership. This is
linked investigation evidence, not a separate follow-up queue.

## Implemented candidates

### 1. Remove the already-ordered initial entity sort

In [application lowering](../../../crates/icydb-core/src/db/schema/application_lowering.rs),
`proposal_definitions` inserts entities into a `BTreeMap<EntitySourceKey, _>`.
`lower_initial_schema_proposal` iterates that map in source-key order and appends
each entity to its store-local vector. Every vector is therefore an ordered
subsequence of the same traversal. The removed per-store compact sort used
that identical source-key comparison and changed no ordering.

The sort loop is removed, preserving ordered store traversal and entity allocation.
No new index, sorted marker or runtime check is needed. The identity allocator
still receives the same store-path/source-key order. Qualification should compare
initial accepted IDs and snapshots across shuffled fragment/assignment order,
multiple stores and same-proposal cross-entity relations.

**Do not remove the existing-head sort:** `existing_proposal_entities_by_store`
pushes entities in proposal assignment order, so its caller must establish source
order separately. Similar-looking loops have different upstream guarantees.
**Complexity delta:** one redundant ordering pass removed; no replacement
mechanism. IC instruction/cycle and raw Wasm total/code-section deltas remain
unmeasured.

### 2. Borrow immutable catalogues in the initial/additive context

`ExistingStoreCandidateState::add_new_entities` previously cloned enum and composite
catalogues into `InitialStoreContext` before calling the shared new-entity
constructor. The context reads those catalogues; they remain owned by the
candidate state and are later moved into the final bundle. Initial installation
already creates its named catalogues locally before lowering entities.

The context now borrows both catalogues while callers retain ownership and
share the same lowering. Initial catalogue construction is local to its caller;
additive construction borrows its existing catalogue fields. Final bundle
ownership transfer remains unchanged; the unused owning constructor is removed.

The context also carries named-type bindings, while the constructor separately
mutates final source bindings. That is a real borrow/validation distinction:
retaining the necessary binding snapshot is preferable to adding interior
mutability, a general overlay or a lazy catalogue. Start with the two immutable
catalogue copies, not every clone in the function.

**Complexity delta:** eliminate two full immutable catalogue copies on
additive construction; change context ownership without a second representation
or cache. Qualification:
initial named types/defaults, additive named entities, recursive composites,
cross-store relations and unchanged accepted identities. No format change or
runtime fallback to generated metadata is proposed.

## Provisional bindings: retain until a simpler design is proved

`lower_new_entity_snapshots` builds provisional entity/field snapshots, clones
bindings, and attaches new entity/field identities to that temporary catalogue.
Indexes, relations and constraints resolve against this complete forward-reference
view. A final attachment then includes the complete object bindings.

[Source-binding admission](../../../crates/icydb-core/src/db/schema/source_binding.rs)
`append_new_entities` rejects reused entity scopes and validates every child
binding belongs to a newly admitted entity before mutating the catalogue.
Appending fields first and object bindings later through the same method does
not work: the second append lacks fresh entity scopes or would reuse them.
Eliminating the temporary catalogue requires a different staged attachment
contract or a borrowed lookup overlay. Neither is yet proven simpler than the
existing bounded construction. Do not weaken duplicate/closure validation merely
to remove clones, or confuse these temporary maps with independently persisted
authorities. This is a redesign hypothesis, not a ready deletion.

## Existing convergence and boundaries to keep

- Installation and additive entity creation already share
  `lower_new_entity_snapshots`, field lowering, index/constraint construction and
  relation resolution. Do not build another constructor to avoid the context.
- Explicit and generated reconciliation share `lower_existing_store_candidates`.
  Generated ingress rejects removals and avoids retaining removal machinery;
  explicit ingress owns removal admission. A merged entrypoint could enlarge
  generated consumers without removing semantic duplication.
- `ExistingStoreCandidateState::finish` already consumes its catalogue and
  snapshots into the candidate. There is no additional final deep-copy pass
  analogous to the migration copy removed earlier.
- [Startup observation](../../../crates/icydb-core/src/db/startup/observe.rs)
  requests an accepted head only when matching a schema-failure receipt. Its
  ordinary ready check uses the receipt-only path; merging those helpers would
  introduce head derivation into the common path. Matching terminal failures
  intentionally precede pending-control checks.
- [Cardinality rebuild](../../../crates/icydb-core/src/db/schema/cardinality_build.rs)
  shares cursor/header validation, count preparation and page publication after
  separate row/index collection. Rows and indexes have distinct key decoding,
  component/witness checks, prefix-update budgets and checkpoints. Their visible
  loop similarity does not justify a generic scanning framework. The immutable
  accepted domain is already cached by verified root; source watermark/root
  checks remain per-page authority and must not be cached away.

## Verdict and qualification limits

No obsolete independent installation/reconciliation engine was found in these
selected paths. The concrete opportunities are an unnecessary initial sort and
catalogue ownership copies; the larger provisional-binding rewrite is conditional.
This is not exhaustive review of all DDL, migration transforms or index builders.

Implementation delta: **-24 non-test and +68 test code lines (net +44)**.
The regression permutes three entities across two stores and compares accepted
fingerprints and source bindings, retaining two entities in one store to exercise
subsequence ordering. The source-binding snapshot and existing-head sort remain.
No package versions, dependencies, snapshots, commits or releases are changed by
this cleanup. The complete audit now totals **-256 non-test, +42 test, net -214**.

Strict Core lint with SQL/migration/metrics and 76 focused tests pass: 27 proposal
lowering, 47 additive entity creation/recovery and two collection-relation tests.
Formatting, documentation and diff checks pass. The initial offline lint attempt
found incoming locked Wire 0.21.2 absent from cache; fetching that exact selection
allowed locked validation without changing dependencies. Broad workspace/release
gates are skipped user-owned validation, not required by this continuation.
