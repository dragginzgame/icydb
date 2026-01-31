# Same-Canister Validation for Nested Relations (Schema Build)

Status: Implemented
Audience: icydb-schema maintainers
Scope: crates/icydb-schema build-time validation only

## Problem
Relations (Item.relation) can appear deep inside reusable types (record, enum, tuple, list, map, set, newtype).
Nodes are structural and lack canister context; runtime must remain relation-agnostic.
We must reject any relation target that crosses canister boundaries, regardless of nesting depth.

## Invariant
Any Item.relation reachable from an entity's value graph must resolve to an entity in the same canister as the source entity.

## Goals
- Enforce the invariant at schema build time.
- Detect nested relations reachable from entity value graphs.
- Preserve current architecture: no node-level canister awareness, no runtime checks, no schema redesign.

## Constraints
- Single centralized validation pass.
- Deterministic, synchronous.
- Runtime (icydb-core) sees no relation semantics.

## Where This Lives
- Centralized pass in crates/icydb-schema/src/validate/mod.rs, wired through validate_schema.
- This phase has:
  - Full schema graph (Schema.nodes).
  - Entity -> store -> canister mapping (Entity.store, DataStore.canister).
  - Access to all type nodes needed to traverse value graphs.

## Why Node-Local Validation Is Insufficient
- Type nodes are reused across entities and canisters; they cannot validate locality alone.
- Nodes have no owner context; adding parent pointers is out of scope.
- Therefore the check must be performed per entity with global context.

## Design Overview
- Two-phase pipeline: collect -> validate.
- Collection walks each entity's value graph and records relation edges; validation resolves canisters and enforces locality.

## Phase 1: Collection (per entity)
- Input: Schema, entity root.
- Output: list of (source_entity_path, target_entity_path, field_path) triples.
- Traversal is limited strictly to value shapes; index definitions, metadata, and non-value schema nodes are intentionally excluded.
- Traversal:
  - Start at Entity.fields -> Field.value -> Value.item.
  - On Item.relation: record target path and current field_path.
  - On ItemTarget::Is(path): recurse into referenced type node.
  - Handle nested structures:
    - Record: field values
    - Enum: variant values (if any)
    - Tuple: elements
    - List/Set: item
    - Map: key item + value
    - Newtype: item
- Cycle safety: track a visiting set of type paths per entity traversal to avoid infinite recursion.
- Optional cache: memoize relations reachable from a type path. Memoized results must be relative (no entity or canister info) so they remain reusable across entities.

## Phase 2: Validation (global)
- Build entity_canister map from Entity.store -> DataStore.canister.
- For each collected edge:
  - Resolve target entity (already validated by Item::validate).
  - Compare source_canister vs target_canister.
  - Emit schema error if mismatched.
- Errors join the existing ErrorTree flow (same as other schema validation errors).

## Error Reporting
- Preserve a precise field_path during traversal:
  - record field: foo
  - record field nesting: foo.bar
  - enum variant: foo.Variant
  - tuple element: foo.[i]
  - list/set item: foo.item
  - map: foo.key / foo.value
  - newtype: foo.item (or foo.value)
- Example error wording (match existing tone):
  - "entity A (canister X), field foo.bar.[0].value, has a relation to entity B (canister Y), which is not allowed"

## Non-Goals
- No cross-canister referential integrity or distributed transactions.
- No runtime validation or model changes in icydb-core.
- No changes to schema language or relation syntax.
- Primitive cross-canister identifiers remain allowed and unchecked.

## Checklist
- [x] Add centralized build-time pass in crates/icydb-schema/src/validate/mod.rs after existing node validation.
- [x] Implement per-entity traversal that collects (source, target, field_path) for all reachable Item.relation.
- [x] Ensure traversal handles Record, Enum, Tuple, List, Set, Map, Newtype.
- [x] Add cycle protection (visiting set) per entity traversal.
- [x] Build entity_canister map and validate source_canister == target_canister for all edges.
- [x] Emit errors with precise field paths and both canister names.
- [x] Keep runtime unchanged; no changes to icydb-core models or planners.
- [x] Document that primitive cross-canister IDs remain allowed and unchecked.
