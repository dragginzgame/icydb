# 0.165 Access Shape Facts Naming

## Status

Complete.

## Accepted Renames

### `AccessCapabilities` -> `AccessShapeFacts`

Role proof:

- Owning module: `db::access::shape_facts`
- Payload: immutable plan-level structural facts derived from semantic or
  executable access plans
- Main consumers: route planning, secondary-order pushdown, aggregate fast-path
  policy, stream traversal, and access-plan validation
- Chosen family: `*Facts`
- Rejected alternatives:
  - `*Capabilities`: too vague because this value carries derived access-shape
    facts plus path summaries, not a raw capability set
  - `*Descriptor`: wrong because this is not a renderable or observable
    description
  - `*Context`: wrong because the value is reused across downstream decisions
    instead of being one owner-local traversal input
- Public-surface impact: none; visibility remains `pub(in crate::db)`
- Hard-cut rule: remove the old type, module, accessor, helper, and test
  vocabulary from live code

### `SinglePathAccessCapabilities` -> `SinglePathAccessShapeFacts`

Role proof:

- Owning module: `db::access::shape_facts`
- Payload: immutable structural facts for one access path
- Main consumers: route capability facts, stream-window checks, aggregate
  direct-fold checks, and secondary-index scan helpers
- Chosen family: `*Facts`
- Rejected alternatives:
  - `*Capabilities`: would keep the same ambiguous capability-set wording
  - `*Shape`: too weak because the value carries derived booleans and index
    details, not only a coarse access-path enum
- Public-surface impact: none
- Hard-cut rule: remove the old type and method vocabulary from live code

### `access::capabilities` -> `access::shape_facts`

Role proof:

- Owning module: `db::access`
- Payload: module containing derived access-shape fact helpers
- Main consumers: access-plan, executor-route, aggregate, and traversal modules
- Chosen family: `shape_facts`
- Rejected alternatives:
  - `capabilities`: too broad after the route capability-facts cleanup
  - `descriptor`: inconsistent with the module's non-observable role
- Public-surface impact: none
- Hard-cut rule: remove the old module name from live code

## Kept Names

### `IndexShapeDetails`

Kept because it is a small details bundle for one index-backed access shape.
It carries index identity and slot arity; it is not a broad plan-level
capability set.

### `RouteCapabilityFacts`

Kept because it is the route-owned, derived capability-fact snapshot from
0.165.0. The access-layer facts feed that route layer, but they are not the
same owner or same role.

## Old-Vocabulary Scan Terms

Live-code scans for this slice:

```bash
rg -n "AccessCapabilities|SinglePathAccessCapabilities|access::capabilities|access_capabilities|single_path_capabilities|\\.capabilities\\(\\)" crates/icydb-core/src/db/access crates/icydb-core/src/db/executor crates/icydb-core/src/db/query
rg -n "AccessShapeFacts|SinglePathAccessShapeFacts|access::shape_facts|access_shape_facts|single_path_facts|\\.shape_facts\\(\\)" crates/icydb-core/src/db/access crates/icydb-core/src/db/executor crates/icydb-core/src/db/query
```

Remaining `capability` wording outside this family is retained where it names
route capability facts, predicate capability gates, aggregate capability policy,
or materialization capabilities.
