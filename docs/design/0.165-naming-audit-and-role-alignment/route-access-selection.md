# 0.165 Route And Access Selection Naming

## Status

Complete.

## Accepted Renames

### `LoadOrderRouteContract` -> `LoadOrderRouteMode`

Role proof:

- Owning module: `db::executor::planning::route::contracts::execution`
- Payload: one selected ordered-load route mode without reason text
- Main consumers: route capability derivation, page planning, continuation
  validation, explain diagnostics, route tests
- Chosen family: `*Mode`
- Rejected alternatives:
  - `*Contract`: too strong because this enum is a selected routing mode, not
    an admission proof surface
  - `*Decision`: already used by `LoadOrderRouteDecision`, which pairs the mode
    with a reason
- Public-surface impact: none; visibility is executor-internal
- Hard-cut rule: remove the old type, field, method, and helper vocabulary from
  live code

### `GroupedExecutionModeProjection` -> `GroupedExecutionModeContext`

Role proof:

- Owning module: `db::executor::planning::route::contracts::execution`
- Payload: owner-local input bundle used while deriving one grouped execution
  mode from planner strategy and route facts
- Main consumers: route stage assembly, grouped feasibility checks, grouped
  route tests
- Chosen family: `*Context`
- Rejected alternatives:
  - `*Projection`: conflicts with query projection vocabulary and does not
    describe an output projection
  - `*Facts`: too broad; this bundle is a local input bundle for one decision
- Public-surface impact: none; visibility is executor-internal
- Hard-cut rule: remove the old type name from live code

### `RouteCapabilities` -> `RouteCapabilityFacts`

Role proof:

- Owning module: `db::executor::planning::route::contracts`
- Payload: one derived, read-only fact bundle for route eligibility, route
  hints, bounded fetch safety, field-extrema fast-path eligibility, and the
  ordered-load route decision
- Main consumers: route feasibility derivation, route execution-stage
  selection, route hint helpers, explain-facing route plan accessors, and
  route tests
- Chosen family: `*Facts`
- Rejected alternatives:
  - `*Capabilities`: too vague because this bundle carries derived route facts
    plus the load-order decision, not just raw path capabilities
  - `*Context`: would imply owner-local input state instead of a reusable
    derived snapshot
  - `*Contract`: too strong because this is not an admission proof or persisted
    runtime contract
- Public-surface impact: none; visibility is executor-internal
- Hard-cut rule: remove the old type, derivation helper, route-plan field, and
  test vocabulary from live code

Companion module renames:

- `route::capability` -> `route::capability_facts`
- `route::contracts::capabilities` -> `route::contracts::capability_facts`

These module names now match the accepted route capability-facts role instead
of preserving the broad capability/capabilities vocabulary as live structure.

### Planner Access Selection Helper Renames

Role proof:

- Owning modules: `db::query::plan::planner`, `db::query::plan::access_plan`,
  and query-plan pipeline helpers
- Payload: private planner access-selection values and constructors that carry
  selected access plans, projection selection, and non-index winner reasons
- Main consumers: scalar/grouped planning, candidate reranking, and planner
  tests
- Chosen family: explicit access-selection and projection vocabulary
- Rejected alternatives:
  - `*Parts`: too weak because these helpers assemble or unpack planner
    access-selection payloads rather than temporary decompositions
  - `*Context`: wrong because these values are returned selection state, not
    owner-local traversal inputs
  - `*Descriptor`: wrong because the values drive planning and explain
    snapshots rather than rendering descriptions
- Public-surface impact: none
- Hard-cut rule: remove the old private helper names from live code

Accepted code examples:

```text
PlannedAccessSelection::into_parts() -> into_access_and_non_index_reason()
AccessPlannedQuery::from_parts_with_projection(...) -> from_logical_access_and_projection(...)
AccessPlannedQuery::from_planned_parts_with_projection(...) -> from_planned_access_with_projection(...)
```

## Kept Names

### `LoadOrderRouteDecision`

Kept because it is the selected ordered-load route mode plus its reason. That
matches the `*Decision` policy.

Rejected alternatives:

- `LoadOrderRouteFacts`: would hide the selected-outcome role
- `LoadOrderRouteContext`: would imply local traversal/input state

### `LoadRouteCapabilityFacts`

Kept because it is a private, derived, read-only snapshot reused during route
capability derivation. That matches the `*Facts` policy.

## Old-Vocabulary Scan Terms

Live-code scans for this slice:

```bash
rg -n "LoadOrderRouteContract|load_order_route_contract|access_order_satisfied_by_route_contract|GroupedExecutionModeProjection" crates/icydb-core/src
rg -n "LoadOrderRouteMode|load_order_route_mode|access_order_satisfied_by_route_mode|GroupedExecutionModeContext" crates/icydb-core/src
rg -n "RouteCapabilities|derive_execution_capabilities_for_model|route_capabilities|route::capability\\b|route::contracts::capabilities|mod capability;|mod capabilities;" crates/icydb-core/src
rg -n "RouteCapabilityFacts|derive_execution_capability_facts_for_model|route_capability_facts|route::capability_facts|route::contracts::capability_facts|mod capability_facts;" crates/icydb-core/src
rg -n "PlannedAccessSelection::into_parts|from_parts_with_projection|from_planned_parts_with_projection|into_access_and_non_index_reason|from_logical_access_and_projection|from_planned_access_with_projection" crates/icydb-core/src/db/query/plan docs/design/0.165-naming-audit-and-role-alignment
```

Generic `route contract` wording remains valid where it names broader
executor-route contract DTOs. It should not describe the ordered-load mode
after this slice.
