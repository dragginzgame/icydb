# 0.165 Covering Reads Naming

## Status

In progress.

## Accepted Renames

### `CoveringProjectionContext` -> `CoveringProjectionFacts`

Role proof:

- Owning module: `db::query::plan::covering`
- Payload: component index, bound-prefix arity, and output-order contract for
  one index-backed covering projection
- Main consumers: aggregate projection execution, `bytes_by` planning, adjacent
  distinct eligibility, and covering planner tests
- Chosen family: `*Facts`
- Rejected alternatives:
  - `*Context`: this value crosses planner/executor boundaries and is reused by
    multiple consumers, so it is not owner-local traversal state
  - `*Plan`: this is not the selected covering read payload; it is supporting
    derived projection facts
  - `*Contract`: this is not an admission proof boundary with independent
    invariants; the order contract remains a field inside the facts
- Public-surface impact: none; visibility is internal to `crate::db`
- Hard-cut rule: remove the old type, helper, alias, test, and active-doc
  vocabulary from live code

### `CoveringAccessMetadata` -> `IndexCoveringAccessFacts`

Role proof:

- Owning module: `db::query::plan::covering`
- Payload: derived index order terms, coverable component fields, prefix
  values, prefix arity, and index-range flag for index-backed covering access
- Main consumers: covering projection fact derivation, constant covering
  projection resolution, and pure/hybrid covering plan derivation
- Chosen family: `*Facts`
- Rejected alternatives:
  - `*Metadata`: too broad and suggests descriptive or persisted metadata rather
    than one derived planner fact bundle
  - `*Context`: this is reused by several helpers and is not a single traversal
    input bundle
  - `*Plan`: this is not the selected covering read or projection plan
- Public-surface impact: none; private to the covering planner module
- Hard-cut rule: remove the old type/helper vocabulary from live code

## Kept Names

### `CoveringReadPlan`

Kept because it is a planner-selected covering read payload. It contains output
fields, prefix arity, and order contract, and it feeds the execution-grade
covering read plan.

Rejected alternatives:

- `CoveringReadFacts`: would hide that this is selected planner output
- `CoveringProjectionPlan`: less precise because the concept is the covering
  read route, not generic projection evaluation

### `CoveringReadExecutionPlan`

Kept because it is an execution-grade plan with row-presence semantics attached.
The database `ExecutionPlan` vocabulary is appropriate here.

Rejected alternatives:

- `CoveringReadContract`: too broad; this is executor-ready route payload
- `CoveringReadFacts`: would hide selected execution behavior

### `CoveringProjectionFieldSourcePolicy`

Kept because it controls how projected fields are admitted as strict covering or
hybrid row-fallback sources. `Policy` is appropriate for this behavior switch.

### `CoveringProjectionSourceContext`

Kept because it is a private, owner-local input bundle for one projection walk.
That matches the 0.165 `*Context` policy.

## Old-Vocabulary Scan Terms

Live-code scans for this slice:

```bash
rg -n "CoveringProjectionContext|covering_index_projection_context|projection context" crates/icydb-core/src docs/architecture docs/design/0.165-naming-audit-and-role-alignment README.md
rg -n "CoveringProjectionFacts|covering_index_projection_facts" crates/icydb-core/src
rg -n "CoveringAccessMetadata|covering_access_metadata" crates/icydb-core/src
rg -n "IndexCoveringAccessFacts|index_covering_access_facts" crates/icydb-core/src
```

Archived 0.68 design notes may retain the old context vocabulary as historical
prose.
