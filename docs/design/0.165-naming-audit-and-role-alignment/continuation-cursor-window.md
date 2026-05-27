# 0.165 Continuation, Cursor, Page, And Window Naming

## Status

Complete.

## Accepted Renames

### `PlannedCursor` -> `ValidatedCursor`

Role proof:

- Owning module: `db::cursor`
- Payload: decoded scalar continuation boundary, optional validated index-range
  anchor, and initial offset after cursor-token validation
- Main consumers: prepared execution, scalar load entrypoints, pagination tests,
  route continuation resolution, and cursor revalidation
- Chosen family: `Validated*`
- Rejected alternatives:
  - `PlannedCursor`: implies planner ownership, but this value is produced by
    cursor validation and consumed by executor runtime
  - `CursorPlan`: would confuse the validated cursor state with planning policy
  - `RuntimeCursor`: too broad; it does not say validation has already happened
- Public-surface impact: none; visibility remains inside `crate::db`
- Hard-cut rule: remove the old type, module, helper, test, and active-doc
  vocabulary from live code

### `GroupedPlannedCursor` -> `ValidatedGroupedCursor`

Role proof:

- Owning module: `db::cursor`
- Payload: decoded grouped continuation boundary and initial offset after
  grouped cursor-token validation
- Main consumers: grouped execution, grouped continuation preparation,
  grouped pagination, and grouped cursor tests
- Chosen family: `Validated*`
- Rejected alternatives:
  - `GroupedPlannedCursor`: same planner-ownership problem as scalar cursor
  - `GroupedRuntimeCursor`: hides the validation boundary
  - `GroupedCursorState`: too broad and does not distinguish decoded validated
    input from external token wire state
- Public-surface impact: none; visibility remains inside `crate::db`
- Hard-cut rule: remove the old type and module vocabulary from live code

Companion helper/module renames:

- `cursor::planned` -> `cursor::validated`
- `validate_planned_cursor(...)` -> `validate_cursor_token(...)`
- `validate_planned_cursor_state(...)` -> `validate_cursor_state(...)`

### Cursor Token Decode Payloads

Role proof:

- Owning module: `db::cursor::token::codec`
- Payload: decoded scalar/grouped token wire payloads handed from the bounded
  token codec into the scalar or grouped continuation token domain type
- Main consumers: scalar and grouped token decode constructors
- Chosen family: `Decoded*Payload`
- Rejected alternatives:
  - `*Parts`: too weak because these values are named wire decode payloads, not
    general decompositions
  - `*Context`: wrong because the values are returned codec payloads rather
    than owner-local traversal/input contexts
  - `*Contract`: too strong because validation policy lives above the wire
    codec
- Public-surface impact: none; visibility remains cursor-token-internal
- Hard-cut rule: remove the old token `Parts` type names and grouped token
  `into_parts` helper from live cursor code

Accepted renames:

```text
ScalarTokenParts -> DecodedScalarTokenPayload
GroupedTokenParts -> DecodedGroupedTokenPayload
GroupedContinuationToken::into_parts() -> into_components()
```

### `GroupedWindowProjection` -> `GroupedContinuationWindowDraft`

Role proof:

- Owning module: `db::query::plan::continuation`
- Payload: private grouped continuation window draft assembled from one
  planner-owned continuation contract and one validated grouped cursor
- Main consumers: `PlannedContinuationContract::project_grouped_paging_window`
- Chosen family: `*Draft`
- Rejected alternatives:
  - `*Projection`: misleading because this value is not SQL projection or
    output projection; it is intermediate grouped paging-window state
  - `*Parts`: too weak because the value is a named local construction step,
    not only tuple decomposition
  - `*Context`: wrong because the value is finalized into an outward DTO rather
    than used as owner-local traversal context
- Public-surface impact: none; the type remains private to continuation
  planning
- Hard-cut rule: remove the old private type and comment vocabulary from live
  code

Accepted rename:

```text
GroupedWindowProjection -> GroupedContinuationWindowDraft
```

## Kept Names

### `PlannedContinuationContract`

Kept because the value is planner-owned continuation semantics. It carries the
shape signature, boundary arity, window size, order contract, access plan, and
grouped cursor policy that runtime layers must not re-derive.

Rejected alternatives:

- `ContinuationFacts`: too weak; the value is an invariant contract consumed by
  cursor preparation and revalidation
- `ContinuationPlan`: too broad and would collide with route-level continuation
  planning vocabulary

### `ScalarAccessWindowPlan`

Kept for this slice because it is a small planner-projected access-window DTO
that is immediately lowered into `RouteContinuationPlan`. The name is tolerable
while route continuation still uses `*Plan` vocabulary.

Deferred trigger:

- Revisit together with `RouteContinuationPlan` if route continuation names move
  from `Plan` to `Decision` or `Contract` vocabulary.

### `GroupedContinuationWindow`

Kept because it is the outward grouped paging-window contract returned from
`PlannedContinuationContract`. It is not a cursor token and not a planner route.

## Old-Vocabulary Scan Terms

Live-code scans for this slice:

```bash
rg -n "PlannedCursor|GroupedPlannedCursor|validate_planned_cursor|planned_cursor|mod planned|planned::|cursor::planned" crates/icydb-core/src docs/design/0.165-naming-audit-and-role-alignment
rg -n "ValidatedCursor|ValidatedGroupedCursor|validate_cursor_token|validate_cursor_state|mod validated" crates/icydb-core/src/db
rg -n "PlannedContinuationContract|ScalarAccessWindowPlan|GroupedContinuationWindow|GroupedWindowProjection|GroupedContinuationWindowDraft|RouteContinuationPlan" crates/icydb-core/src/db/query/plan crates/icydb-core/src/db/executor/planning/continuation
rg -n "ScalarTokenParts|GroupedTokenParts|GroupedContinuationToken::into_parts|\\.into_parts\\(\\)" crates/icydb-core/src/db/cursor docs/design/0.165-naming-audit-and-role-alignment
rg -n "DecodedScalarTokenPayload|DecodedGroupedTokenPayload|into_components" crates/icydb-core/src/db/cursor
```

Remaining old-name hits are allowed only inside this family note as accepted
rename history and scan terms.
