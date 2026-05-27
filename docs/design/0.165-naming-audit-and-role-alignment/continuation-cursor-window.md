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

### `GroupedWindowProjection`

Kept as a private construction decomposition. The name is local and temporary,
but a future cleanup may prefer `GroupedContinuationWindowDraft` if nearby
window construction vocabulary moves away from `Projection`.

## Old-Vocabulary Scan Terms

Live-code scans for this slice:

```bash
rg -n "PlannedCursor|GroupedPlannedCursor|validate_planned_cursor|planned_cursor|mod planned|planned::|cursor::planned" crates/icydb-core/src docs/design/0.165-naming-audit-and-role-alignment
rg -n "ValidatedCursor|ValidatedGroupedCursor|validate_cursor_token|validate_cursor_state|mod validated" crates/icydb-core/src/db
rg -n "PlannedContinuationContract|ScalarAccessWindowPlan|GroupedContinuationWindow|GroupedWindowProjection|RouteContinuationPlan" crates/icydb-core/src/db/query/plan crates/icydb-core/src/db/executor/planning/continuation
```

Remaining old-name hits are allowed only inside this family note as accepted
rename history and scan terms.
