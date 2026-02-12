# Query Contract (Intent + Facade)

This document freezes the intent-level contract for the query facade. It is a
source-of-truth boundary: anything not stated here is not a facade guarantee.

This document defines **what the facade guarantees** at intent, planning, and
execution boundaries. Predicate semantics, coercion rules, diagnostics, and
testing practices live in `docs/QUERY_PRACTICE.md`.

## Scope

- Applies to query intent construction, planning, and execution boundaries.
- Does not describe storage internals, macro expansion, or encoding.
- This is an intent contract, not an implementation spec.

## Query Intent: What It Is

A query intent represents the caller's desired result without committing to a
physical access path.

Implementation note: the public facade exposes session-bound wrappers
(`SessionLoadQuery` and `SessionDeleteQuery`) that wrap `Query<E>` and route
execution through `DbSession`.

Minimum intent surface (conceptual):
- entity type (E)
- predicate (optional)
- projection (implicit all-fields unless specified)
- order specification (optional)
- pagination (optional)
- consistency (missing-row policy; explicit; see below)

## Guarantees Before Planning

The facade guarantees that a constructed intent is:
- structurally well-formed (syntactically valid AST; coercions declared explicitly,
  but not yet validated against schema)
- deterministic in representation (same inputs produce equivalent intent)
- schema-agnostic (no schema or index metadata consulted)
- projection defaults to all fields unless explicitly specified

The facade does NOT guarantee:
- field existence or type compatibility
- index eligibility
- access-path feasibility

## Facade Coercion Defaults (Locked)

`FilterExpr` is lowered with **explicit coercions**. For ordering operators:

- `Lt`, `Lte`, `Gt`, `Gte` use `NumericWiden` coercion.

This now matches the builder `FieldRef` surface.

## What Intent Must NOT Encode

Intent must not encode or imply any of the following:
- access paths (key, index, scan, range)
- index names or index choices
- execution ordering or physical plan steps
- plan cache keys or executor hints
- read modes hidden in access paths

## Primary-Key Semantics

Primary keys are regular field values with uniqueness and indexing guarantees.
They are queryable through the normal predicate surface, just like other fields.

The planner may optimize validated primary-key predicates into key/index access
paths when that preserves query semantics.

`by_id(...)` and `by_ids(...)` are ergonomic helpers for typed primary-key
values (`Id<E>`). They are not privileged access paths and are not required for
primary-key filtering.

IDs in query inputs are public values and may come from untrusted sources.
Query matching by ID is a lookup operation only; it does not imply authorization,
ownership, or entity existence beyond what execution returns.

## Projection Semantics

If no projection is specified, the intent is interpreted as “all fields.”
Projection is an intent concern and must not be introduced by the planner.

## Pagination Guarantees

IcyDB provides deterministic, cursor-based pagination for ordered queries.

Pagination is forward-only and bound to a canonical query shape.
It is not snapshot-based and does not provide transactional consistency across page requests.

### 1. Requirements for Pagination

Pagination requires:

- An explicit ORDER BY clause.
- A total ordering over the result set.

IcyDB enforces canonical total ordering by automatically appending the entity's
primary key as a terminal tie-break field when necessary.

Unordered pagination is rejected at planning time.

### 2. Canonical Ordering

All paginated queries execute over a canonical order defined as:

`(user-specified order fields) + (primary key)`

This ensures:

- No two rows compare equal.
- Continuation boundaries are unambiguous.
- Deterministic traversal is guaranteed.

### 3. Cursor Semantics

Pagination uses an opaque continuation cursor.

The cursor:

- Encodes the last returned row's canonical boundary values.
- Is cryptographically hashed against the canonical query shape.
- Is versioned and type-validated during decode.
- Is rejected if reused with a different entity, predicate, access path, or ordering.

Continuation is strict:

`next_page := rows WHERE row > boundary`

The boundary comparison is strictly greater-than (`>`), not greater-or-equal (`>=`).

For fixed query shape + stable ordered keys between requests, this guarantees:

- No duplicate rows across pages.
- No overlap between page windows.

### 4. Forward-Only Iteration

Pagination is forward-only.

IcyDB does not provide:

- Backward cursors.
- Random page access via cursor.
- Offset + cursor mixing.

If a cursor is provided, offset-based pagination is disallowed.

### 5. Determinism Guarantees

For a fixed query shape and stable data state:

- Identical inputs produce identical page sequences.
- Rows will not be duplicated across pages.
- Rows will not be skipped due to ordering ambiguity.
- Cursor reuse with a modified query shape results in a validation error.

### 6. Live-State Semantics (No Snapshot Isolation)

Pagination operates over the current live state of the database.

IcyDB does not provide snapshot isolation across page requests.

This means:

- Inserts occurring before the current boundary may not appear in subsequent pages.
- Inserts occurring after the boundary may appear in subsequent pages.
- Updates that modify ordered fields may shift rows between pages.
- Deletes may reduce the total remaining result set.

This behavior is intentional and documented.

Applications requiring snapshot-consistent iteration must implement external
version pinning or application-level snapshot mechanisms.

### 7. Performance Model

Cursor continuation is currently applied in the post-access phase.

Each page request:

- Executes the canonical access path.
- Materializes candidate rows.
- Applies filtering and ordering.
- Applies cursor boundary.
- Applies window slicing.

Cursor boundary conditions are not currently pushed down into index seek/range
operations.

This ensures correctness and determinism, but may result in full candidate-set
evaluation for large result sets.

Future versions may introduce access-path pushdown optimizations without
changing pagination semantics.

### 8. Non-Goals

Pagination does not provide:

- Snapshot isolation.
- Multi-entity transactional guarantees.
- Backward or bidirectional cursors.
- Implicit ordering.
- Automatic page stabilization under concurrent writes.

### Summary

IcyDB pagination guarantees:

- Deterministic total ordering.
- Strict forward-only continuation.
- No duplication across pages (for fixed query shape and stable ordered keys).
- Cursor validation against canonical query shape.
- Live-state iteration semantics.

These guarantees are stable for the 0.8 contract.

## Missing-Row Semantics (Explicit)

Missing-row behavior is an explicit part of intent and must be preserved through
planning and execution. It cannot depend on access-path choice.
Missing-row refers to any referenced logical row that cannot be materialized
from storage during execution.
Consistency is currently defined solely in terms of missing-row policy.

Required policy:
- MissingOk: missing rows are ignored (no error) and do not affect results.
- Strict: missing rows are treated as corruption and surface an error.

If a query intent does not specify this policy, it is invalid.

## Planner Responsibilities

Given a valid intent and a schema/model:
- validate field existence, types, operators, and coercions
- normalize predicates without changing semantics
- select an access plan (which may be composite) that is valid for the schema
- produce an executor-ready plan that fully encodes access path, filters,
  ordering, pagination, projection, and missing-row policy

The planner may return Unsupported errors but must not return Internal errors
for user input.

## Validation Ownership (Locked)

Validation is intentionally multi-layered and each rule must have one semantic owner:
- Logical validation (`validate_logical_plan_model`) owns user-facing query semantics.
- Planner invariant validation (`validate_plan_invariants_model`) owns planner-internal consistency.
- Executor validation (`validate_executor_plan`) owns defensive execution-boundary safety checks.

Ownership constraints:
- Non-owning layers may re-check invariants defensively, but must not reinterpret semantics.
- Executor validation must not introduce new user-visible query semantics.
- Disagreement between layers indicates a bug, not a recoverable condition.

## Executor Assumptions (Defensive Re-Validation)

When the executor receives a plan, it may assume:
- the plan is schema-valid and executor-safe
- access paths are compatible with the entity schema
- predicates are normalized and safe to evaluate
- ordering and pagination are valid for the schema
- missing-row policy is explicit and stable

The executor does not perform planning, but it may defensively re-validate plan
and schema invariants before execution. Those checks must preserve logical
validation semantics and error-class boundaries.
Composite access plans are an internal planning detail; the executor resolves
them deterministically before applying filters, ordering, and pagination.

## Plan Lifecycle

Executable plans are single-use by default. Reuse must be an explicit, opt-in
choice in the facade API.

## Error Classification by Stage

Construction:
- Unsupported: invalid intent structure or invalid coercion specification
- Internal: only for bugs in the facade itself

Planning:
- Unsupported: unknown fields, invalid operators, invalid coercions,
  non-orderable fields, invalid primary key types, invalid index metadata
- Internal: planner bugs or violated planner invariants

Execution:
- Corruption: missing rows under Strict policy, index corruption,
  malformed persisted bytes
- Unsupported: only if the executor receives a plan that violates this contract
- Internal: executor logic bugs

## Stability Guarantees

- Equivalent intents produce equivalent plans under the same schema.
- Planning determinism is required for fingerprinting and diagnostics.
- Access-path choice must not change query correctness or missing-row behavior.
