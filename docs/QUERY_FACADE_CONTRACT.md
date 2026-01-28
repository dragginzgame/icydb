# Query Facade Contract (Intent-Level)

This document freezes the intent-level contract for the query facade. It is a
source-of-truth boundary: anything not stated here is not a facade guarantee.

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
- entity identity (E)
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

## What Intent Must NOT Encode

Intent must not encode or imply any of the following:
- access paths (key, index, scan, range)
- index names or index choices
- execution ordering or physical plan steps
- plan cache keys or executor hints
- read modes hidden in access paths

## Projection Semantics

If no projection is specified, the intent is interpreted as “all fields.”
Projection is an intent concern and must not be introduced by the planner.

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

## Executor Assumptions (No Re-Validation)

When the executor receives a plan, it may assume:
- the plan is schema-valid and executor-safe
- access paths are compatible with the entity schema
- predicates are normalized and safe to evaluate
- ordering and pagination are valid for the schema
- missing-row policy is explicit and stable

The executor must not re-run planning or schema validation.
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
