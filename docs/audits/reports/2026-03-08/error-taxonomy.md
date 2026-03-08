# Strict Error Taxonomy Audit - 2026-03-08

Scope: class/origin mapping integrity across planner, cursor, executor, and query boundary.

## Domain Mapping Snapshot

| Domain | Primary Surface | Status |
| ---- | ---- | ---- |
| Corruption | `ErrorClass::Corruption` constructors in `crates/icydb-core/src/error.rs` | PASS |
| Unsupported | `ErrorClass::Unsupported` | PASS |
| Invalid Input | `PlanError`, `ValidateError`, cursor token/shape failures | PASS |
| Invariant Violation | `ErrorClass::InvariantViolation` + invariant constructors | PASS |
| System/Internal Failure | `ErrorClass::Internal` | PASS |

## Enumeration and Mapping Checks

| Check | Evidence | Result | Risk |
| ---- | ---- | ---- | ---- |
| Query boundary remains typed | `crates/icydb-core/src/db/query/intent/errors.rs` (`QueryError`, `ExecutionError`) | PASS | Low |
| Runtime class-to-surface mapping preserved | `impl From<InternalError> for ExecutionError` | PASS | Low |
| Planner policy/user/cursor separation retained | `crates/icydb-core/src/db/query/plan/validate/mod.rs` (`PlanError` split) | PASS | Low-Medium |
| No corruption downgrade observed in upward mappings | `error.rs` + `query/intent/errors.rs` mappings | PASS | Low |

## Findings

- No critical class/origin mismatch found.
- Highest drift-sensitive seam remains cursor decode -> planner/runtime boundary mapping, but current mapping stays explicit.

## Overall Taxonomy Risk Index

**4/10**
