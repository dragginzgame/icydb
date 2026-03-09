# Strict Error Taxonomy Audit - 2026-03-05

Scope: error-class/origin mapping integrity across planner, cursor, executor, index/store, and public query boundary.

## Domain Mapping Snapshot

| Domain | Primary Type Surface | Status |
| ---- | ---- | ---- |
| Corruption | `ErrorClass::Corruption` + corruption constructors in `crates/icydb-core/src/error.rs` | PASS |
| Unsupported | `ErrorClass::Unsupported` | PASS |
| Invalid Input | `ValidateError`, `PlanError`, cursor decode/shape failures | PASS |
| Invariant Violation | `ErrorClass::InvariantViolation` + invariant constructors | PASS |
| System/Internal Failure | `ErrorClass::Internal` | PASS |

## Enumeration Coverage

| Enum Family | Location | Notes |
| ---- | ---- | ---- |
| `QueryError`, `QueryExecuteError`, `IntentError` | `crates/icydb-core/src/db/query/intent/errors.rs` | Query boundary remains explicitly typed |
| `PlanError` + split plan sub-enums | `crates/icydb-core/src/db/query/plan/validate/mod.rs` | Planner taxonomy stays split by concern |
| `CursorPlanError`, `CursorDecodeError`, `TokenWireError` | `crates/icydb-core/src/db/cursor/error.rs`, `db/codec/cursor.rs`, `db/cursor/token/error.rs` | Cursor decode vs payload invariants separated |
| `ErrorClass`, `ErrorOrigin` | `crates/icydb-core/src/error.rs` | Runtime class and origin remain explicit |

## Upward Mapping Verification

| Source | Mapping | Classification Preserved? | Risk |
| ---- | ---- | ---- | ---- |
| `InternalError` -> `QueryExecuteError` | `From<InternalError> for QueryExecuteError` in `query/intent/errors.rs` | Yes (class-preserving match) | Low |
| Cursor plan failure -> runtime internal error | `InternalError::from_cursor_plan_error` in `error.rs` | Yes (cursor invariant semantics retained) | Medium |
| Access plan validation -> runtime internal error | `InternalError::from_executor_access_plan_error` in `error.rs` | Yes (query invariant mapping) | Low-Medium |

## Corruption and Input Containment

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Persisted decode failures map to corruption | `store_corruption`, `index_corruption`, `serialize_corruption` constructors | PASS | Low |
| Cursor malformed payload stays cursor-domain invalid input/invariant | cursor token decode + plan validation paths | PASS | Medium |
| No observed corruption downgrade to unsupported/input | mapping paths in `error.rs` and `query/intent/errors.rs` | PASS | Low |

## Findings

- No critical misclassification found.
- Highest drift-sensitive edge remains cursor plan/payload mapping across decode and runtime revalidation boundaries.

## Overall Taxonomy Risk Index

**4/10**
