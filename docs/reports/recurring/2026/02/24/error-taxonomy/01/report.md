# Strict Error Taxonomy Audit - 2026-02-24

Scope: classification discipline and containment of error domains.

## Step 0 - Semantic Domain Definitions

| Domain | Intended Meaning | Primary Class/Surface |
| ---- | ---- | ---- |
| Corruption | persisted bytes/state malformed or hostile | `ErrorClass::Corruption` |
| Unsupported | caller-supplied shape/value not supported | `ErrorClass::Unsupported` |
| Invalid Input | query/schema validation input failure before execution | `ValidateError`, `PlanError` families |
| Invariant Violation | internal contract breach or impossible state | `ErrorClass::InvariantViolation` |
| System Failure | runtime/internal failures outside user input | `ErrorClass::Internal` |

## Step 1 - Full Error Enumeration

| Family | Location | Count/Shape |
| ---- | ---- | ---- |
| Runtime classes | `crates/icydb-core/src/error.rs:457` | 6 classes |
| Runtime origins | `crates/icydb-core/src/error.rs:487` | 7 origins |
| Plan validation errors | `crates/icydb-core/src/db/query/plan/validate/mod.rs:56` | 29 variants across split enums |
| Query surface error | `crates/icydb-core/src/db/query/intent/mod.rs:587` | wrapper enum |
| Cursor plan errors | `crates/icydb-core/src/db/query/plan/validate/mod.rs:165` | 9 variants |

## Step 2 - Per-Variant Semantic Classification

| Variant Group | Expected Domain | Actual Mapping | Correct? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Unknown projection field | Unsupported | `executor_unsupported` in aggregate terminal path | Yes | Low |
| Cursor compatibility mismatches | Invalid input at plan boundary -> invariant at executor boundary | mapped through `PlanError` / `InternalError::query_invariant` | Yes (boundary-owned) | Medium |
| Corrupt index/store bytes | Corruption | decode guards in index/commit/data layers | Yes | Low |
| Unique constraint conflicts | Conflict | `ErrorClass::Conflict` via index violation constructor | Yes | Low |

## Step 3 - Upward Mapping Verification

| Source Error | Upward Mapping | Classification Preserved? | Risk |
| ---- | ---- | ---- | ---- |
| `PlanError` in executor path | `InternalError::from_executor_plan_error` | mapped to invariant class intentionally | Medium |
| Projection field-slot errors | aggregate helper -> `executor_unsupported` | Yes | Low |
| Cursor plan errors | `from_cursor_plan_error` mapping | Yes (with specialized messages) | Medium |

## Step 4 - Corruption Containment Audit

| Corruption Boundary | Evidence | Risk |
| ---- | ---- | ---- |
| index key decode | `index/key/codec.rs` corruption guards | Low |
| commit marker decode | `commit/decode.rs` bounded decode checks | Low |
| data row decode | `db/codec/mod.rs` corruption mapping | Low |

## Step 5 - Invalid Input Containment Audit

| Invalid Input Surface | Evidence | Risk |
| ---- | ---- | ---- |
| predicate/schema validation | query predicate validation modules | Low |
| plan-shape validation | split plan error enums | Low |
| projection field target validation | aggregate field-slot resolver pre-scan | Low |

## Step 6 - Invariant Violation Audit

| Invariant Surface | Evidence | Risk |
| ---- | ---- | ---- |
| query/executor invariant constructors | `error.rs` invariant constructors | Medium |
| route/plan mismatch guards | executor/context/route invariant checks | Medium |
| cursor pk slot invariants | cursor continuation + tests | Medium |

## Step 7 - Origin Fidelity Audit

| Class | Expected Origin | Observed | Risk |
| ---- | ---- | ---- | ---- |
| Corruption | store/index/serialize | consistent | Low |
| Unsupported | executor/index/store/interface | consistent for projection terminals | Low |
| InvariantViolation | query/executor/store | consistent | Medium |

## Step 8 - Layer Violation Detection

| Violation Type | Found? | Notes | Risk |
| ---- | ---- | ---- | ---- |
| string-only untyped catch-alls replacing typed variants | No | typed enums remain in plan surface | Low |
| user input mapped directly to corruption | No | projection unknown field maps to unsupported | Low |
| corruption downgraded to unsupported | No | decode paths preserve corruption class | Low |

## Step 9 - Cross-Path Consistency

| Scenario | Load Path | Aggregate/Projection Path | Consistent? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| unknown field target | unsupported | unsupported | Yes | Low |
| cursor mismatch | plan error -> invariant | same mapping through shared converter | Yes | Medium |
| storage decode failure | corruption | corruption | Yes | Low |

## Step 10 - Mixed-Domain Enum Detection

- `PlanError` family intentionally mixes validation domains but remains split into sub-enums (`OrderPlanError`, `AccessPlanError`, `PolicyPlanError`, `CursorPlanError`).
- Mixed-domain pressure is manageable and explicit.

## Step 11 - Incorrect Classification List

- No concrete misclassification found in the `0.28.1` projection additions.

## Step 12 - Error Classification Matrix

| Domain | Example | Class | Origin | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Corruption | malformed raw index key bytes | Corruption | Index | Low |
| Unsupported | `values_by("missing_field")` | Unsupported | Executor | Low |
| Invalid input | unordered pagination plan | PlanError/ValidateError | Query | Low |
| Invariant violation | cursor boundary shape mismatch during execution revalidation | InvariantViolation | Query/Executor | Medium |
| System failure | internal commit/store orchestration failure | Internal | Store/Executor | Medium |

## Step 13 - Overall Taxonomy Risk Index

Taxonomy Risk Index (1-10, lower is better): **4/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
