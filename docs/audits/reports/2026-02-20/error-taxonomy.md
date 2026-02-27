# Error Taxonomy Audit - 2026-02-20

Scope: classification correctness and mapping fidelity only.

## Step 0 - Semantic Domains Used

- Corruption
- Unsupported
- Invalid Input
- Invariant Violation
- System Failure

## Step 1 - Full Error Enumeration (Core Runtime Surface)

| Enum | Variant | Declared Meaning | Layer |
| ---- | ---- | ---- | ---- |
| `ErrorClass` | `Corruption` | persisted/state integrity failure | runtime core |
| `ErrorClass` | `Unsupported` | policy fence / intentional rejection | runtime core |
| `ErrorClass` | `InvariantViolation` | internal assumption break | runtime core |
| `ErrorClass` | `Internal` | internal/system fault path | runtime core |
| `ErrorClass` | `NotFound` | missing expected state | runtime core |
| `ErrorClass` | `Conflict` | expectation conflict (e.g. unique violation) | runtime core |
| `StoreError` | `NotFound`, `Corrupt`, `InvariantViolation` | store-scoped detail payload | runtime core |
| `CursorDecodeError` | `Empty`, `OddLength`, `InvalidHex` | malformed user token | cursor boundary |
| `IdentityDecodeError` | `InvalidSize`, `InvalidLength`, `NonAscii`, `NonZeroPadding` | malformed identity bytes | identity boundary |
| `PlanError` | `PredicateInvalid`, `Order`, `Access`, `Policy`, `Cursor` | query plan validation failure | planner boundary |
| `CursorPlanError` | 9 variants including `ContinuationCursorWindowMismatch` | continuation payload/shape failures | planner boundary |
| `QueryError` | `Validate`, `Plan`, `Intent`, `Response`, `Execute` | public query boundary wrappers | intent/session boundary |

Evidence: `crates/icydb-core/src/error.rs:327`, `crates/icydb-core/src/error.rs:309`, `crates/icydb-core/src/db/cursor.rs:13`, `crates/icydb-core/src/db/identity.rs:30`, `crates/icydb-core/src/db/query/plan/validate/mod.rs:56`, `crates/icydb-core/src/db/query/plan/validate/mod.rs:165`, `crates/icydb-core/src/db/query/intent/mod.rs:581`.

## Step 2 - Per-Variant Semantic Classification

| Variant | Semantic Domain | Justification |
| ---- | ---- | ---- |
| `CursorDecodeError::*` | Invalid Input | untrusted caller token parse failure |
| `CursorPlanError::ContinuationCursorSignatureMismatch` | Invalid Input | caller provided cursor for different query shape |
| `CursorPlanError::ContinuationCursorWindowMismatch` | Invalid Input | caller token does not match requested pagination window |
| `InternalError::store_corruption(...)` | Corruption | persisted store/data bytes violated invariants |
| `InternalError::index_corruption(...)` | Corruption | persisted index bytes/layout mismatch |
| `InternalError::query_invariant(...)` | Invariant Violation | planner/executor boundary contract failure |
| `InternalError::store_unsupported(...)` | Unsupported | intentionally unsupported policy/input |
| `InternalError::executor_internal(...)` | System Failure / internal fault bucket | runtime failure not caller-shape issue |
| `ErrorClass::Conflict` | Invalid Input (taxonomy framing) | caller intent conflicts with current state (e.g. unique) |

## Step 3 - Upward Mapping Verification

| Source Variant | Mapped To | Domain Preserved? | Escalation? | Downgrade? | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `CursorDecodeError` | `PlanError::Cursor(InvalidContinuationCursor)` | Yes | No | No | Low |
| `CursorPlanError` | `PlanError::Cursor` -> `QueryError::Plan` | Yes | No | No | Low |
| `PlanError` at executor boundary | `InternalError::query_invariant` (`from_executor_plan_error`) | Yes (Invariant Violation) | No | No | Medium |
| `PlanError::Cursor` during revalidation | `InternalError::query_invariant` (`from_cursor_plan_error`) | Yes | No | No | Medium |
| `Store decode failures` | `InternalError::store_corruption` -> `QueryError::Execute` | Yes | No | No | Low |
| `MergePatchError` | `InternalError { class: Unsupported, origin: Interface }` | Yes | No | No | Low |

Evidence: `crates/icydb-core/src/error.rs:237`, `crates/icydb-core/src/error.rs:261`, `crates/icydb-core/src/db/query/intent/mod.rs:598`, `crates/icydb-core/src/db/query/intent/mod.rs:607`.

## Step 4 - Corruption Containment Audit

| Corruption Variant | Public Classification | Origin | Correct? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `store_corruption` from data/commit decode | Execute/InternalError with class `Corruption` | Store | Yes | Low |
| `index_corruption` from index decode/lookup | Execute/InternalError with class `Corruption` | Index | Yes | Low |
| `serialize_corruption` from row decode | Execute/InternalError with class `Corruption` | Serialize | Yes | Low |

## Step 5 - Invalid Input Containment Audit

| Invalid Input Variant | Final Classification | Correct? | Risk |
| ---- | ---- | ---- | ---- |
| `CursorDecodeError::*` | `QueryError::Plan` via `PlanError::Cursor` | Yes | Low |
| malformed continuation payload | `QueryError::Plan` | Yes | Low |
| plan-shape policy rejects (`IntentError`) | `QueryError::Intent` | Yes | Low |
| offset mismatch continuation (`ContinuationCursorWindowMismatch`) | `QueryError::Plan` | Yes | Low |

## Step 6 - Invariant Violation Audit

| Invariant Variant | Propagation Path | Classification Preserved? | Risk |
| ---- | ---- | ---- | ---- |
| executor cursor revalidation invariant failures | `PlanError` -> `InternalError::query_invariant` | Yes | Medium |
| executor plan-shape invariant failures | `PlanError` -> `InternalError::query_invariant` | Yes | Medium |
| store/index invariant checks | direct `InternalError::store_invariant`/`index_invariant` | Yes | Low |

## Step 7 - Origin Fidelity Audit

| Variant | True Origin | Reported Origin | Match? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| store decode failure | Store | Store | Yes | Low |
| index decode failure | Index | Index | Yes | Low |
| cursor parse failure | Query/cursor boundary | `PlanError::Cursor` (later `QueryError::Plan`) | Yes | Low |
| plan mismatch at executor boundary | Query | Query | Yes | Low |
| patch merge failure | Interface | Interface | Yes | Low |

## Step 8 - Layer Violation Detection

| Violation | Location | Classification Impact | Risk |
| ---- | ---- | ---- | ---- |
| None critical observed in current mapping chain | n/a | n/a | Low |
| Mild concentration: cursor plan errors collapse to query invariant at executor revalidation | `crates/icydb-core/src/error.rs:237` | classification preserved but message-shape drift possible | Medium |

## Overall Taxonomy Risk Index

Overall Error Taxonomy Risk Index (1-10, lower is better): **4/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
