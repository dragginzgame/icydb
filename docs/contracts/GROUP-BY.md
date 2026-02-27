# IcyDB GROUP BY Contract

Stability: Frozen in `0.33`  
Applies to: Planner, Executor, Session API, Response Surface  
Scope: Behavioral guarantees for grouped query execution

---

## 1. Plan Shape

### 1.1 Explicit Variant

Grouped queries MUST produce `LogicalPlan::Grouped(GroupPlan)`.

- Scalar and grouped plans are mutually exclusive.
- `LogicalPlan::Scalar` MUST NOT contain grouped semantics implicitly or via optional fields.

### 1.2 No Implicit Coercion

- Scalar APIs MUST NOT automatically execute grouped logic.
- Scalar wrappers MUST reject grouped intent with typed errors.
- Grouped execution is reachable only through explicit grouped APIs.

## 2. Group Identity Semantics

### 2.1 Canonical Equality

Group identity is determined by canonical `Value` equality.

Two rows belong to the same group if and only if:

`canonical_eq(group_key_a, group_key_b) == true`

No structural or reference-based identity is used.

### 2.2 Deterministic Hashing

Hashing of group keys MUST be deterministic.

Grouping behavior MUST remain invariant under:

- insertion-order variation
- hash-collision variation
- access-path variation

### 2.3 NULL Semantics

If `NULL` values are present:

- all `NULL` values in the same grouping field are considered equal
- `NULL` groups form a single group

If this rule changes in the future, this document MUST be updated.

### 2.4 Composite Keys

For multi-field grouping, group identity is defined by ordered tuple equality.

`(a, b)` and `(b, a)` are distinct unless explicitly equivalent under canonical equality.

## 3. Ordering Semantics

### 3.1 GROUP BY Without ORDER BY

`GROUP BY` alone does not imply lexical or sorted ordering.

Result order is:

- deterministic
- stable across insertion-order permutations
- stable across hash-collision permutations
- stable across equivalent access plans

However, order is otherwise unspecified.

Clients MUST NOT rely on lexical ordering unless `ORDER BY` is explicitly present.

### 3.2 GROUP BY With ORDER BY

If `ORDER BY` is supplied:

- ordering semantics follow explicit order-by evaluation
- deterministic ordering MUST still hold under grouping

## 4. Execution Shape

### 4.1 Blocking Semantics

Grouped execution is blocking.

- all relevant rows are processed before grouped results are emitted
- streaming/grouped hybrid execution is not supported

### 4.2 No Partial Emission

If grouped execution fails due to:

- memory limits
- group count limits
- execution budget constraints

No partial grouped output is returned.

Failure is atomic at the grouped-result level.

## 5. Memory Limits

### 5.1 Group Count Limit

If `max_groups` is configured:

- number of unique groups MUST NOT exceed this value
- exceeding this value produces a typed grouped-domain error

### 5.2 Group Memory Limit

If `max_group_bytes` is configured:

- estimated grouped memory usage MUST NOT exceed this value
- exceeding this value produces a typed grouped-domain error

Memory accounting includes:

- group key storage
- aggregate state storage
- internal grouped structure overhead (coarse-grained estimation allowed)

### 5.3 Deterministic Failure

Limit enforcement MUST be deterministic for equivalent inputs.

## 6. Cursor Semantics (Grouped Continuation)

When grouped continuation is enabled:

### 6.1 Group Boundary Resume

Continuation tokens resume at group boundaries, not row boundaries.

### 6.2 Token Requirements

Grouped continuation tokens MUST include:

- signature
- direction
- group anchor (canonical representation)
- offset or resume discriminator

Offset-only continuation is insufficient without a group anchor.

### 6.3 Deterministic Resume

Resuming grouped execution MUST:

- preserve deterministic ordering
- avoid duplicating groups
- avoid skipping groups

## 7. Response Surface

Grouped results MUST be represented by a distinct response variant:

`QueryResponse::GroupedRows(...)`

- scalar response variants MUST NOT encode grouped results
- clients MUST branch explicitly on response type
- no implicit shape-switching is permitted

## 8. Error Taxonomy

- grouped-domain errors MUST be distinct from scalar-domain errors
- scalar APIs encountering grouped intent MUST return explicit grouped-intent rejection errors
- grouped budget failures MUST return grouped-domain limit errors
- grouped errors MUST NOT be surfaced as generic scalar execution errors

## 9. Determinism Requirements

Grouped results MUST be invariant under:

- row insertion-order permutations
- hash-collision permutations
- equivalent access-path rewrites
- equivalent planner route choices

Violation of this invariant is a correctness defect.

## 10. Compatibility Guarantees

Grouped behavior introduced in `0.33` is additive.

Existing scalar APIs and scalar execution semantics remain unchanged.

No existing scalar method will:

- change return type
- implicitly execute grouped logic
- return grouped results without explicit grouped invocation

---

## Summary

`GROUP BY` in IcyDB guarantees:

- canonical group identity
- deterministic finalization
- explicit plan-shape separation
- blocking execution semantics
- atomic budget enforcement
- explicit response surface
- deterministic continuation behavior

This contract is frozen as of `0.33`.

Future changes to grouped semantics MUST update this document.
