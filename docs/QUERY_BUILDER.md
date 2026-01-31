
# Query Builder — Predicate + Coercion Specification

This document freezes the **canonical predicate semantics and coercion model** for the Query Builder.
It is the single source of truth for query evaluation. All indexes, planners, and executors **must match these rules exactly**.

Any deviation from this specification is a **semantic bug**, not an optimization.

**Scope:** predicate AST, coercion data model, evaluator rules, normalization, and property tests.
This step intentionally excludes indexes, planning, and access-path selection.

---

## Goals

* One canonical evaluator used by:

  * full scans
  * index verification
  * post-fetch predicate evaluation
* Coercions are **declared as data**, never implicit or ad-hoc.
* Normalization is deterministic and strictly semantics-preserving.
* Property tests prove:

  * `eval(p, row) == eval(normalize(p), row)`
  * scan results are invariant under normalization.

---

## Non-Goals

* Index encoding, access paths, or planner behavior.
* Optimization rules or cost-based decisions.
* API ergonomics or builder design.

---

## API Surface Defaults (Locked)

The coercion model is shared, but **different API surfaces set different defaults**.
This is intentional and must not be “unified” or “fixed.”

* `FieldRef` (builder API): ordering operators (`Lt`, `Lte`, `Gt`, `Gte`) use `NumericWiden`.
* `FilterExpr` (facade API): ordering operators (`Lt`, `Lte`, `Gt`, `Gte`) use `Strict`.

Equivalent logical predicates **may differ** depending on which API is used.
This is a contract difference, not a bug.

---

## Example: Ordering Coercion Divergence

Same logical intent, different API surface, different result:

```rust
// Builder API (FieldRef): ordering uses NumericWiden.
// Field value: Int(10), predicate: age > Uint(5)  → true (numeric widen).
let pred = field("age").gt(5u64);
```

```rust
// Facade API (FilterExpr): ordering uses Strict.
// Field value: Int(10), predicate: age > Uint(5)  → false (type mismatch).
let pred = FilterExpr::Gt {
    field: "age".to_string(),
    value: Value::Uint(5),
};
```

Both predicates are valid per their respective API contracts, but they are **not**
interchangeable. Use the API that matches the coercion guarantees you want.

---

## Core Data Model

### Field Presence

Each field in a row is in exactly one of the following states:

* `Present(value)` — a value exists for the field (including `Null`).
* `Missing` — the field is not present in the row at all.

`Missing` is **distinct from `Null`**.
This distinction is observable and intentional.

---

### Value Domain

Predicate semantics operate over the existing IcyDB value families:

* numeric
* text
* identifier
* boolean
* enum
* list / set
* map
* other scalar families already defined in the engine

All comparisons, coercions, and orderings are defined **in terms of these families**.

---

## Predicate AST (Logical Form)

A `Predicate` is defined as:

* `True`
* `False`
* `And(Vec<Predicate>)`
* `Or(Vec<Predicate>)`
* `Not(Predicate)`
* `Compare { field, op, value, coercion }`
* `IsNull { field }`
* `IsMissing { field }`
* `IsEmpty { field }`
* `IsNotEmpty { field }`
* `MapContainsKey { field, key, coercion }`
* `MapContainsValue { field, value, coercion }`
* `MapContainsEntry { field, key, value, coercion }`

### Comparison Operators

For `Compare`:

* `Eq`, `Ne`
* `Lt`, `Lte`, `Gt`, `Gte`
* `In`, `NotIn`
* `Contains`, `StartsWith`, `EndsWith`

#### Notes

* Logical operators use **strict two-valued boolean logic** with short-circuiting.
* Predicates always evaluate to `true` or `false`; there is no `Unknown` state.
* `Compare` predicates are **field-to-literal only**.
  Field-to-field comparisons are explicitly out of scope for the query builder.

---

## Coercion Model (Data, Not Behavior)

Every `Compare` or map predicate carries an explicit `CoercionSpec`.

Coercion is:

* declarative
* validated ahead of execution
* evaluated by a shared coercion engine

There is **no implicit coercion**.

### CoercionId (Baseline Set)

* `Strict` — no conversion; types must match.
* `NumericWiden` — numeric values are widened to a common numeric form.
* `IdentifierText` — identifiers may be compared to text via parsing.
* `TextCasefold` — text comparisons use canonical casefolding.
* `CollectionElement` — element-level coercion for list/set membership.

### CoercionSpec Example

```text
CoercionSpec {
  id: TextCasefold,
  params: { locale: "root" }
}
```

### Coercion Table (Conceptual)

The evaluator uses a static, declarative conversion table:

* `(Identifier, Text, IdentifierText)` → parse text into identifier
* `(Text, Text, TextCasefold)` → casefold both operands
* `(Numeric, Numeric, NumericWiden)` → promote to common numeric type
* `(Any, Any, Strict)` → no conversion

Unsupported conversions **never occur at runtime**.
They are rejected during validation.

---

## Canonical Evaluation Semantics

Given a row `R` and predicate `P`:

1. `True` / `False` evaluate to the corresponding constant.
2. `And`, `Or`, `Not` apply standard boolean logic with short-circuiting.
3. `IsMissing(field)` → `true` iff the field is `Missing`.
4. `IsNull(field)` → `true` iff the field is `Present(Null)`.
5. `IsEmpty` / `IsNotEmpty`:

   * valid only for text or collection fields
   * otherwise rejected by validation.
6. `Compare`:

   * if the field is `Missing`, **return false**.
   * if the field is `Present(value)`:

     * apply coercion per `CoercionSpec`
     * apply the operator to coerced values.
   * if coercion fails at runtime, return false; this condition must be
     unreachable after successful validation and is treated as a validation bug.
7. `MapContains*`:

   * if the field is `Missing`, return false.
   * field must be a map; otherwise validation rejects the predicate.
   * key/value coercion is applied exactly as declared.

### Missing Semantics (Non-Negotiable)

**Compare predicates never match `Missing` fields.**
`Missing` can only be observed via `IsMissing`.

---

## Operator Semantics (High-Level)

* `Eq` / `Ne` — equality under declared coercion.
* `Lt` / `Lte` / `Gt` / `Gte` — total ordering over coerced values in the same domain.
* `In` / `NotIn` — membership in a literal list; coercion applies per element.
* `Contains` / `StartsWith` / `EndsWith`:

  * for text: substring / prefix / suffix under declared coercion
  * for collections: element containment under declared coercion

Operators are **never overloaded with incompatible semantics** across domains.

---

## Canonical Ordering Rules

Each value family defines a **total, deterministic ordering** used consistently by:

* predicate evaluation
* range semantics
* ordered indexes

Ordering must be:

* stable across runs
* independent of access path
* fully determined by the coerced value representation

Byte-level encoding is out of scope for this step, but the logical ordering contract
is frozen here.

---

## Normalization (Semantics-Preserving)

Normalization produces a deterministic, canonical predicate **without changing meaning**.

Permitted rewrites:

* Flatten nested `And` / `Or`.
* Remove neutral constants:

  * `And(..., True)` → remove `True`
  * `Or(..., False)` → remove `False`
* Short-circuit constants:

  * `And(..., False)` → `False`
  * `Or(..., True)` → `True`
* Eliminate double negation.
* Sort children of `And` / `Or` by a stable structural key.

Normalization must **not**:

* Rewrite or weaken comparisons.
* Change coercion specs.
* Distribute predicates (`AND`/`OR`) in ways that alter observable behavior.
* Introduce or remove evaluation of sub-predicates.

---

## Property Tests (Required)

Property tests are mandatory and must pass before proceeding to Step 2.

### Test Set

1. **Normalization equivalence**

   ```
   eval(p, row) == eval(normalize(p), row)
   ```

   * random predicate generator
   * random row generator across all value families

2. **Scan invariance**

   ```
   scan(query) == scan(query with normalized predicate)
   ```

   * compare result sets (set equality, order ignored)

3. **Coercion invariants**

   * each coercion is:

     * deterministic
     * stable across runs
     * symmetric or intentionally asymmetric (documented)

Indexes and planners are **not** involved in these tests.

---

## Validation Rules (Pre-Execution)

Validation is mandatory and occurs before evaluation:

* Field exists in schema and has a known type.
* Operator is valid for the field type.
* `CoercionSpec` is allowed for the field type and operator.
* List/map predicates use correctly typed literals.
* Ordering operators are only used on orderable domains.

Validation failures produce **Unsupported** errors.
Evaluation must never panic.

---

## Semantic Contract (Non-Negotiable)

These constraints are binding for all future work in the query engine:

* Indexes must be semantics-preserving or superset-only.
* Access paths must not change results.
* Missing vs Null rules are non-negotiable.
* Validation failures are `Unsupported(Query)`.
* Executors never panic on user input.

---

## Implementation Notes

* The evaluator is a pure function of `(row, predicate)`.
* The coercion table is declarative data, not embedded logic.
* All later subsystems (indexes, planners, executors) must either:

  * call into this evaluator directly, or
  * share its core logic without semantic divergence.

This document defines the **semantic contract** for the Query Builder.
