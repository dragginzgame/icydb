# 0.95 HAVING Unification

## Thesis

`0.95` should eliminate the remaining clause-specific HAVING expression model and unify HAVING onto the shared planner-owned expression system, so that:

> **HAVING becomes a standard post-aggregate boolean `Expr`, evaluated through the same expression and execution model as the rest of the query system.**

This slice does **not** introduce new SQL capabilities. It removes structural duplication and finalizes the expression-phase model established across `0.85–0.94`.

---

## Problem statement

After `0.90–0.94`, the system has largely converged on:

* `SqlExpr` → parser-owned syntax
* `Expr` → planner-owned semantics
* shared lowering via `lower_sql_expr(...)`
* phase-gated evaluation (pre-aggregate vs post-aggregate)

However, HAVING still deviates:

* separate types:

  * `GroupHavingExpr`
  * `GroupHavingValueExpr`
  * `ResolvedHavingExpr`
* partial reuse of shared expression logic, but not full ownership
* compare-shell outer structure instead of general boolean `Expr`
* special aggregate-resolution paths

This creates:

* unnecessary duplication
* additional compile work
* a second semantic lane for boolean expressions
* risk of drift between WHERE / FILTER / HAVING semantics

HAVING is now the **last major clause-specific semantic structure** in the system.

---

## Design goal

Unify HAVING onto the shared expression model by:

* removing HAVING-specific expression types
* lowering HAVING into planner-owned `Expr`
* evaluating HAVING as a post-aggregate boolean expression
* reusing the existing scalar expression evaluator

The goal is:

> **one expression system, one lowering path, one evaluation model — with phase determining meaning, not clause-specific types.**

---

## Non-goals

This slice does not aim to:

* add new SQL features
* widen HAVING capabilities
* support new aggregate shapes
* introduce subqueries or windows
* change SQL semantics
* modify aggregate behavior
* optimize compile or runtime cost (beyond incidental simplification)

This is a structural unification slice.

---

## Target model

### Before

```text
HAVING
  → GroupHavingExpr
    → GroupHavingValueExpr
      → special compare shell
```

### After

```text
HAVING
  → SqlExpr
    → lower_sql_expr(..., PostAggregate)
      → Expr (boolean)
```

No HAVING-specific expression types remain.

---

## Core design

## 1. HAVING becomes a post-aggregate `Expr`

HAVING is lowered as:

```rust
lower_sql_expr(sql_expr, SqlExprPhase::PostAggregate) -> Expr
```

The resulting `Expr` must:

* evaluate to boolean
* respect post-aggregate phase rules

There is no HAVING-specific expression representation.

### Lowering guardrail

Lowering remains phase-directed, not semantics-repairing.

That means:

* alias references in HAVING must be rewritten to their underlying expressions
  before lowering
* lowering must not reinterpret invalid references to satisfy post-aggregate
  rules
* lowering must preserve invalid references as ordinary `Expr` structure so
  validation can reject them explicitly

Examples of forbidden lowering-time coercion:

* binding a non-grouped raw field onto some grouped slot
* rewriting an invalid field reference into an aggregate
* silently accepting a non-boolean HAVING shape by clause-specific conversion

---

## 2. Phase ownership

HAVING is strictly:

```text
PostAggregate phase
```

Meaning it may reference:

* grouped key fields
* aggregate outputs
* post-aggregate scalar expressions

It must not reference:

* raw row fields (unless part of group key)
* pre-aggregate-only expressions

Validation must enforce this.

### Mixed-expression rule

For mixed expressions such as:

```sql
HAVING SUM(x) > AVG(y) + z
```

all non-aggregate field references must resolve to declared group keys.
Aggregate-bearing expressions do not grant raw-field visibility to sibling
subexpressions.

---

## 3. Boolean semantics

HAVING uses the same boolean boundary as other row-admission clauses:

```text
TRUE  → keep group
FALSE → drop group
NULL  → drop group
```

### Explicit rule

* only `TRUE` admits the group
* no clause-specific boolean semantics may be introduced
* HAVING must reuse the same boolean boundary logic used for row filtering
* HAVING must use the same boolean-collapse function used by WHERE and FILTER

There must not be a HAVING-only boolean admission helper.

---

## 4. Aggregate reference model

HAVING must refer to aggregates via planner-owned `AggregateExpr`.

Examples:

```sql
HAVING SUM(strength) > 10
```

This must resolve to the same aggregate expression identity used in:

* projection
* ORDER BY
* FILTER (post-0.94)

### Important invariant

> HAVING must not introduce a separate aggregate lookup or indexing model.

Aggregate identity must be shared across the query.

### Canonicalization contract

All aggregate expressions must be canonicalized into one shared planner-owned
aggregate registry before HAVING binding.

That means:

* equivalent aggregate expressions map to one canonical `AggregateExpr`
* HAVING must reference the same canonical aggregate nodes used by projection
  and ORDER BY
* planner/executor wiring must not depend on clause-local aggregate copies

This prevents:

* duplicate aggregate computation
* identity mismatch between projection and HAVING
* cache / fingerprint drift from clause-local aggregate duplication

---

## 5. Removal of compare shell

Current HAVING implementations often wrap expressions in:

```text
Compare(...)
```

This must be removed.

HAVING expressions become:

```rust
Expr::BinaryOp(...)
Expr::Unary(...)
Expr::Case(...)
```

Exactly the same shapes as WHERE and projection.

### Boolean normalization rule

HAVING must obey the same boolean-normalization contract as WHERE.

That means:

* non-boolean HAVING expressions are rejected unless they are already admitted
  by the shared boolean expression rules
* HAVING must not gain clause-specific truthiness behavior
* no implicit compare shell or implicit clause-local coercion may survive this
  slice

---

## 6. Execution model

Grouped execution becomes:

```text
for each group:
  evaluate HAVING Expr
  if TRUE:
    emit group
  else:
    skip group
```

No special HAVING evaluator.

No clause-specific runtime logic.

### Evaluation context

HAVING evaluation operates over the finalized grouped row:

* grouped key values are materialized
* aggregate outputs are materialized
* raw input rows are no longer visible

This is the same row-shape contract used by grouped projection.

### Pipeline position

HAVING is a grouped row-admission boundary, not a projection feature.

Therefore:

* HAVING must run before grouped projection emission
* HAVING may reuse the shared post-aggregate expression evaluator and grouped
  row contract
* HAVING must not be embedded into projection-shaping logic as a separate
  projection concern

---

## 7. Parser and lowering

### Parser

No change required to SQL surface.

HAVING continues to parse into `SqlExpr`.

### Lowering

Replace:

* HAVING-specific lowering paths
* compare-shell construction
* value-expression separation

with:

```rust
lower_sql_expr(having_sql_expr, PostAggregate)
```

### Alias-resolution rule

HAVING must lower from alias-resolved SQL expressions.

For example:

```sql
SELECT SUM(x) AS s
FROM t
HAVING s > 10
```

must rewrite `s` to its underlying semantic expression before
`lower_sql_expr(..., PostAggregate)` runs.

---

## 8. Validation

HAVING validation must enforce:

* expression is boolean
* expression is valid in post-aggregate phase
* no illegal references to pre-aggregate-only values
* aggregate usage is valid
* all non-aggregate field references are declared group keys
* mixed expressions do not smuggle raw-field access through aggregate siblings

Validation must occur:

* before execution
* not inside runtime

---

## 9. Explain / fingerprint / cache identity

HAVING must:

* appear in explain as a standard expression
* be included in structural fingerprint
* contribute to cache identity

Examples that must remain distinct:

```sql
HAVING SUM(x) > 10
HAVING SUM(x) > 20
HAVING COUNT(*) > 10
HAVING SUM(x) > 10 AND COUNT(*) > 5
```

No special-case hashing.

HAVING must reuse the same shared expression hashing path already used for
projection and predicate structures. This slice must not introduce
`hash_having(...)` or any other clause-local hashing seam.

---

# Part II — Contracts enforced by this slice

## Contract 1 — Single expression system

All clauses must use:

```text
SqlExpr → Expr
```

No clause-specific expression trees.

---

## Contract 2 — Phase defines meaning

Meaning is determined by phase:

| Clause               | Phase         |
| -------------------- | ------------- |
| WHERE                | PreAggregate  |
| FILTER               | PreAggregate  |
| HAVING               | PostAggregate |
| Projection (grouped) | PostAggregate |

No clause-specific semantic reinterpretation.

Lowering may reject unsupported syntax families, but it must not rewrite invalid
references into phase-admitted meanings.

---

## Contract 3 — No parallel boolean systems

Boolean semantics must be:

* unified
* owned by `Expr`
* collapsed only at clause boundaries

No HAVING-specific boolean handling.

---

## Contract 4 — No clause-specific aggregate handling

Aggregates must:

* be planner-owned
* be shared across clauses
* not be reinterpreted per clause
* be canonicalized so equivalent aggregate expressions map to one shared
  identity

HAVING must bind to those same shared aggregate identities rather than creating
clause-local aggregate nodes.

---

## Contract 5 — Compilation is total

After validation:

* HAVING compilation must not fail
* no runtime rejection
* no late semantic interpretation

---

# Part III — Acceptance criteria

## Functional

* existing HAVING queries continue to work
* HAVING supports full expression shapes allowed in post-aggregate phase
* HAVING works with:

  * CASE
  * FILTER aggregates
  * alias-rewritten expressions (via SQL normalization)
* HAVING evaluation runs over finalized grouped rows only

---

## Architectural

* `GroupHavingExpr` removed
* `GroupHavingValueExpr` removed
* HAVING lowering uses `lower_sql_expr`
* no clause-specific HAVING evaluator exists
* aggregate identity is shared across clauses
* HAVING binds through the shared aggregate registry used by projection /
  ORDER BY / FILTER
* no clause-specific HAVING hashing path exists

---

## Validation

The following must be rejected:

```sql
HAVING strength > 10          -- raw field not grouped
HAVING SUM(x) + AVG(y)        -- if invalid type/shape
HAVING COUNT(*) FILTER (...)  -- if FILTER not admitted yet
HAVING SUM(x) > AVG(y) + z    -- z is not grouped / aggregated
```

(Adjust based on actual admitted surface.)

---

## Observability

* explain shows HAVING as standard expression
* fingerprint reflects HAVING structure
* cache identity distinguishes HAVING variations

---

# Part IV — Likely code seams

This slice will touch:

### Parser (minimal)

* existing HAVING parsing remains

### Lowering

* `sql/lowering/select/aggregate.rs`
* `sql/lowering/expr.rs`
* remove HAVING-specific lowering paths

### Planner model

* `db/query/plan/model.rs`
* remove HAVING-specific expression types

### Validation

* `db/query/plan/validate/grouped/*`
* replace HAVING-specific validation with phase-based validation

### Executor

* grouped aggregate runtime / grouped pipeline admission stage
* evaluate HAVING via shared post-aggregate expression evaluator
* keep HAVING admission before projection emission, not inside projection
  shaping

### Explain / fingerprint / cache

* remove HAVING-specific handling
* ensure generic expression handling covers HAVING

---

# Out of scope

* performance optimization
* compile-cost reduction
* SQL surface widening
* DISTINCT + FILTER
* window functions
* subqueries

---

# Follow-on

After `0.95`, the system should have:

* one expression system
* one lowering path
* one boolean model
* phase-based semantics

This enables:

* further simplification
* safer optimization
* clearer future feature work

---

## Thesis sentence

> **0.95 removes HAVING-specific expression structures and unifies HAVING onto the shared planner-owned expression system as a post-aggregate boolean `Expr`, finalizing the phase-based query model and eliminating the last major clause-specific semantic path in IcyDB.**
