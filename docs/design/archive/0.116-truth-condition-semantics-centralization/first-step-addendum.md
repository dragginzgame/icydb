# 0.116 First-Step Addendum

## Purpose

This addendum picks the safest first contraction target inside
`0.116 Truth-Condition Semantics Centralization`.

It does not introduce a patch-number plan.

It defines one bounded first step that can land independently while preserving
the full `0.116` thesis:

* planner owns truth-condition meaning
* predicate and lowering adapt into that authority
* prepared/session and executor consume the resulting structure

---

## Chosen First Target

The safest first contraction target is:

* truth-wrapper normalization

That means planner-owned logic becomes the only authority for admitted wrapper
families such as:

* `expr = TRUE`
* `TRUE = expr`
* `expr IS TRUE`
* `expr IS FALSE`
* equivalent admitted wrappers that currently mean “treat this expression as a
  truth condition”

This is the best first step because it is:

* narrow
* highly reused across `WHERE` and `HAVING`
* easy to prove by equivalence tests
* lower-risk than moving all compare/null-test truth shaping at once
* a clean way to shrink `db/predicate/bool_expr.rs` without reopening the full
  predicate adaptation story

---

## Why This Target First

The alternative first cuts are larger:

* compare/null-test truth shaping touches more family-specific meaning and is
  more likely to reopen grouped family details too early
* full predicate boolean canonicalization handoff is the right direction, but
  it is wider and has more blast radius than necessary for the first step

Truth-wrapper normalization is the smallest slice that still proves the real
`0.116` direction:

* one planner-owned truth-condition canonicalization path
* fewer owner-local wrapper match ladders
* shared `WHERE` / `HAVING` equivalence where the admitted family already
  overlaps

---

## Scope

### In Scope

* planner-owned normalization of admitted truth wrappers
* predicate-layer removal of independent truth-wrapper meaning
* lowering-layer removal of any local wrapper semantics beyond structural
  construction
* parity tests proving shared planner-owned wrapper canonicalization across:
  * `WHERE`
  * `HAVING`
  * prepared vs non-prepared execution
  * explain-visible canonical shape where applicable

### Out Of Scope

* compare/null-test family contraction beyond wrapper ownership
* searched `CASE` truth-family contraction beyond wrapper interaction
* broad grouped-semantic changes
* prepared template widening
* executor changes beyond consuming already-canonical structure

---

## Intended Ownership Shift

### Planner Owns

* wrapper-family equivalence
* canonical “truth-condition expression” form after wrapper collapse
* shared admitted-family behavior for scalar and grouped truth contexts where
  the semantics are already intentionally aligned

### Predicate Owns

* adapting predicate-shaped APIs into planner-owned truth expressions
* rebuilding predicate-facing structures from planner-owned canonical output

Predicate must stop owning:

* independent `= TRUE` / `IS TRUE` / `IS FALSE` meaning
* local wrapper collapse rules

### Lowering Owns

* parsed expression structure
* raw syntax lowering into planner inputs

Lowering must stop owning:

* wrapper-family truth meaning beyond structural parse/lower shape

---

## Concrete Work

### 1. Inventory Current Wrapper Logic

Map every admitted truth-wrapper rule across:

* [/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/expr](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/expr:1)
* [/home/adam/projects/icydb/crates/icydb-core/src/db/predicate](/home/adam/projects/icydb/crates/icydb-core/src/db/predicate:1)
* truth-condition-related lowering code under
  [/home/adam/projects/icydb/crates/icydb-core/src/db/sql/lowering](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/lowering:1)

Classify each occurrence as:

* planner-owned truth
* structural adaptation
* duplicate local meaning

### 2. Make Planner Canonicalization The Only Wrapper Authority

Add or consolidate one planner-owned helper path that:

* recognizes admitted truth wrappers
* collapses them to one canonical truth-condition form
* preserves the current bounded semantics and fail-closed behavior

The owner should live with planner expression canonicalization, not predicate
or lowering-local helpers.

### 3. Remove Predicate-Local Wrapper Semantics

Refactor
[/home/adam/projects/icydb/crates/icydb-core/src/db/predicate/bool_expr.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/predicate/bool_expr.rs:1)
so it consumes planner-owned wrapper-canonical output instead of deciding local
wrapper meaning itself.

This should reduce:

* wrapper-specific match ladders
* duplicate “truthy enough” classification logic
* local scalar/grouped wrapper special handling where planner can own the
  semantic decision

### 4. Keep Lowering Structural

Any lowering logic that currently recognizes wrappers should remain limited to:

* building raw expression structure
* preserving enough shape for planner canonicalization to decide meaning

Do not let lowering keep a parallel wrapper equivalence model.

### 5. Freeze With Tests

Required proof targets:

1. `expr = TRUE` and `TRUE = expr` canonicalize to the same planner-owned
   truth-condition form
2. `expr IS TRUE` canonicalizes through the same planner-owned authority as the
   admitted equality wrapper family where semantics overlap
3. `WHERE` and `HAVING` use the same planner-owned wrapper semantics for the
   admitted shared family
4. prepared and non-prepared execution preserve the same canonical truth
   wrapper plan shape
5. predicate-layer helpers no longer need owner-local wrapper meaning to
   preserve current behavior

---

## Success Criteria

This first step is successful when:

* planner is the only owner of admitted truth-wrapper semantics
* predicate and lowering no longer duplicate wrapper collapse rules
* wrapper-related truth-condition follow-through touches fewer files than the
  pre-contraction shape would have
* the next `0.116` work can move on to compare/null-test truth shaping from a
  narrower base

---

## Follow-On After This Step

If this lands cleanly, the next best follow-on inside `0.116` is:

* compare/null-test truth shaping centralization

That should come only after wrapper ownership is visibly planner-owned, so the
second step builds on a real semantic-owner contraction instead of mixing
multiple truth families in the first cut.
