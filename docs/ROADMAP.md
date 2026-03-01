# IcyDB Roadmap

This document defines the long-term direction and architectural identity of IcyDB.

This roadmap is directional and planning-oriented.
Behavioral guarantees are defined exclusively in:

- `docs/contracts/ATOMICITY.md`
- `docs/contracts/REF_INTEGRITY.md`
- `docs/contracts/RESOURCE_MODEL.md`
- `docs/contracts/TRANSACTION_SEMANTICS.md`

This document defines *what IcyDB is*, *what it will become*, and *what it will not become*.

---

# 1. System Identity

IcyDB is a **deterministic, single-entity analytical execution engine**.

It provides:

- Typed-entity-first APIs
- Rule-based deterministic planning
- Streaming-first execution
- Explicit transactional semantics
- Stable continuation and cursor guarantees
- Strict separation between planner and executor authority

IcyDB intentionally operates within a **bounded relational algebra model**.

---

# 2. Algebra Boundary (Intentional Scope)

IcyDB supports:

- `SELECT`
- `WHERE`
- `GROUP BY`
- `HAVING`
- `ORDER BY`
- `LIMIT` / continuation-based pagination
- Deterministic aggregates
- Typed field projection

All queries operate over a **single logical entity root**.

The system is equivalent to:

> Relational algebra (projection + selection)
> Extended with aggregation
> Without relational joins

This boundary is intentional and stabilizing.

---

# 3. Planner Philosophy

IcyDB uses a **capability-driven, rule-based planner**.

The planner:

- Proposes eligibility for index usage
- Proposes grouping and ordering strategies
- Does not perform probabilistic cost modeling
- Does not enumerate join trees
- Does not rely on cardinality estimation

The executor:

- Revalidates planner decisions
- May downgrade strategy
- Never upgrades planner assumptions
- Preserves deterministic behavior

IcyDB does **not** implement a cost-based optimizer.

---

# 4. Execution Model

The execution model is:

- Streaming-first when structurally possible
- Explicit when materialization is required
- Memory-bounded by contract
- Explicit about downgrade paths
- Explicit about DISTINCT and grouping budget enforcement

No hidden buffering.
No silent execution-mode shifts.

Continuation envelopes are stable and versioned.

---

# 5. Transaction Model

Current guarantees:

- Single-entity save/delete operations are atomic.
- Non-atomic batch helpers are fail-fast and non-atomic.
- Atomic batch helpers are atomic per single entity type per call.
- Multi-entity transaction semantics are not part of the current contract.

Future transactional expansion will:

- Be explicit
- Have formal semantics
- Include replay/recovery test coverage
- Never be implicit or inferred

---

# 6. Engine Completion Goals (1.0 Target)

The system is considered architecturally complete when:

- Algebra boundary is frozen (no joins, no windows).
- Planner/executor authority boundaries are strictly enforced.
- Continuation and cursor semantics are fully hardened.
- Numeric core is consolidated under a single decimal model.
- Aggregate execution paths are strategy-stable and downgrade-safe.
- Storage invariants are formally documented and test-backed.
- Structural identity is unified between normalization and fingerprinting.
- Observability surface is stable.

At that point, growth becomes incremental refinement, not architectural expansion.

---

# 7. Near-Term Focus

## Stability & Consolidation

- Preserve grouped invariants and HAVING semantics.
- Harden continuation envelope boundaries.
- Complete numeric consolidation under unified decimal.
- Remove legacy numeric split paths.
- Maintain strict resource-model compliance.
- Eliminate semantic duplication across layers.

## Execution Optimization (Within Scope)

- Aggregate-aware fast paths (provably equivalent only).
- Composite aggregate direct-path routing.
- Covering-index detection improvements.
- Strategy selection clarity (without cost-based planning).
- Deterministic downgrade pathways.

---

# 8. Medium-Term Expansion (Bounded)

Expansion remains within single-entity algebra.

Potential additions:

- Richer aggregate library (SUM, AVG, statistical aggregates).
- COUNT DISTINCT variants (bounded memory only).
- Expression support for projection and grouping.
- Extended predicate operators.
- Storage and cardinality metrics exposure.
- Operational CLI over stable engine surface.

All expansion must preserve:

- Determinism
- Explicit semantics
- Streaming preference
- Resource-model clarity

---

# 9. Long-Term Direction

## Multi-Entity Transactions

If implemented:

- Must have formal semantics specification.
- Must preserve deterministic replay guarantees.
- Must not introduce implicit cross-entity commit coupling.
- Must not relax current atomicity contracts.

## Operational Surface

A first-class CLI may provide:

```bash
icydb schema create
icydb collection create
icydb insert
icydb query --explain
icydb index inspect
icydb check
icydb rebuild
icydb export
icydb import
```

This surface remains an operational wrapper over a stable engine core.

---

# 10. Explicit Non-Goals

The following are intentionally out of scope:

* Relational joins
* Cost-based optimization
* Window functions
* Recursive queries
* Implicit transactional inference
* Hidden execution buffering
* Silent downgrade/upgrade semantics
* Heuristic plan instability

IcyDB does not aim to be a general-purpose relational database.

It aims to be:

> A deterministic, bounded, single-entity analytical engine with explicit semantics and strong correctness guarantees.

---

# 11. Evolution Philosophy

IcyDB evolves deliberately:

* No silent behavioral changes.
* No implicit semantic upgrades.
* No scope creep beyond declared algebra boundary.
* No probabilistic planner behavior.
* No relaxation of documented guarantees.

Readiness and safety drive versioning — not feature pressure.

---

# Summary

IcyDB has transitioned from exploratory architecture to a defined system class.

Its future growth is:

* Refinement over expansion
* Determinism over heuristics
* Explicit contracts over inference
* Stability over surface-area inflation

The algebra boundary is intentional.
The planner model is intentional.
The execution determinism is intentional.

That constraint is the foundation of long-term sustainability.
