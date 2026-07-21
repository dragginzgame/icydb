# IcyDB Roadmap

This document defines the long-term direction and architectural identity of IcyDB.

This roadmap is directional and planning-oriented.
Behavioral guarantees are defined by the current files under
[`contracts/`](contracts/), especially:

- [query semantics](contracts/QUERY_CONTRACT.md),
  [read admission](contracts/READ_ADMISSION.md), and
  [SQL scope](contracts/SQL_SUBSET.md);
- [write admission](contracts/WRITE_ADMISSION.md),
  [atomicity](contracts/ATOMICITY.md), and
  [transaction semantics](contracts/TRANSACTION_SEMANTICS.md);
- [durability](contracts/DURABILITY.md),
  [persisted-format policy](contracts/PERSISTED_FORMAT_POLICY.md), and the
  [persisted-format inventory](contracts/PERSISTED_FORMAT_INVENTORY.md); and
- [cursor](contracts/CURSOR.md),
  [referential-integrity](contracts/REF_INTEGRITY.md), and
  [resource-model](contracts/RESOURCE_MODEL.md) contracts.

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

Continuation envelopes are stable for the active internal format,
signature-checked, and fail-closed. Before `1.0`, IcyDB keeps one active
internal cursor encoding rather than maintaining parallel legacy decoders.

---

# 5. Transaction Model

Current guarantees:

- Single-entity save/delete operations are atomic.
- Non-atomic batch helpers are fail-fast and non-atomic.
- Atomic batch helpers are atomic per single entity type per call.
- Multi-entity transaction semantics are not part of the current contract.
- Returning `Err` from a canister update method does not roll back prior
  successful writes in that method.

Future transactional expansion will:

- Be explicit
- Have formal semantics
- Include replay/recovery test coverage
- Never be implicit or inferred
- Not infer Postgres-style transaction blocks or isolation levels

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

The active delivery sequence is intentionally serial:

1. [0.205 grouped early
   materialization](design/0.205-grouped-early-materialization/0.205-design.md)
   is complete and closes the eligible ordered-group retention boundary.
2. [0.206 SQL performance
   remediation](design/0.206-sql-performance-remediation/0.206-design.md)
   is complete: bounded expression-order remediation meets its retained-row
   target, the exact hard-cut cohort passed review, and the ordinary gate
   reproduced the selected post-remediation baseline.
3. [0.207 redo-only schema index
   publication](design/0.207-redo-only-schema-index-publication/0.207-design.md)
   hard-cuts index publication to the maintained marker/recovery authority.
4. [0.208 exact composite
   contracts](design/0.208-exact-composite-contracts/0.208-design.md)
   completes exact recursive field contracts.
5. [0.209 temporal defaults and versioned row
   layouts](design/0.209-temporal-defaults-and-versioned-row-layouts/0.209-design.md)
   owns the next current-form row-envelope cut. It replaces the older vague
   offset-row roadmap item; no parallel row codec is planned.
6. [0.210 exact and resumable bulk
   update](design/0.210-exact-and-resumable-bulk-update/0.210-design.md),
   [0.211 accepted-catalog
   constraints](design/0.211-accepted-catalog-constraints/0.211-design.md),
   [0.212 bounded resumable integrity
   checking](design/0.212-bounded-resumable-integrity-check/0.212-design.md), and
   [0.213 exact unsigned identity
   generation](design/0.213-exact-unsigned-identity-generation/0.213-design.md)
   then consume those accepted contracts in order.
7. [0.214 SQL structural coverage and range
   remediation](design/0.214-sql-structural-coverage-and-range-remediation/0.214-design.md)
   then expands typed SQL interaction evidence, establishes a fresh
   post-0.213 performance profile, and hard-cuts one proven duplicate
   compound-range traversal without reopening 0.204 or 0.206.

All proposed lines remain subject to review at their implementation boundary.
Before 1.0, format and protocol replacements are hard cuts: one current form,
typed failure for obsolete state, and no compatibility path.

---

# 8. Medium-Term Expansion (Bounded)

Expansion remains within single-entity algebra.

Potential additions:

- Statistical aggregate expansion beyond the current baseline aggregate set.
- Additional distinct aggregate variants and statistical reducers, all bounded
  by explicit memory contracts.
- Further expression widening for grouped paths and boolean/computed forms.
- Extended predicate operators.
- Prepared-query widening beyond fixed-route compare-family parameterization,
  starting with explicit value-sensitive route-template specialization for
  prefix-style predicates such as `LIKE 'a%'` once structural prepared-shape
  identity and per-slot bind contracts are stable.
- Storage and cardinality metrics exposure.
- Broader operational CLI coverage over the stable engine surface.

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

The current CLI exposes the implemented developer and operator surfaces:

```bash
icydb sql --canister <name> --sql "SELECT ..."
icydb snapshot <name>
icydb metrics <name>
icydb diagnostic <code>
icydb schema show <name>
icydb schema check <name>
icydb config init|show|check ...
icydb canister list|deploy|refresh|upgrade|status ...
```

SQL commands provide the admitted query, mutation, DDL, EXPLAIN, and index
inspection vocabulary. There is no separate collection-management, rebuild,
export, or import command today. Their possible 1.0 status remains an explicit
decision in [the 1.0 readiness checklist](1.0-TODO.md); raw stable-memory import
is not a supported product path. CLI growth remains an operational wrapper over
the accepted engine contracts rather than a second mutation authority.

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
