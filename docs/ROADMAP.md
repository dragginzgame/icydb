# IcyDB Roadmap

This document describes the long-term direction of IcyDB.

Current guarantees, invariants, and limits for shipped behavior are defined in:

- `docs/contracts/ATOMICITY.md`
- `docs/contracts/REF_INTEGRITY.md`
- `docs/contracts/TRANSACTION_SEMANTICS.md`

This roadmap is directional and planning-oriented, not a release contract.

Active execution/planning references:

- `docs/design/0.30-execution-kernel.md`
- `docs/design/0.31-deterministic-keys.md`
- `docs/design/0.32-aggregate-execution-stability.md`
- `docs/design/0.33-planner-group-by-integration.md`
- `docs/status/0.30-execution-kernel-status.md`
- `docs/status/0.31-deterministic-keys-status.md`
- `docs/status/0.32-aggregate-execution-stability-status.md`
- `docs/status/0.33-planner-group-by-status.md`

---

## Current Baseline

Today, the system is built around:

- Typed-entity-first APIs
- Deterministic planning and execution
- Explicit, enforced invariants
- Clear boundaries between public API and engine internals

Core save/delete semantics remain explicit:

- single-entity save/delete operations are atomic
- non-atomic batch helpers are fail-fast and non-atomic
- atomic batch helpers are atomic per single entity type per call
- multi-entity transaction guarantees are not part of the current contract

---

## Short-Term Goals

Focus: finish current execution hardening and reduce drift risk before larger architectural work.

- Complete aggregate execution hardening (`count`, `exists`, `min`, `max`) with parity-first behavior guarantees.
- Land `count` pushdown safely, constrained by the shared streaming eligibility gate.
- Prioritize reverse streaming hardening in the short term (DESC traversal parity and early-stop behavior).
- Extend streaming aggregate capability in safe, gated steps:
  - streaming `DISTINCT` on order-safe access paths
  - constrained streaming `GROUP BY` for ordered index-prefix shapes
  - streaming scalar folds (`sum`, `avg`) where semantics stay deterministic
  - broader early-termination wins (`exists`, `min`/`max`, and limit-aware streaming)
- Keep load and aggregate safety decisions centralized to avoid rule divergence.
- Complete `0.32.3` grouped-readiness scaffolding so `0.33` focuses on enablement rather than contract discovery.
- Continue cleanup passes that reduce cross-cutting complexity (error mapping, boundary handling, and test-surface maintainability).
- Keep changelog/status docs aligned as features move from design to shipped.
- Keep milestone tracking current in `docs/status/` as each feature closes.

---

## Medium-Term Goals

Focus: simplify the numeric core while preserving deterministic query semantics.

- Execute `0.23` as numeric consolidation around an internal decimal primitive.
- Replace `E8s`/`E18s` split paths with a single owned decimal path and schema-level scale enforcement.
- Remove external decimal dependency as part of the `0.23` consolidation.
- Add aggregate-aware fast paths where behavior is provably equivalent.
- Ship `0.24` composite aggregate direct-path routing with parity-first safeguards and canonical fallback guarantees.
- Continue cursor and continuation hardening, including stronger envelope/signature boundaries.
- Advance data-integrity hardening for replay safety, migration safety, and corruption detection tooling.

---

## Long-Term Goals

Focus: expand capability while preserving explicit semantics.

- Multi-entity transactions with a formal semantics spec, explicit APIs, and replay/recovery test coverage.
- First-class operational CLI over a stable engine command surface.
- Structural identity projection to remove drift between normalization and plan fingerprinting.

Conceptual CLI surface (illustrative):

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

---

## Non-Goals (Near Term)

The following are explicitly out of near-term scope:

- implicit or inferred transactional behavior
- hidden retries that mask failure semantics
- relaxing existing atomicity guarantees
- relational query semantics beyond the documented model

Correctness remains explicit, bounded, and testable.

---

## Summary

IcyDB evolves deliberately:

- strict current guarantees
- explicit future semantics
- no silent behavioral upgrades

Roadmap items move by readiness and safety, not by fixed future version numbering.
