# IcyDB Roadmap

This document describes the long-term direction of IcyDB.

Current guarantees, invariants, and limits for shipped behavior are defined in:

- `docs/contracts/ATOMICITY.md`
- `docs/contracts/REF_INTEGRITY.md`
- `docs/contracts/RESOURCE_MODEL.md`
- `docs/contracts/TRANSACTION_SEMANTICS.md`

This roadmap is directional and planning-oriented, not a release contract.

Active execution/planning references:

- `docs/design/0.30-execution-kernel.md`
- `docs/design/0.31-deterministic-keys.md`
- `docs/design/0.32-aggregate-execution-stability.md`
- `docs/design/0.33-planner-group-by-integration.md`
- `docs/design/0.35-group-by.md`
- `docs/design/0.35.1-hardening.md`
- `docs/design/0.36-ordered-group-execution.md`
- `docs/design/0.36-having.md`
- `docs/design/0.37-aggregate-fluent-api-consolidation.md`
- `docs/status/0.30-execution-kernel-status.md`
- `docs/status/0.31-deterministic-keys-status.md`
- `docs/status/0.32-aggregate-execution-stability-status.md`
- `docs/status/0.33-planner-group-by-status.md`
- `docs/status/0.35-group-by-status.md`
- `docs/status/0.35.1-hardening-status.md`
- `docs/status/0.36-grouped-hardening-status.md`
- `docs/status/0.37-aggregate-fluent-api-status.md`
- `docs/audits/reports/2026-03-01/resource-model-compliance.md`
- `docs/audits/reports/2026-03-01/resource-model-compliance-0.36.3.md`
- `docs/changelog/0.36.md`
- `CHANGELOG.md`

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

Focus: build on shipped `0.36` grouped contracts while planning `0.37` expansion.

- Preserve `0.36` grouped invariants (strategy revalidation, HAVING stage semantics, DISTINCT budget guardrails, continuation shape safety) as hard regression gates.
- Expand grouped capability only through bounded, explicit contracts (no implicit buffering, no hidden cardinality state).
- Keep planner/executor authority boundaries strict:
  - planner proposes eligibility
  - executor revalidates and may downgrade, never upgrade
- Continue cursor and continuation hardening for any new grouped/query shape.
- Keep the resource contract (`docs/contracts/RESOURCE_MODEL.md`) synchronized with shipped executor behavior.
- Keep milestone tracking and release docs synchronized across `docs/status/`, `docs/changelog/`, and `CHANGELOG.md`.

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
