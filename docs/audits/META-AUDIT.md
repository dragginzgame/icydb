# Audit Governance (META-AUDIT)

This document defines the architecture contracts that recurring audits enforce.

## 1. Layered Boundaries

IcyDB audit governance enforces these layered boundaries:

1. `value/representation`
2. `storage (data/index)`
3. `execution (executor/kernel/group)`
4. `query planning`
5. `commit/recovery`
6. `infrastructure (registry/wiring)`

Every recurring audit must evaluate directional ownership against these layers.

## 2. Architectural Invariants

Recurring audits must enforce at least these invariants:
- Dependency direction remains downward or lateral within allowed boundaries.
- Semantic ownership stays in its canonical layer.
- Cross-layer orchestration does not absorb domain logic.
- Planning semantics do not leak into executor-only responsibilities.
- Value canonicalization and hashing stay in value-owned boundaries.
- Grouping and ordering contracts remain deterministic and explicitly validated.
- Recovery behavior preserves execution invariants and replay equivalence.

## 3. Forbidden Dependency Edges

The following edges are forbidden unless explicitly approved and documented:
- `storage -> execution`
- `storage -> query planning`
- `index/data -> query semantic types`
- `commit/recovery -> query semantics`
- `value/representation -> executor orchestration`
- `infrastructure -> business/domain semantics`

## 4. Required Audit Coverage

Recurring domains under `docs/audits/recurring/` must continuously cover:
- planner contracts
- executor boundaries
- cursor/order guarantees
- access/index integrity
- storage/recovery consistency
- invariants and boundary preservation
- error/contracts taxonomy integrity

Cross-domain findings belong in `recurring/crosscutting/`.

## 5. Governance Enforcement

Recurring audits are contract enforcement, not advisory style review.

Required:
- classify violations by architectural risk
- identify broken boundary or invariant
- record concrete evidence path
- preserve all historical reports
- keep reports grouped by day under `docs/audits/reports/YYYY-MM-DD/`

Prohibited:
- deleting prior reports
- collapsing historical records
- redefining contract boundaries ad hoc in a run

## 6. Source of Truth Paths

Audit governance paths are:
- `docs/audits/AUDIT-HOWTO.md`
- `docs/audits/META-AUDIT.md`
- `docs/audits/recurring/`
- `docs/audits/oneoff/`
- `docs/audits/reports/`
- `docs/audits/domains/`
