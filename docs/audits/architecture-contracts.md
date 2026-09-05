# Audit Architecture Contracts

This document defines the architecture contracts that recurring audits enforce.

## 1. Layered Boundaries

IcyDB audit governance enforces these layered boundaries:

1. `value/representation`
2. `storage (data/index)`
3. `execution (executor/kernel/group)`
4. `query planning`
5. `commit/recovery`
6. `infrastructure (registry/wiring)`

Every structural audit, and every domain audit that crosses layers, must
evaluate directional ownership against these layers.

## 2. Architectural Invariants

Recurring audits must enforce at least these invariants:

- Dependency direction remains downward or lateral within allowed boundaries.
- Semantic ownership stays in its canonical layer.
- Cross-layer orchestration does not absorb domain logic.
- Planning semantics do not leak into executor-only responsibilities.
- Value canonicalization and hashing stay in value-owned boundaries.
- Accepted schema remains runtime constraint authority; generated models and
  SQL text remain proposal/front-end inputs rather than fallback semantics.
- Constraint diagnostics and integrity tooling observe accepted activation and
  validation-job truth instead of reconstructing lifecycle state.
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

Recurring definitions under `docs/audits/recurring/` must collectively cover
the following contracts. Individual runs follow
[Domain Scope And Change Triggers](README.md#domain-scope-and-change-triggers),
not a mandatory whole-system sweep:

- range/boundary contracts
- executor boundaries
- cursor/order guarantees
- access/index integrity
- storage/recovery consistency
- duplicated semantic and execution flow prevention
- state-space and evidenced technical-debt pressure
- error/contracts taxonomy integrity
- canonical semantic authority continuity across schema, build, frontends,
  planner, runtime, explain, and replay
- wasm footprint continuity with attribution output for size-growth attribution

Assign findings to the audit owning the violated contract, even when the
defect crosses domains. Apply
[Finding Ownership And Shared Evidence](README.md#finding-ownership-and-shared-evidence)
to link adjacent reports without duplicating findings or proof. Executed results
belong under `docs/reports/`, not beside audit definitions.

## 5. Governance Enforcement

Recurring audits are contract enforcement, not advisory style review.

Required:

- apply [Authorization And Read-Only Work](README.md#authorization-and-read-only-work)
  before executing verification or writing report output
- classify violations as `LOW`, `MEDIUM`, or `HIGH` architectural risk
- use [Findings And Verdicts](README.md#findings-and-verdicts) for severity,
  overall verdicts, and follow-up; do not derive composite scores
- identify broken boundary or invariant
- record concrete evidence path
- preserve all historical reports
- classify executed results under `docs/reports/recurring/`,
  `docs/reports/releases/`, or `docs/reports/investigations/`
- keep recurring reports under
  `docs/reports/recurring/YYYY/MM/DD/<scope>/<run>/report.md`
- colocate structured findings and artifacts with their owning report run
- include report preamble fields (scope, baseline path, method tag, and
  comparability status)
- enforce daily baseline discipline per scope:
  - run `01` is the canonical daily baseline
  - same-day reruns compare to run `01`, not to prior reruns
- include verification readout outcomes with explicit `PASS`, `FAIL`, or
  `BLOCKED` status
- document method changes and mark non-comparable deltas when formulas or scope
  change
- give every medium/high finding an owner, current friction, disposition, and
  action trigger
- treat audit-local findings as evidence rather than automatic implementation
  authority or a competing active debt ledger

Prohibited:

- deleting prior reports
- collapsing historical records
- redefining contract boundaries ad hoc in a run

## 6. Source of Truth Paths

Audit governance paths are:

- `docs/audits/README.md`
- `docs/audits/architecture-contracts.md`
- `docs/audits/recurring/`
- `docs/audits/targeted/`
- `docs/reports/`

## 7. Report-Quality Controls

Structural audit review must additionally check:

- metric-method consistency drift against the latest comparable run;
- whether non-comparable metrics are explicitly labeled;
- whether blocked verification steps are recorded with concrete reasons;
- whether active findings are limited to five and have dispositions;
- whether accepted duplication names its boundary, safety, or measured hot-path
  reason; and
- whether the report avoided turning line, branch, file, test, or score counts
  into targets.
