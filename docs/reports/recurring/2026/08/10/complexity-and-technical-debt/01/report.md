# Complexity And Technical Debt

## Preamble And Comparability

- scope: `complexity-and-technical-debt`
- definition:
  `docs/audits/recurring/crosscutting/crosscutting-complexity-and-technical-debt.md`
- method: `CTD-1.0`
- run: `2026-08-10/01`
- auditor: `Codex`
- released snapshot: `d8d6a33c0e238fb90eb3b3f0c189b4e5332aa57d`
  (`0.223.4`), tree `f0e51668fa938d1fd6b4c2bbf8afe0f5f59a580d`
- worktree relevance: clean at audit start; shared FCD evidence then produced
  one same-owner runtime consolidation and these governance artifacts
- compared baseline: `N/A`
- comparability: non-comparable first `CTD-1.0` baseline; older complexity,
  module-structure, and velocity reports are historical context only
- run mode: broad current-state baseline; no active debt ledger created

Mechanical size and visibility counts were treated as discovery signals. A
finding required current state-space, ownership, or extension friction.

## Verdict

`PASS WITH FINDINGS`

Current runtime authority is coherent and the complete 0.223 mutation-job
lifecycle does not create a second mutation or recovery engine. One production
ownership debt remains: startup readiness is internal and ordinary database
entrypoints may perform recovery work, leaving applications to infer readiness
from a generic recovery-origin conflict and timer ordering. That debt is
already isolated behind the explicit 0.226 promotion gates and is not authority
to begin that minor.

## State-Space Map

| Axis | Values | Canonical owner | Combining axes | Invalid combinations |
| --- | --- | --- | --- | --- |
| Default memory lifecycle | unbootstrapped, committed, typed failure | `ic-memory`; IcyDB validates its declarations | generated database authority | a committed runtime is never bootstrapped again with an IcyDB policy identity |
| Read frontend | SQL, dynamic, typed/prepared | session lowering into structural query | public/trusted lane; scalar/grouped; live/exhaustive page mode | unsupported surface/shape combinations reject before execution |
| Mutation kind | insert, update, replace, delete | accepted structural mutation boundary | SQL/dynamic/typed frontend; heap/journaled store | mode/key/before-image mismatches fail before commit |
| Mutation-job lifecycle | active, completed, restart-required | mutation-job record and transition owner | Forward/Verify phase; sequence and idempotency key | terminal state cannot advance; replay identity is exact; only active state carries engine progress |
| Schema change lifecycle | direct publication or bounded activation/application/migration progress | schema application and migration owners | generated proposal or SQL DDL frontend | generated models cannot reconstruct accepted runtime authority |
| Startup recovery | complete or pending; typed failure | commit recovery | marker presence, journal tail, volatile index readiness, caller execution context | ordinary work must not observe partially recovered state, but observation and advancement are not yet separated publicly |

These axes are constrained rather than freely multiplicative. In particular,
mutation-job phase is record-owned, read modes share one planner/executor, and
memory policy is not caller-configurable.

## Decision And Ownership Spread

| Decision | Owner | Semantic consumers | Plumbing consumers | Cross-owner switch sites |
| --- | --- | ---: | ---: | ---: |
| Memory runtime already exists | `ic-memory::committed_allocations` | 1 | generated IcyDB ensure/cache | 0 policy rediscovery sites |
| Read access route | structural planner | executor and `EXPLAIN` | SQL/dynamic/typed projections | 0 downstream route classifiers found |
| Canonical row after-image | accepted structural mutation owner | commit/index/relation preparation | frontend result adapters | 0 alternate mutation engines found |
| Mutation job may advance | mutation-job record/transition | phase coordinator and progress commit | facade receipt projection | 1 phase dispatch, record-owned validation |
| Accepted schema may publish | schema application/DDL admission | schema publication and runtime root | proposal/DDL result adapters | separate frontend binders, one candidate authority |
| Database is ready for ordinary work | commit recovery, but not publicly projected | every recovered store/session operation | generated timers and application scheduling | spread between `ensure_recovered`, database gates, generated continuation scheduling, and application retry policy |

The broad `executor` and `schema` module hubs are visibility signals, not
findings. Inspected decisions remain in planner, executor, schema, or commit
owners rather than being re-derived by the hubs.

## Extension Rehearsals

### 1. Expose explicit startup readiness

- expected owner: commit/recovery state
- semantic changes: separate constant-cost observation from replicated
  recovery advancement; retain one recovery engine
- layers crossed: core recovery, database/session gate, facade, generated timer
  wiring, focused canister evidence
- current blocker: `recovered_store` calls `ensure_recovered`, which advances a
  page before it can return the generic recovery-origin conflict
- simplicity condition: one automatic driver and `Ready`/`Recovering`; no
  caller batch size, policy, callback, or grace-period configuration

### 2. Preserve an unknown SQL field diagnostic

- expected owner: query/planner error projected through the canonical
  diagnostic envelope
- semantic changes: retain one bounded query-visible field and clause/term role
- layers crossed: planner error, diagnostic code/envelope, facade wire, CLI
  renderer
- current blocker: the planner retains the field string, while the public
  error carries only numeric facts and deliberately drops that string
- assessment: product-surface gap, not current technical debt; ownership is
  clear. Add the smallest bounded detail rather than a parallel error model.

### 3. Add one bounded indexed relation traversal

- expected owner: structural query intent and planner
- semantic changes: one fixed-depth indexed semi-join contract carried into
  executor and `EXPLAIN`
- layers crossed: SQL lowering, query intent/planner, access planning, executor,
  diagnostics
- current blocker: no admitted intermediate-key contract exists
- assessment: expected layered extension cost, not present ownership debt. A
  general join optimizer, arbitrary nesting, or caller-selected depth would be
  incidental complexity.

## Findings

| ID | Debt family | Risk | Owner | Evidence | Present friction | Disposition | Trigger |
| --- | --- | --- | --- | --- | --- | --- | --- |
| CTD-001 | `OwnershipDebt` | `HIGH` | commit recovery and generated startup coordination | internal `RecoveryProgress` is only `Complete`/`Pending`, but ordinary `recovered_store` entry advances recovery; Toko observed read-only query budget exhaustion, shared timer-envelope exhaustion, and application grace-period/retry logic | database readiness is inferred outside its owner and recovery work can consume the arriving operation's budget | `FIX WHEN TOUCHED` | explicit user authorization to start 0.226 after its predecessor, baseline, and promotion gates |

The `DuplicatedFlowDebt` found by the shared FCD run was fixed immediately by
the authorized same-owner consolidation and is not retained as active debt.

## Accepted And Not-Debt Signals

- The complete mutation-job lifecycle adds necessary status, phase, sequence,
  replay, and restart state for work that cannot safely finish in one message.
  It reuses the existing excluded progress allocation, commit marker, journal,
  structural mutation flow, and recovery path.
- Generic revision-strict jobs and durable mutation jobs intentionally differ:
  one protects an application operation with a read-set proof, while the other
  owns canonical database intent and target convergence. Do not introduce a
  generic job framework without a demonstrated third shared invariant.
- Forward and Verify remain separate effects but now consume one eligibility
  and compiled-scope preparation contract.
- Large `schema::application`, `application_lowering`, `session::write`, and
  executor files are not debt solely by size. Their production sections retain
  explicit responsibility headers and canonical downstream contracts; much of
  their physical size is direct tests.
- SQL, dynamic, typed, generated, facade, diagnostic, and `EXPLAIN` surfaces
  add projection and boundary cost but not independent runtime semantics where
  the audit traced a shared carried contract.
- IcyDB has no Canic dependency or memory-policy configuration axis. It consumes
  `ic-memory` runtime capability automatically.
- The unreviewed 0.225 umbrella is correctly prohibited from implementation.
  Its candidate count is backlog evidence, not product state or an approved
  minor-line plan.

## Future Design Scope Gate

Continuous journal convergence and explicit readiness have one demonstrated
operational cause and can share one recovery/convergence owner. Durable exact
cardinality adds an independent `Building`/`Ready`/unavailable lifecycle and
may overlap a future 0.225 statistics owner. The 0.226 Patch 1 rescope clause is
therefore a hard simplicity gate: split or defer cardinality unless predecessor
evidence proves that synchronizing it with convergence removes more state and
reconstruction work than it adds.

This is a design promotion constraint, not current runtime debt and not
authorization to edit 0.226.

## Complexity Delta

`CTD-1.0` has no comparable prior baseline.

Contextually, 0.223 adds one necessary durable mutation lifecycle while deleting
the former application-custodied continuation lifecycle. The audit correction
removes one remaining duplicate Forward/Verify semantic branch and 12 net
runtime lines. It changes no public API, Candid, persisted format, cursor,
configuration, or state axis. Current implementation structure becomes
simpler; maintained raw Wasm moves only +13/+142 bytes on the dynamic/typed
subjects and Candid remains byte-identical. The explicit-readiness debt remains
unchanged and isolated.

## Focused Verification Readout

| Verification | Status | Result |
| --- | --- | --- |
| Released snapshot and clean-start check | `PASS` | `0.223.4` at `d8d6a33c0`; no pre-existing worktree changes |
| State-space and owner trace | `PASS` | current public, persisted, route, recovery, and configuration axes inspected |
| Mutation-job focused native tests | `PASS` | 11 passed; 0 failed; 1,639 filtered out |
| Maintained raw Wasm | `PASS` | dynamic 2,632,176 bytes (+13); typed 1,819,128 bytes (+142) |
| Candid | `PASS` | byte-identical at 4,670/64 bytes with released hashes |
| Old custody and historical-label absence | `PASS` | no maintained production occurrence |
| Memory dependency and automatic bootstrap trace | `PASS` | `ic-memory` only; no Canic manifest dependency or caller policy |
| Full repository suite | `BLOCKED` | push-owned and prohibited for this focused correction |
| 0.226 readiness/canister reproduction | `BLOCKED` | future explicitly gated design work; released Toko evidence was inspected but not rerun |
