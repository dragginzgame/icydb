# State-Machine Integrity Audit - 2026-05-13

## Report Preamble

- scope: execution-state transition integrity, schema runner publication gates,
  write transition barriers, route handoff, commit-window lifecycle, and
  recovery write-gate handoff
- compared baseline report path:
  `docs/audits/reports/2026-03/2026-03-12/state-machine-integrity.md`
- code snapshot identifier: `5352aaba9`
- method tag/version: `Method V4`
- comparability status: `non-comparable`
  - reason: audit definition was refreshed before this run to add adjacency
    rules and mandatory modern transition samples. The prior report only
    sampled recovery replay and `cargo check`.

## Audit Maintenance Decision

| Question | Decision | Rationale |
| ---- | ---- | ---- |
| Is this audit redundant with recovery consistency? | No, retain standalone | Recovery consistency owns replay equivalence. This audit owns whether transition gates can be entered, skipped, widened, or published out of order. |
| Is the prior method current enough? | No, updated | The previous result did not sample schema mutation runner publication, route-plan handoff, or SQL/fluent transition barriers. |
| Merge target? | None | Keep adjacency with recovery, cursor ordering, invariant preservation, and layer-violation audits, but do not merge. |

## Execution-State Model

| State | Owner | Entry Condition | Exit Condition | Notes |
| ---- | ---- | ---- | ---- | ---- |
| unplanned / accepted-intent | SQL/fluent/session entrypoints | caller supplies accepted user intent and entity surface | plan lower/bind succeeds | unsupported schema transition must reject here before staging |
| planned | query planner / schema transition policy | validated plan or accepted schema mutation plan exists | executor or runner handoff receives staged/validated input | route construction must not bypass validated planner output |
| executing | executor / schema mutation runner | validated plan or runner input begins execution | commit window opens, staged runner work validates, or cursor continuation returns | execution must not widen access, mutate plan shape, or publish runner state |
| commit-window-open | `db::executor::mutation::commit_window`, `db::commit::guard` | marker-backed commit guard exists | finish clears marker or error preserves marker | write apply is illegal without this state |
| commit-marker-persisted | `db::commit::store` | marker bytes persist before apply | applied and cleared, or recovery consumes it | marker shape is bounded and fallible |
| applied | commit finish / schema runner publication | row ops applied or validated runner work published | marker cleared / accepted snapshot visible | publication requires the full preflight boundary |
| recovered | `db::commit::recovery` / startup gate | recovery completes marker replay and index rebuild checks | writes become legal | deep replay parity is delegated to recovery consistency audit |

## State Exclusivity Verification

| State Pair | Can Coexist? | Expected Result | Observed | Risk |
| ---- | ---- | ---- | ---- | ---- |
| executing / commit-window-open | No for a single write context | apply only after commit guard exists | `commit_marker_round_trip_clears_after_finish` passed; commit-window owner remains explicit | Low |
| commit-marker-persisted / recovered | No for incomplete marker | recovery consumes or fails closed before write gate opens | `recovery_startup_gate_rebuilds_secondary_indexes_from_authoritative_rows` passed | Low-Medium |
| executing / recovered | No for startup write gate | execution starts after recovery completion | recovery startup gate test passed | Low-Medium |
| commit-window-open / applied | No after finish | finish clears marker and exits commit window | `commit_marker_round_trip_clears_after_finish` passed | Low |

## Modern Transition Samples

| Family | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| schema mutation runner | `cargo test -p icydb-core field_path_runner --features sql -- --nocapture` | PASS | Low-Medium |
| schema runner publication preflight | `cargo test -p icydb-core schema_mutation_publication --features sql -- --nocapture` | PASS | Low |
| schema transition barrier | `cargo test -p icydb-core execute_sql_write_rejects_unsupported_schema_transition_before_staging --features sql -- --nocapture` | PASS | Low |
| route-plan handoff | `cargo test -p icydb-core execution_route_plan_is_only_built_from_staged_planner --features sql -- --nocapture` | PASS | Low |
| commit-window lifecycle | `cargo test -p icydb-core commit_marker_round_trip_clears_after_finish --features sql -- --nocapture` | PASS | Low |
| recovery handoff | `cargo test -p icydb-core recovery_startup_gate_rebuilds_secondary_indexes_from_authoritative_rows --features sql -- --nocapture` | PASS | Low-Medium |

## Transition Completeness Check

| State | Legal Outgoing Transitions | Missing Transition? | Unreachable? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| unplanned / accepted-intent | planned, rejected-before-staging | No | No | Low |
| planned | executing, rejected-before-executor | No | No | Low |
| executing | commit-window-open, staged-runner-validation, cursor continuation, rejected-before-mutation | No | No | Low-Medium |
| commit-window-open | commit-marker-persisted, failed-with-marker | No | No | Low |
| commit-marker-persisted | applied, recovery-replay, fail-closed corrupt marker | No | No | Low-Medium |
| applied | marker-cleared, accepted snapshot visible | No | No | Low |
| recovered | writes-allowed, fail-closed startup | No | No | Low-Medium |

## Illegal Transition Rejection Checks

| Illegal Transition | Expected Result | Evidence | Status |
| ---- | ---- | ---- | ---- |
| execute without validated route plan | reject / construction unavailable | `execution_route_plan_is_only_built_from_staged_planner` | PASS |
| publish schema runner before preflight | reject / not publishable | `schema_mutation_publication_boundary_uses_runner_preflight` via `schema_mutation_publication` filter | PASS |
| write through unsupported schema transition | reject before staging | `execute_sql_write_rejects_unsupported_schema_transition_before_staging` | PASS |
| apply/finish without commit lifecycle | marker-backed finish required | `commit_marker_round_trip_clears_after_finish` | PASS |
| write/rebuild before recovery completion | recovery gate rebuilds from authoritative rows before exposing writes | `recovery_startup_gate_rebuilds_secondary_indexes_from_authoritative_rows` | PASS |

## Findings

| Finding | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Prior audit method was too narrow for current code | Prior report sampled only `recovery_replay_is_idempotent` and `cargo check`; recurring definition now requires modern transition samples | IMPROVED | Medium if left stale |
| Schema field-path runner has explicit staged -> validation -> invalidation/publication handoff coverage | `field_path_runner_orchestrates_staging_to_publication_handoff`, rollback and mismatch tests under `field_path_runner` filter | PASS | Low-Medium |
| Publication preflight remains source-guarded against generic runner diagnostics | `schema_mutation_publication_boundary_uses_runner_preflight` | PASS | Low |
| SQL writes reject unsupported accepted-schema transitions before mutation staging | `execute_sql_write_rejects_unsupported_schema_transition_before_staging` | PASS | Low |
| Route-plan handoff is still locked to staged planner construction | `execution_route_plan_is_only_built_from_staged_planner` | PASS | Low |

## Overall State-Machine Risk Index

**3/10**

The refreshed audit has broader current coverage than the 2026-03-12 baseline.
Residual risk remains around full end-to-end schema mutation publication
because 0.154 developer-stability work is still expected to turn the supported
field-path index add path into a developer-trustworthy surface.

## Follow-Up Actions

- Keep this audit separate from recovery consistency, but continue using one
  recovery gate sample here to prove the transition handoff.
- In 0.154, add one end-to-end supported schema mutation test to this audit
  once the developer-facing accepted-schema mutation path exists.

## Verification Readout

- `cargo test -p icydb-core field_path_runner --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core schema_mutation_publication --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core execute_sql_write_rejects_unsupported_schema_transition_before_staging --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core execution_route_plan_is_only_built_from_staged_planner --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core commit_marker_round_trip_clears_after_finish --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_startup_gate_rebuilds_secondary_indexes_from_authoritative_rows --features sql -- --nocapture` -> PASS
