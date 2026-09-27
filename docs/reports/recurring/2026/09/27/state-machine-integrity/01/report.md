# State Machine And Transition Integrity Audit

## Metadata And Frozen Scope

- Request: select the next least recently run active audit, improve its method,
  and execute it.
- Definition: `docs/audits/recurring/executor/executor-state-machine-integrity.md`.
- Method: **STATE-2 + DOMAIN-1**, fixed for this run.
- Snapshot: `b6111f5f7185136b56860ee35287b206d9debafa`, workspace `0.261.11`,
  with pre-existing Cargo dependency edits and the boundary-audit storage test.
  The earlier boundary definition, reports and changelog changes are retained.
- Compared baseline: **N/A**, no comparable STATE-2 run exists.
- Historical reference: [2026-05-13 run 01](../../../../05/13/state-machine-integrity/01/report.md),
  Method V4. No prior behavioral results are counted as new execution.
- Comparability: **non-comparable (method change)**. Affected deltas are
  `N/A (method change)`; marker-before-apply and admission-before-execution
  remain qualitative anchors.

Selection follows the latest canonical report for each active definition.
The boundary audit now has a 2026-09-27 run. State-machine integrity's latest
run is 2026-05-13; the next oldest active scopes (error taxonomy, recovery
consistency and resource model) were last run 2026-05-14. Archived methods and
summary reports are ineligible.

Requested baseline: sample all five transition families in STATE-2. Included:
plan validation/route handoff; catalog/schema admission and publication;
save/update/delete preflight and marker lifecycle; scalar continuation
admission; startup/recovery/readiness and online convergence. Cover negative
admission, representative interruption cuts and expected-state recovery.

Excluded: exhaustive replay equivalence and corruption matrices, all schema
operations and feature combinations, grouped cursor ordering, full mutation-job
lifecycle, IC trap rollback/upgrade/timer-delivery qualification, performance
and Wasm. A mutation-progress test is included only as a commit/wake-up/gate
sample. Native tests do not establish canister scheduling or message rollback.

### Source Identity

SHA-256 inputs:

| Input | Hash |
| --- | --- |
| Revised audit definition | `d02d80be050018300ead408c11c1b743d52e796728c55062991d6a8c44b1000a` |
| `Cargo.toml` | `b34b059d3b555740d4d2ccdfb2a5ad39f102381294ed87313beee8d5cc78242c` |
| `Cargo.lock` | `695051feaba3c8de26eed05f5db590dddd2568f1f7493924e24b87ca08767716` |
| Pre-existing `db/index/scan/tests.rs` change | `635996f907893ddb6d3ab4b2dd34930e2d1bed95707b2d5639940d1b0dd6f938` |

All other Rust source is unchanged from HEAD. Earlier boundary reports remain
independent evidence; their PASS verdict is not substituted for transition
proof here. No production code, tests, Cargo inputs or release metadata were
changed in this run.

## Audit Review And Method Changes

The previous 593-line definition had three material problems:

- It explicitly delegated deep replay equivalence to recovery consistency,
  then required that audit's full equality/idempotence inventory here.
- Its readiness language could conflate committed online journal debt with
  incomplete startup recovery. Current `continue_recovery_with_failure_authority`
  and `ensure_recovery_admitted` deliberately distinguish them.
- It repeated state/transition/attack inventories, used a competing `PARTIAL`
  status, and referenced an obsolete daily report filename. Historical
  exclusivity claims and raw-cursor assumptions also needed current owner
  inspection rather than mechanical reuse.

STATE-2 reduces the definition to 175 lines, retains all five sampled
transition families, names protected state and admission boundaries, separates
native fault injection from IC rollback/delivery proof, and requires explicit
negative evidence and feature-correct test discovery. It allows structural
proof for private construction instead of demanding artificial illegal inputs.
It does not weaken marker durability, schema authority or readiness requirements.

## State And Transition Model

Source paths in the following tables are relative to
`crates/icydb-core/src/db/`. These are lifecycle facts, not a newly proposed
runtime enum.

| State/fact | Owner and entry condition | Legal exit and gate |
| --- | --- | --- |
| Accepted intent/proposal | Session/schema application resolves current accepted authority; generated models and SQL remain proposal inputs | Reject unsupported drift or plan against that authority; no accepted state published merely by constructing a proposal |
| Validated access plan | `query/plan/pipeline.rs::finalize_query_model_plan` finalizes route profile, prepares projection and validates semantics | Only after validation, finalize static execution contract and return the plan |
| Prepared execution | `executor/prepared_execution_plan/core.rs`, `shared_plan.rs` carry accepted authority, plan and lowered specs | Route planner consumes a shared plan; construction caches are not an alternate schema authority |
| Executing read | `executor/planning/route/planner/entrypoints.rs` builds intent -> feasibility -> execution stages | Read result or authenticated continuation; no commit marker needed for a read |
| Prepared write | `executor/mutation/commit_window.rs::open_commit_window_structural_inner` validates deletion relations, all prepared row ops, identity and positioned effects | Fail without protected row/index changes, or persist the exact prepared marker |
| Marker persisted / window open | `commit/guard.rs::begin_commit`; `commit/store/mod.rs::publish_prepared_marker` rechecks empty slot and predecessor proof | Apply under owned `CommitGuard`, or retain marker and recovery ownership on returned error |
| Partially/fully applied with marker | `commit_window.rs::apply_prepared_row_ops`, `guard.rs::finish_commit` | Success retires marker; apply/clear error retains authority and requests wake-up; normal admission rejects a retained marker |
| Marker retired, committed journal debt | Successful `finish_commit` clears marker, requests convergence if journal batches remain | Recovered domain may admit normal work while `continue_online_convergence` folds committed tails |
| Startup/interrupted recovery | `commit/recovery.rs::recover_domain` marks recovery in progress; Replay -> Fold -> Verify | Verify marker-owned effects/revisions, clear marker, mark index stores and recovery domain ready; ordinary admission never drives this work |
| Recovered but unreconciled | `startup/observe.rs::observe` has no marker/in-progress work, but exact generated-schema reconciliation is absent | Remain Recovering until reconciliation matches the accepted head/submission |
| Ready or terminal failure | Startup observer requires recovery and reconciliation; matching failure receipt takes priority | Ready admits service; terminal failure remains explicit until authority correction/receipt clearing, not an implicit success |

### Permitted And Incompatible Combinations

| Combination | Result | Evidence |
| --- | --- | --- |
| Executing + marker + partial application | Permitted within one write lifecycle | Retained-marker interruption tests; durable marker is recovery authority |
| Fully applied + marker | Permitted before retirement or after an apply-stage error | Interruption after row/state publication still recovers forward |
| Recovered + normal execution | Permitted | Recovery readiness is a prerequisite, not a disjoint execution phase |
| Normal admission + retained marker/in-progress recovery | Rejected | `ensure_recovery_admitted`; interrupted write test checks typed recovery-pending rejection before later allocation |
| Ready + committed journal tails | Permitted after successful live commit in an already recovered domain | Source separates `continue_online_convergence`; online update/delete fold test exercises maintained convergence |
| Fresh startup + no marker + tails | Still requires recovery | `recover_domain` takes fast completion only when tails are empty; marker-free restart in the mutation-progress test re-enters the driver |
| Recovered + missing exact generated receipt | Recovering, not Ready | Executed startup receipt test |
| Matching terminal receipt + retained marker | Failure receipt remains visible | Executed startup driver test checks failure priority rather than hiding it behind Recovering |

## Authority, Entrypoints And Illegal Handoffs

| Handoff / bypass attempt | Canonical gate and inspected producer/consumer | Evidence and limit |
| --- | --- | --- |
| Unvalidated plan -> executable route | Query finalizer validates before static contract publication; route entrypoints accept `AccessPlannedQuery`, build staged feasibility, then assemble `ExecutionRoutePlan` | Source construction proof. `capability_facts` is route-private; production literal assembly is in `planner/stages.rs`. Test-only constructors and grouped debug assertions are not counted as production rejection. |
| Silent widening during retry | `executor/kernel/mod.rs::unbounded_retry_route_plan` clones the route and removes fetch/scan hints, retaining access and logical predicate | Source inspection supports authorized physical retry, not a second query semantics path. No new behavioral proof of every retry mode is claimed. |
| Unsupported schema drift -> publication | `schema/transition.rs::decide_schema_transition`; schema application proposal admission; SQL DDL's `require_sql_ddl_transition_plan` requires the expected accepted transition kind | Identity drift test and rename proposal rejection check typed diagnostics plus unchanged head, lineage and physical state. |
| Staged DDL/index work -> accepted schema | `schema/sql_ddl/user_index_domain.rs` validates accepted-before authority and stages a complete domain; `commit/schema_publication.rs` preflights candidates and uses `begin_commit`/`finish_commit` | Executed domain tests prove staging is zero-write and exhaustion cannot finish; schema rename tests exercise durable compound publication/recovery. Full SQL DDL operation coverage is not claimed. |
| Dynamic/typed/SQL row mutation -> durable apply | `session/write.rs` converges accepted batches on `commit_structural_row_ops_with_window` or its mutation-progress sibling | Both consume `AcceptedMutationConstraintBatch` and create `OpenCommitWindow`; private `apply_prepared_row_ops` consumes that window. Source call-site inspection plus mixed-batch and interruption tests. |
| Direct schema/control publication -> durable apply | `commit/schema_publication.rs::{publish_live_candidate_with_prepared_domains,publish_journaled_candidate,publish_candidates_atomically,publish_database_control_atomically}` | Separate row scheduler is legitimate; all inspected publication branches use marker authority. Rename recovery includes schema receipt and lineage. |
| Second marker or stale prepared predecessor | `CommitStore::prepare_set_if_empty` and `publish_prepared_marker` reject occupied marker or changed control proof | Source guard; no concurrent-message simulation is claimed. |
| Apply/clear failure -> return without recovery | `finish_commit` requests wake-up on apply error and clear error; missing wake-up rejects before marker publication | Executed missing-wakeup and counted retained-error wakeup tests. Clear-failure branch is source-inspected, not fault-injected here. |
| Ordinary access -> recovery work | `ensure_recovery_admitted` checks readiness/in-progress/marker state only; `startup::driver` owns advancement | Mutation-progress test verifies admission rejection leaves rows and retained progress unchanged; startup observation test verifies no stable writes. |
| Invalid scalar cursor -> row execution | `session/query/dynamic.rs` decodes MAC-protected token, resolves pinned current route, compares signature/authority/mode/window/order before building continuation | Executed tamper/wrong-key, foreign-root, and internal out-of-envelope rejection cases; raw chunk anchors are separate from public token progress. |

The write and schema-publication paths inspected here are synchronous with no
await/re-entry point. Their current Rust call graph, private durable handles
and marker predecessor checks support the sampled authority boundaries. This
is not a claim that every internal helper is safe to call in arbitrary order.

## Failure Cuts And Visibility

| Cut/scenario | Expected protected state and gate | Evidence |
| --- | --- | --- |
| Preflight rejects late batch member | No preceding staged row update becomes committed | Mixed batch test rejects missing delete and insert collision, then proves original unique value remains |
| Schema staging exhausts | No physical index write or finishable partial candidate | Domain construction test checks typed budget facts, sticky finish rejection and identical physical entries |
| Invalid schema proposal | Accepted head, lineage and physical state unchanged | Rename admission test supplies missing/unexplained transitions and checks these authorities |
| Marker persisted before apply | Durable marker retained; later normal write rejects until driver completes | Mutation-progress and journaled interruption tests |
| Mid-apply / delete row-prefix cut | Partial work remains marker-owned, including mixed update/delete | Journaled identity test covers marker, journal, row-prefix, rows and state-materialized cuts, then validates recovered rows/allocation state |
| Application complete before retirement | Applied effects may coexist with marker; recovery converges instead of rolling back by assumption | Rows/state interruption cases and rename receipt-before-catalog/lineage case |
| Apply returns error | Wake-up registered and marker retained; ordinary admission has no side effects | Counted callback assertions and retained progress/row snapshots in mutation-progress test |
| Marker clear fails | Wake-up before returning error; clear preflights encoding before stable write | `finish_commit` and `CommitStore::clear_encoded_marker` source proof; this branch is not directly fault-injected |
| Recovery verification fails | Marker and admission barrier remain across retries | Test removes one derived index effect after replay; two verification attempts return typed corruption and retain marker |
| Restart after completed fold | Watermark and row effects persist; gate stays closed until verify completes | Test forgets volatile recovery stage, replays, checks unchanged watermark, then completes verification |
| Successful online update/delete | Retired unique keys become reusable and updated row is selectable | Paired online/startup fold tests check current output and reuse both old unique values |
| Invalid cursor / later mutation | Reject invalid admission; live continuation has no snapshot promise | Cursor tests plus current authenticated contract source. No whole-session atomicity claim across pages. |

`CommitApplyGuard` rollback is `cfg(test)` support. Maintained interruption
hooks deliberately forget that helper when retaining partial state. Their
normally returned native errors do not prove IC traps roll back a message, nor
do callback counts prove generated timers are delivered. Those are explicitly
outside this baseline.

`finish_commit` performs no fallible validation after successful marker clear;
its caller separately marks synchronized index handles Ready. That call is a
readiness projection, not a substitute for durable marker/effect verification.
No failure injection at that post-commit projection is claimed in this sample.

### Logical Overlap

Overlapping save attempts cannot independently occupy the one marker slot;
preflighted publication rechecks its predecessor. Within one mixed batch,
`PreflightStoreOverlay` includes final row images and delete absence before
relation/unique checks. The selected mixed and interrupted update/delete cases
exercise that synchronous batch boundary. Across messages, retained-marker
work blocks the next ordinary mutation until recovery completes. Successful
commits with online debt are a different case and remain usable. A cursor
between mutation messages is bound to its query/authority but does not make a
live data view immutable.

## Findings And Verdict

No production transition violation was demonstrated in the declared scope.
The audit-definition defects above were corrected before this run. The initial
test listing omitted three required feature-gated schema cases; this was
caught before behavioral execution and corrected without dropping coverage.
The verification readout preserves that failed selection.

Drift triggers: new marker publication callers must preserve preflight and
wake-up ownership; new await/re-entry points require rechecking synchronous
predecessor assumptions; startup readiness changes must preserve the distinction
between terminal failure, startup recovery, reconciliation and online debt;
new route retry or cursor shapes require accepted-contract binding evidence.
No debt ledger, implementation tracker or release metadata was changed.

**Verdict: PASS for the declared sampled transition baseline.** Required
behavioral samples passed, and source inspection supports the structural
handoffs. No active finding or implementation follow-up remains. Source-only
rows and explicitly excluded platform/deep-replay obligations are not promoted
to executed proof or a whole-system verdict.

## Verification Readout

Toolchain: `rustc 1.98.1 (48a229cea 2026-09-01)`; native `icydb-core` unit target;
repository Cargo home/target paths from Makefile; `RUST_TEST_THREADS=8`.
No ignored-test override, test-count substitution or full-suite execution.

The exact final temporary wrapper (`/tmp/icydb-state-audit-selection.sh`) was:

```bash
#!/usr/bin/env bash
set -euo pipefail
cd /home/adam/projects/icydb
CARGO_HOME="$(make --no-print-directory -s print-cargo-home)" \
CARGO_TARGET_DIR="$(make --no-print-directory -s print-cargo-target-dir)" \
RUST_TEST_THREADS=8 \
cargo test --locked -p icydb-core --lib --features sql,migration -- \
  missing_startup_recovery_wakeup_rejects_before_marker_publication \
  fresh_observation_is_recovering_and_performs_no_stable_write \
  completed_recovery_stays_recovering_until_exact_generated_schema_receipt_then_is_ready \
  driver_completes_one_recovery_page_then_memoizes_only_terminal_schema_failure \
  generated_transition_rejects_identity_policy_drift \
  complete_domain_construction_exhaustion_prevents_partial_finish_or_store_writes \
  complete_domain_stage_builds_field_and_expression_projection_without_writes \
  companion_requires_complete_rename_explanation_and_explicit_transition \
  populated_rename_recovers_ \
  mixed_batch_commits_cross_entity_then_rejects_late_failures_atomically \
  mutation_progress_and_target_rows_recover_as_one_marker_transition \
  journaled_identity_recovery_quiesces_every_publication_interruption_before_reallocation \
  online_fold_preserves_updated_and_deleted_unique_keys \
  startup_fold_preserves_updated_and_deleted_unique_keys \
  recovery_verification_failure_retains_marker_and_admission_barrier \
  recovery_restarts_after_a_completed_fold_without_losing_committed_progress \
  authenticated_scalar_token_rejects_tampering_and_wrong_database_key \
  pinned_cursor_rejects_a_foreign_accepted_root \
  resume_bounds_for_continuation_rejects_out_of_envelope_anchor \
  "$@"
```

| Command / check | Outcome | Selected / passed / failed / ignored | Interpretation |
| --- | --- | --- | --- | --- |
| Initial wrapper with `--features sql`, invoked with `--list` | FAIL | 17 of 20 required / 0 / 0 / 0 | Process exited successfully, but three required rename admission/publication cases were absent; no behavioral execution occurred. |
| `bash /tmp/icydb-state-audit-selection.sh --list` with final `sql,migration` features | PASS | 20 / N/A / N/A / 0 | All required cases listed after inspecting the `cfg(feature = "migration")` gate in `schema/application.rs:3154`. |
| `bash /tmp/icydb-state-audit-selection.sh` | PASS | 20 / 20 / 0 / 0 | All selected cases executed and passed; the two-test rename prefix accounts for the extra case beyond 19 selectors. |
| `git diff --check`, report-link and recorded-input verification | PASS | N/A | Audit definition and new report only; pre-existing inputs remain unchanged. |

The first selection's missing feature is resolved, not an outstanding runtime
failure. Its `sql`-only build reported the same 65 unused/dead-code warnings
seen in the preceding boundary audit. The final `sql,migration` build and test
run reported no warnings. No clippy run or failure occurred.

Evidence coverage: schema/accepted-authority admission and publication 6 cases;
write preflight/commit/wake-up/recovery 8 cases (including missing lifecycle
wiring); startup observation/reconciliation 3 cases; cursor/raw-anchor rejection
3 cases. Assertions were inspected for those obligations.
Failure-cut loops are not counted as extra tests. Plan assembly and marker-clear
failure handling remain explicitly source-inspected rather than claimed as
injected behavioral tests.

Full workspace/repository suites were skipped as user-owned. IC upgrade,
message-trap and timer-delivery checks were excluded; no local IC or PocketIC
network was started, stopped or reconfigured. Raw logs remain disposable under
`/tmp`; commands, selection failure, correction and outcomes are retained here.

## Complexity And Follow-Up

Two Markdown files changed in this run: the definition and this new report.
The method shrank from 593 to 175 lines; including this report, the net change
is 156 fewer lines. Runtime implementation shape and behavior axes are
unchanged; no alternate execution path, configuration, persisted state or
technical-debt ledger entry was added. No release-note edit or Rust formatting
was needed for these governance/report changes.

Raw Wasm size, IC cycles and instruction deltas are **unmeasured**; no runtime
performance claim is made. No follow-up is required for this run. The next
oldest active scopes are tied at 2026-05-14 and were not started here.
