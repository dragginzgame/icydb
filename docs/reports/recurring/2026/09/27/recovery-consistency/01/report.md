# Recovery Consistency And Replay Equivalence Audit

## Metadata And Declared Scope

- Request: improve and run the next least recently executed active audit.
- Definition: `docs/audits/recurring/storage/storage-recovery-consistency.md`.
- Method: **RECOVERY-5 + DOMAIN-1**, frozen before verification.
- Snapshot: `b6111f5f7185136b56860ee35287b206d9debafa`, workspace `0.261.11`,
  with pre-existing Cargo dependency edits and the boundary-audit scan test.
  Earlier audit/governance/report/changelog edits are retained.
- Compared baseline: **N/A**, no comparable RECOVERY-5 run exists.
- Historical reference: [2026-05-14 run 01](../../../../05/14/recovery-consistency/01/report.md),
  Method V4. No historical executions are counted as fresh proof.
- Comparability: **non-comparable (method change)**; affected deltas are
  `N/A (method change)`. Marker-before-apply and fail-closed admission remain
  qualitative anchors.

Recovery consistency and resource-model compliance were tied at 2026-05-14
for the oldest active run. Alphabetical scope order selects recovery. The
retired invariant-preservation definition is ineligible.

Requested baseline: direct durable row replay, journaled live/fold behavior,
mixed-entity and nested relation mutations, unique-value handoffs, retained
marker and stage-loss idempotence, accepted schema publication, physical
migration, compound controls, and startup admission. Include the required
populated schema/index replacement interruption obligation even though proof
could not be located.

Excluded: all feature combinations and mutation families, exhaustive migration
abort/nested-copy qualification, every corruption codec case, cross-canister
or arbitrary process failure, actual IC trap/upgrade/timer delivery, whole-store
integrity inspection, and performance. Native returned-error injection, volatile
stage resets, and stable-memory reopen fixtures have their stated scope only.
They do not prove IC message rollback or deployed recovery scheduling.

### Source And Verification Identity

| Input | Identity |
| --- | --- |
| Revised definition SHA-256 | `e3598124eb3fea57096367d7df31ca5f2179bc265881af2b35cd5288ebffa961` |
| Cargo.toml SHA-256 | `b34b059d3b555740d4d2ccdfb2a5ad39f102381294ed87313beee8d5cc78242c` |
| Cargo.lock SHA-256 | `695051feaba3c8de26eed05f5db590dddd2568f1f7493924e24b87ca08767716` |
| Pre-existing index scan test SHA-256 | `635996f907893ddb6d3ab4b2dd34930e2d1bed95707b2d5639940d1b0dd6f938` |
| Rust | `rustc 1.98.1 (48a229cea 2026-09-01)` |
| Target/features | `icydb-core`, native `--lib`, `sql,migration`, locked inputs |
| Cargo environment | Makefile Cargo home `.cache/cargo/icydb`, target `target/icydb`; eight test threads |

All other Rust source is unchanged from HEAD. No source, tests, Cargo inputs,
release metadata, or earlier reports were changed in this run. No network
lifecycle action was taken.

## Audit Review And Method Changes

The previous 426-line definition demanded identical mutation and validation
ordering, described the marker as sole durable authority, and carried obsolete
owner/type assumptions. Current journaled commits can clear the marker before
canonical folding: the retained journal tail and watermark then own durable
convergence. Recovery has Replay/Fold/Verify stages and distinct live versus
canonical readers. Accepted-root publication can occur before index application
inside a guarded message; requiring the root's physical write to be last would
misclassify that ordering without examining visibility and recovery authority.

RECOVERY-5 reduces the definition to 169 lines and checks equivalent observable
state plus safety dependencies. It requires stage-loss and retained-authority
idempotence, distinguishes live-only storage from persistent storage, and
separates native evidence from IC qualification. It retains the prior method's
requirement for interrupted schema/index-domain publication rather than treating
staging or metadata-only replay as equivalent proof.

The historical report treated separately persisted schema work as future work.
Current schema/index journal records and physical migration are present; that
old limitation cannot establish current publication recovery coverage.

## Observable State And Owner Trace

Core paths below are relative to `crates/icydb-core/src/`.

| Family | Live flow and authority | Recovery flow / deliberate difference | Observable state and evidence |
| --- | --- | --- | --- |
| Direct identity-bound mixed rows | `session/write.rs` and `commit/prepare.rs` prepare accepted row/index transitions; `commit/guard.rs` binds the durable marker before application | `publish_marker_bound_journal_batches` preflights direct batches, then `apply_prepared_journal_batch` uses prepared row application | Updated/deleted/inserted rows, index count, reusable retired unique values and identity successor; three direct interruption fixtures |
| Journaled mixed entities | Marker owns exact batches; live projection and identity/revision materialization precede marker retirement | Replay appends unretired batches; Fold chooses committed predecessors and writes canonical effects; Verify checks completion | Five mixed-entity cuts assert one indexed result per entity, reverse relation restriction, identity high-water and per-entity revision. This is a semantic oracle, not byte equality of the whole database |
| Unique and partial index transitions | Accepted row preparation derives final-batch uniqueness and index effects | Canonical predecessor selection permits value handoff without treating live already-applied state as a new conflicting proposal | Six unique reader-state tests; online/startup update/delete tests; normalized predicates compare full serialized index entries against uninterrupted execution |
| Nested relations | One batch derives reverse edges from accepted relation contracts | Replay/fold retain shared targets and remove obsolete edges | Five cuts each exercise insert, replace, and delete; exact edge sets and deletion restrictions, plus repeated recovery |
| Tail convergence | Successful live commit may leave admitted live overlays and durable tails with no marker | `select_oldest_journal_head` orders by database sequence, allocation, and journal sequence; `fold_selected_journal_head` preflights full batch, then applies effects/retirement/watermark | Tail-control append/replay/retire/reopen fixture; stage-loss fixture preserves the completed watermark. Global cross-store ordering is source-inspected, not behaviorally matrix-tested here |
| Accepted schema and controls | `schema_publication.rs` binds candidate plus application/lineage/migration operations into a marker | Recovery restores exact checkpoints, applies bound controls, folds candidates and verifies terminal effects | Schema store replay/fold and reopen; live-only compound interruption; two populated rename recovery cases. These do not replace index-domain publication proof |
| Populated schema/index replacement | `schema/sql_ddl.rs` routes staged replacement to `publish_accepted_schema_candidate_with_user_index_domain`; candidate validation binds accepted-before identity and accepted-after fingerprint | Journaled path carries AcceptedSchemaPublish and AcceptedSchemaIndexDelete/Put; recovery validates chunks against candidate and store, applies Fold, and verifies marker effects | Source trace and three staging tests only; interrupted publication remains **RCV-01** |
| Physical migration | Exact persisted plan owns isolated candidate row/index generations and progress | Interrupted rewrite is recovered before advancing final validation and publication | Rewrite test covers marker, journal, physical-apply cuts; checks three converted row values, index count, changed accepted head and terminal admission |
| Mutation progress | Target rows and progress successor share marker authority and recovery wake-up | Replay restores exact successor with rows and entity revision; neither-side progress rejects | Four cuts inside one progress test, plus a repeated corrupt-progress rejection test |
| Readiness | `ensure_recovery_admitted` observes state and marker; it does not drive replay | Production startup driver calls `continue_recovery_with_failure_authority`; test-only `continue_recovery` is a wrapper, not an alternate production admission path | Startup receipt test checks pure observation, exact generated receipt, marker precedence and readiness; recovery failure tests retain admission barriers |

`finish_commit` retains the marker on returned apply/clear failure and requests
recovery wake-up; it requests convergence when successful retirement leaves
tails. This is inspected production behavior. The progress fixture exercises
the returned-error wake-up path, not actual timer delivery or injected clear
failure. `finish_recovery` verifies marker-owned effects and terminal tails
before clear, then restores index readiness. A later fallible readiness failure
remains covered by the in-progress recovery gate in source, not by a dedicated
clear-to-readiness interruption fixture in this baseline.

No generated metadata fallback was found in the selected replay preparation:
checkpoint/candidate and canonical/live accepted selection supply authority.
Complete current batch preflight separates fallible validation from mechanical
application. Impossible apply contradictions trap; atomic platform rollback
for that path remains an explicit unexecuted assumption.

## Interruption And Equivalence Matrix

| Cut / scenario | Proof and postcondition | Result |
| --- | --- | --- |
| Marker only, journal published, row prefix, all rows, state materialized | All five mixed-entity and all five nested-relation cases execute; semantic row/index/relation and identity/revision outcomes checked | PASS |
| Direct marker/prefix/all-row cuts | Three heap identity-bound mixed-batch tests recover delete/update/insert and permit reuse of released unique values | PASS |
| Unique handoff from predecessor, partial, already-applied, online, delayed-fold states | Six reader-state tests recover intended owners, reject duplicate reuse, and admit the next distinct value | PASS |
| Uninterrupted versus replayed normalized partial predicates | Six predicate shapes; uninterrupted, marker-only and row-prefix paths in independent fixtures; exact serialized index-entry equality and row values | PASS |
| Relation-only update and update/delete folding | Unchanged secondary key remains usable, obsolete relation releases its target, retired unique values become reusable | PASS |
| Lost stage after Replay / completed Fold | Replay restarts while marker is retained; identity range is not consumed twice; watermark survives and row/index outcome converges | PASS |
| Lost accepted catalog / candidate predecessor | Direct checkpoint restores exact accepted bundle; journaled candidate folds before interrupted delete, retaining candidate generation/activation identity | PASS |
| Repeated verification failure | Missing derived effect rejects twice, keeps marker and admission barrier | PASS |
| Late corrupt record / late malformed row | Complete preflight rejects before first canonical row write, preserves watermark and retained tail | PASS |
| Journal/schema store reopen and repeated application | Exact control counts/bytes/head, candidate revision, stable reopen and repeated fold remain consistent | PASS |
| Compound publication / physical rewrite | Bound candidate, receipt, lineage and migration controls restored; rename preserves physical state; rewrite reaches complete accepted candidate | PASS for sampled cases |
| Populated accepted-index publication interrupted at marker or partial domain apply | Required producer/replay fixture not located; staging and codec assertions do not observe this transition | BLOCKED: RCV-01 |
| Malformed marker/row/journal | Bounded malformed corpus rejects; marker payload/version/batch binding tests retain typed failure | PASS for selected corpus |

The nested-relation repeated post-completion calls establish stable recovered
state. Retained-marker idempotence is supported separately by the volatile-stage
loss fixtures; post-clear no-ops alone are not used for that claim.

## Finding And Verdict

**BLOCKED** for the complete requested baseline. The 48 selected behavioral
tests pass, and no executed case demonstrates replay divergence. One required
publication obligation lacks behavioral evidence.

### RCV-01 — Populated Accepted-Index Publication Interruption Proof Missing

- Severity: **MEDIUM**, verification gap, not a demonstrated runtime defect.
- Owner boundary: `commit/schema_publication.rs` user-index-domain publication
  through `commit/recovery.rs` accepted-schema index chunk validation/folding
  and final verification; SQL DDL is the caller, not recovery authority.
- Present consequence: the baseline cannot establish that interrupted schema
  and derived-index publication converges to one exact accepted-after domain
  without exposing incomplete state or changing unrelated entries.
- Evidence: `complete_domain_stage_builds_field_and_expression_projection_without_writes`
  proves staging and unchanged physical state; mismatch and candidate-unique
  tests prove rejection. `db/journal/tests.rs` has index-chunk round-trip and
  binding checks. Live-only compound and populated rename recovery fixtures
  carry schema/control records, not populated index replacement chunks.
  Physical migration rewrite uses a different record path. None proves the
  selected domain-publication transition.
- Search scope: production publisher/callers, all accepted-schema index chunk
  constructors and their test uses across crates/canisters, schema mutation and
  publication fixtures, session tests, and integration/audit canister surfaces.
  `canisters/audit/sql_perf/src/lib.rs::publish_promotion_index_fixture` performs
  uninterrupted DDL and checks counts; it does not inject either required cut.
  No matching current interruption fixture or attributable proof was located.
- Disposition: leave runtime unchanged; add focused producer/recovery coverage
  in a separately authorized correction, then rerun this audit.
- Acceptance: seed a populated accepted-before domain plus unrelated entity and
  system entries; publish field/expression index replacement through the current
  owner; retain the exact marker before application and after a partial derived
  effect; reset volatile state and recover. Compare accepted root, complete
  index entries and rows to uninterrupted output, verify unrelated preservation
  and admission fencing, and repeat with retained authority/stage loss.
- Action trigger: close this gap before claiming the recovery baseline complete
  or accepting a change to this publication/replay boundary as qualified.

No debt ledger or implementation change is created from this finding.

## Verification Readout

| Check | Outcome | Selected | Passed | Failed | Ignored |
| --- | --- | ---: | ---: | ---: | ---: |
| Core test listing, exact final feature/filter configuration | PASS | 48 | 0 | 0 | 0 |
| Initial audit-local family-count assertion | FAIL | 48 | 0 | 0 | 0 |
| Corrected selector/family count verification | PASS | 48 | 0 | 0 | 0 |
| Focused core execution | PASS | 48 | 48 | 0 | 0 |
| Required index-publication interruption proof | BLOCKED | 0 | 0 | 0 | 0 |

The initial count assertion expected four nested-relation cases. Listing and
source inspection showed a fifth maintained StateMaterialized case. The count
was corrected to five before execution; no selector or proof obligation was
dropped. All 32 selectors match; mixed-entity and nested families each have five
cases, direct mixed has three, reader-state has six, rename has two, and each
other selector has one. No behavioral failure or compiler warning occurred.

Exact command below was first executed with a trailing `--list`, then without
it. It filtered out 2835 unrelated core tests. No full workspace/repository
suite, canister deployment, or platform upgrade probe ran.

```bash
export CARGO_HOME="$(make --no-print-directory -s print-cargo-home)"
export CARGO_TARGET_DIR="$(make --no-print-directory -s print-cargo-target-dir)"
export RUST_TEST_THREADS=8
selectors=(
  mixed_entity_recovery_after_
  heap_identity_mixed_batch_recovers_after_
  replay_construction_tests::reader_state_tests::
  online_fold_preserves_updated_and_deleted_unique_keys
  startup_fold_preserves_updated_and_deleted_unique_keys
  recovery_preserves_unchanged_secondary_keys_after_a_relation_only_update
  recovery_verification_failure_retains_marker_and_admission_barrier
  recovery_restarts_replay_after_losing_its_volatile_stage
  recovery_restarts_after_a_completed_fold_without_losing_committed_progress
  normalized_partial_indexes_replay_the_same_effects_as_uninterrupted_writes
  candidate_schema_folds_before_an_interrupted_delete_and_survives_stage_loss
  direct_recovery_restores_candidate_authority_from_the_live_schema_checkpoint
  nested_relation_recovery_after_
  exact_controls_append_replay_retire_and_reopen
  journaled_schema_candidate_replay_and_fold_are_idempotent
  marker_owned_application_publishes_one_live_only_store_and_receipt
  interrupted_live_only_application_recovers_candidate_and_receipt_from_marker
  populated_rename_recovers_
  physical_migration_rewrite_recovers_and_publishes_one_complete_candidate
  mutation_progress_and_target_rows_recover_as_one_marker_transition
  mutation_progress_neither_side_mismatch_blocks_recovery
  complete_batch_validation_rejects_a_late_record_before_canonical_writes
  prepared_batch_row_evidence_rejects_a_late_malformed_row_before_canonical_writes
  complete_domain_stage_builds_field_and_expression_projection_without_writes
  complete_domain_stage_rejects_physical_before_projection_mismatch
  complete_domain_stage_rejects_unique_collision_from_candidate_logical_fill
  completed_recovery_stays_recovering_until_exact_generated_schema_receipt_then_is_ready
  commit_marker_rejects_truncated_envelope_payload
  commit_marker_rejects_unbound_journal_batch
  commit_marker_future_version_fails_closed
  persisted_row_envelope_malformed_corpus_fails_closed
  journal_batch_malformed_corpus_fails_closed
)
cargo test --locked -p icydb-core --lib --features sql,migration -- "${selectors[@]}"
```

## Follow-Up And Complexity

Close RCV-01 with focused interruption coverage, then write a new immutable run
comparing with today's run 01. No production correction is justified by the
missing proof alone. Full-suite and deployed qualification remain outside this
run's scope.

Two documentation files changed/added. The definition shrinks by 257 lines
(426 to 169); the report retains source, selection and finding evidence.
Runtime behavior axes, execution flows and production debt remain unchanged;
the audit method is simpler. Raw Wasm, cycles and instruction deltas are
unmeasured; no performance claim is made. Governance-only edits need no
changelog entry.
