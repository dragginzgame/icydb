# Recovery Consistency And Replay Equivalence Audit

## Metadata And Declared Scope

- Request: implement the approved RCV-01 correction and rerun recovery.
- Definition: `docs/audits/recurring/storage/storage-recovery-consistency.md`.
- Method: **RECOVERY-5 + DOMAIN-1**, unchanged and frozen before execution.
- Compared baseline: [2026-09-27 run 01](../01/report.md).
- Comparability: **comparable**; same obligations, capabilities, feature
  selection and classification, with the required missing proof added.
- Snapshot: `b6111f5f7185136b56860ee35287b206d9debafa`, workspace `0.261.11`,
  plus four recovery test/instrumentation files identified below. The earlier
  Cargo dependency edits, boundary scan tests, audit/governance/report edits
  and active release notes remain in the worktree.

Scope remains direct durable row replay, journaled live/fold behavior,
mixed-entity and nested relation mutations, unique-value handoffs, retained
marker and stage-loss idempotence, accepted schema/index publication, physical
migration, compound controls, and startup admission. All 48 baseline tests
execute again; the two new publication tests add eight fixture scenarios.
Historical results are not counted as fresh proof.

Excluded as before: exhaustive feature/mutation and migration-abort matrices,
every corruption codec case, whole-store integrity qualification, arbitrary
process failure, actual IC trap/upgrade/timer delivery, and performance.
Native returned errors and volatile-stage resets do not prove IC rollback or
deployed scheduling. No network lifecycle action or deployment was performed.

### Source And Verification Identity

Paths in this table are repository-relative; identities are SHA-256.

| Input | Identity |
| --- | --- |
| Recovery definition | `e3598124eb3fea57096367d7df31ca5f2179bc265881af2b35cd5288ebffa961` |
| Cargo.toml | `b34b059d3b555740d4d2ccdfb2a5ad39f102381294ed87313beee8d5cc78242c` |
| Cargo.lock | `695051feaba3c8de26eed05f5db590dddd2568f1f7493924e24b87ca08767716` |
| `crates/icydb-core/src/db/index/scan/tests.rs` (pre-existing) | `635996f907893ddb6d3ab4b2dd34930e2d1bed95707b2d5639940d1b0dd6f938` |
| `crates/icydb-core/src/db/commit/mod.rs` | `07a7376243c170244e5a53b9ec6f74cb664b629732f826229c40862f4c9ecb63` |
| `crates/icydb-core/src/db/commit/schema_publication.rs` | `a48f295472b17add2dc1cac589889631429b5ec240ec6064ac1bb46de26e6d70` |
| `crates/icydb-core/src/db/session/write.rs` | `c5cec6e199c57e0b97c5519ab79e19a55c68a2e45d5aa48fcea1112b3ae15db2` |
| `crates/icydb-core/src/db/session/write/identity_pre_key_tests/schema_publication_tests.rs` | `8cb6e597f849b22d26489030bbcc3f230fa0e8cd8bf30c6cd14f52592344b097` |

Toolchain: `rustc 1.98.1 (48a229cea 2026-09-01)`. Core native library tests use
`sql,migration`, locked dependencies, Makefile Cargo home `.cache/cargo/icydb`
and target `target/icydb`, with eight test threads. Other Rust source is
unchanged from HEAD except the pre-existing scan test. Cargo versions and
dependency edits were not changed. Run 01 remains immutable.

## Observable State And Owner Trace

The baseline observables remain accepted schema identity, rows/keys, exact
index/reverse entries or the stated semantic oracle, identity high-water,
entity revisions, control receipts/progress, marker, tail and watermark.
Live projections and canonical state are compared after convergence; their
transient physical ordering need not be identical.

Core paths below are relative to `crates/icydb-core/src/`.

| Family | Live owner and carried authority | Recovery flow / deliberate difference | Fresh evidence and limit |
| --- | --- | --- | --- |
| Direct mixed rows | `session/write.rs`, `commit/prepare.rs`, marker guard bind exact accepted row/index effects | Direct replay preflights the batch before prepared application | Three direct cuts check rows, unique reuse and identity successor; semantic oracle |
| Journaled rows, uniqueness and relations | Marker-bound batch publishes live effects and identity/revision state | Replay retains the batch; Fold uses canonical predecessors; Verify gates retirement | Five mixed and five nested-relation cuts, six unique-reader cases, normalized-predicate exact entries and relation-only/update/delete checks |
| Tail convergence | Successful marker retirement can leave committed live overlays and tails | Ordered Fold preflights the complete batch, advances its watermark and retires it | Tail reopen/accounting and stage-loss tests pass; global cross-store ordering remains source-inspected |
| Accepted schema/control publication | `commit/schema_publication.rs` binds candidate, checkpoint and compound controls | Replay restores authority; Fold applies candidate; Verify checks terminal effects | Store replay/fold, live-only compound interruption and two populated rename cases pass |
| Populated index replacement | `schema/sql_ddl.rs` calls staged catalog-native publication; marker carries AcceptedSchemaPublish and AcceptedSchemaIndexDelete/Put chunks | Replay appends only unretired batches; Fold validates accepted-after binding, applies canonical schema/index effects; Verify clears marker and restores readiness | New field/expression fixtures compare complete index bytes, accepted root identity and rows with uninterrupted publication |
| Physical migration/progress | Exact persisted plan and marker own candidate generations, row rewrites and compound progress | Recover interrupted work before final candidate publication; reject neither-side progress | Rewrite fixture, four progress cuts and repeated corrupt-progress rejection pass; deeper migration matrices remain excluded |
| Readiness and malformed input | Admission observes state; durable marker and tail own unfinished work | Production recovery driver runs Replay/Fold/Verify; complete preflight precedes protected writes | Receipt/admission, late malformed record/row, marker binding/version and bounded corpus tests pass |

Reinspection confirms `finish_commit` retains authority on returned apply
failure; the new hooks take this ordinary error path. The hook before journal
append runs only after `begin_commit` persists the marker. Partial-domain cuts
run after a real deletion or insertion, before index readiness and position
publication. The journal already carries the accepted candidate and full
derived-domain replacement at these cuts.

`validate_accepted_schema_index_chunk` requires the selected journaled store,
Fold mode and matching accepted-after fingerprint; inserted keys must belong
to that candidate's entity/index domain. Recovery resets live projections and
uses canonical predecessors. Completed watermarks prevent retained markers
from appending already-folded batches. `finish_recovery` verifies effects and
tail completion before marker clear and index readiness. These source facts
are exercised by the new scenarios; they add no generated-model fallback.
Actual clear failure, clear-to-readiness interruption and timer delivery remain
unexecuted limitations, as recorded in run 01.

## Interruption And Equivalence Matrix

| Cut / scenario | Fresh proof and postcondition | Result |
| --- | --- | --- |
| Five maintained row/relation cuts | All five mixed-entity and five nested-relation cases, including StateMaterialized, execute with row/index/relation and progress assertions | PASS |
| Direct marker/prefix/all-row and unique handoffs | Three direct mixed cases and six reader-state cases preserve intended owners and released values | PASS |
| Normalized predicates and secondary/reverse preservation | Exact uninterrupted/recovered index bytes plus unchanged secondary-key and relation oracles | PASS |
| Volatile-stage loss after Replay / Fold | Existing row recovery fixtures retain marker, identity and completed watermark safely | PASS |
| Candidate authority, controls and physical migration | Direct checkpoint, journal candidate-before-delete, compound publication, rename, rewrite and progress fixtures | PASS |
| Late malformed input / repeated Verify rejection | Batch preflight protects canonical writes/watermark; rejected verification retains marker and admission | PASS |
| Schema/index marker-only cut | Both new DDL cases leave accepted-before schema and complete indexes unchanged, retain marker and reject admission before recovery | PASS |
| Schema/index partial deletion | Both cases stop after the first real deletion with a changed domain and Building state; recovery matches uninterrupted output | PASS |
| Schema/index partial insertion | Both cases stop after the first insertion with a changed domain and Building state; recovery matches uninterrupted output | PASS |
| Schema/index retained-marker restarts | Every interrupted case forgets recovery stage after Replay and again after completed Fold; marker/admission barrier persist, watermark stays fixed after the second restart | PASS |
| Terminal publication state | Exact selected accepted root (including bundle hash), canonical/live bundle equality, complete serialized index entries, unchanged raw rows and unrelated entries; marker absent, tail empty, Ready | PASS |

The new fixture seeds two text-payload rows with an existing user index, plus
a different entity's indexed row and a real reverse-relation entry. It uses
ordinary `execute_admin_sql_ddl` to add either `(id, payload)` or
`LOWER(payload)` through the existing complete-domain replacement owner.
Each uninterrupted run and each of the three cuts starts in independent stable
and volatile test storage. The final root and serialized row/index vectors
are compared across those runs. The root binds the complete accepted bundle;
each thread also checks canonical/live bundle equality, revision 2 and two
accepted indexes. Exact unrelated-domain preservation covers both the other
entity's user index and its reverse edge.

Each interrupted run restarts twice while the marker is still retained:
after Replay and after Fold. Pending checks use the typed startup diagnostic.
The terminal extra recovery call separately proves quiescence; it is not the
retained-authority idempotence claim. Recovery pages exercise returned-error
and stage-loss behavior, not platform message rollback or stable-memory reopen
of these new fixtures.

## Findings, Verdict And Comparable Delta

**PASS** for the declared baseline. **RCV-01 resolved**: the previously missing
populated producer/recovery proof now executes for field and expression
indexes at marker-only and two partial-derived-state cuts. All 50 selected
tests pass. No executed case demonstrates replay divergence; no production
behavior correction was needed and no new finding is opened.

Compared with run 01: verdict BLOCKED → PASS; open verification gaps 1 → 0;
selected/passed tests 48 → 50 (+2); behavioral failures remain 0. Method and
scope are unchanged. The two tests contain eight fixture scenarios; those
scenarios are not counted as eight separate tests.

## Verification Readout

| Check | Outcome | Selected | Passed | Failed | Ignored |
| --- | --- | ---: | ---: | ---: | ---: |
| Initial new-fixture listing compile | FAIL | 0 | 0 | 0 | 0 |
| Corrected new-fixture listing | PASS | 2 | 0 | 0 | 0 |
| New-fixture focused execution | PASS | 2 | 2 | 0 | 0 |
| Audit listing and required-family count assertions | PASS | 50 | 0 | 0 | 0 |
| Audit focused execution | PASS | 50 | 50 | 0 | 0 |
| Non-test core library check | PASS | 0 | 0 | 0 | 0 |
| Formatting and diff whitespace checks | PASS | 0 | 0 | 0 | 0 |

The first compile rejected returning an Rc-backed schema bundle across test
threads (E0277); no tests executed. The fixture now returns the selected
accepted root, whose identity binds the complete bundle, and performs bundle
assertions within each thread. No production ownership was widened. Final
compilation and execution have no warnings or failures.

All 33 selectors match: mixed-entity 5, direct mixed 3, reader-state 6,
nested-relation 5, rename 2, new publication 2; each other selector matches 1.
The audit filters out 2835 unrelated tests. The new-fixture execution is a
subset of the final 50, not an additional unique-test count.

Exact command configuration follows. The two test commands were each listed
first by adding `--list` to their test arguments, then executed without it.

```bash
export CARGO_HOME="$(make --no-print-directory -s print-cargo-home)"
export CARGO_TARGET_DIR="$(make --no-print-directory -s print-cargo-target-dir)"
export RUST_TEST_THREADS=8
cargo test --locked -p icydb-core --lib --features sql,migration schema_publication_tests::
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
  schema_publication_tests::
)
cargo test --locked -p icydb-core --lib --features sql,migration -- "${selectors[@]}"
cargo check --locked -p icydb-core --lib --features sql,migration
cargo fmt --all
git diff --check
```

No full repository/workspace suite, clippy run, canister deployment or IC
upgrade probe ran. Full-suite execution remains user-owned. Source inspection
supports the declared boundaries; no omitted platform check is counted as PASS.

## Follow-Up And Complexity

No correction remains for RCV-01. Keep platform qualification and broader
mutation/feature matrices separate from this bounded native baseline.

Seven files change in this correction: four Rust files (about 366 net lines,
including the 317-line fixture), two existing release-note files and this new
immutable report, about 600 net lines overall. Production behavior axes,
formats and execution routes stay
unchanged. Test instrumentation grows; production implementation shape stays
neutral. Raw Wasm, IC cycles and instruction deltas are unmeasured; no
performance claim is made. The earlier audit definition and reports are
unchanged by this correction.
