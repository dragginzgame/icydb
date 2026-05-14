# Recovery Consistency Audit - 2026-05-14

## 0. Run Metadata + Comparability Note

- scope: commit replay equivalence, startup rebuild consistency, and schema
  mutation startup publication recovery boundaries
- compared baseline report path:
  `docs/audits/reports/2026-03/2026-03-12/recovery-consistency.md`
- code snapshot identifier: `499a8478a` plus local uncommitted audit/design and
  schema-reconcile split changes
- method tag/version: `Method V4`
- comparability status: `non-comparable`
- non-comparable reason: Method V4 expands the audit beyond commit-marker smoke
  checks to include schema mutation startup publication and staged/physical
  index visibility boundaries.

## 1. Mutation Inventory

| Mutation Type | Normal Execution Entry Point | Recovery Entry Point |
| ------------- | ---------------------------- | -------------------- |
| insert / replace | `open_commit_window`, `begin_commit`, `apply_prepared_row_ops` | `ensure_recovered` replay of persisted `CommitMarker` row ops |
| delete | delete executor through `open_commit_window_structural` | `ensure_recovered` replay of persisted delete row ops |
| reverse relation update | prepared row op index/relation mutation during commit apply | marker replay plus startup reverse-index rebuild tests |
| index entry creation/removal | `PreparedRowCommitOp::apply` under `finish_commit` | marker replay and startup index rebuild from authoritative rows |
| commit marker transition | `begin_commit` / `begin_single_row_commit` and `finish_commit` | persisted marker load, replay, clear-on-success |
| supported schema field-path index add | startup reconciliation invokes the field-path runner | startup adapter gates old snapshot visibility until physical work validates |
| accepted snapshot publication | schema mutation publication decision inserts accepted-after snapshot | startup revalidates row/schema/physical state before schema-store insertion |

## 2. Side-by-Side Flow Tables

| Phase | Normal Execution | Recovery Replay / Startup | Identical? | Risk |
| ----- | ---------------- | ------------------------- | ---------- | ---- |
| pre-mutation invariant checks | commit-window preflight prepares row ops before marker persistence | marker decode validates shape and recovery prepares replay row ops | yes for row commits | low |
| referential integrity validation | save/delete paths preflight row ops and relation changes | replay uses marker payload and reverse-index recovery tests cover relation state | mostly | low-medium |
| unique constraint validation | live apply rejects conflicts before durable success | replay conflict parity test preserves classification and marker | yes | low |
| reverse relation mutation | prepared row op apply mutates relation/index state | reverse replay and rollback tests cover replay and repair | yes | low |
| index entry mutation | index guards verify store generation before apply | startup rebuild reconstructs secondary/conditional/expression indexes from rows | yes | low-medium |
| store mutation | prepared row op apply mutates data/index state under commit guard | replay applies the same row-op payload | yes | low |
| commit marker write / persistence | `begin_commit` persists marker before apply | persisted marker is the replay authority | yes | low |
| finalization / marker clear | `finish_commit` clears marker only after success | replay clears marker only after successful recovery | yes | low |
| staged physical-store validation | field-path runner validates before publication | startup adapter rejects populated target and physical drift before publication | yes for supported path | low-medium |
| runtime invalidation | runner report must include invalidation before publication is allowed | source guard and runner report enforce phase presence | yes for supported path | low |
| accepted snapshot publication | startup publication decision inserts accepted-after snapshot last | row/schema/physical gates run before schema-store insertion | yes for supported path | low |

## 3. Invariant Enforcement Parity Table

| Invariant | Enforced in Normal | Enforced in Recovery | Enforced at Same Phase? | Risk |
| --------- | ------------------ | -------------------- | ----------------------- | ---- |
| identity match | row-op key/entity path preflight | marker row-op decode and replay path checks | mostly | low |
| key namespace | data/index key construction before apply | marker decode rejects invalid key shape/length | yes | low |
| index id consistency | index-store guards verify generation before apply | startup rebuild derives index entries from accepted row truth | mostly | low-medium |
| component arity | prepared row/index entry encoding | marker decode and startup rebuild tests reject malformed state | yes | low |
| reverse relation symmetry | relation mutation tests cover live behavior | reverse replay, restore, drop-orphan, and rollback tests cover recovery | yes | low |
| unique constraint enforcement | live apply rejects conflicts | replay conflict parity tests preserve classification and marker | yes | low |
| expected-key vs decoded-key match | accepted row preflight validates row contracts | replay and startup rebuild reject malformed/future rows | yes | low |
| accepted snapshot authority | reconciliation starts from accepted persisted snapshots | startup field-path adapter uses accepted snapshots and accepted row contracts | yes | low |
| generated metadata exclusion | generated models remain proposal/test-only | source guards prevent generated index authority in recovery/schema publication | yes | low |

## 4. Ordering Equivalence Table

| Mutation | Normal Order | Recovery Order | Equivalent? | Risk |
| -------- | ------------ | -------------- | ----------- | ---- |
| row commit | preflight -> persist marker -> apply -> clear marker | load marker -> prepare replay -> apply -> clear marker | yes | low |
| failed row commit | preflight -> persist marker -> apply fails -> marker remains | marker remains for retry/fail-closed replay | yes | low |
| unique conflict | live conflict rejects before durable success | replay conflict rejects and marker remains | yes | low |
| reverse relation update | apply relation/index deltas with row mutation | replay applies marker and startup rebuild repairs drift | yes | low-medium |
| startup secondary/expression rebuild | recovery marker gates startup rebuild from rows | recovery rebuilds from authoritative data rows | yes | low-medium |
| schema field-path index add | row scan -> runner -> validate -> invalidate -> publish physical -> publish snapshot | startup adapter uses rebuild gate and publication decision before insert | yes for supported path | low |

## 5. Error Classification Equivalence Table

| Failure Scenario | Normal Error Type | Recovery Error Type | Equivalent? | Risk |
| ---------------- | ----------------- | ------------------- | ----------- | ---- |
| unique violation | live apply conflict | replay conflict parity preserves class | yes | low |
| referential integrity violation | relation validation error | reverse recovery rollback/fail-closed tests preserve state | mostly | low-medium |
| corrupt commit marker | not a live apply state | recovery rejects corrupt marker bytes/version/shape | n/a but fail-closed | low |
| corrupt index entry | index/store validation error | startup rebuild rejects corrupted/future row state | mostly | low-medium |
| invalid commit phase | commit guard invariant | marker remains or clear invariant asserts | yes | low |
| double-apply replay | not allowed | idempotence test proves second recovery no-op | yes | low |
| failed apply with marker still present | marker must remain | recovery tests preserve marker after replay failure | yes | low |
| staged schema mutation not publishable | normal runner report not publishable | startup publication decision rejects | yes | low |
| ready physical store not referenced by accepted snapshot | unsupported visible state | current supported path rejects populated target before rebuild | partial | medium-low |
| accepted snapshot references missing physical index state | unsupported visible state | physical-store revalidation blocks before insertion in supported path | yes | low |

## 6. Divergence Risks

| Location | Difference | Consequence | Risk |
| -------- | ---------- | ----------- | ---- |
| accepted schema-transition replay | explicit forward/replay and old-layout accepted-contract tests now replace the stale `migration_` filter | current replay surface is audited by active tests rather than a dead name filter | low |
| schema mutation startup partial physical work | supported path now rejects non-ready target stores before rebuild and populated targets before publication | future persisted staged-store work will still need a dedicated restart fixture when it exists | low-medium |
| recovery report method history | prior 2026-03-12 report collapsed to two smoke checks | historical trend comparison is weak until Method V4 becomes the baseline | low-medium |

No evidence found of duplicate replay application, skipped store mutation,
generated-as-recovery-authority, or schema snapshot publication before physical
readiness in the current supported path.

## 7. Idempotence Verification

| Scenario | Idempotent? | Why / Why Not | Risk |
| -------- | ----------- | ------------- | ---- |
| repeated recovery marker replay | yes | `recovery_replay_is_idempotent` passes and marker clears after first success | low |
| duplicate reverse-index replay | yes | reverse replay tests pass and startup rebuild repairs missing/orphan entries | low |
| duplicate store rows | yes | marker replay is no-op after marker clear | low |
| failed replay retry | yes, fail-closed | conflict replay tests preserve marker on failure | low |
| schema field-path publication retry | mostly covered | publication gates prevent publish on row/schema/physical drift, and non-ready target stores now fail closed before rebuild | low-medium |

## 8. Partial Failure Symmetry Table

| Failure Point | Recovery Outcome | Safe? | Risk |
| ------------- | ---------------- | ----- | ---- |
| after reverse-index mutation before store write | rollback test restores relation/index state | yes | low |
| after store write before `finish_commit` | commit marker remains durable and replay resumes | yes | low |
| between `begin_commit` and first index mutation | marker remains durable and recovery owns replay | yes | low |
| during replace | marker replay/idempotence tests cover retry behavior | yes | low |
| during delete | reverse mixed save/delete replay covers delete sequencing | yes | low-medium |
| during schema field-path rebuild before snapshot publish | old accepted snapshot remains visible; publication decision rejects non-publishable report/drift | yes for supported path | low |
| after physical field-path state changes before snapshot publish | final physical-store revalidation rejects target drift | yes | low |

## 9. Schema Mutation Startup Recovery Table

| Schema Mutation State | Startup Decision | Snapshot Visible? | Physical Store Visible? | Risk |
| --------------------- | ---------------- | ----------------- | ----------------------- | ---- |
| accepted-before and rows unchanged, empty target index | run field-path runner and publish accepted-after | accepted-after after final revalidation | ready target after runner publication | low |
| row changes after scan | reject before physical work or schema publication | accepted-before | old state | low |
| schema changes after plan | reject before physical work or schema publication | accepted-before | old state | low |
| building target physical index before rebuild | reject startup index addition before scanning into the target | accepted-before | building target remains non-visible | low |
| populated target physical index before rebuild | reject startup index addition | accepted-before | existing target not accepted as new schema | low |
| physical target drift after runner report | reject accepted-after publication | accepted-before | drift not accepted | low |
| staged/unreferenced persisted physical work after restart | no separate staged physical-store persistence exists in the supported path yet | accepted-before expected | add fail-closed/discard fixture when that storage exists | low-medium |
| generated model/index metadata available | not used for accepted recovery authority | accepted snapshot authority only | n/a | low |

## 10. Attack and Boundary Answers

| Question | Answer | Risk |
| -------- | ------ | ---- |
| Is commit-marker durability the sole durable authority? | Yes. `finish_commit` documents rollback as non-authoritative cleanup and preserves marker on failure. | low |
| Can a successful apply leave a persisted marker behind? | Tests and `finish_commit` assert marker clear after success. | low |
| Can a failed apply clear the marker incorrectly? | `finish_commit` asserts marker remains on failure; conflict replay tests pass. | low |
| Can replay observe marker state without row-op ownership? | Marker decode validates row-op shape; unsupported entity path markers fail closed. | low |
| Can recovery proceed before `ensure_recovered` gates write-side entry? | `Db::recovered_store` and mutation execution call `ensure_recovered`. | low |
| Can accepted schema-transition replay and normal replay diverge on the same marker contract? | Covered by forward/replay equivalence tests and old accepted-layout replay tests; the stale `migration_` filter has been removed from the recurring audit. | low |
| Can schema mutation startup publish accepted-after before physical readiness? | The supported path checks runner publishability and final physical store state before insertion. | low |
| Can staged/building schema mutation physical work become runtime-visible after restart? | Building target stores are rejected before rebuild in the current supported path; separate staged physical-store restart coverage belongs with future persisted staged-store work. | low-medium |
| Can ready-but-unreferenced physical index state be silently accepted? | Supported path rejects populated target before rebuild and drift before publish; restart fixture still recommended. | medium-low |
| Can generated model/index metadata recover accepted authority? | Source guards and startup adapter keep accepted snapshots/row contracts as authority. | low |

## 11. Overall Recovery Risk Index

**3/10**

Risk remains manageable. The commit-marker replay path is well covered, and the
0.154 supported schema mutation path now fails closed for non-ready physical
target stores before rebuild. The remaining recovery concern is future-facing:
when schema mutation work gains separate persisted staged physical stores, the
audit should add a restart fixture for discard-or-fail-closed behavior.

## 12. Verification Readout

- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_replay_interrupted_conflicting_unique_batch_fails_closed -- --nocapture` -> PASS
- `cargo test -p icydb-core unique_conflict_classification_parity_holds_between_live_apply_and_replay -- --nocapture` -> PASS
- `cargo test -p icydb-core commit_marker_ -- --nocapture` -> PASS
- `cargo test -p icydb-core commit_forward_apply_and_replay_preserve_identical_store_state_for_mixed_marker_sequence -- --nocapture` -> PASS
- `cargo test -p icydb-core conditional_index_forward_apply_and_replay_preserve_identical_store_state_for_membership_matrix -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_replay_updates_old_nullable_row_before_image_with_accepted_contract -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_startup -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_replays_reverse -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_rollback -- --nocapture` -> PASS
- `bash scripts/ci/check-memory-id-invariants.sh` -> PASS
- `cargo test -p icydb-core reconcile_runtime_schemas_rejects_field_path_index_addition_with_building_index_store --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core schema::reconcile --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core schema_mutation_publication_boundary_uses_runner_preflight --features sql -- --nocapture` -> PASS

## Follow-Up Actions

1. When schema mutation gains separate persisted staged physical stores, add a
   dedicated restart fixture proving unpublished staged work is discarded when
   safe or startup fails closed with typed diagnostics.
2. Keep accepted schema-transition replay commands in this audit. Do not add a
   `migration_` baseline filter until a real migration subsystem exists.
3. Use this Method V4 report as the next comparable baseline.
