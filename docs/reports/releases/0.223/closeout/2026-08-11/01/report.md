# IcyDB 0.223 Durable Resumable Mutation Jobs Closeout Audit

## 1. Reconstructed Scope And Audited Range

This is an implementation-first audit of the complete 0.223 durable mutation
job line. Maintained code, persisted-format validators, recovery behavior, and
measured PocketIC results are treated as authority. The implementation
tracker's earlier `ACCEPTED` label was an input to challenge, not evidence.

- Baseline tag: annotated `v0.222.4`; its peeled commit is
  `2a18f0aa3aade9cd209e8cec9e040d732e35b7af` and its tree is
  `09dc2a58d260932adac03c40c4cbdfbb2f11c1f2`.
- Audited committed range:
  `2a18f0aa3aade9cd209e8cec9e040d732e35b7af..36ee7e1a2154fced211fd7b60a1fcf2f58415c9e`.
- Audited HEAD: `36ee7e1a2154fced211fd7b60a1fcf2f58415c9e`
  (`v0.223.5`), tree `27233db68237af04e476f82020f357d23422c6f7`.
- Audited committed footprint: 134 files, approximately
  `+12,505/-2,134` lines.
- Audited committed post-closeout correction: 0.223.5's shared Forward/Verify
  traversal preparation.
- Audited uncommitted state: the current worktree, including the post-release
  SQL performance workflow correction, the separately requested `rusqlite`
  update, and this audit's corrections. Existing dirty changes were preserved.

The following authorities were read in full:

- `docs/design/0.223-durable-resumable-mutation-jobs/0.223-design.md`
- `docs/design/0.223-durable-resumable-mutation-jobs/0.223-patch-1-authority.md`
- `docs/design/0.223-durable-resumable-mutation-jobs/0.223-status.md`
- `testing/integration/src/durable_mutation_job_contract.rs`
- `docs/audits/recurring/crosscutting/crosscutting-flow-convergence-and-duplication.md`
- `docs/audits/recurring/crosscutting/crosscutting-complexity-and-technical-debt.md`
- the corresponding 2026-08-10 FCD and CTD reports
- the relevant ownership, convergence, codec, recovery, cost, and closeout
  sections of the 0.210 and 0.221 design/status lines

The reconstructed 0.223 outcome is one engine-custodied durable SQL update
lifecycle over the existing 0.210 bounded Forward/Verify executor. It extends
the existing excluded progress allocation and commit marker, while keeping the
0.221 generic revision-strict `ResumableJob` separate and unchanged. It does
not create a generated endpoint, a second executor/WAL, a public continuation
protocol, a configurable batching policy, or a compatibility decoder.

## 2. Severity-Ordered Findings

| ID | Severity | Status | Finding | Precise evidence |
| --- | --- | --- | --- | --- |
| 223-CO-001 | High | Fixed | Exact retained replay was recognized only after advance operation accounting. A lost-response retry could therefore fail with E273 under an exhausted request budget, and exact replay performed accounting forbidden by the design. | `advance_trusted_mutation_job` in `crates/icydb-core/src/db/session/mutation_job.rs` loaded/charged before calling `MutationJobRecord::exact_replay`; the corrected order is now load, exact replay, charge, stale/terminal validation, dispatch. Regression: `exact_replay_precedes_advance_budget_accounting`. |
| 223-CO-002 | Medium | Fixed | Current-format validation admitted `Active` records and transitions with an empty private engine continuation. That is an impossible durable state: active Forward/Verify work has no checkpoint from which it can resume. | `MutationJobRecord::validate` and `MutationJobTransition::validate` in `crates/icydb-core/src/db/mutation_job.rs` now reject `Active && engine_continuation.is_empty()` as typed `CorruptProgressStore`; the decoder calls the same validation. Constructor, transition, and persisted-decode regressions prove rejection. |
| 223-CO-003 | Medium | Corrected and verified | The scheduled SQL performance workflow selected a download policy but supplied no `POCKET_IC_BIN`; every evidence shard stopped at Makefile preflight. P2 and instrumentation carried the same latent defect. | The failed scheduled run was `31454024040`. The current `.github/workflows/sql-performance.yml` derives PocketIC 15.0.0 from `Cargo.lock`, verifies the exact binary, carries it with the Wasm artifact, restores executable permission, and exposes one global path to P1, scale, P2, and instrumentation jobs. `make lint-workflows` passes and no repository-level resolver was restored. |
| 223-CO-004 | Low | Corrected | The tracker still declared unconditional `ACCEPTED` before this independent final audit and did not record the two confirmed implementation corrections. | This report and `0.223-status.md` now record `ACCEPTED WITH CORRECTIONS`, the correction evidence, and the remaining delivery-only workflow rerun. |

No unresolved correctness, atomicity, replay, boundedness, recovery,
public-surface, or evidence-contract finding remains.

## 3. Corrections Made And Design Preservation

### 223-CO-001 — replay before accounting

`advance_trusted_mutation_job` now loads the retained record and performs exact
sequence/key replay before charging the mutation lane. Non-replay calls still
charge before stale-sequence/terminal rejection, planning, traversal, or
mutation. The change therefore implements the contract's explicitly frozen
precedence without changing the request, receipt, progress, SQL, or budget
formats.

The regression constructs a durable advanced record, uses a session whose
advance budget is zero, repeats the exact sequence/key, and proves that the
retained receipt is returned and state remains unchanged. A non-replay request
against the same exhausted budget still returns E273 without progress change.

### 223-CO-002 — impossible active state fails closed

Both construction/transition validation and persisted record validation now
require a non-empty continuation whenever status is `Active`. Terminal
`Completed` and `RestartRequired` records continue to require an empty
continuation. The current decoder remains one bounded, fallible current-format
decoder and reports the typed corruption error; no fallback, repair, legacy
version, or panic path was added.

### 223-CO-003 — exact PocketIC workflow input

The workflow-only correction prepares one PocketIC binary at the exact version
locked by the workspace, uploads it beside the shared canister artifact, and
sets the same `POCKET_IC_BIN` for all evidence jobs. It does not alter runtime
code, public configuration, persisted data, Wasm, Candid, or local resolution
semantics.

### Release truth

The root `Unreleased` section records the runtime and evidence corrections.
The tracker links this audit and replaces the pre-audit verdict. No patch
number, version bump, release, commit, or push was created.

## 4. Invariant-By-Invariant Disposition

| Invariant | Disposition | Implementation/evidence |
| --- | --- | --- |
| Sequence zero precedes target mutation | Pass | `start_trusted_sql_mutation_job` prepares immutable intent/continuation, inserts the initial progress record, and returns without traversal or target writes. The PocketIC start fixture reports zero changed rows and proves exact start replay. Both application phases start durably before any corresponding target change in the 10,001-row fixture. |
| Forward target and next progress are atomic | Pass | Forward constructs the exact after record/receipt, creates `MutationProgressRecordOp::replace`, and commits it with the target batch in marker version 3. Recovery accepts only exact before or exact after progress state and completes journal, rows, progress, then marker clear. Nine frozen failpoints prove before/recover-to-after/after outcomes. |
| Exact replay has absolute precedence | Pass after correction | Load and `exact_replay` occur before accounting, stale sequence, terminal rejection, dispatch, planning, scan, or mutation. Exact replay returns the retained receipt and leaves target/progress unchanged. |
| Immutable intent is complete | Pass | Current intent binds database incarnation, accepted authority/revision and fingerprint, entity target, canonical scope and patch, operation timestamp, allocation/continuation identity, and versioned policy identity. The policy fixes 2 KiB continuation, 256 scanned keys, and 64 updates; no caller parameter exists. |
| Persisted decode is bounded and fallible | Pass after correction | Record envelope, payload, intent, continuation, receipt, key, marker, and shared-capacity bounds are checked. Typed tests cover corruption/checksum, future version, max-plus-one, sequence arithmetic overflow, totals overflow, transition overflow, and impossible lifecycle states. No production panic path or compatibility decoder is present. |
| Forward bounded exhaustion is not completion | Pass | Forward uses one lookahead beyond the bounded page, retains continuation while active, and changes phase only on proved traversal exhaustion. Every step reuses the frozen intent timestamp. |
| Only clean stable Verify completes | Pass | Verify performs pre/post durable revision checks and no target mutation. Residual eligible work or revision drift produces an active Forward restart with an unchanged target; only clean exhaustion at the same revision returns `Completed`. |
| Pre-commit failure is atomic | Pass | E273 and all validation/planning/preflight failures occur before marker persistence; failpoint and zero-budget tests prove target and sequence remain unchanged. Recovery never admits a third progress state. |
| Replay and acknowledgement are lost-response idempotent | Pass | Terminal exact replay precedes terminal rejection. Acknowledgement requires the terminal sequence, rejects active/stale calls, removes one terminal record, and succeeds when repeated after a lost response. |
| Application-custodied API is hard-cut | Pass | Repository-wide scans found no maintained prepare/resume methods, aliases, shims, importers, obsolete facade exports, or active documentation. Historical design/changelog archaeology and the negative invariant string are not callable surfaces. |
| Generic `ResumableJob` is unchanged | Pass | `crates/icydb-core/src/db/resumable_job.rs` and `session/resumable_job.rs` have no 0.223 range changes; focused revision/proof and shared-capacity tests pass. Mutation jobs use a distinct key/record in the same excluded allocation. |
| No forbidden public/architectural axis | Pass | No automatic public endpoint, bearer-authority inference, raw SQL/value/key/continuation leakage, generated-model runtime authority, batching knob, second executor, second WAL, or compatibility route exists. The trusted facade requires application-chosen authorization before entry. |
| Forward/Verify traversal authority converges | Pass | The 0.223.5 `prepare_mutation_job_traversal_runtime` owns fixed eligibility, accepted row contract, scope compilation, and `IntentIneligible` mapping for both phases. Forward alone adds the mutation row layout. No remaining semantic duplicate was found. |
| 10,001-row fixtures are exact | Pass | Tier and scoring jobs each take 157 Forward and 40 Verify calls, preserve 17 unrelated rows, prove both application phases durable up front, replay exact pages, recover across upgrade/failpoints, and never complete from bounded exhaustion. |
| Frozen cost/size/surface contract | Pass | Native/PocketIC instruction gates, stable record/marker bounds, raw Wasm gates, deterministic gzip context, Candid bytes/hashes, method modes, and the full inherited 0.222 executor matrix all pass without ceiling or fixture changes. |
| Performance workflow supplies exact server | Pass locally; delivery rerun pending | The exact locked 15.0.0 binary is prepared once and supplied to all four workflow evidence families. Workflow lint passes. A new scheduled GitHub run cannot exist until the uncommitted workflow correction is pushed by the user. |

Repository-wide searches also found no contradictory maintained authority,
second execution path, historical 0.223 patch label in production code,
string-matched error assertion, newly unchecked lifecycle arithmetic, unbounded
current-format decoder, non-exhaustive current lifecycle match, or happy-path-
only test gap at the audited boundaries. The inherited rollback guard's
`catch_unwind` is pre-0.223, restores staged in-memory state, and is not a
persisted-decoder recovery path or newly introduced production panic.

## 5. Exact Validation Commands And Results

All commands were run from `/home/adam/projects/icydb`. No ceiling, fixture,
scale, or named test was weakened.

### Source, contract, and compile gates

| Command | Result |
| --- | --- |
| `bash scripts/ci/check-mutation-atomicity-invariants.sh` | Pass |
| `bash scripts/ci/check-executor-no-production-panics.sh` | Pass |
| `bash scripts/ci/check-generated-endpoint-invariants.sh` | Pass |
| `bash scripts/ci/check-layer-authority-invariants.sh` | Pass |
| `bash scripts/ci/check-schema-model-boundary-invariants.sh` | Pass |
| `bash scripts/ci/check-wasm-post-link-invariants.sh` | Pass |
| `cargo test --locked -p icydb-testing-integration --lib durable_mutation_job_contract -- --test-threads=1` | Pass, 4/4 |
| `cargo test --locked -p icydb --test compile_pass_trusted_sql --features sql` | Pass, 1/1 |
| `cargo check --locked --target wasm32-unknown-unknown -p icydb-core --no-default-features --features sql` | Pass |
| `cargo check --locked --target wasm32-unknown-unknown -p icydb --no-default-features --features sql` | Pass |
| `make test-canister-artifact-contract` | Pass, 1/1 |
| `make lint-workflows` | Pass |

### Native lifecycle, corruption, recovery, and generic-job gates

| Command | Result |
| --- | --- |
| `cargo test --locked -p icydb-core --lib --features sql mutation_job -- --test-threads=1` | Pass, 12/12 |
| `cargo test --locked -p icydb-core --lib --features sql mutation_progress -- --test-threads=1` | Pass, 5/5 |
| `cargo test --locked -p icydb-core --lib resumable_job -- --test-threads=1` | Pass, 2/2 generic-job tests |
| `cargo test --locked -p icydb-core --lib --features sql,diagnostics journaled_job_advance_is_idempotent_and_revision_checked_on_both_sides -- --test-threads=1` | Pass, 1/1 |
| `cargo test --locked -p icydb-core --lib --features sql,diagnostics proof_and_progress_controls_charge_one_shared_request_scope -- --test-threads=1` | Pass, 1/1 |
| `cargo test --locked -p icydb-core --lib --features sql current_payload_round_trips_every_lifecycle -- --test-threads=1` | Pass, 1/1 |
| `cargo test --locked -p icydb-core --lib --features sql resumable_batch_policy_identity_covers_every_compatibility_input -- --test-threads=1` | Pass, 1/1 |
| `cargo test --locked -p icydb-core --lib --features sql retained_continuation_distinguishes_unsupported_format_from_corruption -- --test-threads=1` | Pass, 1/1 |
| `cargo test --locked -p icydb-core --lib --features sql mutation_record_sizes_are_fixed_for_current_and_maximal_states -- --test-threads=1` | Pass, 1/1 |
| `cargo test --locked -p icydb-core --lib --features sql commit_marker_version_three_round_trips_one_bounded_mutation_progress_effect -- --test-threads=1` | Pass, 1/1 |
| `cargo test --locked -p icydb-core --lib --features sql mutation_progress_stable_growth_is_measured_at_one_eight_and_sixty_four_jobs -- --test-threads=1` | Pass, 1/1 |

### PocketIC behavior and frozen instructions

Each command used
`POCKET_IC_BIN=/tmp/pocket-ic-server-15.0.0/pocket-ic`; the binary reported
`pocket-ic-server 15.0.0`.

| Command | Result |
| --- | --- |
| `cargo test --locked -p icydb-testing-integration --test sql_perf_audit sql_perf_mutation_forward_steps_stay_bounded -- --exact --nocapture --test-threads=1` | Pass; eight Forward samples below 30M |
| `cargo test --locked -p icydb-testing-integration --test sql_perf_audit sql_mutation_job_verify_restarts_on_revision_drift_and_completes_stably -- --exact --nocapture --test-threads=1` | Pass; drift restarts, stable Verify completes |
| `cargo test --locked -p icydb-testing-integration --test sql_perf_audit sql_perf_mutation_job_start_is_durable_replayable_and_non_mutating -- --exact --nocapture --test-threads=1` | Pass; sequence zero durable, zero target changes |
| `cargo test --locked -p icydb-testing-integration --test durable_mutation_job_scale collection_scale_jobs_finish_across_calls_and_upgrade -- --exact --nocapture --test-threads=1` | Pass; exact 10,001-row multi-page/recovery proof |
| `cargo test --locked -p icydb-testing-integration --test streaming_execution_baseline -- --nocapture --test-threads=1` | Pass, 6/6 inherited 0.222 executor fixtures |

The first sandboxed PocketIC attempt failed environmentally before canister
execution: the server could not bind `127.0.0.1:0`, and the waiting harness was
interrupted. Re-running the same named tests with approved host execution and
the same exact binary passed. This is an execution-sandbox restriction, not a
product failure.

### Workspace and artifact gates

| Command | Result |
| --- | --- |
| `cargo fmt --all` | Pass; corrections formatted |
| `make check` | Pass; formatting, invariants, feature matrix, and workspace checks |
| `make test` | Pass; full repository-owned Clippy, no-default, workspace/all-target, canister-library, compile/UI, and artifact matrix |
| `make wasm-size-report SIZE_REPORT_ARGS="--canister one_entity_dynamic_query --canister one_entity_typed_query"` | Pass |
| `cargo metadata --locked --no-deps --format-version 1` | Pass |
| `git diff --check` | Pass after final audit artifacts |

## 6. Final Cost, Size, Surface, And Complexity Evidence

### Durable mutation instruction evidence

| Operation | Current evidence | Frozen ceiling | Result |
| --- | ---: | ---: | --- |
| Start, first call | 1,059,265-1,062,180 | 5,000,000 | Pass |
| Start, exact replay | 1,169,638 | 2,000,000 | Pass |
| 512-row Forward pages | 25,510,409-27,015,173 | 30,000,000 | Pass |
| 10,001-row tier Forward maximum | 29,195,414 | 30,000,000 | Pass |
| 10,001-row scoring Forward maximum | 27,564,954 | 30,000,000 | Pass |
| 10,001-row tier Verify maximum | 7,867,844 | 8,000,000 | Pass |
| 10,001-row scoring Verify maximum | 7,733,742 | 8,000,000 | Pass |
| Exact replay maximum at scale | 127,518 | 2,000,000 | Pass |
| State load | 105,137 | 2,000,000 | Pass |
| Terminal replay | 106,267 | 2,000,000 | Pass |
| Acknowledgement | 118,156 | 2,000,000 | Pass |

The 10,001-row run produced 157 Forward and 40 Verify calls for both tier and
scoring fixtures. Recovery re-entry maxima were 9,798,127 Forward and
9,323,312 Verify instructions. The inherited 0.222 executor matrix remains
within every original threshold; the sparse two-index intersection control is
2,033,649 instructions, 41 index entries, one store get, and one decoded row.

### Stable bytes and persisted bounds

| Evidence | Bytes/result | Limit | Result |
| --- | ---: | ---: | --- |
| Initial record | 97 | 65,536 | Pass |
| Active fixture record | 167 | 65,536 | Pass |
| Completed record | 165 | 65,536 | Pass |
| Restart-required record | 166 | 65,536 | Pass |
| Maximum admitted initial record | 18,524 | 65,536 | Pass |
| Maximum admitted active record | 18,842 | 65,536 | Pass |
| Maximum current retained receipt | 319 | 8,192 | Pass |
| Maximum mutation progress marker effect | 37,797 | 16 MiB marker payload | Pass |
| Isolated stable allocation, 1 and 8 small records | 4,390,912 / 4,390,912 | bounded | Pass |
| Isolated stable allocation, 64 records | 38,993,920 | shared 64-record cap | Pass |

The fixed continuation and intent bounds remain 2,048 and 16,384 bytes;
idempotency keys remain 256 bytes. Max-plus-one cases reject rather than
truncate or allocate unboundedly.

### Wasm, gzip, Candid, and method surface

Measurements use raw non-gzipped final Wasm as the primary gate. Deterministic
gzip is context only.

| Subject | `v0.222.4` raw | 0.223.5 prior raw | Audited raw | Delta from prior | Audited deterministic gzip |
| --- | ---: | ---: | ---: | ---: | ---: |
| One-entity dynamic query | 2,607,381 | 2,632,176 | 2,632,181 | +5 | 1,025,270 |
| One-entity typed query | 1,792,657 | 1,819,128 | 1,819,002 | -126 | 689,135 |

Both correction deltas are far below the 64 KiB review gate and the full line
remains far below its 256 KiB gate. Tool context was `ic-wasm 0.11.1` and
`wasm-opt 108`.

| Candid subject | Bytes | SHA-256 | Result |
| --- | ---: | --- | --- |
| Dynamic query | 4,670 | `c973e18eb0b6188564557bf165f22ff2514e8429d91f8a5eb05341d762c4d8c1` | Byte-identical |
| Typed query | 64 | `1005c63f5415d1de4bb7da89190f33212d1c926c881108a1acfeadfa93fff766` | Byte-identical |

The artifact contract confirms unchanged exported method names and query/update
modes and confirms that no mutation-job endpoint was generated.

### Complexity delta

This audit's runtime/test correction touches two source files at approximately
`+86/-10` lines, mostly focused regressions. Runtime shape adds two guard
conditions and reorders one existing accounting call. It adds no mode,
configuration, persisted field/version, cursor, endpoint, executor, WAL,
compatibility branch, or authority. The implementation shape is neutral to
slightly simpler: exact replay now has one literal precedence point, impossible
active state is rejected by the existing validator, and the 0.223.5 shared
traversal preparation removes the prior Forward/Verify semantic duplicate.

The complete current worktree handoff spans nine changed or untracked paths at
approximately `+619/-24` lines, including 427 lines of closeout report/JSON and
the pre-existing dependency and workflow work. Those totals are delivery-scope
signals, not runtime complexity: the durable mutation code delta remains the
two-file `+86/-10` correction above.

## 7. Remaining Risks And Carry-Forward Classification

- **Delivery verification, not a product blocker:** the corrected scheduled
  SQL performance workflow cannot have a post-correction GitHub run until the
  user commits/pushes it. After push, one scheduled or manually dispatched run
  must confirm P1, scale, P2, and instrumentation shards all consume the
  uploaded exact `POCKET_IC_BIN`. Local workflow lint and all equivalent named
  PocketIC evidence pass.
- **0.226-owned, outside 0.223:** explicit constant-cost startup readiness and
  continuous journal convergence remain the only accepted high-risk ownership
  direction. No 0.226 implementation was started.
- **Watch-only ingestion signal:** the reference application populated-ingestion slowdown remains
  provisional until application replay/repeated lookup work is removed and the
  new-record cost is remeasured. Generic UPSERT/import is not a 0.223 contract.
- **Separate dependency slice:** the existing `rusqlite` 0.40.2 worktree update
  is not attributed to the durable mutation implementation. It remains
  preserved and independently validated.
- **Historical archaeology:** the 0.223.5 code commit subject contains a
  `0.233.5` typo. The tag, package versions, tree, tracker, and runtime code are
  correct; rewriting published history is neither necessary nor authorized.

None of these items requires a 0.223 correctness, persistence, recovery,
boundedness, hard-cut, or public-surface change.

## 8. Final Verdict

`ACCEPTED WITH CORRECTIONS`

The two independently confirmed implementation defects are fixed with focused
regressions, the post-release evidence workflow correction is structurally
valid, every required current and inherited product gate passes, and no
correctness, atomicity, replay, boundedness, recovery, public-surface, or
evidence-contract failure remains unresolved. The only pending evidence is the
delivery-only GitHub rerun that necessarily follows the user's eventual push.
