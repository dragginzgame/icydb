# Shared commit completion and row application (F57–F59)

2026-09-19; active notes 0.259.6. User authorized these three findings together.

## Scope and proof

| Item | Change | Preserved contract |
|---|---|---|
| F57 | Both mutation entrypoints pass OpenCommitWindow into the existing apply owner | Store selection precedes application; readiness publication follows successful commit closure |
| F58 | Remove unused phase/key arguments and replace unpacked apply arguments with the window | Key duplicate detection, row/constraint checks and error order remain; Db is used by shared store selection |
| F59 | One production row loop and finalization path | Positioned/direct apply, identity/schema/progress order and commit guards remain |

The single-row rollback snapshot is retained under cfg(test), not as a second
production execution route. RowPrefixPublished remains multi-row-only; marker,
journal, all-rows and state/progress interruptions remain at their boundaries.
No new mode, configuration, format, authority, fallback or generic runner.
Raw key validation still occurs at its existing consumers; only the unused
row-local validation parameter disappears. Recovery/trap policy is unchanged.

Four Rust files change relative to the F56 candidate: commit_window.rs,
constraint_scheduler.rs, session/write.rs and commit/guard.rs. Net reduction:
75 Rust lines (43 for F57/F58, 32 for F59), including native-test support.
Existing semantic tests are reused. Root/detail notes, tracker and this report
bring this handoff to eight changed files. Structure is simpler.

## Matched inputs

HEAD: 140dd0d0697bb021878e5e69158395f4301e921e, tag v0.259.5. The existing dirty
F53–F56 work is preserved. Before is that candidate plus F57/F58; after adds
only F59 in commit_window.rs. This isolates the row-branch removal, not the
combined cost of all three cleanups or the full active release.

- Rust 1.98.1 (48a229cea 2026-09-01).
- Cargo.lock SHA256 (unchanged):
  `3fa6cb12d7bf876fabcf0c687c850270fe7a3cf10cd4d768e0c543e39f17a5b2`.
- Unchanged probe testing/integration/tests/sql_canister.rs SHA256:
  `f1c8f4568b452261929dc2c7b823f2ed7664f167cef9430b551cf58a3f71ca21`.
- commit_window.rs before:
  `1428d09e41983f0c477a8ea2c3d9ae1596565ba26c5233ca976fdaefca340d08`.
- commit_window.rs after:
  `f0a0bf0dce5ed0a0e4482bb8eb82378e8ac89f647087e1e2b64b5fbcb9237567`.
- Shared session/write.rs:
  `445b86407bbdd184bda5c95234a45501977894299e0b6c9aa894866eb26a761b`.
- Shared commit/guard.rs:
  `ac55d32183a4b8269de04434da856d086afdccc5db57717ba525ba521dcfd589`.
- Shared mutation/constraint_scheduler.rs:
  `682ac0203643e7607087a8f86393f4e09e5cfa5a41e13e79e7ededb7110f6505`.
- Shared commit/marker.rs:
  `f4caa5d3c95186ccf13b13b56fede9e61b3e8c46e4a74d71b564225ab4453daf`.

Both use the maintained retained Cargo/post-link builder for canister_test_sql:
wasm32-unknown-unknown, wasm-release (z, fat LTO, one codegen unit, panic abort,
stripped symbols), LocalTest, defaults disabled; explicit features
candid-export,local-sql-query,test-admin-api. Retention covers reading bytes.

## Measurement limits

The unchanged returning_selected_cells_wasm_cost_matrix is selected explicitly
with its manual ignored-test flag. Six fresh fixtures per actor cover small and
1,050,000-byte-text rows, count/id/all response shapes, and three sequential
exact-ID UPDATE literals (37/38/39). Wide RETURNING * rejects before committing;
it is a control, not successful row application. Every call has an independent
query readback for the new value or the unchanged rejected value (35).

Cycles are the whole update balance difference. Instructions cover the actor
request-execution interval, excluding ingress/egress encoding. Installation,
reset/seed, 64 settling ticks and separate verification reads are excluded.
No IC-time advance or auto-progress. This is neither a cache-hit study nor
statistical replication; it does not measure multi-row cost. No native timing,
peak-heap, gzip, function-count or new production-ceiling claim. Rejection
controls never reach the changed branch; their deltas cannot be attributed
directly to its removal. F57/F58 incremental cost is unmeasured.

## Qualification and decision

PASS for this scoped cleanup. Keep the shared production loop: **702 fewer raw
Wasm bytes** (4,217,244 -> 4,216,542; -0.0166%), with **203–476 more cycles and
instructions** per successful measured update (maximum +0.00276% cycles).
This is a size/structure improvement, not a cycle speedup. All three rejected
controls have exactly unchanged cycles and instructions. All 18 response hashes
match, and independent stored-value readbacks pass.

Both actors are retained under namespace
`823dde03c7560a3664bd3b412f48ad81c265e1cc1d77fb12be6d3b9aad847199` in
target/icydb/canister-artifact-cache/.ic-testkit/artifact-sets/namespaces/.
Entry paths append entries/<key>/outputs/0000.artifact. Both hashes were
independently reread from those retained files.

| Actor | Entry key | SHA256 |
|---|---|---|
| Before F59 | 73f2bb0b27c88daecd7851d2b7b6d679f25e32421e4bfc176cfc5b9832edf89d | 3497124abbed364d06535bde0c439fd1467bda4abc4c960034b9fe346048badb |
| After F59 | 9d8bf4ae3674983c32637896b50b77f719e17013ee70e62cc6b2f68f8a946ca3 | c5d5f1b0b1b45cf477e9491cd36e49d9186214c4af726c9744a9c1af7b371aa4 |

| Wide | Shape | Call | Cycles before | Cycles after | Instructions before | Instructions after | Delta cycles/instructions |
|---|---|---:|---:|---:|---:|---:|---:|
| false | count | 0 | 16767373 | 16767688 | 5068706 | 5069021 | +315 / +315 |
| false | count | 1 | 17124766 | 17125111 | 4971194 | 4971539 | +345 / +345 |
| false | count | 2 | 17302490 | 17302966 | 4884059 | 4884535 | +476 / +476 |
| false | id | 0 | 18758826 | 18759141 | 6994158 | 6994473 | +315 / +315 |
| false | id | 1 | 19199393 | 19199708 | 7126076 | 7126391 | +315 / +315 |
| false | id | 2 | 19391916 | 19392231 | 7158000 | 7158315 | +315 / +315 |
| false | all | 0 | 18874590 | 18874949 | 7070908 | 7071267 | +359 / +359 |
| false | all | 1 | 19319952 | 19320404 | 7209636 | 7210088 | +452 / +452 |
| false | all | 2 | 19503772 | 19504087 | 7232403 | 7232718 | +315 / +315 |
| true | count | 0 | 384704854 | 384705169 | 373249458 | 373249773 | +315 / +315 |
| true | count | 1 | 384918453 | 384918848 | 372288202 | 372288597 | +395 / +395 |
| true | count | 2 | 384946541 | 384946744 | 372035567 | 372035770 | +203 / +203 |
| true | id | 0 | 396102522 | 396102837 | 384579704 | 384580019 | +315 / +315 |
| true | id | 1 | 396381064 | 396381379 | 383823854 | 383824169 | +315 / +315 |
| true | id | 2 | 396412331 | 396412646 | 383771375 | 383771690 | +315 / +315 |
| true | all | 0 | 165369326 | 165369326 | 153859109 | 153859109 | 0 / 0 |
| true | all | 1 | 165938367 | 165938367 | 153473948 | 153473948 | 0 / 0 |
| true | all | 2 | 166147910 | 166147910 | 153763596 | 153763596 | 0 / 0 |

29 focused native tests pass before and after F59 (58 executions). These cover
commit-work and generation guards, batch relations and uniqueness, heap/journaled
parity, exact progress replacement/mismatch rejection and single/multi-row
interrupted recovery. Strict core all-target/all-feature lint, formatting and
whitespace checks pass. Existing behavior tests are retained; no shape-only
test framework was introduced.

Two fresh PocketIC probes pass (36 measured calls). Twelve disposable instances
were created/dropped; no application network changed. Logs are transient:
/tmp/icydb-unified-apply-{before,after,before-tests,after-tests,clippy}.log.
No Cargo/version changes, commits, pushes or full repository/workspace suites.
Full release validation remains user-owned.
