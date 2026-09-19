# End the preflight overlay at its last consumer (F54)

Date: 2026-09-19. Active release notes: 0.259.6.

## Scope and correctness

Preflight still constructs the complete final-row overlay before checking
relations. Each row still prepares against that view, and earlier prepared rows
still stage canonical data and index changes for subsequent rows. The last row
does not stage copies that no later reader can consume. Every row still enters
PreparedRowOpBatch::push for commit-work admission and index generation guards.
The caller drops the temporary overlay before preparing journal effects.

The overlay owns only temporary maps and a borrowed database reference. Journal
preparation and publication use separately owned prepared rows and original
row operations. No accepted authority, ordering, recovery bytes, constraints,
format, mode, API, cache or budget changes.

The existing unique-index test now attempts two updates to the same unique
value and checks the typed constraint rejection. Subsequent unchanged-value
mutations report zero affected rows for both original values, before the
existing swap and delete/release checks. Existing focused tests also cover
complete-batch relation visibility, late invalid rows and interrupted recovery.

Incremental footprint: two Rust files, +5 net production lines and +14 test
lines, plus root/detail notes, tracker and this report (six files). The combined
F53/F54 worktree has seven files, including both reports: +7 net production
lines and +14 test lines. Temporary ownership is shorter; there is no new
execution route or abstraction.

## Exact comparison inputs

Before is the F53 candidate, not clean 0.259.5. Both actors use HEAD
140dd0d0697bb021878e5e69158395f4301e921e (tag v0.259.5) plus F53; after adds F54.
Both probe executions are fresh. Only commit_window.rs changes production
source between them; session/write.rs changes only its test module.

- Rust: 1.98.1 (48a229cea 2026-09-01).
- Unchanged Cargo.lock SHA256:
  `3fa6cb12d7bf876fabcf0c687c850270fe7a3cf10cd4d768e0c543e39f17a5b2`.
- Unchanged testing/integration/tests/sql_canister.rs SHA256:
  `f1c8f4568b452261929dc2c7b823f2ed7664f167cef9430b551cf58a3f71ca21`.
- commit_window.rs before SHA256:
  `2a215c247dc472815c7edb6827c3e645504a84ab45de6b6a8d6187b7f2d8c661`.
- commit_window.rs after SHA256:
  `a9659a21cb638c1eec80c45fdbdfe7645ca07bd5f3e7fdcc81af86875761abfc`.
- session/write.rs after SHA256:
  `efe08dde6c849c5cff47f71cd4ea4d393a4c010d9f8cddba6e1997fbd3770422`.

Both actors use the maintained retained Cargo/post-link builder:
canister_test_sql, wasm32-unknown-unknown, wasm-release (opt-level z, fat LTO,
one codegen unit, panic abort, stripped symbols), LocalTest, defaults disabled,
explicit features candid-export,local-sql-query,test-admin-api. Retention lives
through reading bytes. No additional build cache or copied worktree was added.

## Measurement boundaries

The unchanged returning_selected_cells_wasm_cost_matrix probe is explicitly
selected, including its manual ignored-test flag. Six fixtures per actor cover
small/wide rows and count/id/all responses, each with three sequential exact-ID
UPDATE literals (37/38/39). Wide text has 1,050,000 bytes. Wide RETURNING * is a
typed pre-commit rejection, not a successful full-row response. Independent
query readbacks check changed values or the unchanged rejected value (35).

Cycles are complete update balance differences. Instructions cover the actor's
request-execution interval, excluding ingress/egress encoding. Install, reset,
seed, 64 settling ticks and separate verification reads are excluded. No IC-time
advance or auto-progress is used. This is not a cache-hit or statistical study.
Rejection controls do not reach the changed preflight path; their movement is
not direct evidence of savings from this change. No peak-heap measurement,
native/wall-clock metric, gzip, function count or production ceiling is claimed.

## Results

Retained artifact namespace:
`823dde03c7560a3664bd3b412f48ad81c265e1cc1d77fb12be6d3b9aad847199`.
Under target/icydb/canister-artifact-cache/.ic-testkit/artifact-sets/namespaces/
that namespace, entries/<key>/outputs/0000.artifact:

| Actor | Raw bytes | Entry key | SHA256 |
|---|---:|---|---|
| Before (F53) | 4217955 | 5021a3ceff1d737604e2c73bdc16225dae7b665c32b2d5ec85adb9bd17806586 | b2f5013b2d55aa9f030458d44c8776ca362e7ff459f410172cd66c460413e0ea |
| After (F54) | 4218000 | b416cc91b6724d9de19ee5e45acb352700d3474a4f7717d159bc8e0812b651b8 | 8396848bdc7e3abf5246f5c048cc3fd3ca8dbf5676b4b944b381bb7162f3e528 |

Both artifact hashes were independently reread from retained files.
Raw Wasm grows **45 bytes (+0.0011%)** versus F53, or **467 bytes** versus the
published-.5 actor recorded in the F53 report. All 18 response hashes match.

This is a lifetime/copy cleanup, **not a demonstrated overall cycle win**.
Wide successful updates cost 718,548–721,619 more cycles on their first call
(+0.184–0.188%), then save 116,379–275,834 cycles (-0.030–0.072%) on later calls.
Their instruction deltas range from -436,996 to +559,512. Small-row cycles
range from -10,031 to +10,470 (-0.059–+0.061%); instructions range from -166,005
to +164,207. Wide rejection controls cost 161,145–326,307 more cycles
(+0.099–0.199%) and 5,001–164,271 more instructions, despite not reaching the
changed path. The source of these actor-level shifts is unlocalized; the
removed staging must not be credited with every delta or regressions dismissed
as noise. Whole-update cycles and actor-interval instructions have different
boundaries.

Signed cycle deltas are after minus before.

| Wide | Shape | Call | Cycles before | Cycles after | Cycle delta | Instructions before | Instructions after |
|---|---|---:|---:|---:|---:|---:|---:|
| false | count | 0 | 16758086 | 16754806 | -3280 | 4978726 | 4977763 |
| false | count | 1 | 17125928 | 17115897 | -10031 | 4975959 | 4961696 |
| false | count | 2 | 17315918 | 17326388 | +10470 | 4893426 | 5057633 |
| false | id | 0 | 18759897 | 18752145 | -7752 | 7073635 | 6907630 |
| false | id | 1 | 19210909 | 19212411 | +1502 | 7135266 | 7137239 |
| false | id | 2 | 19391412 | 19393837 | +2425 | 7316881 | 7323226 |
| false | all | 0 | 18875980 | 18871542 | -4438 | 7153198 | 6989301 |
| false | all | 1 | 19316894 | 19320196 | +3302 | 7204193 | 7209473 |
| false | all | 2 | 19509016 | 19513493 | +4477 | 7235387 | 7241339 |
| true | count | 0 | 381589842 | 382308390 | +718548 | 369657121 | 370214454 |
| true | count | 1 | 382805333 | 382529499 | -275834 | 370175055 | 369738059 |
| true | count | 2 | 382824270 | 382574371 | -249899 | 369676442 | 369501015 |
| true | id | 0 | 392986158 | 393707777 | +721619 | 380982277 | 381541789 |
| true | id | 1 | 394246283 | 393982317 | -263966 | 381687533 | 381262135 |
| true | id | 2 | 394144075 | 394027696 | -116379 | 381427077 | 381306770 |
| true | all | 0 | 162958230 | 163119375 | +161145 | 150965500 | 150970501 |
| true | all | 1 | 163380760 | 163701219 | +320459 | 150998309 | 151077089 |
| true | all | 2 | 163585507 | 163911814 | +326307 | 151363777 | 151528048 |

## Focused qualification

- 31 focused native tests pass: commit-window accounting/overlay gates,
  mixed-relation/unique-index batch behavior, late rejection, heap/journaled
  exact-key parity, identity/progress recovery and SQL response bounds.
- Strict icydb-core all-target/all-feature clippy passes.
- Formatting and whitespace checks pass.
- Both explicitly selected PocketIC probe runs pass: 36 measured calls with
  independent query readbacks. Twelve disposable PocketIC instances were
  created/dropped in this F54 comparison; no application network changed.
- No full repository/workspace suite, release/version operation, commit or
  push. Full validation remains user-owned.

Local transient logs: /tmp/icydb-overlay-final-{before,after,tests,clippy,format}.log.
The maintained probe and per-call evidence above are the reproducible source;
temporary logs are not a persisted receipt protocol.
