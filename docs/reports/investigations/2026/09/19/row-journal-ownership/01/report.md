# Move preflighted row operations into journal records (F53)

Date: 2026-09-19. Active release notes: 0.259.6.

## Scope and correctness

The commit-window owner now consumes its original row operations after preflight.
Journal construction moves the key and after-image into the existing validated
RowPut/RowDelete constructors. Prepared operations independently retain their
canonical apply rows. Original recovery bytes are not replaced by canonicalized
apply bytes. Preflight overlay construction/replacement, record order, schema
authority, journal validation, backlog admission and replay remain unchanged.

One Rust file changes: crates/icydb-core/src/db/executor/mutation/commit_window.rs.
Its diff has 10 added / 8 removed lines: +2 net production comment lines.
No types, modes, formats, caches, budgets or public APIs are added. No new tests
are needed for incidental ownership shape; existing behavioral gates are reused.
Root/detail notes, tracker and this report bring the handoff to five files.

## Exact comparison inputs

Before is clean published 0.259.5:
HEAD 140dd0d0697bb021878e5e69158395f4301e921e, tag v0.259.5.
After is the same HEAD plus the one Rust-file change above; notes do not enter
the actor. Both measurements are fresh executions, not older recorded results.

- Rust: 1.98.1 (48a229cea 2026-09-01).
- Unchanged Cargo.lock SHA256:
  `3fa6cb12d7bf876fabcf0c687c850270fe7a3cf10cd4d768e0c543e39f17a5b2`.
- Unchanged testing/integration/tests/sql_canister.rs SHA256:
  `f1c8f4568b452261929dc2c7b823f2ed7664f167cef9430b551cf58a3f71ca21`.
- commit_window.rs before SHA256:
  `9ad599a1b4908c4e6b74611e862b141acbb18fe75d2b53964a832e747bb49445`.
- commit_window.rs after SHA256:
  `2a215c247dc472815c7edb6827c3e645504a84ab45de6b6a8d6187b7f2d8c661`.

Both use the existing retained Cargo/post-link builder for canister_test_sql:
wasm32-unknown-unknown, wasm-release (opt-level z, fat LTO, one codegen unit,
panic abort, stripped symbols), LocalTest, defaults disabled, explicit features
candid-export,local-sql-query,test-admin-api. The retained handle lives through
reading bytes. No copied worktree, custom build cache or large temporary dump.

Retained artifact namespace:
`823dde03c7560a3664bd3b412f48ad81c265e1cc1d77fb12be6d3b9aad847199`.
Under target/icydb/canister-artifact-cache/.ic-testkit/artifact-sets/namespaces/
that namespace, entries/<key>/outputs/0000.artifact:

| Actor | Raw bytes | Entry key | SHA256 |
|---|---:|---|---|
| Before | 4217533 | 4aff10712172cf2bf922f68d6a2c62711d12d316e620c7f6b8217518a1adb9c9 | ad45afbb56d32483f471496c34270f6fe25ccc8dbfe8f41c01e10e704be25321 |
| After | 4217955 | 5021a3ceff1d737604e2c73bdc16225dae7b665c32b2d5ec85adb9bd17806586 | b2f5013b2d55aa9f030458d44c8776ca362e7ff459f410172cd66c460413e0ea |

Both hashes were independently reread from the retained artifact files.
Raw Wasm increases **422 bytes (+0.010%)**.

## Measurement and limits

Use unchanged returning_selected_cells_wasm_cost_matrix, explicitly selecting
its manual ignored test. Six fresh fixtures per actor cover small/wide rows and
count/id/all response shapes, with three sequential exact-ID UPDATE literals
37/38/39. Wide text is 1,050,000 bytes. Wide RETURNING * is a typed pre-commit
rejection, not a successful full-row response. Each call independently checks
the resulting stored value; all 18 response hashes match between actors.

Cycles are the complete measured update balance difference. Instructions are
the maintained actor request-execution interval, not ingress/egress encoding.
Install, reset/seed, 64 settling ticks and independent verification reads are
excluded. No IC-time advance or auto-progress is used. These are sequential
changing-value calls, not cache-hit trials or statistical replication.

Large successful count/id updates save **2,817,792–4,568,414 cycles
(0.712–1.145%)** and 3,139,005–4,568,313 instructions.
Small-row cycle deltas are mixed: **-9,789 to +2,077** (-0.0505% to +0.0120%).
Some small RETURNING calls have **150,439–160,070 more actor instructions**
despite nearly flat or lower whole-call cycles. These intervals are different;
the source of that shift is unlocalized, not dismissed as noise.

Wide rejection controls save 2,229,676–2,378,083 cycles (1.346–1.433%).
They reject before the changed journal-construction path, so those savings
cannot be attributed directly to eliminating its clone. This is an actor-level
comparison and not proof that the removed copy caused every observed delta.
No universal performance improvement, allocation peak, production ceiling,
native/wall-clock metric, gzip or function-count result is claimed.

Signed cycle deltas below are after minus before.

| Wide | Shape | Call | Cycles before | Cycles after | Cycle delta | Instructions before | Instructions after |
|---|---|---:|---:|---:|---:|---:|---:|
| false | count | 0 | 16756096 | 16758086 | +1990 | 4975800 | 4978726 |
| false | count | 1 | 17128734 | 17125928 | -2806 | 4976854 | 4975959 |
| false | count | 2 | 17313841 | 17315918 | +2077 | 4893343 | 4893426 |
| false | id | 0 | 18759714 | 18759897 | +183 | 6913565 | 7073635 |
| false | id | 1 | 19212352 | 19210909 | -1443 | 6977274 | 7135266 |
| false | id | 2 | 19401201 | 19391412 | -9789 | 7166442 | 7316881 |
| false | all | 0 | 18879706 | 18875980 | -3726 | 6995149 | 7153198 |
| false | all | 1 | 19318763 | 19316894 | -1869 | 7207602 | 7204193 |
| false | all | 2 | 19516759 | 19509016 | -7743 | 7241828 | 7235387 |
| true | count | 0 | 384417156 | 381589842 | -2827314 | 372802499 | 369657121 |
| true | count | 1 | 387025770 | 382805333 | -4220437 | 374394971 | 370175055 |
| true | count | 2 | 387075104 | 382824270 | -4250834 | 374164639 | 369676442 |
| true | id | 0 | 395803950 | 392986158 | -2817792 | 384121282 | 380982277 |
| true | id | 1 | 398814697 | 394246283 | -4568414 | 386255846 | 381687533 |
| true | id | 2 | 398685680 | 394144075 | -4541605 | 385967410 | 381427077 |
| true | all | 0 | 165203101 | 162958230 | -2244871 | 153533830 | 150965500 |
| true | all | 1 | 165610436 | 163380760 | -2229676 | 153466099 | 150998309 |
| true | all | 2 | 165963590 | 163585507 | -2378083 | 153739183 | 151363777 |

## Validation and exclusions

20 focused native tests pass, none ignored, covering preflight limits/overlay
visibility, missing wake-up rejection, heap and journaled exact-key writes,
SQL counts/RETURNING rejection, five mixed-entity interruption points, identity
recovery, atomic progress and typed-batch replay, a 129-row recovery batch, and
late malformed-record/row rejection before canonical writes.

Strict core all-target/all-feature clippy, formatting and whitespace checks pass.
Two manual integration executions pass, comprising 36 measured calls with
independent assertions. Twelve disposable PocketIC instances were created and
dropped; no application network was changed.

No full suites, Cargo/version changes, commits, pushes or downstream edits.
Full repository validation and publication remain user-owned.
Logs: /tmp/icydb-row-journal-{before,after,tests,clippy}.log.
