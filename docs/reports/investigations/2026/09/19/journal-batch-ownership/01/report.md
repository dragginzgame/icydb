# Single-owner mutation journal batches (F49)

Date: 2026-09-19. Active release notes: 0.259.5.

## Scope and ownership

Mutation preparation moves each JournalBatch into its existing CommitMarker,
rather than retaining one batch in PreparedJournalAppend and cloning another
into the marker. PreparedCommitEffects retains that marker through application;
publication borrows it, and tail append borrows the batch at the existing marker
ordinal. The intermediate CommitWindowPayload wrapper is removed.

CommitGuard still owns the exact encoded control slot. Backlog admission,
startup wake-up checks, fixed-header identity checks, marker bytes, append
ordering and persisted replay validation are unchanged. This is not raw-byte
reuse in place of canonical validation and adds no route, configuration, format,
reference counting or compatibility mechanism. The borrowed begin_commit
signature is propagated to schema/migration callers; their typed markers are
explicitly dropped after publication to preserve their previous lifetime.
Their separate batch clones are outside this mutation-only change.

## Matched inputs

HEAD: ec00262af954e9b9d01b9dc62f06cd59d911c0d1 (v0.259.4).
Before: HEAD plus F45–F48, using the measured retained artifact from the
[write-cost report](../../write-cost-reduction/01/report.md), not published HEAD.
After: the same worktree plus F49. Earlier dirty changes are preserved.

Both use Rust 1.98.1; canister_test_sql; wasm32-unknown-unknown;
wasm-release (opt-level z, fat LTO, one codegen unit, panic abort, stripped
symbols); defaults disabled; candid-export,local-sql-query,test-admin-api.
The maintained retained Cargo/post-link builder owns artifacts until read.
No actor fixture or measurement code changed.

Cargo.lock SHA256:
`74d4a2830b1600476e4322867593bcd6fc40238c29b23e503788c2f09cfbe235`.
Unchanged host probe SHA256:
`f1c8f4568b452261929dc2c7b823f2ed7664f167cef9430b551cf58a3f71ca21`.

Current whole-file source hashes (db-relative; includes cfg-test text):

| Source | SHA256 |
|---|---|
| commit/guard.rs | b58814241821422abfa54192e4f431163605e47b62806506025fa204da6f014d |
| commit/schema_publication.rs | d4f755acc2a097506f45b0b04052aa980f8bc83581d7aa5e35676ec73641e3f7 |
| executor/mutation/commit_window.rs | 89b1f825b183bb8a2b66ec64c828e1c5ba68afa1ba7ac7e58eb2edd14801c12a |
| schema/migration_execution.rs | 218bf5610969ca8a6dcde6517a66dccfcfe18d824cf3e79aef68bdae77987946 |
| startup/mod.rs (test-only changes) | ef897cccb5418abaae0eb373236543a4b284a625fcda0f126d6e0866eba43e5d |

The additional test-only caller update is
schema/application/tests/nested_migration/terminal_handoff.rs.

## Measurement scope

Reuse returning_selected_cells_wasm_cost_matrix unchanged: six disposable
PocketIC cases, small/wide rows × count/id/all response shapes, three sequential
exact-ID UPDATEs with values 37/38/39 per case. Wide text is 1,050,000 bytes.
These are changing-literal calls, not cache-hit or statistical-replication trials.
Wide RETURNING * rejects before commit; it is not a successful full-row response.

Cycles cover the measured update endpoint's balance difference. Instructions
cover the actor's maintained request-execution interval, excluding ingress/egress
Candid encoding. Install, reset/seed, 64 settling ticks and independent readbacks
are excluded. There is no IC time advance or auto-progress. No native timing,
total workload speedup, peak-heap estimate or new production ceiling is claimed.

## Results

Both fresh runs pass all 18 cases and reproduce identical per-message costs.
All response hashes match the baseline; each run independently checks stored
values and the typed rejection. Twelve disposable local PocketIC instances
were created/dropped; no application network changed.

Raw Wasm: 4,219,650 -> 4,219,672 bytes (**+22**, +0.00052%).
Before SHA256:
`2fa4b908662a046eccd560f41ba4ee06b95a7c17ffa0a44157f92a1d61752524`.
After SHA256:
`b5d5c4eedb83770d7df7f4fc5fd24a0dff503f5e05182e7b75c54a0bfc83b7f3`.
Retained post-link keys, under the common artifact-set namespace
`823dde03c7560a3664bd3b412f48ad81c265e1cc1d77fb12be6d3b9aad847199`:

- Before: `89f732fc80d19b33810959e7b0fc1b35f33700b38cb005e9bc6779a4e23a6eb7`.
- After: `a452a95d1e188b3cc20fb3ab88d5dad477211b0e54ea9ee678a7f56440828eed`.

Both artifact hashes were independently reread from the retained output files.

Successful wide count/id writes save 3,442,403–5,869,622 cycles (0.856–1.503%)
and 3,278,779–5,547,543 instructions per measured message. Small-row cycle
changes are mixed (-7,707 to +5,709), not a general small-write improvement.

**Measured trade-off:** wide RETURNING * rejects before the changed commit path,
yet its third call costs 3,472,999 more cycles (+2.077%) and 3,474,205 more
instructions. The repeated current actor reproduces this exactly; it is not
dismissed as noise. The runtime cause is unlocalized. This is an actor-level
cost comparison, not proof that the removed clone caused every delta. No
blanket improvement or cost-neutral rejection-path claim is made. Peak heap,
gzip and defined function counts are unmeasured.

Signed deltas are after minus before:

| Wide | Shape | Call | Cycles before | Cycles after | Cycle delta | Instructions before | Instructions after |
|---|---|---:|---:|---:|---:|---:|---:|
| false | count | 0 | 16768132 | 16769571 | +1439 | 4979303 | 4981294 |
| false | count | 1 | 17146730 | 17139023 | -7707 | 4982584 | 4894386 |
| false | count | 2 | 17325412 | 17323755 | -1657 | 4890613 | 4890838 |
| false | id | 0 | 18782434 | 18780401 | -2033 | 6927242 | 6924235 |
| false | id | 1 | 19233070 | 19227982 | -5088 | 7147649 | 7142034 |
| false | id | 2 | 19418230 | 19419911 | +1681 | 7334794 | 7335283 |
| false | all | 0 | 18893052 | 18895491 | +2439 | 7000443 | 7002565 |
| false | all | 1 | 19344720 | 19343304 | -1416 | 7224658 | 7221396 |
| false | all | 2 | 19523432 | 19529141 | +5709 | 7242506 | 7247727 |
| true | count | 0 | 390429411 | 384559789 | -5869622 | 378483839 | 372936296 |
| true | count | 1 | 390665799 | 387202569 | -3463230 | 378180676 | 374560779 |
| true | count | 2 | 390702486 | 387243664 | -3458822 | 377779441 | 374322596 |
| true | id | 0 | 401823403 | 395979179 | -5844224 | 389808401 | 384282729 |
| true | id | 1 | 402129561 | 398687158 | -3442403 | 389558060 | 386279281 |
| true | id | 2 | 402168149 | 398713776 | -3454373 | 389437387 | 385984988 |
| true | all | 0 | 166425289 | 166427356 | +2067 | 154428635 | 154751511 |
| true | all | 1 | 167010864 | 166846354 | -164510 | 154538226 | 154215080 |
| true | all | 2 | 167219038 | 170692037 | +3472999 | 154666077 | 158140282 |

## Qualification and complexity

26 focused native test executions pass, none ignored: five SQL-disabled
mixed-entity recovery boundaries, four more SQL-disabled heap/append checks
using that freshly built test binary, and 17 SQL/migration-enabled executions.
These cover marker/journal/row interruption, atomic mutation progress, heap and
journaled exact-key semantics, a 129-row recovery batch, missing startup wake-up,
typed fixed-header rejection and migration terminal handoff. Existing semantic
tests are reused; no implementation-shape or compatibility tests are added.

Strict icydb-core all-target/all-feature clippy, formatting and whitespace checks
pass. The two explicitly selected manual PocketIC probes pass (36 measured
calls); ordinary full suites remain user-owned. No Cargo/version changes,
commits, pushes or downstream edits.

Incremental scope: six Rust files, approximately **+7 net production lines**,
zero net test lines, plus root/detail notes, tracker and this report. Two Rust
files carry the ownership change; the other four are direct caller propagation.
One deep-copy boundary and one wrapper type are removed. No independent behavior
axis or semantic/recovery route is added. Existing marker/guard authority remains
the convergence point; ownership is simpler.

Logs: `/tmp/icydb-journal-owner-{tests,heap,boundaries,clippy,cycles,cycles-repeat}.log`.
Baseline measurements: `/tmp/icydb-write-cost-cycles-after.log`.
The only unresolved measurement follow-up is the reproducible rejection-control
increase above; no production performance ceiling is changed.

