# Count Candid output for SQL RETURNING admission (F50)

Date: 2026-09-19. Active notes: 0.259.5.

## Change and scope

Both size-only RETURNING checks now use the existing Candid IDLBuilder with a
checked std::io::Write counter, instead of Encode!(...).len(). The Candid encoder
still constructs its type/value buffers and decides every wire byte. Only the
final output Vec and its payload copy are removed. There is no handwritten wire
size calculation, streaming value encoder, early cutoff, new admission policy,
new public API or compatibility route. Row-count ordering, per-row conservative
checks, complete-payload checks, diagnostic facts and pre-commit rejection remain.

The preceding read-only investigation reproduced the F49 rejection cost on
both unmodified retained actors. Temporary bulk-memory instrumentation showed
one extra 1,050,036-byte reallocation copy on the third rejected call: 14 large
copies instead of 13, with no linear-memory growth during that call. Its dynamic
caller chain included a single-byte LEB128 append followed by Vec reserve and
allocator relocation. This supports heap-layout-sensitive buffer growth rather
than a new SQL execution pass. Those temporary diagnostic files were deleted by
the user for disk space; their observations remain in the conversation, not as
re-runnable artifacts in this report. This change does not claim to remove all
internal Candid reallocations.

## Source and comparison identity

HEAD remains ec00262af954e9b9d01b9dc62f06cd59d911c0d1 (v0.259.4).
Before measurements are the twice-reproduced F49 results in the
[journal ownership report](../../journal-batch-ownership/01/report.md).
The retained before actor is no longer present after cleanup, so this is a
fresh candidate run compared with a **recorded historical baseline**, not a
fresh two-actor paired run.

The incremental runtime/test edit is confined to
crates/icydb-core/src/db/session/sql/execute/write_returning/bounds.rs.
Current whole-file SHA256:
`fa92d346482b6ec4ae68df55b8d47445aac08cdb5fcaac83bb2afc53e3da00fb`.
Prior F45–F49 edits are preserved; there are no Cargo/version changes.

Rust 1.98.1 and unchanged Cargo.lock SHA256:
`74d4a2830b1600476e4322867593bcd6fc40238c29b23e503788c2f09cfbe235`.
Unchanged host probe SHA256:
`f1c8f4568b452261929dc2c7b823f2ed7664f167cef9430b551cf58a3f71ca21`.

Use the maintained retained canister_test_sql builder: wasm32-unknown-unknown,
wasm-release (opt-level z, fat LTO, one codegen unit, panic abort, stripped
symbols), LocalTest, SQL/Candid enabled, defaults disabled with explicit
candid-export,local-sql-query,test-admin-api. Retention stays alive through reading
the bytes; no additional artifact cache, copied worktree or large WAT dump.

## Measurement scope

The unchanged returning_selected_cells_wasm_cost_matrix covers small/wide rows,
count/id/all response shapes, and three sequential exact-ID UPDATEs per case
(values 37/38/39). Wide text is 1,050,000 bytes. Wide RETURNING * is a typed
pre-commit rejection with the original stored value preserved, not a successful
full-row response.

Cycles cover the measured update's balance difference. Instructions cover the
maintained actor request-execution interval, not ingress/egress Candid encoding.
Installation, reset/seed, settling ticks and verification reads are excluded.
No timing metric, peak-heap estimate, workload-wide speedup or production ceiling
is inferred. Gzip and function counts are unmeasured.

## Results

Seven focused native tests pass, none ignored, including exact Candid byte-count
parity at sizes 0/127/128/16383/16384/1050000, complete and empty envelopes,
exact-limit admission, one-byte-over rejection, counter overflow, selected-field
admission and pre-commit mutation rejection. Strict core all-target/all-feature
clippy, formatting and whitespace checks pass. The SQL-only native selection
retains the same 65 previously recorded unused-code warnings.

The unchanged manual probe passes 18 fresh calls. All response hashes match the
recorded F49 baseline; independent readbacks and typed rejection checks pass.
Six disposable PocketIC instances were created/dropped, with no application
network change. No full suites, version edits, commits or pushes were performed.

Raw actor Wasm: **4,219,672 -> 4,219,684 bytes (+12)**.
Current SHA256: `799e33260a2a3813eb43c400886903f148667154ffcb652d0a61f43112ea5b82`.
Current retained artifact:
`/home/adam/projects/icydb/target/icydb/canister-artifact-cache/.ic-testkit/artifact-sets/namespaces/823dde03c7560a3664bd3b412f48ad81c265e1cc1d77fb12be6d3b9aad847199/entries/dc26a9fa68005d757e790e733862e7709518fb31822395f1a42defd5512166c2/outputs/0000.artifact`.

Wide RETURNING * rejection saves 1,055,703–3,615,291 cycles (0.618–2.172%)
and 1,055,832–3,615,388 instructions against recorded F49.
Successful RETURNING cases save 5,955–9,441 cycles. Count-only controls have
identical measured actor instructions and +25 whole-call cycles; that small
external-interval difference is reported, not attributed to the helper.

**The prior regression is reduced, not fully resolved.** The third wide
rejection is 169,636,334 cycles, versus F49's 170,692,037 and the earlier
pre-F49 167,219,038: still +2,417,296 (+1.446%) against the pre-rise result.
The first two wide rejections are now below both historical baselines.
This is not evidence that Candid's internal value-buffer relocation disappeared;
the removed final-output copy and the internal value buffer are distinct.
Any follow-up should target the latter without duplicating the Candid wire codec
or restoring the journal clone.

Signed deltas are after minus recorded F49:

| Wide | Shape | Call | Cycles before | Cycles after | Cycle delta | Instructions before | Instructions after |
|---|---|---:|---:|---:|---:|---:|---:|
| false | count | 0 | 16769571 | 16769596 | +25 | 4981294 | 4981294 |
| false | count | 1 | 17139023 | 17139048 | +25 | 4894386 | 4894386 |
| false | count | 2 | 17323755 | 17323780 | +25 | 4890838 | 4890838 |
| false | id | 0 | 18780401 | 18774446 | -5955 | 6924235 | 6918255 |
| false | id | 1 | 19227982 | 19219418 | -8564 | 7142034 | 7133445 |
| false | id | 2 | 19419911 | 19411438 | -8473 | 7335283 | 7326785 |
| false | all | 0 | 18895491 | 18888606 | -6885 | 7002565 | 6995655 |
| false | all | 1 | 19343304 | 19334381 | -8923 | 7221396 | 7212448 |
| false | all | 2 | 19529141 | 19519700 | -9441 | 7247727 | 7238261 |
| true | count | 0 | 384559789 | 384559814 | +25 | 372936296 | 372936296 |
| true | count | 1 | 387202569 | 387202594 | +25 | 374560779 | 374560779 |
| true | count | 2 | 387243664 | 387243689 | +25 | 374322596 | 374322596 |
| true | id | 0 | 395979179 | 395971408 | -7771 | 384282729 | 384274933 |
| true | id | 1 | 398687158 | 398678980 | -8178 | 386279281 | 386271078 |
| true | id | 2 | 398713776 | 398704752 | -9024 | 385984988 | 385975939 |
| true | all | 0 | 166427356 | 162812310 | -3615046 | 154751511 | 151136440 |
| true | all | 1 | 166846354 | 163231063 | -3615291 | 154215080 | 150599692 |
| true | all | 2 | 170692037 | 169636334 | -1055703 | 158140282 | 157084450 |

## Complexity and remaining qualification

Incremental scope: one Rust file, about **+25 production / +66 test lines**,
plus root/detail notes, tracker and this report. A small private I/O adapter
replaces two allocate-then-count sites; there is no separate encoding authority,
behavior axis or recovery route. Output retention is simpler, but this is not
a zero-allocation Candid encoder.

Full suites remain user-owned. The before actor and old temporary logs were
removed in cleanup, so the comparison cannot be described as a fresh paired
execution. New logs are /tmp/icydb-returning-count-{tests,clippy,cycles}.log;
no temporary worktree or large WAT dump was recreated. The remaining third-call
regression is explicitly open; no new production ceiling is proposed.

