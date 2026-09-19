# Selected RETURNING cells: matched Wasm cost measurement

Date: 2026-09-19. Scope: F45, active 0.259.5 notes.

## Result

Borrowing the full row and cloning only selected cells reduces the measured
single-row UPDATE RETURNING id cost. Small rows save 11,774–19,387 cycles
(0.059–0.097% of the complete measured update call); rows with a 1,050,000-byte
unreturned text field save 2,113,876–2,116,417 cycles (0.347%).
Corresponding instruction savings are 13,665–18,936 and
2,113,793–2,116,190. Count-only and RETURNING * controls are exactly unchanged
in both metrics, including the oversized RETURNING * rejection.

Raw actor Wasm grows from 4,217,387 to 4,218,395 bytes: +1,008 bytes (+0.024%).
This is a small, scoped cycle gain with a small code-size tradeoff, not a
general database speedup. Full-row decoding and writing remain; this probe
does not separately attribute their cost.

## Source and artifact identity

Before source: HEAD `ec00262af954e9b9d01b9dc62f06cd59d911c0d1`
(tag v0.259.4), archived into `/tmp/icydb-returning-cycles.qipsfe`.
After source: that same HEAD plus the F45 worktree edits in the two
write_returning owners. Native tests, host measurement and documentation
changes do not compile into the actor. No actor or schema fixture edits.

Both use Rust 1.98.1, the same Cargo home and lockfile, wasm32-unknown-unknown,
wasm-release (opt-level z, fat LTO, codegen-units 1, panic abort, symbol stripping),
SQL enabled, Candid export enabled and LocalTest. The maintained actor build
resolves defaults disabled with features
`candid-export,local-sql-query,test-admin-api`.
The ordinary retained Cargo/post-link owner builds package canister_test_sql;
retention stays alive until the Wasm bytes are read. Canonical path remapping
maps workspace roots to /w, registry to /c and Rust sources to /r.

Shared SHA256 inputs:

- Cargo.lock: `74d4a2830b1600476e4322867593bcd6fc40238c29b23e503788c2f09cfbe235`.
- canisters/test/sql/src/lib.rs: `959070ef568e8c9b233bc69625d9c6986133a469a46e9e500bc453fbb3a7475b`.
- schema/test/sql/src/sql.rs: `7821fe3b48034f04fcae7d9435309587fa38edfe02b7da034235fae2f2e0e681`.
- Identical host sql_canister.rs probe in both trees:
  `f1c8f4568b452261929dc2c7b823f2ed7664f167cef9430b551cf58a3f71ca21`.

Changed source SHA256 (whole files, including cfg-test text):

| Owner | Before | After |
|---|---|---|
| projection.rs | `cf965c06b063a2d9fbd99728f57ecab651c221fca96fe9c7d1b11cc60b66bba9` | `c48ce2b757e00fc5a3c9162ea558c0338e72783963dd933cdd7accfcc3a7a9a8` |
| bounds.rs | `eaaee38b3adc166661792c71c2743c1a2c7e97ea012e3ad79052706bd959a6c3` | `a30318beffc865bd222222238e7b731c625405bd2e8cb2387a47cb5e55cd2ab0` |

| Identity | Before | After |
|---|---|---|
| Raw Wasm SHA256 | `46be6625c61164c4dc3d967dea84ca99e104d91c2214e4411250df3b05e428ef` | `29d428d39787c35e5db050ceba412996e26e5ab019cc3f3d26b486b5194bfb7e` |
| Cargo artifact fingerprint | `2df44b473275ed1f59100dc66de8cac7216558cd5bd8323b1da80fb982add0dd` | `2a7053b783485a83f84c204dcf3f64afc5b80fdb5866a686ea83656f37df383a` |
| Retained post-link key | `f4ca53e498a94192a21296fe2fe27b12ab976df5b81be1a0e44d6ec126e6602a` | `31114923673aadf980e98f98d65add7880876bca04ba6785360842e55b9ad8ad` |

Retained artifact root:
`target/icydb/canister-artifact-cache/.ic-testkit/artifact-sets/namespaces/823dde03c7560a3664bd3b412f48ad81c265e1cc1d77fb12be6d3b9aad847199/entries/<post-link-key>/outputs/0000.artifact`.

## Method and attribution

The explicitly selected manual test
`returning_selected_cells_wasm_cost_matrix` in
testing/integration/tests/sql_canister.rs reuses the maintained actor's
`measure_trusted_sql_exact_update_instructions` endpoint. It adds no production
endpoint, report type, cache, build owner or runtime mode.

For each actor, six fresh disposable PocketIC instances cover small/wide rows
and count/id/all response shapes. Reset and deterministic seed occur before
measurement. Each case performs three exact-ID UPDATE calls with successive
int32_value literals 37, 38 and 39. Calls are numbered in order, not labeled
cold/warm: changing SQL literals do not establish exact command-cache hits.

Before each measured message, 64 ticks drain queued work without advancing IC
time. Cycles are the canister balance difference surrounding the single
measurement update call. They include the whole endpoint's message/encoding
cost. Instructions cover the actor's existing request execution interval,
excluding ingress/egress Candid encoding. These are different scopes, so their
absolute counts need not coincide.

Installation, fixture reset/seed, settling ticks and separate verification reads
are outside cycle intervals. Every accepted result has one row and the selected
ID where applicable. Postreads confirm the requested stored value. Wide/all
must return the typed SQL_WRITE_RETURNING_RESPONSE_TOO_LARGE error and leave
the stored value at 35. SHA256 of the Candid-encoded result (excluding the
instruction counter) matches in all 18 before/after pairs.

## Per-message results

Negative cycle deltas mean savings. Instructions and cycles are independent
observations, not converted from one another.

| Row | RETURNING | Call | Instructions before | Instructions after | Cycles before | Cycles after | Cycle delta |
|---|---|---:|---:|---:|---:|---:|---:|
| small | count | 1 | 5,615,392 | 5,615,392 | 17,404,156 | 17,404,156 | 0 |
| small | count | 2 | 5,613,548 | 5,613,548 | 17,776,881 | 17,776,881 | 0 |
| small | count | 3 | 5,531,691 | 5,531,691 | 17,965,358 | 17,965,358 | 0 |
| small | id | 1 | 7,532,879 | 7,519,214 | 19,387,159 | 19,374,406 | -12,753 |
| small | id | 2 | 7,753,482 | 7,739,621 | 19,836,816 | 19,825,042 | -11,774 |
| small | id | 3 | 7,945,702 | 7,926,766 | 20,029,589 | 20,010,202 | -19,387 |
| small | all | 1 | 7,592,415 | 7,592,415 | 19,485,024 | 19,485,024 | 0 |
| small | all | 2 | 7,816,630 | 7,816,630 | 19,936,692 | 19,936,692 | 0 |
| small | all | 3 | 7,834,478 | 7,834,478 | 20,115,404 | 20,115,404 | 0 |
| 1.05 MB | count | 1 | 593,256,982 | 593,256,982 | 605,202,554 | 605,202,554 | 0 |
| 1.05 MB | count | 2 | 592,948,952 | 592,948,952 | 605,434,075 | 605,434,075 | 0 |
| 1.05 MB | count | 3 | 592,547,793 | 592,547,793 | 605,470,766 | 605,470,766 | 0 |
| 1.05 MB | id | 1 | 597,265,697 | 595,149,613 | 609,281,032 | 607,164,615 | -2,116,417 |
| 1.05 MB | id | 2 | 597,013,065 | 594,899,272 | 609,584,649 | 607,470,773 | -2,113,876 |
| 1.05 MB | id | 3 | 596,894,789 | 594,778,599 | 609,625,430 | 607,509,361 | -2,116,069 |
| 1.05 MB | all (rejected) | 1 | 179,785,915 | 179,785,915 | 191,782,569 | 191,782,569 | 0 |
| 1.05 MB | all (rejected) | 2 | 179,895,506 | 179,895,506 | 192,368,144 | 192,368,144 | 0 |
| 1.05 MB | all (rejected) | 3 | 180,023,357 | 180,023,357 | 192,576,318 | 192,576,318 | 0 |

Do not subtract count-only from id results as a pure projection cost:
different query shapes and allocation histories make that a separate question.
No production ceiling, gzip size, function count or native timing is claimed.

## Qualification and orchestration

Both selected measurement executions pass: 18 measured calls per actor,
36 total, plus independent readbacks. The manual probe is intentionally ignored
by normal runs and was explicitly executed here; it is not a mandatory release
qualification silently skipped. Strict focused integration-test clippy passes.
The prior eight focused native tests and strict core lint passed on the
unchanged F45 runtime sources. Formatting and whitespace checks pass.
Full repository/workspace validation remains user-owned.

One initial current-tree attempt was discarded before fixture startup:
the shared native Cargo target reused a host binary linked from the temporary
baseline tree. Inspection of embedded roots and dependency metadata exposed
the mismatch. That exact test process and Cargo parent were stopped.
After invalidating/rebuilding the current host test unit, its compile log,
dependency manifest path and executable root identified the current workspace.
Only the final current run below contributes measurements. A temporary comment
used to invalidate the host unit was removed; integration/src/lib.rs has no
content diff. No alternate runtime/build protocol was introduced.

Network lifecycle: the successful runs created and dropped 12 disposable local
PocketIC instances. The discarded duplicate was stopped before creating a
fixture. No application network was changed.

Local evidence (temporary, not committed):

- Before: `/tmp/icydb-returning-cycles-before.log`.
- After: `/tmp/icydb-returning-cycles-after-final.log`.
- Focused lint: `/tmp/icydb-returning-probe-clippy.log`.
- Current host rebuild: `/tmp/icydb-returning-current-host-final.log`.
- Discarded attempt: `/tmp/icydb-returning-cycles-after.log`.

## Complexity

Complete F45 candidate: four Rust files, approximately +19 net production lines
and +271 test/measurement lines (166 native, 105 host). Four documentation files
include this report, the root ledger, detailed notes and tracker. One existing
projection owner replaces two full-row clone sites with selected-cell borrowing;
no independent behavior axis or retained state. This follow-up adds measurement
and documentation only, not another runtime change. Cargo versions, lockfile,
commits and publication are untouched.

