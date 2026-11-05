> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

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

Do not subtract count-only from id results as a pure projection cost:
different query shapes and allocation histories make that a separate question.
No production ceiling, gzip size, function count or native timing is claimed.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/19/returning-selected-cells/01/report.md).
