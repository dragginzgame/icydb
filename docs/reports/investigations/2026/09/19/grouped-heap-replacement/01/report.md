# Bounded grouped heap replacement — 2026-09-19

## Scope and inputs

Compare the local 0.259.3 candidate immediately before and after F38, not the
published 0.259.2 release. HEAD is
`46a054b6466c75e63e3ad93475188013139500dd`; both actors include the already-dirty
F33–F37 changes. The only actor-source difference is `peek_mut` root replacement
in grouped COUNT `count/window.rs` and generic `generic/page_finalize.rs`.
Added native tests, the host measurement and documentation are not actor code.
Unbounded staging is unchanged by explicit user choice (F39 deferred).

Both artifacts use the existing retained Cargo/post-link builder for `sql_perf`:
Rust 1.98.1, `wasm-release`, SQL enabled, Candid export enabled, LocalTest profile.
The profile uses size optimization, fat LTO, one codegen unit and panic abort.
No Cargo manifest, toolchain, lockfile, actor or schema source was edited.

Shared SHA-256 inputs:

- Cargo.lock: `a4ba8bd930774bc7e00531bc361c668e1af4e784d68f68d17a1c597ab5f107c3`
- Actor `canisters/audit/sql_perf/src/lib.rs`: `1b56f404e581a9311f4f130c20dbf47877e706b0c985c88faf87f1e899698e13`
- Schema `schema/audit/sql_perf/src/sql_perf.rs`: `07459fd471c7626a14bbc3b76f27fb9c21263cee1d1f192a871c0d64e0463e56`

Changed owner SHA-256, before → after:

- COUNT window: `d8d57ec28ad4e482d2c87a7072545716e6ef905fbc448df58b72c9d7924fbc85` → `9a8c6785c109319c678fd45d7ef9ad243d90332a647002af342e2620d93a04c4`
- Generic page finalization: `47d7597cf8bd256d299f0c6256f49fb258a810ae95b0770047ef991fd35f65e7` → `7b69c1b902439ae51a479e626674d34134d5fe5e9ff8ae14b0196d9cfa32ed0b`

| Artifact | Raw bytes | SHA-256 |
| --- | ---: | --- |
| Before | 4,477,650 | `3b59e21cac6bbcfb5084e8efd4075f8576a7b06bc88ce69f59bb66767e2bcf39` |
| After | 4,477,458 | `1cf7a92d3d7471b5da0bd678d19da8ce4db7b915567c2adaa2dea6556150e623` |

Raw Wasm delta: **-192 bytes**. Gzip and defined-function deltas unmeasured.

## Probe and observations

The manual `grouped_heap_wasm_cost_matrix` test in
`testing/integration/tests/sql_perf_audit.rs` reuses maintained actor seeds.
Six distinct, unindexed rank groups force materialized selection. Each shape
uses a fresh disposable PocketIC fixture; each fixture executes the same update
three times (compile, first reuse, repeated reuse). These phase labels describe
call position, not an independently measured cache-hit counter.

Queries all use `FROM PerfAuditUser GROUP BY rank` and `LIMIT 1`:

- COUNT: `SELECT rank, COUNT(*)`, `ORDER BY rank DESC`.
- Generic: `SELECT rank, SUM(age)`, `ORDER BY rank DESC`.
- Top-k: `SELECT rank, SUM(age)`, `ORDER BY SUM(age) DESC`.

Instruction samples cover the maintained endpoint's request-ready through
query-complete interval, including session acquisition/drop. Cycles are the
whole update's before/after cycle-balance difference. Installation, reset/seed,
and 64 settling ticks before each call are excluded. No IC time advance or
automatic progress loop is used. The two units have different scopes even
though their before/after deltas happen to match here.

| Shape / phase | Instructions before | After | Cycles before | After | Delta in each unit |
| --- | ---: | ---: | ---: | ---: | ---: |
| COUNT / compile | 4,653,386 | 4,649,334 | 17,284,244 | 17,280,192 | -4,052 |
| COUNT / hit | 4,243,119 | 4,239,067 | 17,805,893 | 17,801,841 | -4,052 |
| COUNT / repeat | 4,085,388 | 4,081,336 | 17,980,573 | 17,976,521 | -4,052 |
| Generic / compile | 4,868,583 | 4,864,164 | 17,417,249 | 17,412,830 | -4,419 |
| Generic / hit | 3,943,378 | 3,938,959 | 17,906,261 | 17,901,842 | -4,419 |
| Generic / repeat | 4,347,864 | 4,343,445 | 18,083,808 | 18,079,389 | -4,419 |
| Top-k / compile | 4,649,956 | 4,643,121 | 17,196,515 | 17,189,680 | -6,835 |
| Top-k / hit | 3,791,621 | 3,784,786 | 17,674,749 | 17,667,914 | -6,835 |
| Top-k / repeat | 4,193,837 | 4,187,002 | 17,851,622 | 17,844,787 | -6,835 |

Results and cursor bytes match between artifacts and repeated calls. Selected
rank is 43; COUNT is 1, SUM(age) is 43, consistent with the maintained seeds.
Native regressions independently check sorted COUNT windows and aggregate top-k
values/order, including ties; the measurement is not the sole correctness test.

Savings are approximately 0.087–0.180% of sampled query instructions. This tiny
six-group workload is not a high-cardinality scaling study, an overall .3
performance comparison, or a new production ceiling. No timing metric is used.

## Qualification and lifecycle

- 40 focused SQL-enabled and 22 SQL-disabled core tests pass, none ignored.
- Both manual measurement runs pass when explicitly selected (nine calls each).
- Strict core all-feature/all-target and focused measurement-target lint pass;
  formatting and whitespace checks pass. SQL-only test compilation retains 65
  existing unused-code warnings.
- Initial sandboxed PocketIC startup failed to bind localhost; its stalled test
  process was stopped. The exact retained baseline artifact was then loaded
  using `ICYDB_PREPARATION_WASM` with local-network permission, without rebuild.
  The successor used the ordinary retained build. Both successful runs created
  and dropped three disposable PocketIC instances. No application network changed.
- Logs: `/tmp/icydb-grouped-heap-{before,before-local,after,sql,no-sql,clippy,probe-clippy}.log`.
  The retained artifact entry keys are
  `8320ab0eb1909885b22ed82b7909c0c3e8554f56c75ee486ec4dc874ca6ac9c9` (before)
  and `9863455cf06613b39856aa7067c87483dc0e05b0a118731c3eba37a55af3269c` (after).
  Local caches/logs are disposable; exact hashes and observations are recorded here.

Incremental implementation footprint: five Rust files, -2 net production lines
and +154 test/measurement lines; four documentation files. Two existing owners
use a standard-library root guard instead of two heap operations. No new helper,
cache, mode, persisted state, endpoint or budget. Full-suite validation and
publication remain user-owned.
