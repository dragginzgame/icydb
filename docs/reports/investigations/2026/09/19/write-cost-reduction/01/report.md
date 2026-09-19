# Write-cost reduction: CRC32C, count-only output and preserved slots

Date: 2026-09-19. Active release notes: 0.259.5. Items F46–F48 were explicitly
authorized together; the preceding F45 edits are preserved.

## Implemented scope and safety

- F46 replaces per-byte bit iteration with one compile-time CRC32C byte table
  at db/database_format. The checksum polynomial, initialization, final
  complement and all corruption gates are unchanged. There is one production
  implementation and no format revision, compatibility decoder or new dependency.
- F47 derives the shared writer's existing capture_output_values flag from SQL
  RETURNING presence, covering INSERT, UPDATE and DELETE. Count-only writes
  retain row cardinality without allocating their result Value payloads. Full
  row validation, constraints, Identity, staging and commit are unchanged.
  Dynamic results and RETURNING continue to capture values. Unused descriptor
  arguments are removed from both internal batch entrypoints, with callers
  updated directly and no wrapper.
- F48 skips the second canonical encoding of preserved/historical-fill slots
  during update logical-change comparison. Phase 2 has already validated and
  canonically encoded those same baseline values. Assigned/defaulted fields
  still compare normally; managed timestamp validation and refresh remain at
  their original owner. This does not reuse unvalidated raw field bytes, remove
  whole-row validation, or redesign canonical-before/commit preparation.

No new public mode, configuration, cache, persisted state or execution route.
The existing internal output-capture choice is now derived from SQL result
shape for complete batches as well as the existing bounded-prefix use.

## Matched baseline and source identity

HEAD remains ec00262af954e9b9d01b9dc62f06cd59d911c0d1 (v0.259.4).
The baseline is **HEAD plus F45**, not the published actor: reuse the successfully
measured final F45 artifact from the
[preceding report](../../returning-selected-cells/01/report.md).
Its raw size is 4,218,395 bytes and SHA256 is
`29d428d39787c35e5db050ceba412996e26e5ab019cc3f3d26b486b5194bfb7e`.
Its retained post-link key is
`31114923673aadf980e98f98d65add7880876bca04ba6785360842e55b9ad8ad`.
The comparison attributes the combined F46–F48 change, not individual savings.

Both actors use Rust 1.98.1, wasm32-unknown-unknown, wasm-release (opt-level z,
fat LTO, one codegen unit, panic abort, symbol stripping), SQL enabled,
Candid export enabled and LocalTest. Defaults are disabled; maintained explicit
features are candid-export,local-sql-query,test-admin-api. No actor endpoint or
schema fixture changed. The ordinary retained Cargo/post-link owner builds
canister_test_sql; retained artifacts stay alive until their bytes are read.

Shared Cargo.lock SHA256:
`74d4a2830b1600476e4322867593bcd6fc40238c29b23e503788c2f09cfbe235`.
Shared unchanged host probe (testing/integration/tests/sql_canister.rs) SHA256:
`f1c8f4568b452261929dc2c7b823f2ed7664f167cef9430b551cf58a3f71ca21`.

The five changed production owners were unchanged from HEAD in the F45 baseline.
Current whole-file SHA256, including cfg-test text:

| Owner | SHA256 |
|---|---|
| db/database_format/mod.rs | `6301b4b7bdc10022427be63f8d18c416c514c151ce64cccf6f7246aea68a5acc` |
| db/data/persisted_row/patch.rs | `53e34620df65d6d27aa37b062544181c900987608482ef81f111d7455bb59ea4` |
| db/session/write.rs | `f2b6edc735e26e8b3a5a30853e077ffec8a3bca372527e98eeae5b4340ad4499` |
| db/session/sql/execute/write/mod.rs | `7940ac78e7a089b697d8af38d9841858c476b274bb23b46a80cdd1007fc85570` |
| db/session/sql/execute/write/delete.rs | `c8280b4d46dc23a55eda3a49c8e497ae91fccf1bc6956221204b35fc41e8cd13` |

## Measurement contract

Reuse returning_selected_cells_wasm_cost_matrix without modifying its fixture,
SQL or observations. Six fresh disposable PocketIC cases cover small/wide rows
and count/id/all result shapes; three exact-ID UPDATEs per case set successive
values 37, 38 and 39. The wide fixture contains a 1,050,000-byte unreturned text
field. Calls are sequential samples with changing SQL literals, not declared
cache-hit or statistical-replication trials.

Cycles cover the complete single measured update endpoint call via canister
balance differences. The maintained actor instruction interval covers request
execution, not ingress/egress Candid encoding. Installation, reset/seed, 64
settling ticks before each message and independent postreads are excluded.
No IC time advance or auto-progress is enabled.

Accepted results must have one row and the expected ID where selected.
Postreads verify updated values. Wide RETURNING * must reject with the typed
SQL_WRITE_RETURNING_RESPONSE_TOO_LARGE error before changing the stored value
of 35. Response hashes cover the Candid result only, not its instruction count.
No attribution subtraction across unlike query shapes is used.

## Results

All 18 before/after response hashes match. Fresh execution passes all result,
typed rejection and independent stored-value assertions. These are aggregate
savings from the three changes together; individual attribution was not measured.

| Row | RETURNING | Cycles saved per call | Cycle reduction | Instructions saved per call |
|---|---|---:|---:|---:|
| small | count | 630,151–639,946 | 3.545%–3.654% | 630,964–641,078 |
| small | id | 591,972–591,972 | 2.958%–3.055% | 591,972–591,972 |
| small | all | 591,972–591,972 | 2.943%–3.038% | 591,972–591,972 |
| 1.05 MB | count | 214,768,276–214,773,143 | 35.471%–35.488% | 214,768,276–214,773,143 |
| 1.05 MB | id | 205,341,212–205,341,212 | 33.801%–33.820% | 205,341,212–205,341,212 |
| 1.05 MB | all (rejected) | 25,357,280–25,357,280 | 13.167%–13.222% | 25,357,280–25,357,280 |

Small-row updates save roughly 2.9–3.7% of whole-message cycles. Wide count-only
updates save about 214.77 million (35.5%); wide RETURNING id saves 205.34 million
(33.8%). Rejected wide RETURNING * saves 25.36 million (13.2%), a separate
failure-path measurement, not a successful large response.

Raw Wasm increases **1,255 bytes**, from **4,218,395 to 4,219,650** (+0.030%).
Final raw SHA256:
`2fa4b908662a046eccd560f41ba4ee06b95a7c17ffa0a44157f92a1d61752524`.
Final retained post-link key:
`89f732fc80d19b33810959e7b0fc1b35f33700b38cb005e9bc6779a4e23a6eb7`.
Both artifacts are retained under the namespace/root documented in the F45
report; the baseline file's actual hash/size were rechecked during this run.

This is a worthwhile measured tradeoff for this actor and these writes. It is
not a universal workload percentage or production ceiling. SQL INSERT/DELETE
are covered behaviorally but were not separately cycle-measured. No native
timing, gzip or defined-function-count comparison is claimed.

### Per-message observations

| Row | RETURNING | Call | Instructions before | Instructions after | Cycles before | Cycles after |
|---|---|---:|---:|---:|---:|---:|
| small | count | 1 | 5,615,392 | 4,979,303 | 17,404,156 | 16,768,132 |
| small | count | 2 | 5,613,548 | 4,982,584 | 17,776,881 | 17,146,730 |
| small | count | 3 | 5,531,691 | 4,890,613 | 17,965,358 | 17,325,412 |
| small | id | 1 | 7,519,214 | 6,927,242 | 19,374,406 | 18,782,434 |
| small | id | 2 | 7,739,621 | 7,147,649 | 19,825,042 | 19,233,070 |
| small | id | 3 | 7,926,766 | 7,334,794 | 20,010,202 | 19,418,230 |
| small | all | 1 | 7,592,415 | 7,000,443 | 19,485,024 | 18,893,052 |
| small | all | 2 | 7,816,630 | 7,224,658 | 19,936,692 | 19,344,720 |
| small | all | 3 | 7,834,478 | 7,242,506 | 20,115,404 | 19,523,432 |
| 1.05 MB | count | 1 | 593,256,982 | 378,483,839 | 605,202,554 | 390,429,411 |
| 1.05 MB | count | 2 | 592,948,952 | 378,180,676 | 605,434,075 | 390,665,799 |
| 1.05 MB | count | 3 | 592,547,793 | 377,779,441 | 605,470,766 | 390,702,486 |
| 1.05 MB | id | 1 | 595,149,613 | 389,808,401 | 607,164,615 | 401,823,403 |
| 1.05 MB | id | 2 | 594,899,272 | 389,558,060 | 607,470,773 | 402,129,561 |
| 1.05 MB | id | 3 | 594,778,599 | 389,437,387 | 607,509,361 | 402,168,149 |
| 1.05 MB | all (rejected) | 1 | 179,785,915 | 154,428,635 | 191,782,569 | 166,425,289 |
| 1.05 MB | all (rejected) | 2 | 179,895,506 | 154,538,226 | 192,368,144 | 167,010,864 |
| 1.05 MB | all (rejected) | 3 | 180,023,357 | 154,666,077 | 192,576,318 | 167,219,038 |

## Validation, failures and exclusions

- Final SQL-enabled focused selection: **11 passed**, none failed/ignored.
  Includes checksum vectors and bit recurrence, narrow integer patches,
  managed timestamps, not-null identity, count-only/RETURNING semantics,
  response-limit atomicity, targeted rules, Identity/precommit rejection,
  corrupt control checksum, and late malformed journal/row rejection.
- Final SQL-disabled focused selection: **5 passed**, none failed/ignored:
  checksum, the three patch-owner tests, and corrupt control checksum.
- **make clippy passes**, including workspace all-target and the maintained
  SQL/core and actor feature-specific gates. Formatting and whitespace pass.
  SQL-enabled native tests retain the same 65 pre-existing unused-code warnings.
- Fresh explicitly selected manual PocketIC probe: **1 passed**, none ignored,
  covering 18 measured calls; paired with the preceding run's 18 baseline calls.
  This existing opt-in measurement test remains ignored in normal test runs.
- Development failures were corrected before final qualification: a missed
  internal constructor call and seven obsolete test descriptor bindings after
  the hard cut; a new UPDATE test used the wrong frontend and correctly received
  code 189, then was changed to the maintained exact-update API; lint rejected
  truncating casts and the enlarged width-matrix helper. Casts were replaced;
  no-op coverage shares the existing matrix, whose cohesive length has a
  documented lint expectation. No runtime check was weakened to pass tests.
- Full repository/workspace test execution and publication remain user-owned.
  Cargo versions and Cargo.lock are unchanged. No commit, push or downstream
  application edits were performed.

Network lifecycle: this run created and dropped **six disposable local
PocketIC instances**, using the local backend server. No application network
was started, stopped or changed.

Logs are local ephemeral evidence:

- Baseline: `/tmp/icydb-returning-cycles-after-final.log`.
- Current: `/tmp/icydb-write-cost-cycles-after.log`.
- SQL tests: `/tmp/icydb-write-cost-boundaries-final.log`.
- SQL-disabled tests: `/tmp/icydb-write-cost-no-sql.log`.
- Complete lint gate: `/tmp/icydb-write-cost-make-clippy-qualified.log`.
- Earlier failing native/lint attempts: `/tmp/icydb-write-cost-tests.log`,
  `/tmp/icydb-write-cost-boundaries.log`,
  `/tmp/icydb-write-cost-clippy.log` and
  `/tmp/icydb-write-cost-make-clippy{,-final}.log`.

## Complexity and handoff

Incremental F46–F48 footprint: six Rust files, approximately **+32 net production
lines and +119 net test lines**, plus root/detailed release notes, status and
this report. Existing F45 work is preserved. The complete candidate has nine
Rust files (+51 production, +390 test/measurement lines) and five documentation
files including the two measurement reports.

Runtime work is reduced at existing owners. The static checksum table adds
fixed read-only data, not a cache or lifecycle. Count-only output reuses the
existing internal writer choice; provenance remains the single owner of
preservation facts. No duplicated semantic flow or compatibility route is added.
No broader canonical-row/commit rewrite is included in this handoff.
