> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

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
[preceding report](returning-selected-cells.md).
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

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/19/write-cost-reduction/01/report.md).
