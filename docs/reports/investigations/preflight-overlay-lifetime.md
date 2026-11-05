> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

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

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/19/preflight-overlay-lifetime/01/report.md).
