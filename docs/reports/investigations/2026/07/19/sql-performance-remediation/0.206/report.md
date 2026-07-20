# 0.206 SQL Performance Remediation Investigation

Status: complete; the corrected Slice 2 candidate and hard-cut subject identity
passed exact three-run cohort review, the ordinary baseline gate reproduced all
maintained evidence, and reviewed run `29751989919` is selected.

## Decision

The accepted post-0.205 cohort freezes a 14-family leading set. Exact selection
run `29696076149` freezes 206-003 as the sole `OptimizeNow` finding. The selected
family is
`scale.user.unsupported_order.all.window10`: at 2,048 rows it costs 104,312,717
instructions, including 35,243,583 in the kernel order-window phase, and the
current source retains the complete qualifying row set before applying the
bounded expression order.

The Slice 1 measurement patch added only
`kernel_row_peak_retained_candidates` at the canonical kernel scan collector.
It changed neither semantics nor dispatch. The exact current-source run records
16, 256, and 2,048 retained candidates at the corresponding cardinalities, so
the predeclared 2,048-to-at-most-11 structural budget is now measured and
frozen. The direct-field incompatible-order guard already records 11 retained
candidates at 2,048 rows.

No directly coupled family is proposed. The leading incompatible-filter/order
family already uses direct-field bounded kernel collection and does not share
the missing expression-order key materialization invariant.

## Evidence Identity

The opening evidence is the accepted exact 0.205.2 closeout cohort. Raw bundles
remain workflow artifacts and are not copied into the repository.

| Role | Run ID | Source revision | Profile / environment identity | Artifact | SHA-256 |
| --- | --- | --- | --- | --- | --- |
| opening P2 | `29691074553` | `8bba5367e58a9fffc39754cdb0b117c768cb87e1` | profile 1; P1 `a6823a84`; P2 `306c6bd3`; accepted snapshot `04c1f5d6` | `sql_perf_p2_report.json` | `19bf38762537b5699c0d535b410ac4b2f0e57525502f947e10fe35ca86f0dc10` |
| opening scale | `29691074553` | `8bba5367e58a9fffc39754cdb0b117c768cb87e1` | profile 1; scale `afa7c342`; fixture `66ae745f` | `sql_perf_scale_report.json` | `94b33a88f03cfdcee4f157003ebf9126eb734ddd6b4afc95b576f0a7a8f0d664` |
| opening P1 | `29691074553` | `8bba5367e58a9fffc39754cdb0b117c768cb87e1` | profile 1; P1 `a6823a84`; accepted snapshot `04c1f5d6` | `sql_perf_deterministic_matrix.json` | `f7f747ac26b4694aaf350d9606597c971d04a2015187083373709eac7cde3474` |
| cohort review | `29693321199` | `8bba5367e58a9fffc39754cdb0b117c768cb87e1` | cohort `0.205.2-8bba5367e5-closeout`; three accepted ordinals | `sql_perf_calibration_review.json` | `43dcb041a58442ca9e2738fb5d2e52f9c7b26605ee1c944a0f95b69baf2b82a7` |
| selection P1 | `29696076149` | `a30b71d27e2c6d3611217357a2f9c8c9c2a96a9c` | profile 1; diagnostics schema 2; P1 `a6823a84` | `sql_perf_deterministic_matrix.json` | `2dc13c033e0052947af883a1d2ec0cd0c5afd54136d544e1f2f72c0ed95b2b12` |
| selection scale | `29696076149` | `a30b71d27e2c6d3611217357a2f9c8c9c2a96a9c` | profile 1; scale `afa7c342`; fixture `66ae745f` | `sql_perf_scale_report.json` | `05a972becb1721f6195952855539b69050dcb2ad6b3e17353bc4f9202f9f5b97` |
| selection P2 | `29696076149` | `a30b71d27e2c6d3611217357a2f9c8c9c2a96a9c` | 424 confirmations; P2 `306c6bd3` | `sql_perf_p2_report.json` | `4a174b383b2de9773ad9bfab4f281d682783d9ee0ecea4776c9960c894e19f82` |
| selection attribution | `29696076149` | `a30b71d27e2c6d3611217357a2f9c8c9c2a96a9c` | 22,883 instructions; 350 basis points; observation only | `sql_perf_instrumentation.json` | `b7c79f4677c84e6903864339478eaf3472804ef03198e9876087f4fc177cb86c` |
| candidate P1 | `29742278537` | `f066894cb5709b64af3cb6c9d559c52ba3acfc93` | profile 1; diagnostics schema 2; P1 `a6823a84`; 1,787 passed | `sql_perf_deterministic_matrix.json` | `6f33390852890bdb7f78f0f0771657cefd4ec9acf83277b7e3223eea9195d993` |
| candidate scale | `29742278537` | `f066894cb5709b64af3cb6c9d559c52ba3acfc93` | scale `afa7c342`; fixture `66ae745f` | `sql_perf_scale_report.json` | `f4746ceab9d92885bd3bf31cf2542429c412ea7aae3af331128d2c66946b7d46` |
| candidate P2 | `29742278537` | `f066894cb5709b64af3cb6c9d559c52ba3acfc93` | 424 confirmations; P2 `6ad8c63a` | `sql_perf_p2_report.json` | `c900fc1260bea2921a6810105e1dd3887f712885c62c29e97ecca959bae20a41` |
| candidate attribution | `29742278537` | `f066894cb5709b64af3cb6c9d559c52ba3acfc93` | 22,883 instructions; 350 basis points; observation only | `sql_perf_instrumentation.json` | `0ae417caa41072ac2b2254ea77976930733c8e7f47942f80b06b0f9807b2887e` |
| candidate cohort review | `29746556799` | `f066894cb5709b64af3cb6c9d559c52ba3acfc93` | cohort `0.206.2-f066894cb-closeout`; runs `29742278537`, `29743875425`, `29744294981` | `sql_perf_calibration_review.json` | `fec75651bed485a32b145f9460db3141608111d5b23185ea5d2f04c3afd38c2d` |
| final baseline P1 | `29751989919` | `c2f5856f49192d6d1187d2e1ef1adcecea79284a` | hard-cut subject shape; 1,787 passed | `sql_perf_deterministic_matrix.json` | `720665314dfe107c585dd7cdcd4729d53016bd4ab295f26720010f7fc160ad3d` |
| final baseline scale | `29751989919` | `c2f5856f49192d6d1187d2e1ef1adcecea79284a` | scale `afa7c342`; fixture `66ae745f` | `sql_perf_scale_report.json` | `cdb5ab73cae8a20b8599db29f0cf0972920babe57b629fb7677b1f5ca08869a3` |
| final baseline P2 | `29751989919` | `c2f5856f49192d6d1187d2e1ef1adcecea79284a` | 424 confirmations; P2 `6ad8c63a` | `sql_perf_p2_report.json` | `b5744d52fc3b3db3cd47b05a65e404bc5708cc5ec2376c4df72435c3a0bf2cbc` |
| final baseline attribution | `29751989919` | `c2f5856f49192d6d1187d2e1ef1adcecea79284a` | 22,883 instructions; 350 basis points; observation only | `sql_perf_instrumentation.json` | `ad3319e05aeeed9d658fe1e62c1282807acf8c2f91ef1073ba9384ac51669a81` |
| final cohort review | `29756834526` | `c2f5856f49192d6d1187d2e1ef1adcecea79284a` | cohort `0.206.3-c2f5856f4-closeout`; runs `29751989919`, `29753941708`, `29753956883` | `sql_perf_calibration_review.json` | `db49cbbdd4ac1646fe941d8d2e49bec264b59f8f8335f347fa0c662c2f967681` |
| ordinary baseline gate | `29757215564` | `c2f5856f49192d6d1187d2e1ef1adcecea79284a` | baseline `29751989919`; passed with zero changed measurements or regressions | `sql_perf_comparison.json` | `1d9ea872fc118b2c9cac533459b9f607369969aa3027ae9402fe24e257c89153` |

The opening subject is raw Wasm SHA-256
`e40cf2756a9b714d232eff6a488b81db6939a0a3ed882863f82716e0b91008fb`,
3,915,460 non-gzipped bytes. The environment uses Rust 1.97.0, PocketIC
14.0.0, `wasm-release`, and the `diagnostics`, `sql`, and `sql-explain`
features. All three cohort ordinals produced the same P2 selection hash and
the calibration review accepted the cohort.

The selection subject is raw Wasm SHA-256
`2fd45f0fe3fe2f50e31440c727ef2aa539d9d323f5f9db01f653fef426f3b8e1`,
3,915,688 non-gzipped bytes: 228 bytes above the opening subject. It uses
diagnostics-attribution schema 2 because the retained-candidate field changes
the typed artifact shape; there is no schema-1 compatibility decoder. The
first Scale shard 5 attempt failed before IcyDB execution while downloading
PocketIC; the failed job alone was rerun, passed, and the complete workflow
then succeeded against the same revision and Wasm.

## Post-Change Diagnostic Attempts

Scheduled run `29714691461` correctly rejected the former diagnostics-schema-1
repository baseline before comparison; there is no compatibility decoder for
the removed artifact shape. Manual run `29731714587` instead selected exact
schema-2 baseline run `29696076149`. Its shared Wasm and all eight P1 and scale
shards passed, but P1 merge correctly rejected the tagged `0.206.1` subject as
incomparable because the release commit changed workspace versions in
`Cargo.lock`.

The rejected run is diagnostic evidence only. Its raw non-gzipped Wasm is
3,916,944 bytes with SHA-256
`70cfa769bf5a44d469ba0d4c8eb9eae687240ca2b26e355f8869e3dc05637abd`:
1,256 bytes above the exact selection subject. Its shard observations exposed
both the intended large-input gain and one small-input guard miss:

| Rows | Peak retained | Total instructions | Total delta | Order-window instructions | Order-window delta | Data gets | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 16 | `16 -> 11` | 1,776,019 | `+5.20%` | 135,934 | `-40.29%` | 16 | unchanged IDs 1-10 |
| 256 | `256 -> 11` | 11,398,295 | `-13.93%` | 137,682 | `-96.77%` | 256 | unchanged IDs 1-10 |
| 2,048 | `2,048 -> 11` | 87,494,924 | `-16.12%` | 137,607 | `-99.61%` | 2,048 | unchanged IDs 1-10 |

At 16 rows, scan-time expression-key evaluation added 179,555 scan-phase
instructions while canonical final ordering evaluated the 11 retained tuples
again. That duplicate work violated the predeclared no-increase guard even
though the ordinary regression threshold would not reject an 87,846-instruction
increase. The corrected implementation now carries the already-evaluated tuple
cache to canonical post-access ordering under an exact resolved-order and keep-
count proof; mismatches fail closed with the existing query-executor invariant.

Focused local measurement of the corrected candidate passes every selected
guard cardinality. This is source-dirty diagnostic evidence captured while two
registry packages had transiently refreshed, not the final comparable workflow
verdict:

| Rows | Peak retained | Total instructions | Total delta | Order-window instructions | Order-window delta | Data gets | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 16 | 11 | 1,680,600 | `-0.45%` | 55,896 | `-75.45%` | 16 | unchanged IDs 1-10 |
| 256 | 11 | 11,430,062 | `-13.69%` | 58,121 | `-98.64%` | 256 | unchanged IDs 1-10 |
| 2,048 | 11 | 88,472,993 | `-15.18%` | 58,022 | `-99.84%` | 2,048 | unchanged IDs 1-10 |

The measurement subject's raw non-gzipped Wasm is 3,921,878 bytes with SHA-256
`8f17d067a0ab5d88d5d328d2c7762284d91376c0c4c8419990253cbe30376944`:
4,934 bytes above tagged `0.206.1` and 6,190 bytes above the exact selection
subject. After restoring the checked-in lockfile, the final local candidate
build is 3,919,866 raw bytes with SHA-256
`51e3323de6ac2ee4b14098732f43181667b9ce78148ed048e3bb79e91f818d3d`:
2,922 bytes above tagged `0.206.1` and 4,178 bytes above selection. Its focused
shard attempt stopped before sampling when PocketIC could not bind its ephemeral
loopback listener, so only the raw-byte result is current-lockfile evidence.
Both local size identities are superseded by the clean remote subject below. A
focused complexity pass replaced the earlier cache DTO plus scan-row
enum with one opaque pending-row container, reducing the correction from about
211 to 123 net production lines and removing 1,571 raw Wasm bytes from the prior
current-lockfile candidate.

The exact clean candidate cohort measures raw non-gzipped Wasm SHA-256
`5cc72cd7b7d07ad7acf4ad42d3a1d4610c849f86b68521e00cd2eb863ea98d09`,
3,920,987 bytes, under lockfile SHA-256
`b44f68a4d5527b3310484ed14a57c6fee4e23dbe4bf4a1f7978ec820931336a4`.
All three ordinals are bit-for-bit identical for the selected family:

| Rows | Peak retained | Total instructions | Order-window instructions | Data gets | Result |
| ---: | ---: | ---: | ---: | ---: | --- |
| 16 | 11 | 1,685,600 | 61,137 | 16 | unchanged IDs 1-10 |
| 256 | 11 | 11,435,036 | 63,337 | 256 | unchanged IDs 1-10 |
| 2,048 | 11 | 88,478,158 | 63,387 | 2,048 | unchanged IDs 1-10 |

All 1,787 P1 scenarios passed in each ordinal. The review records 424 P2
candidates and identical 22,883-instruction, 350-basis-point attribution
overhead in all three runs. Its 31 unresolved promotion-review scenarios are
the exact same set as the accepted opening cohort: no candidate was added or
removed by Slice 2. This accepts candidate stability, not the final baseline or
the before/after comparison.

## Closeout Comparison Decision

Selection run `29696076149` and the corrected candidate cohort differ at
`Cargo.lock`: workspace versions changed from 0.206.0 to 0.206.2, and the
candidate lock resolves `serde_json` 1.0.151 and `syn` 3.0.2. Treating that
lockfile as external environment was the ownership error that made ordinary
release versioning invalidate the baseline. 0.206.3 hard-cuts the artifact
shape so the lock hash is recorded with source revision and raw Wasm as measured
subject provenance. Dependency changes remain visible, and their performance
effects participate in the candidate verdict instead of making it inadmissible.

There is no decoder for the superseded environment shape and no synthetic
historical-source or source-override workflow lane. Existing 0.206 artifacts
remain immutable historical evidence; they are not rewritten to manufacture a
new exact comparison. Ordinary run `29748512253` was cancelled during its
shared-Wasm build, before shard execution, once the ownership hard cut made its
superseded-shape result non-authoritative.

The maintained performance claims are therefore narrower and exact:

- peak retained candidates fall from an exactly observed 2,048 in the selected
  old implementation to exactly 11 in every clean candidate ordinal;
- the candidate's absolute instruction, store-call, result-signature, and raw
  Wasm observations are exact and stable across the accepted three-run cohort;
- all 1,787 candidate P1 scenarios pass and all three ordinals produce the same
  424-scenario P2 selection and attribution overhead; and
- a fresh cohort under the hard-cut subject/environment shape must become
  future regression authority after the development commit is pushed.

For historical context only, the selected 2,048-row case moves from 104,312,717
to 88,478,158 total instructions and from 35,243,583 to 63,387 order-window
instructions, approximately 15.18% and 99.82% lower respectively. The raw Wasm
moves from 3,915,688 to 3,920,987 bytes. These cross-lockfile deltas are useful
directional evidence but are not published as exact causal measurements.

Future performance slices use the direct flow: baseline, complete candidate
source-and-dependency change, comparison, then release. The workflow measures
only its trigger revision. Actual external environment changes—toolchain,
fixture, build configuration, accepted schema, PocketIC, diagnostics, or
counter policy—still fail closed and require a fresh baseline rather than a
compatibility or historical-source mode.

## Final Baseline Selection

Runs `29751989919`, `29753941708`, and `29753956883` form final cohort
`0.206.3-c2f5856f4-closeout`. Ordinals 2 and 3 reused ordinal 1's exact Wasm;
review run `29756834526` accepted the cohort. The measured subject records
source `c2f5856f49192d6d1187d2e1ef1adcecea79284a`, lockfile SHA-256
`0b7a298d024d8c4cfeb99dffe7cb72fdb850d20f6c79746be703e1069dc83e6a`,
and raw non-gzipped Wasm SHA-256
`ef400795f433de0adf1acf4888afb24d9c67667d230d095db902bf5aa0dd2c64`
at 3,921,037 bytes. `Cargo.lock` is absent from comparable environment
identity and present in measured subject identity, as required by the hard
cut. The final baseline is 5,349 raw bytes above selection run `29696076149`;
like the historical instruction delta, that cross-lockfile size delta is
contextual rather than an exact causal attribution.

The release lock also resolves newer transitive `clap`, `clap_derive`,
`hyper`, `serde_json`, and `syn` packages without changing a direct dependency
requirement in `Cargo.toml`. Dependency-graph inspection places `clap` on the
CLI, `hyper` on the integration/PocketIC runner, `serde_json` on this package's
test tooling, and `syn` on host-side macros. Any resulting code-generation or
Wasm effect is nevertheless included in the final measured subject above.

The cohort reproduces the selected family exactly at all three cardinalities:

| Rows | Peak retained | Total instructions | Order-window instructions | Data gets | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 16 | 11 | 1,685,600 | 61,137 | 16 | unchanged IDs 1-10 |
| 256 | 11 | 11,435,036 | 63,337 | 256 | unchanged IDs 1-10 |
| 2,048 | 11 | 88,478,158 | 63,387 | 2,048 | unchanged IDs 1-10 |

All three ordinals have 1,787 successful P1 scenarios, the same 424-candidate
P2 set, 22,883-instruction attribution overhead, and the same 31 unresolved
promotion-review scenarios. Ordinary run `29757215564` then rebuilt the same
source and passed the single maintained baseline flow against ordinal 1. Its
63,225 metric comparisons, 72 scale totals, 264 normalized scale costs, and 48
scale slopes contain zero changed values and zero regressions. The rebuilt raw
Wasm has the same 3,921,037-byte size but a different recorded SHA-256; this is
subject provenance rather than comparable-environment identity, and the full
measured behavior is unchanged. Repository variable
`ICYDB_SQL_PERF_BASELINE_RUN_ID` now selects run `29751989919`.

## Deterministic Leading Set

Families are deduplicated after taking the top five for each design-owned
ranking. The normalized ranking deliberately retains its declared denominator;
scores with different denominators are not interpreted as interchangeable
efficiency ratios.

### Total Instructions At 2,048 Rows

| Rank | Family | Instructions |
| ---: | --- | ---: |
| 1 | `scale.user.grouped_hash.many_groups.having_sum.window16` | 129,266,789 |
| 2 | `scale.user.grouped_hash.many_groups.sum.window16` | 128,790,464 |
| 3 | `scale.user.unsupported_order.all.window10` | 104,312,631 |
| 4 | `scale.user.grouped_hash.few_groups.distinct_nat.window16` | 83,539,111 |
| 5 | `scale.user.grouped_ordered.many_groups.count.window100` | 81,216,832 |

### Marginal Instructions Per Row, 256 To 2,048

| Rank | Family | Instructions / added row |
| ---: | --- | ---: |
| 1 | `scale.user.grouped_hash.many_groups.having_sum.window16` | 61,754.08 |
| 2 | `scale.user.grouped_hash.many_groups.sum.window16` | 61,698.99 |
| 3 | `scale.user.unsupported_order.all.window10` | 50,820.34 |
| 4 | `scale.user.grouped_hash.few_groups.distinct_nat.window16` | 40,526.20 |
| 5 | `scale.user.grouped_ordered.few_groups.count.window10` | 39,099.85 |

### Maintained Normalized Cost

| Rank | Family | Denominator | Instructions / unit |
| ---: | --- | --- | ---: |
| 1 | `scale.user.not_paginated.aggregate_quarter` | returned row | 68,123,194.00 |
| 2 | `scale.user.grouped_hash.many_groups.having_sum.window16` | index range scan | 64,633,394.50 |
| 3 | `scale.user.grouped_hash.many_groups.sum.window16` | index range scan | 64,395,232.00 |
| 4 | `scale.user.grouped_hash.few_groups.sum.window1` | returned row | 56,002,042.00 |
| 5 | `scale.user.incompatible_filter_order.quarter.window10` | index range scan | 18,324,783.00 |

### Absolute Cold Compile Plus Planner

| Rank | Family | Scenario | Instructions |
| ---: | --- | --- | ---: |
| 1 | `route.sparse_in.page_only` | `token.collection_id.sparse_in.page_only.limit50` | 9,628,941 |
| 2 | `route.sparse_in.count` | `token.collection_id.sparse_in.count` | 3,540,108 |
| 3 | `route.branch_over_cap_pruned.page_only` | `token.collection_stage_id.overcap_pruned.page_only.limit50` | 3,437,306 |
| 4 | `route.branch_set.wide_noncovered_page_only` | `token.collection_stage_id.branch_set.wide_noncovered_page_only.limit50` | 2,079,325 |
| 5 | `route.branch_set.wide_page_only` | `token.collection_stage_id.branch_set.wide_page_only.limit50` | 2,059,361 |

## Typed Classification Before Source Inspection

| ID | Family | Typed evidence | Initial classification |
| --- | --- | --- | --- |
| 206-001 | grouped hash HAVING SUM | 2,048 rows, 100 groups/states, 2 range scans, 4,096 index entries, no early stop | hash grouping is required, but the duplicate one-sided range traversal is suspicious and lacks child-level instruction attribution |
| 206-002 | grouped hash SUM | 2,048 rows, 100 groups/states, 2 range scans, 4,096 index entries, no early stop | same mixed evidence as 206-001; the artifacts do not isolate instructions spent by the duplicate traversal |
| 206-003 | unsupported expression order | 2,048 gets, 35,243,583 order-window instructions, 10 rows returned | avoidable candidate retention suspected; peak fact missing |
| 206-004 | grouped hash DISTINCT | 2,048 rows, 5 groups/states and 5 peak distinct values | required grouped DISTINCT state under current route |
| 206-005 | ordered many-group COUNT | 2,048 rows, 100 groups finalized, peak one group/state | full result consumes every group; ordered state already bounded |
| 206-006 | ordered few-group COUNT | 2,048 rows, 5 groups finalized, peak one group/state | full result consumes every row; ordered state already bounded |
| 206-007 | scalar COUNT over `active` | 2,048 gets, one result, scalar attribution absent | schema-dependent scan; reducer localization unmeasured |
| 206-008 | grouped hash few-group SUM | 2,048 rows, 4 groups/states, no early stop | required hash grouping work under current route |
| 206-009 | incompatible filter/order | 512 index entries and gets, two retained slots, 10 rows | route mismatch requires candidate scan without a compatible index |
| 206-010 | sparse `IN` page | 2,132,345 compile + 7,496,596 planner | cold work is large; canonicalization/pass count absent |
| 206-011 | sparse `IN` count | 2,304,221 compile + 1,235,887 planner | cold work is large; canonicalization/pass count absent |
| 206-012 | pruned over-cap branch page | 1,340,706 compile + 2,096,600 planner | pruning cost is large; per-member/pass count absent |
| 206-013 | wide non-covering branch page | 543,842 compile + 1,535,483 planner | branch planning cost is large; structural pass count absent |
| 206-014 | wide covering branch page | 536,992 compile + 1,522,369 planner | branch planning cost is large; structural pass count absent |

## Source Ownership And Disposition

Source inspection was performed only after the table above was frozen.

| ID | Current owner | Source finding | Disposition |
| --- | --- | --- | --- |
| 206-001 | AND-range planner | `range::extract::index_range_from_and` requires every AND child to be a literal comparison, so the residual `name = name` child prevents lower/upper range fusion; `predicate::plan_predicate` recursively produces two one-sided range children and the grouped stream traverses both before the required hash fold | `NeedsTypedMeasurement` |
| 206-002 | AND-range planner | same planner shape and symbols as 206-001; HAVING is not the material difference, and current phase attribution does not isolate the instruction cost of either child traversal | `NeedsTypedMeasurement` |
| 206-003 | scalar page order collector | `terminal/page/plan.rs`, `scan.rs`, and `executor/order.rs` bound direct-field order collection, but expression order is collected in full before cached key selection; run 29696076149 proves a 2,048-candidate peak | `OptimizeNow` |
| 206-004 | grouped aggregate bundle | `aggregate/runtime/grouped_fold/bundle.rs` owns per-group DISTINCT state; evidence matches five live distinct values | `ContractRequired` |
| 206-005 | ordered grouped COUNT fold | `aggregate/runtime/grouped_fold/count/mod.rs` already retains one group; LIMIT 100 consumes the complete 100-group result | `ContractRequired` |
| 206-006 | ordered grouped COUNT fold | same owner; all five groups and all rows are needed to finalize exact counts | `ContractRequired` |
| 206-007 | application schema | `schema/audit/sql_perf/src/sql_perf.rs` has no `active` index; engine route upgrades or hidden indexes are forbidden | `SchemaDependent` |
| 206-008 | grouped generic fold | hash mode has four required groups and no order proof for early closure | `ContractRequired` |
| 206-009 | access planner plus application schema | the existing `age,id` and `name` indexes cannot jointly satisfy age filtering and name ordering | `SchemaDependent` |
| 206-010 | prefix access planner | `query/plan/planner/prefix.rs` owns sparse membership access construction, but existing evidence cannot count repeated member work | `NeedsTypedMeasurement` |
| 206-011 | SQL predicate lowering | `sql/lowering/select/binding.rs` and predicate normalization own the compile-heavy membership form; exact repeated work is not measured | `NeedsTypedMeasurement` |
| 206-012 | prefix access planner | `query/plan/planner/prefix.rs` owns exclusion pruning and branch-cap admission; exact passes are not measured | `NeedsTypedMeasurement` |
| 206-013 | prefix access planner | branch construction is shared with 206-014, but projection coverage is a separate downstream fact and no eliminated-work counter exists | `NeedsTypedMeasurement` |
| 206-014 | prefix access planner | same planning owner as 206-013; current evidence supplies instructions only | `NeedsTypedMeasurement` |

`SchemaDependent` does not recommend changing the benchmark fixture. It states
that the required engine work is removable only through an application-declared
compatible index or a different query. Adding an index to claim an engine win
would violate 0.206.

The closeout concerns outside the sole selected family also have explicit
owners and dispositions. Materialized expression order requires the complete
scan but no longer requires complete candidate retention; 206-003 removes that
avoidable engine work. Incompatible direct-field filter/order remains
`SchemaDependent` under 206-009. The maintained residual-primary scenario reads
1,546 rows at 2,048-row scale before producing the ordered ten-row window,
retains only ten candidates, and spends no materialized-order phase work; the
residual ordered-scan owner is therefore behaving according to the accepted
schema, while removing the row reads requires a compatible application index.
Scalar aggregate scan cost is `SchemaDependent` under 206-007. Large-membership
compile/planner work is owned by SQL lowering and the prefix access planner
under 206-010 through 206-014, and remains `NeedsTypedMeasurement` rather than
an unproved optimization claim.

The grouped hash state in 206-001 and 206-002 is contract-required, but their
complete current cost is not. Existing storage counters prove two full index
traversals and 4,096 index-entry reads for 2,048 input rows. Source inspection
explains the shape, but the opening artifact does not attribute instructions to
the individual access children. The findings therefore remain
`NeedsTypedMeasurement`; they are not relabeled `ContractRequired` merely
because the downstream hash fold is semantically necessary, and they cannot
outrank 206-003 under the design's typed avoidable-phase scoring rule.

## Frozen Selection Budget

Slice 1 freezes 206-003 as the sole primary `OptimizeNow` finding with this
budget:

- family: `scale.user.unsupported_order.all.window10`;
- primary scenario: `scale.user.unsupported_order.all.window10.rows2048`;
- guard cardinalities: 16, 256, and 2,048 rows;
- mode: isolated cold scale execution;
- structural metric: `kernel_row_peak_retained_candidates`;
- opening value: 2,048 in run `29696076149`;
- required target: at most `offset + limit + lookahead`, which is 11 for the
  primary scenario;
- sampling variance: zero for the structural metric;
- instruction guardrails: total and kernel order-window medians must not
  increase; the changed family must improve outside the checked-in ordinary
  regression threshold;
- semantic guards: identical result signature, route facts, order direction,
  null and tie behavior, cursor/offset behavior, corruption failures, cache
  identity, scan bounds, and output cardinality;
- resource guards: existing scan/group/output/instruction limits remain in
  force; no byte or heap improvement is claimed without typed evidence.

## Slice 2 Implementation

The initial cursorless materialized-order scan now retains at most
`offset + limit + lookahead` rows for both direct-field and expression-backed
planner-resolved orders. Expression-backed collection evaluates and caches the
complete resolved ordering tuple once for each candidate during scan-time
selection. The tuple includes every declared term, direction, null behavior,
and the planner-appended primary-key tie-breaker. One opaque pending-row
container keeps retained rows paired with those tuples until canonical
post-access ordering consumes them, so there is no optional cache DTO,
parallel-vector split/rejoin, empty-vector sentinel, or repeated expression
evaluation during final sorting.

The change preserves the existing authority boundaries:

- the planner remains the sole source of `ResolvedOrder`;
- the existing kernel window contract remains the sole source of the bounded
  keep count;
- the scan collector reduces only the retained working set;
- the canonical post-access phase validates the cache contract and remains the
  sole owner of final ordering and page projection; and
- continuation queries, route-ordered queries, DISTINCT queries, and shapes
  without a materialized slot layout retain their existing paths.

There is no scenario-specific dispatch, route forcing, hidden index, feature
flag, compatibility path, or retained full-collection expression-order branch.
Focused executor and session coverage passes for cached complete keys,
ascending and descending scalar expressions, nullable text expressions,
unindexed materialization, expression-index covering routes, paged result
parity, one-evaluation cache reuse, and fail-closed cache-contract mismatch.
The clean candidate cohort proves the structural target, candidate instruction
values, and final candidate raw Wasm identity. Historical cross-lockfile deltas
remain explicitly contextual. The accepted hard-cut cohort, ordinary gate, and
selected baseline complete the external Slice 2 closeout evidence.

## Remaining Risks

- `KernelRowAttribution` is public only under the diagnostics feature. The new
  field is retained as a maintained exact regression metric in the checked-in
  performance baseline rather than as slice-only optimization proof; the
  ordinary non-diagnostics query API is unchanged.
- Expression-order bounded collection caches each expression key once per
  input row during scan-time selection and compares the complete deterministic
  order, including the primary-key tie-break. The exact candidate cohort proves
  its stable absolute instruction values; historical percentage deltas remain
  contextual because the selection lockfile differs.
- The full scan remains contract-required. The implementation removes retained
  candidate state; it does not claim index pushdown or early scan stop.
- Peak retained candidates are not peak heap bytes. No memory-byte claim is
  authorized by this counter.

## Validation State

The opening 0.205.2 cohort and strict review passed. Exact selection run
`29696076149` passed all P1, scale, P2, attribution, merge, and bundle jobs after
one infrastructure-only PocketIC-download rerun. Candidate runs `29742278537`,
`29743875425`, and `29744294981` passed every P1, scale, P2, attribution, merge,
and bundle job; strict review run `29746556799` accepted their exact shared
cohort. The local Slice 2 implementation also passes focused expression-order
semantics and package-local Clippy with warnings denied. Final runs
`29751989919`, `29753941708`, and `29753956883` passed every evidence stage;
strict review `29756834526` accepted their exact shared cohort; and ordinary
run `29757215564` passed baseline comparison before run `29751989919` was
selected. Full repository tests remain user-owned under repository policy.
