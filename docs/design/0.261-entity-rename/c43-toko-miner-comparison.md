# C43 — Matched Toko Miner release comparison

2026-09-23. Compare registry IcyDB 0.261.6 with 0.261.8 against the
application revision reviewed in the Toko Miner performance feedback. This is
a focused upstream measurement, not complete downstream qualification.
The gate passes: both builds seal successfully, all eight final focused probe
runs pass, and equivalent-work checks pass. Each release's two repetitions
produce identical selected counters. No new runtime defect or large action-cost
improvement is established; query instruction costs remain unmeasured.

## Frozen inputs and owned gate

- Toko Miner: `37f38533c8f8487f2b754dd3e6bbe50e4f7c33e8`, exported from Git;
  concurrent worktree edits are excluded. The live checkout subsequently
  advanced to `8985663152b0c9b9813681b887fc7e0439ac7694`; that revision is not
  qualified here.
- Assets: `4c9228a114568ad210e47c40780ad92e443c717c`, matching the locally
  resolved asset `origin/main` at freeze time; no asset fetch or publication.
- Canic library/CLI 0.110.38, Rust 1.98.1 and PocketIC 16.0.0. Canonical
  complete Canic `fast` builds use the same application path, target directory,
  fixtures and default features: application metrics and database demo enabled,
  SQL/migration disabled. These are not release-profile size measurements.
- Exactly six IcyDB registry package versions/checksums differ. Non-IcyDB
  packages and dependency edges match. No path-patched production library,
  changed admission limit, artificial warming update or extra timer is used.
- One host binary runs the existing named direct-action probe and a temporary
  same-release recovery/query probe twice against each sealed artefact set.
  Only host test code changes; production application source matches the frozen
  commit. The host-only patch and identities are retained in the
  [structured evidence](c43-toko-miner-measurements.json).

The action gate uses eight ordered cases: 64 inventory transfers each,
stationary and walking, with batch sizes 1, 8, 16 and 32. Caller aggregates are
fresh, but share the managed fixture's three Game Shards. Acceptance requires
inventory/locker conservation, exact retry, conflicting-operation rejection,
expected commit/action counts and equivalent Candid/intent byte counts across
releases. No timing/cadence test or full suite is selected.

## Raw Wasm bytes

These are final non-gzipped artefacts, including their data sections.

| Role | 0.261.6 | 0.261.8 | Delta |
| --- | ---: | ---: | ---: |
| Game Hub | 4,156,733 | 4,156,715 | -18 |
| Game Shard | 10,399,842 | 10,394,129 | -5,713 |
| Translation | 8,739,486 | 8,737,729 | -1,757 |
| User Hub | 7,613,139 | 7,613,238 | +99 |
| User Shard | 7,307,336 | 7,305,633 | -1,703 |

The application-role sum falls by 9,092 bytes. Root, Fleet Coordinator and
Wasm Store sizes are unchanged. This is a complete release comparison, not
isolated attribution to one IcyDB change or marginal per-entity size evidence.

## Action and lifecycle results

Gross-cycle changes are small and mixed. Action-body instructions and whole
delivered-call deductions do not always move together. The stationary batch-1
case regresses in gross cycles despite using fewer action-body instructions;
walking batch-8 regresses in instructions despite using slightly fewer cycles.

| Workload | Batch | Gross cycles .6 | Gross cycles .8 | Cycle delta | Action-body instruction delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| Stationary | 1 | 45,395,304,868 | 45,405,975,745 | +0.02351% | -0.02537% |
| Stationary | 8 | 13,374,803,127 | 13,357,708,005 | -0.12782% | -0.14990% |
| Stationary | 16 | 8,717,680,385 | 8,709,779,092 | -0.09064% | -0.19972% |
| Stationary | 32 | 6,090,511,676 | 6,081,370,138 | -0.15009% | -0.23212% |
| Walking | 1 | 45,224,934,928 | 45,199,487,807 | -0.05627% | -0.09716% |
| Walking | 8 | 13,376,954,090 | 13,372,388,435 | -0.03413% | +0.05406% |
| Walking | 16 | 8,734,986,134 | 8,728,303,325 | -0.07651% | -0.07451% |
| Walking | 32 | 6,097,481,310 | 6,096,606,757 | -0.01434% | -0.05328% |

Stationary batch-1's full update deliveries cost 4,258,682 more cycles. Retry
costs also increase for stationary batch-16 (+22,060), walking batch-1
(+39,269) and walking batch-8 (+253,860). Stationary batch-16's checkpoint
tail grows by 837,514 cycles. These regressions remain in the evidence alongside
the improvements; neither idle subtraction nor an aggregate average hides them.

| Game Shard lifecycle interval | 0.261.6 cycles | 0.261.8 cycles | Delta |
| --- | ---: | ---: | ---: |
| Public enrolment's shard work | 1,572,433,098 | 1,573,125,640 | +692,542 |
| Pre-upgrade cooldown/background | 562,173,076 | 563,748,809 | +1,575,733 |
| Same-release upgrade delivery | 24,394,141,320 | 24,384,095,256 | -10,046,064 |
| Post-upgrade readiness | 121,424,475 | 121,424,825 | +350 |

Both releases return consistent repeated fields and restore their pre-upgrade
field unchanged.
Initial post-enrolment queries succeed immediately; after each upgrade one
query is rejected before readiness, followed by three equal successful reads.
All successful query balance deductions are zero, not evidence of zero CPU.
This gate does not measure the query-instruction benefit of runtime preparation.

## Measurement boundaries

Gross cycles are Game Shard balance deductions over each action window,
including inter-batch background work and the checkpoint tail. Full delivered
update-call deductions, setup, retry, rejection, checkpoint tail and both
neighbouring idle windows are retained separately. Idle costs are not
subtracted. Action-body instructions exclude the Candid envelope; they are not
a complete request invoice. Other canisters' deductions are not included.

The recovery probe uses only a canister's identical-release Wasm. It records
cooldown background charges, upgrade delivery, readiness deductions and three
successful separate query messages on each side. It never upgrades persisted
state from 0.261.6 to 0.261.8. First successful reads follow ordinary fixture
readiness/enrolment, not bare schema publication. No ordinary update is inserted
between restoration and the measured reads.

Query instruction counts and absolute installation/startup cost remain
unmeasured. Query balance deductions cannot substitute for query CPU work.
The fixture funds child creation as well as explicitly topping up installation;
therefore the explicit top-up alone is not a valid initial balance. Exact
post-fixture balances are retained without labelling them an absolute startup
bill. Enrolment measurements cover Game Shard only, not the whole transaction.

## Reproduction and setup failures

The retained measurement root is `/tmp/icydb-c43-toko.YDrded`. `app` owns the
two sealed builds; `host-app` contains the final host-only probe; `host-test`
is shared by both releases. `run-probe.sh` runs only these exact ignored tests:

- `qualification::opening::costs::direct_batches_report_charged_cycles_and_conserved_work`
- `qualification::opening::costs::c43_query_and_same_release_costs`

The structured evidence records source/lock/tool/host hashes, the test patch,
release-build IDs, artefact hashes, complete selected cost categories and raw
report/log hashes. `summarise.py` is a temporary audit extraction script, not
project code. No host timing is used or retained in the structured evidence.

Three setup issues were corrected before final acceptance: a missing host-test
trait import; Canic rejecting the first build because the host probe was edited
during compilation; and a host-only startup-counter underflow caused by assuming
the explicit top-up was the child's entire funding. The rejected build is not
used. Final builds use frozen inputs; the corrected host probe is kept outside
their source tree. Offline dependency resolution also rewrote unrelated Windows
edges; those scratch-only changes were restored before the baseline build, and
the locked graph was verified.

Ten disposable managed PocketIC fixtures were created: eight final probe runs
and two preliminary runs, one of which hit the host-counter error. No existing
ICP network was started, stopped or restarted. The four final action runs cover
2,048 committed actions with their conservation/retry/rejection checks; four
recovery runs cover 24 successful query observations and four same-release
upgrades. Duplicate counters are stored once per release, with independent
raw report/log hashes for both repetitions.

Full downstream CI, generated frontend-binding parity, release validation,
live deployment/capacity and query-instruction attribution remain outside this
gate. Binding diagnostics and downstream gameplay optimisations are separate
follow-ups; this slice adds no production code or runtime state.
