# ICYDB-039 — Shared-shard convergence and virtual scheduling

2026-09-21. User-authorised I3 investigation on the existing 0.261 line.

## Finding

The retained application Wasms reproduce E263 at virtual second 42 with one
explicit PocketIC round per second. The same Wasms and action trace complete
with eight explicit rounds per second. The existing IcyDB convergence timer
continues to succeed; extra ordinary timer service drains the backlog and the
two rejected requests then succeed unchanged. This establishes sensitivity to
the benchmark's service schedule, not a live eight-player capacity limit.

No production runtime correction, higher backlog limit, automatic gameplay
retry, scheduler replacement or additional database is justified by this
comparison. The next owner is the downstream benchmark schedule and its
service accounting. Representative subnet ingress and live capacity remain
unqualified; ICYDB-039 is not closed by a synthetic round-schedule control.

## Matched original-artifact comparison

Both runs install the five application roles from retained Canic release build
`02b415c78f5eb0ef52bf360389b7b9461e762b3abcb4e7a2ed5258bf70eb6492`.
The Game Shard is **10,168,655 raw Wasm bytes**, SHA-256
`65ddc7b8d4ded685add41587ce160485bc48fa9001bc5e6d0fb13f64fac0fd24`.
All five retained artifact sizes and hashes match [artifacts.json](artifacts.json).
The comparison changes the host schedule only: its Wasm-size delta is **0 bytes**.

The frozen [host probe](host-probe-v1.rs.txt) reuses Toko Miner's enrolment,
placement, inventory-action and cadence owners. It places 32 resident Users on
one Game Shard, prepares each player through the existing position call, then
uses eight active players. It retains the benchmark's sampler publication and
140-second idle preparation before the measured trace. It omits the ten
preceding single-player cadence variants; reproducing the exact second-42
failure therefore does not require their accumulated history.

Each active player supplies 60 alternating locker transfers over 120 virtual
seconds: odd-second actions, a five-second collection deadline, three actions
per call and eight calls submitted together at seconds 6, 12, ..., 120. There
is one deliberately discarded committed reply and exact replay per player
after the first wave. A 20-second tail completes the 140-second schedule.
All requests keep the same action, timestamp, acknowledgement and revision
semantics. No new request is substituted for a rejected request.

At each schedule step the host advances virtual time by one second, then calls
`tick()` either once or eight times. It submits the whole ingress wave before
calling `await_call()` for each reply. Those waits can execute additional
rounds; therefore these are explicit-round schedules, not exact total rounds
per second. Timer queries do not explicitly tick or advance virtual time.
PocketIC ticks themselves move the IC clock by small amounts, retained in the
observations. No automatic real-time progression is enabled.

| Original artifact run | One explicit round/second | Eight explicit rounds/second |
| --- | ---: | ---: |
| Fresh committed gameplay calls | 54 before stopping | 160 |
| Exact replay calls | 8 | 8 |
| E263 rejections | 2 at second 42 | 0 |
| IcyDB callback count at start | 124 | 124 |
| IcyDB callback count just after second-42 wave | 161 | 219 |
| Completed trace | No | 480 actions |
| Final inventory and lockers equal their initial state | Not asserted for the interrupted trace | All eight players |

Every observed IcyDB callback succeeds; expected/invariant failure and stale
callback counters remain zero. In the failing run, 128 additional ticks with
no explicit time advance take the timer from 161 to 224 completed callbacks,
then idle. Both rejected requests succeed with exactly their next revision
and `replayed = false`. This is an explicit diagnostic recovery check, not an
application retry policy. The two retained replies decode to typed
`Database { code: 263 }`:
[first](icydb039-1-42-0.txt), [second](icydb039-1-42-7.txt).

The application checkpoint watchdog also shows pressure: at second 42 the
one-round run has one expected checkpoint failure, while its IcyDB convergence
watchdog has none. The eight-round control has no expected checkpoint failures.
Thus successful convergence callbacks do not mean every competing application
writer was admitted.

The successful control ends with 444 IcyDB callbacks and an idle timer.
Its exact end-state and replay assertions pass. The complete timer/counter
observations and raw-log hashes are in [schedule-runs.json](schedule-runs.json).
Counter totals cover their named timer callback roles, not exclusive gameplay
costs. No endpoint instruction or cycle savings are claimed.

## Source explanation

The inspected IcyDB source is committed `v0.261.0`,
`c1a5028eebed7b2f04cce00d49a4ea6b9c58512c`:

- `db/commit/backlog_admission.rs` admits cumulative database-wide journal
  batches, records and encoded bytes. Batch count is bounded at 64; E263 carries
  the resource, current count, proposal and limit as typed numeric facts.
- `db/commit/recovery.rs::fold_oldest_journal_batch` selects and folds one
  complete oldest batch, then checks whether tails are empty.
- `db/startup/driver.rs` reuses that recovery owner for online convergence.
  Optional cardinality work can also use the same callback, so timer success
  counts alone are not a general exact retired-batch counter.
- The generated watchdog in `icydb-model` uses a one-second retry cadence and
  `ContinueImmediately` for remaining successful work. The pinned ic-timers
  0.8.0 runtime dispatches consumer work in a separate later message and
  coalesces repeated immediate scheduling requests.

The frozen application's `action::active` owner sets its checkpoint deadline
to five seconds. These gameplay waves are six seconds apart. A player can
therefore require a checkpoint of earlier accepted work plus a new recovery
record in the next call. One incoming action call need not mean one journal
batch. The same application checkpoint owner also receives scheduled work.
Increasing the journal ceiling would not establish a sustainable service rate.

## Journal accounting qualification

A disposable framework copy adds observation-only log statements at journal
append/retirement and E263 construction. The
[instrumentation patch](framework-instrumentation.patch) changes no admission,
publication, fold, timer or gameplay decision. It is not production code.
The [strengthened host probe](host-probe-v2.rs.txt) additionally checks typed
E263, unchanged inventory/lockers for rejected calls and aggregate conservation.

The canonical eight-artifact build and focused test both pass. All installed
application roles use the same diagnostic release identity:
`9a4a3b66532c406069f12c7cefd3ebd1952fe72cb5a8dbd8bbb5dc19eae7e0a1`.
The diagnostic Game Shard is **10,186,625 raw bytes**. This is a frozen current
source build with logging, not an isolated size comparison against the earlier
retained production build. Package versions remain unchanged; scratch-only path
patches select the clean IcyDB commit plus the recorded logging patch.

The diagnostic run reproduces the original counts and second-42 rejection.
Its [exact accounting](journal-accounting.json) and [events](journal-events.jsonl)
show the following cumulative work after the idle starting state:

| Observation | Appended batches | Retired batches | Retained batches |
| --- | ---: | ---: | ---: |
| After second 6 | 8 | 0 | 8 |
| After second 12 | 24 | 7 | 17 |
| After second 18 | 40 | 13 | 27 |
| After second 24 | 56 | 19 | 37 |
| After second 30 | 72 | 25 | 47 |
| After second 36 | 88 | 31 | 57 |
| After second 42 | 100 | 37 | 63 |
| After 128 drain ticks | 100 | 100 | 0 |
| After both unchanged requests succeed and 16 more ticks | 104 | 104 | 0 |

Three pressure observations each report typed resource **Batches**, current
**64**, proposed **1**, limit **64**: two ingress rejections plus the application
checkpoint watchdog's expected failure. The after-wave sample is 63 because
ordinary convergence retires a batch before that observation. No tail exceeds
64. Extra ticks drain all 63 remaining batches without additional explicit
virtual seconds. The two rejected calls leave their own inventory and lockers
unchanged; retries commit exactly the next revision, and aggregate cargo
quantities remain conserved for every active player. The final timer is idle.

The first wave appends eight batches. Subsequent complete waves append sixteen:
eight new action records plus eight checkpoints of earlier accepted work.
Before pressure, only about six batches retire per six-second interval. This
explains the rising queue without a failed convergence callback or missing
wakeup. The first eight exact replays append no journal batch. Logging adds
uncalibrated instruction overhead, so this run qualifies ordering and accounting,
not a production cost delta. Its schedule/count agreement with the original
artifact is the control for the diagnostic observation.

## Downstream follow-up

Keep the existing admission bound and timer owner. Qualify the benchmark's
virtual-time/round schedule independently of its requested collection interval:
record explicit rounds, ingress-driven rounds where observable, timer progress,
checkpoint writes, admitted/rejected work and retained journal debt. Preserve
simultaneous submission when claiming a simultaneous workload. Do not treat an
arbitrary eight-round setting or a drain-until-success loop as representative
subnet capacity. Then compare a separately authorised representative subnet
workload and retain conservation, atomic rejection and exact replay checks.

## Provenance and limits

[sources.json](sources.json) freezes 298 application/host inputs from Toko Miner
HEAD `4413106f21afb553b54318034743e3c0f9e1c40f`, including concurrent source
changes. That receipt identifies host inputs; the independent retained Wasm
hashes identify the original runtime. It does not claim the current dirty
application source produced the earlier retained Wasms. Source snapshots and
full logs remain under `/tmp/icydb-039-*` and `/tmp/icydb039-*`; hashes alone do
not reconstruct a dirty source tree.

For local reproduction, use `/tmp/icydb-039-investigation` and the single ignored
`icydb039_shared_shard_schedule` test in `canister_toko_miner_user_hub`.
Set `TOKO_MINER_QUALIFICATION_RELEASE_BUILD_ID` and
`TOKO_MINER_QUALIFICATION_ARTIFACTS_DIR` to one complete recorded release;
`ICYDB039_ROUNDS=1` or `8` selects the explicit host schedule. The archived v1
probe is the exact source for the original-artifact runs; v2 adds the rejection
checks used by the diagnostic run. The diagnostic workspace is
`/tmp/icydb-039-instrumented`, built through ordinary Canic `fast` qualification
with `RUSTC_WRAPPER=` and no Binaryen pass. Run only the focused test.

This is local scheduling qualification, not a realistic subnet service-rate
model. Eight rounds per virtual second is a controlled sensitivity test, not
a recommended production cadence or claimed mainnet block rate. It does not
measure live ingress, throughput, receipt latency, endpoint cycles or a
maximum supported population. No wall-clock/native timing is used as a
performance metric. Inventory/locker checks do not qualify every gameplay
system, pushed-body physics or sampler retention.

Three focused managed test runs pass; the expected E263 responses are asserted
experimental outcomes, not failed tests. The diagnostic build, scratch formatting,
artifact/source hashes, local documentation links and whitespace checks pass.
No failing validation gate remains. Full suites and live-subnet checks were not
run; full suites remain user-owned. No production application/framework code,
dependency pin, Cargo version, commit, push, deployment, funding or application
network operation is included. Three disposable PocketIC fixtures were started
and dropped; no application network was reset or upgraded. The pre-existing
IcyDB lockfile change and concurrent downstream
work remain untouched.

Complexity delta for I3: 19 documentation/evidence files across the two
repositories, approximately 2,300 net lines including frozen probes, hashes and
machine-readable observations. Production implementation shape and state space
are unchanged. The matched original-artifact schedule comparison has a zero-byte
Wasm delta; production endpoint instruction and cycle deltas are unmeasured.
