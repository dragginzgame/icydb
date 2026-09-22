# C31–C33: frozen Toko Miner measurements

Measured 2026-09-22 against published IcyDB 0.261.5. The original candidate
contains C31–C33; the retained candidate contains C31+C32. Cargo versions stay at .5.

## Current handoff after correction

The user-approved correction removes C33's map-entry rewrite and retains the
existing checked-add and zero-removal implementation, along with its arithmetic
boundary tests. C31 and C32 remain. The changed production sources match the
measured C31+C32 inputs exactly; retained test modules are the only differences.
The measurements below are reused on that basis, without another Toko build
or run. The original C31–C33 results and source snapshot remain investigation
evidence, not a description of the current runtime candidate.

The retained combination shrinks Game Shard by 834 raw Wasm bytes, avoids
C33's consistent recovery-worker increase and has mixed total-cycle changes
(−0.119% to +0.241%). This correction changes five files: the cardinality
implementation and four documentation files. It removes six production lines
and the occupied/vacant branch split; no tests, dependencies or versions change.

Correction validation passes: 20 focused arithmetic/index-store tests,
strict all-feature core library/test lint, index-range, mutation atomicity
and read-admission invariants, formatting and whitespace. Full release
validation and remote CI remain user-owned. No local network is started,
stopped or restarted during the correction.

## Original C31–C33 result

**The original C31–C33 candidate is not a cycle or Wasm improvement.** All eight
matched direct-action trials pass their functional checks, but total charged
cycles increase by 0.279–0.876%. Action-body instructions decrease in seven of
eight cases. Four of five managed roles also grow in raw Wasm size.

The simpler ownership is real: consumed rows move, parent-prefix slices borrow,
and signed-count updates reuse map entries. These measurements show that fewer
copies and lookups do not by themselves establish lower application costs.
The follow-up isolation below identifies C33's signed-cardinality map-entry
rewrite as a contributor to the regression. The investigation itself did not
alter the runtime edits; the correction above subsequently removed C33.
Removing C33 in a scratch build avoids
the consistent background increase. C31+C32 retain small mixed cycle effects
and reduce raw Wasm in four managed roles; treat them as ownership cleanups,
not a uniform cycle optimization.

## Matched inputs

- Frozen Toko Miner application: `f53c690783a521e3c96a3b1533407d9867ca94c4`,
  using the retained 0.261.5 qualification snapshot. Concurrent gameplay edits
  in the live sibling checkout are excluded.
- Canic libraries and private CLI 0.110.35; Rust 1.98.1; PocketIC 16.0.0;
  `fast` profile, metrics enabled, SQL and migration features disabled.
- Both comparison builds use the same scratch application and the same local
  `icydb-core` dependency path. The initial copy matches published .5 exactly;
  the candidate replaces only `commit/apply.rs`, `index/cardinality.rs` and
  `index/store.rs` from the pending worktree. The store change is test-only.
- Canonical complete Canic builds generate both managed artifact sets. No
  dependency, manifest, release version or source in either live repository
  changes for measurement.
- Exactly one ignored direct-action test runs: eight ordered trials, each with
  64 actions, stationary/walking movement and batch sizes 1, 8, 16 and 32.
  Public enrolment, conserved inventory, exact retries, rejection behavior,
  commit counts and canonical intent-byte counts remain checked.
- Gross cycles are replica balance deltas across the whole simulated workload,
  including background work. Batch updates and checkpoint tails are separate
  observed subwindows. Action-body instructions exclude Candid transport work.
  Idle controls are retained, not subtracted as estimates.
- Host timings are excluded from retained measurements and decisions. The
  wall-clock cadence test and full suites are not run.

The published .5 artifact replay matches the downstream retained result.
The rebuilt baseline differs slightly from that artifact because its dependency
source/build context changes: walking-32 gross cycles are 0.03034% lower and
action-body instructions 0.03609% lower. Therefore the candidate comparison
uses the rebuilt baseline, not an unmatched published artifact.

## Raw Wasm

| Role | Matched .5 bytes | Candidate bytes | Delta |
|---|---:|---:|---:|
| game_hub | 4,127,733 | 4,127,724 | -9 (-0.00022%) |
| game_shard | 10,270,752 | 10,272,688 | +1,936 (+0.01885%) |
| translation | 8,716,644 | 8,719,159 | +2,515 (+0.02885%) |
| user_hub | 7,582,051 | 7,584,669 | +2,618 (+0.03453%) |
| user_shard | 7,276,299 | 7,278,753 | +2,454 (+0.03373%) |

## Direct-action cycles and instructions

| Workload | Batch | .5 gross cycles | Candidate gross cycles | Cycle delta | Action-body instruction delta |
|---|---:|---:|---:|---:|---:|
| Stationary | 1 | 43,947,144,276 | 44,300,531,995 | +0.80412% | -0.27168% |
| Stationary | 8 | 13,133,697,129 | 13,248,789,040 | +0.87631% | +0.10012% |
| Stationary | 16 | 8,650,792,057 | 8,697,717,976 | +0.54245% | -0.22685% |
| Stationary | 32 | 6,094,591,470 | 6,127,841,505 | +0.54557% | -0.04032% |
| Walking | 1 | 43,789,658,371 | 44,125,634,088 | +0.76725% | -0.17884% |
| Walking | 8 | 13,146,895,584 | 13,245,008,913 | +0.74629% | -0.11592% |
| Walking | 16 | 8,673,130,326 | 8,722,772,802 | +0.57237% | -0.11584% |
| Walking | 32 | 6,135,921,636 | 6,153,052,387 | +0.27919% | -0.32946% |

Walking-32 illustrates why the windows must remain separate:

- Action-body instructions: **−12,731,612** (−0.32946%).
- The two batch update calls: **−12,117,434 cycles**.
- Checkpoint tail: **+14,181,159 cycles**.
- Other charged work inside the gross window: **+15,067,026 cycles**.
- Overall: **+17,130,751 cycles** (+0.27919%).

Across all eight cases the checkpoint-tail cycles increase. The initial
measurement localized a follow-up to checkpoint/background work; the isolation
below identifies the responsible source change. Lower action-body instructions
do not establish lower total charged cycles.

## Isolation of the pending edits

The follow-up uses the same frozen application, dependency path, canonical
build command and ordered eight-case workload. Each isolated build starts
from published .5 source and changes only its selected production code. All
edits and instrumentation for these experiments remain under `/tmp`.

A host-only probe queries the existing controller-authorized
`game_shard_timer_inventory` at the start, after update calls and at the end
of each measured window. Canister Wasm is unchanged by the probe. Baseline
and combined-candidate runs reproduce **every original retained metric
exactly**, excluding host timings and the newly added timer observations.
This checks that the extra queries do not perturb the measured workload.

The consistent instruction increase is in the timer identified as
`icydb / startup / recovery`. The name does not imply failed startup: normal
online convergence also folds journal batches through
[`continue_online_convergence`](../../../crates/icydb-core/src/db/commit/recovery.rs#L253).
The combined candidate increases this worker's instructions by **1.715–1.808%**
across the eight cases. Its completion counts remain 72, 16, 8 and 4 for
batch sizes 1, 8, 16 and 32 in both movement workloads.

**C33 alone reproduces the increase:** gross cycles rise **0.203–0.832%**
and recovery-worker instructions rise **1.695–1.818%** across all eight cases.
The isolated source change is
[`apply_signed_count_delta`](../../../crates/icydb-core/src/db/index/cardinality.rs#L777)
and its `Entry` import. This is evidence against retaining that rewrite as
an optimization, despite removing key copies and a second lookup.

C31 alone does not reproduce the consistent recovery increase. Its total
cycle deltas are mixed (**−0.118% to +0.175%**), and recovery instructions
vary **−0.058% to +0.107%**. Game Shard shrinks by 272 raw Wasm bytes;
Translation, User Hub and User Shard shrink by 142 bytes each. These results do not
establish a universal cycle saving from moving consumed row buffers.

C32 alone lowers gross cycles in seven of eight cases, with deltas ranging
from **−0.098% to +0.087%**. Recovery-worker instructions vary from
**−0.057% to +0.021%**. Game Shard shrinks by 820 raw Wasm bytes. It also
does not reproduce the consistent recovery increase.

The final **C31+C32** build measures the actual proposed combination without
C33. Gross cycles fall in six cases and rise in two:

| Workload | Batch | C31+C32 gross cycles | Delta from matched .5 |
|---|---:|---:|---:|
| Stationary | 1 | 43,911,488,857 | -0.08113% |
| Stationary | 8 | 13,130,053,653 | -0.02774% |
| Stationary | 16 | 8,640,505,678 | -0.11891% |
| Stationary | 32 | 6,097,702,667 | +0.05105% |
| Walking | 1 | 43,765,191,169 | -0.05587% |
| Walking | 8 | 13,178,636,150 | +0.24143% |
| Walking | 16 | 8,670,088,362 | -0.03507% |
| Walking | 32 | 6,132,778,635 | -0.05122% |

All eight totals are below the C31–C33 candidate. Recovery-worker instruction
deltas now range from **−0.053% to +0.106%**, so removing C33 avoids its
consistent 1.7–1.8% increase. Raw Wasm deltas are 0 bytes for Game Hub,
**−834** for Game Shard, −263 for Translation, −164 for User Hub and −334
for User Shard. The two cycle increases relative to .5 remain real tradeoffs;
the lower-level mechanism behind these smaller mixed changes is not isolated.

Attribution is at the source-change and timer-work boundaries. Equal timer
completion counts do not establish identical internal row counts; these
measurements do not prove a particular compiler, allocation or instruction-level
mechanism. No failing timer or extra recovery attempt explains the observed
increase. Timer work spans and action-body spans are not an exclusive partition
of gross cycles and must not be added as if they were one.

## Earlier .4 → .5 walking-32 increase

The retained downstream comparison, independently replayed on published .5,
records +17,677,146 gross cycles (+0.28884%) and +13,865,701 action-body
instructions (+0.35997%). Of that gross increase, +14,837,902 cycles occur in
the two update calls; the checkpoint tail decreases by 687,820 cycles.

The reported Resource, OrderCustomer, NftToken and Robot entity instruction
totals decrease. These entity spans are not an exclusive partition of the
action endpoint and cannot assign the remaining increase to a particular
database operation. Separate temporary application phase instrumentation is
used for the diagnostic investigation; its costs are not mixed into the
uninstrumented candidate table above.

Both complete diagnostic builds pass all eight functional trials. For the two
walking-32 calls, the temporary spans report:

| Phase | .4 instructions | .5 instructions | Delta |
|---|---:|---:|---:|
| Admission and Robot read | 26,209,466 | 25,913,467 | -295,999 |
| Active-state preparation | 9,324 | 9,326 | +2 |
| Inventory loading | 19,830 | 19,830 | +0 |
| Action application | 3,485,581,782 | 3,503,740,326 | +18,158,544 |
| Persistence | 26,367,149 | 26,300,864 | -66,285 |
| Response construction | 311,849,434 | 312,030,465 | +181,031 |

The diagnostic difference is concentrated in `apply_actions`, which accounts
for about 90.5% of the captured instructions. The follow-up below splits its
`apply_context` and `apply_action` calls. Persistence is slightly
cheaper in .5. This identifies an application phase, not a specific IcyDB
operation: the phase also invokes application helpers and their database reads.

Instrumentation changes the compiled program: the diagnostic captured-span
increase is 17,977,293 instructions, whereas the uninstrumented published
increase is 13,865,701. Do not substitute one for the other or claim that these
counters assign every instruction of the published regression. The six span
boundaries and exact temporary patch are retained in the evidence JSON. No
instrumentation is added to either live repository.

### Context and command split — 2026-09-22

The requested follow-up is complete. Command application accounts for
**71.6% of the diagnostic walking-32 application increase**; entry context
application accounts for 28.4%. The split does not identify an exclusive
IcyDB operation or establish a database correctness defect.

| Walking-32 span (two calls) | .4 instructions | .5 instructions | Delta |
|---|---:|---:|---:|
| Field and chronology setup | 11,832 | 11,832 | +0 |
| Entry context application | 1,594,800,067 | 1,599,754,992 | +4,954,925 |
| Command application | 1,891,322,237 | 1,903,812,332 | +12,490,095 |
| Final context application | 0 | 0 | +0 |
| Loop and measurement remainder | 33,990 | 33,990 | +0 |
| Total action application | 3,486,168,126 | 3,503,613,146 | +17,445,020 |

Both releases execute **64 context calls and 64 command calls** across the
same two requests. Neither has a final context. The increase comes from
higher instruction costs at those boundaries, not additional calls. Context
instructions increase 0.311%; command instructions increase 0.660%. The
stationary-32 control instead decreases by 1,215,999 application instructions:
context −1,038,119 and commands −177,880. Do not generalize the walking result
to all actions or all movement modes.

The temporary probe accumulates IC instruction-counter differences around
entry contexts, commands and final contexts inside `apply_actions`. It also
captures field/chronology setup and preserves the enclosing phase counters.
It returns the counters to the endpoint and prints only after all endpoint
phase captures, avoiding logging inside the measured application interval.
The unassigned remainder covers loop/control and measurement work; it is
identical across releases. Only successful 32-action requests emit spans.

The workload alternates `TransferLockerItem` withdrawal/deposit at `tok_core`;
both stationary and walking entries carry a context. Source inspection maps
`apply_context` to `outpost::positions::apply_positions`, and `apply_action`
through `apply_field_action` and `outpost::apply_at` to `outpost::core::apply`
and locker dispatch. Both context and command paths decode and re-encode
`CoreActors`; map reads already have a shared projection cache. This motivated
the completed nested split below: **CoreActors decode/encode versus interaction/
locker work inside command application**, with context codecs measured too.
The outer split alone does not prove that any one helper owns the increase.

The application, Canic 0.110.35, Rust 1.98.1, PocketIC 16.0.0 and fast profile
remain frozen. All 313 application files match the earlier frozen application
except the instrumented executor. Only the six IcyDB packages differ between
lockfiles (.4 versus .5); every non-IcyDB package is identical. Both complete
canonical managed builds pass all eight functional trials, **16 cases total**,
with conserved inventory, actions, commits and canonical intent bytes. The
probe adds no endpoint, persisted state or production-repository change.

These are diagnostic builds: walking-32 gross cycles change from
6,116,571,526 to 6,138,437,277 (+21,865,751), and action-body instructions
from 3,850,928,580 to 3,868,305,645 (+17,377,065). Diagnostic Game Shard raw
Wasm changes from 10,281,619 to 10,280,446 bytes (−1,173). These values differ
from the original uninstrumented release comparison above; they are not new
release savings or an exact allocation of its instruction increase. The
current Canic .36 composition and concurrent gameplay changes remain outside
this proof. No host/native timing is used as a performance metric.

Two disposable PocketIC fixtures were started; no persistent network was
modified. Full suites and remote CI were not run. The retained evidence's
`action_application_split` field contains the exact temporary patch, source
and artifact hashes, call rows, whole-trial summaries and dependency checks.
The scratch inputs, canonical build logs and raw test logs remain under
`/tmp/icydb-c31-c33-toko/action-split`; live dependencies and runtime source
are unchanged.
This follow-up changes four documentation/evidence files, adding approximately
220 net lines. Production implementation complexity is unchanged.

### Actor codec and locker dispatch split — 2026-09-22

The next requested drill-down is complete. **Interaction/locker dispatch
accounts for 78.8% of the nested probe's command-instruction increase.**
Read validation and its remainder account for 18.4%, while the Candid
codec accounts for 2.8%. Pure serialization is not the dominant source of
this diagnostic increase.

| Walking-32 command work (64 commands) | .4 instructions | .5 instructions | Delta |
|---|---:|---:|---:|
| Candid decode and encode | 412,340,337 | 412,836,697 | +496,360 |
| Read validation and remainder | 583,458,454 | 586,727,911 | +3,269,457 |
| Interaction / locker dispatch | 887,054,804 | 901,015,675 | +13,960,871 |
| Other command work | 777,835 | 778,299 | +464 |
| Total command application | 1,883,631,430 | 1,901,358,582 | +17,727,152 |

Candid decoding changes from 159,841,105 to 159,873,772 instructions;
encoding changes from 252,499,232 to 252,962,925. Together they occupy 21.7%
of .5 command instructions. Read validation/remainder occupies 30.9%, and
interaction dispatch 47.4%. Context handling also grows mostly outside its
codec: +6,298,961 of its +6,912,520 instruction increase is outside the two
Candid calls. These proportions describe this instrumented workload only.

Both versions make 64 CoreActors decode calls and 64 encode calls in each
of the context and command lanes. Each lane decodes 106,688 bytes and encodes
106,688 bytes; all byte and call counts match. This excludes increased call
or byte volume at these measured boundaries, not all possible internal work
variation or changes in serialized contents.

The scratch-only probe surrounds the actual Candid decode/encode operations
and the command owner's read, map/scene setup, interaction dispatch,
post-processing and final encode/assignment. A resettable thread-local counter
array labels context versus command calls; lane zero excludes receipt and
persistence work. Counters are printed after the enclosing endpoint captures.
Parent read/encode spans include child codec spans; the table subtracts those
children to avoid double counting. Remainders retain validation, wrapper and
measurement work, so do not describe them as pure database charges.

A concrete downstream candidate is present in both frozen and current source:
`game_shard/src/outpost/locker.rs` first projects `MapChanges` in `apply`, then
calls `snapshot`, which projects the same unchanged state again. `snapshot`
also owns locker membership, uniqueness, names and inventory validation.
**The next bounded optimization experiment is to pass the already computed
projection into that existing snapshot logic**, preserving every validation
and error path. This needs no persisted cache or IcyDB API change. The cost
of the repeated projection alone is not isolated, and no savings are claimed
until the candidate is measured. Current Toko source was only inspected;
its concurrent gameplay changes are outside this frozen benchmark.

Both complete canonical builds pass all eight functional cases, **16 total**,
with conserved work, exact retries/rejections and matched call/byte counts.
The same frozen application and Canic .35 composition are used. Only three
of 313 application files differ, all for temporary instrumentation; only the
six IcyDB dependencies differ between releases. No live source, dependency
or version changes. Two disposable PocketIC fixtures were started; no
persistent network changes, full suites, or remote CI runs.

Instrumentation changes the compiled program again: this probe's walking-32
application increase is 24,639,672 instructions, versus 17,445,020 in the
preceding split. Gross cycles change from 6,105,311,289 to 6,135,582,695
(+30,271,406), and action-body instructions from 3,839,862,765 to 3,864,761,406
(+24,898,641). Diagnostic Game Shard raw Wasm changes from 10,278,171 to
10,277,844 bytes (−327). These are diagnostic observations, not replacement
release measurements or an exact assignment of the original release increase.
No exclusive IcyDB defect or underlying compiler/allocation cause is proven.

The evidence JSON's `actor_codec_split` field retains the exact temporary
three-file patch, artifact/source/log hashes, nested counters, byte counts
and matched trial summaries. Scratch reproduction inputs and full logs are
under `/tmp/icydb-c31-c33-toko/codec-split`. Production implementation
complexity remains unchanged. Four documentation/evidence files change,
adding approximately 230 net lines; no production code is added.

## Validation and limits

Seven initial direct-action runs pass: published .5, matched baseline and
candidate, both exact replays, and the two diagnostic versions. Each covers the
same eight ordered 64-action trials. Both matched replays agree in every
retained field, including cycle charges, instruction observations, inventory
conservation, request sizes, commit counts, and retry/rejection outcomes. The
published .5 replay also matches the downstream retained metrics. The pending
runtime and test source hashes remain unchanged throughout measurement; the
prior 44 focused native tests and strict lint therefore still cover that code.

The isolation adds six successful direct-action runs: baseline and combined
candidate with the timer probe, C31 alone, C32 alone, C33 alone and C31+C32.
All **48 cases** pass. All timer counter deltas match the baseline, including
work completions and zero retryable failures, invariant failures and
unacknowledged work. Conserved actions, commits and canonical intent bytes
also match. No production source or dependency changes during this follow-up;
no native test or full suite is rerun for its documentation-only output.

Setup failures were recorded and corrected: the shared Canic CLI had advanced
to .36 and rejected the frozen .35 role contracts; direct Cargo Wasm builds
were rejected by Canic's canonical-build guard; and an unbound single-role
instrumented artifact rejected managed initialization with E61. A private
cached .35 CLI and complete managed builds resolve these issues. These failed
setup attempts contribute no reported performance result. The failed
single-role diagnostic log is retained separately.

Eight disposable local PocketIC fixtures were started for the initial
measurement, including that rejected initialization attempt; no persistent
network was restarted or modified.
The isolation adds six further disposable PocketIC fixtures, with no existing
network lifecycle changes.
Full release validation, remote CI, live deployment and live-network capacity
remain unqualified. No timing benchmark was run and no host timing field is
retained as a performance metric.

Before the correction, the patch touched eight files: zero net production Rust lines,
21 net test lines, and documentation/measurement evidence. The implementation
was simpler in ownership, but the measured cost tradeoff was unfavorable. The
initial measurement added about 1,400 documentation/evidence lines across five
files, with no runtime edits. The follow-up isolation also changes only those
documentation/evidence files; implementation complexity remains unchanged.
This investigation adds approximately 410 net documentation/evidence lines
across five files. It adds no production code, test code or runtime state.

## Reproduction and handoff

The working measurement root is `/tmp/icydb-c31-c33-toko`. Both uninstrumented
builds use `app`, the same local `core` override, and `target`. The diagnostic
builds use `profile-app` and `profile-target`, identical temporary counters, and
registry IcyDB .4/.5 dependencies. Only the scratch diagnostic dependency is
changed to reproduce the older release; live manifests and dependencies are
unchanged. All four artifact sets come from the complete Canic fast build.

The exact selected host test is
`qualification::opening::costs::direct_batches_report_charged_cycles_and_conserved_work`.
The retained `run-direct.sh` supplies each artifact directory, corresponding
release-build ID and report path. `direct-test` is the copied host binary that
reproduces the previously retained .5 metrics. The structured evidence records
its hash, source/lock hashes, artifact identities, raw Wasm sizes, trial
summaries, replay checks, phase observations, diagnostic patch and raw-log hashes.
The temporary analysis scripts are not project code.

The isolation uses the same `app`, `core` and `target` directories. Its
`investigation` directory retains the host-only probe patch/binary, selected
source inputs, `run-probe.sh`, six reports and four canonical build logs.
The evidence JSON's `ablation` field records exact source selection, the
pending candidate patch, artifact/source/report hashes, raw Wasm sizes, timer
observations and functional checks. Its tabular rows use explicit column names
and exclude all host timings.

See [structured measurements](c31-c33-measurements.json). The earlier published
comparison is sourced from Toko Miner's
`docs/upstream/artifacts/icydb-0.261.5-action-comparison.json`, whose hash is
retained in the evidence. The final feedback for Toko is:

- The original walking-32 increase is reproducible; diagnostic attribution
  points to action application. The completed split attributes 71.6% of the
  diagnostic increase to commands and 28.4% to context. The subsequent nested
  probe identifies interaction/locker dispatch as the largest contributor.
  Measure reuse of the already computed locker projection while preserving
  snapshot validation; no exclusive IcyDB defect is established.
- C31–C33 improve most action-body instruction counts but increase total charged
  cycles and most raw Wasm sizes. The source isolation reproduces the consistent
  background increase with C33 alone. Its map-entry rewrite has now been removed
  from the release candidate; arithmetic boundary coverage remains.
- C31+C32 avoid C33's consistent background increase and shrink four managed
  roles, but their two cycle increases relative to .5 must remain disclosed.
  Retain them as ownership cleanups only with that measured tradeoff understood.
