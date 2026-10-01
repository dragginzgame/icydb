# Toko Miner upstream measurement follow-up

Date: 2026-10-01. Scope: ICYDB-033, ICYDB-038, ICYDB-039 and ICYDB-042.

The four investigations now have application measurements. The remaining
checkpoint field clones do not justify a borrowed-input API for this workload.
Journal admission still depends on how much background work the schedule permits:
the underserviced control reaches the 64-batch bound, while the better-serviced
control completes without rejection. Fold-cost attribution and exact backlog
visibility are the next useful upstream work. Mainnet capacity remains unmeasured.

## Inputs and comparison boundary

- Application: Toko Miner `3f73417bc9bb359b0496d114bfb70722b594c860`
  (0.3.18), Canic 0.110.49, default application features, SQL/migration disabled.
- Baseline: registry IcyDB 0.262.2, the application's current dependency pin.
- Candidate: IcyDB `95d161faf2b39803fcaead28a3a7515aaf8bf708` plus the
  eight Rust cleanup changes captured when the experiment began. The commit is
  labelled 0.264.1; its Cargo manifests still say 0.264.0. Neither was edited.
  Later concurrent repository edits are excluded from this measurement.
- Rust 1.98.1, PocketIC server 16.0.0, canonical Canic `fast` builds. All variants
  use the same disposable application path and private build context. Complete
  non-IcyDB Cargo lock package records match between baseline and candidate.

These are working-tree comparison results, not acceptance of a published release.
Application runtime sources and manifests were clean at capture. Dirty application
documentation and tooling were recorded in the input inventory; they do not
establish deployed runtime behaviour. No dependency pin, production source,
release version or existing local network was changed.

## ICYDB-039: exact convergence debt

The maintained driver folds one complete batch per online step and requests
immediate continuation while work remains. Admission bounds remain 64 retained
batches, 16,384 records and 16 MiB of encoded records. A timer callback, player
action, application ActionDelta row and engine journal batch are distinct units.

Both controls use 32 residents on one Game Shard, eight active players, and three
conserved locker transfers per call. Eight requests are submitted before replies
are awaited. Each wave includes six virtual schedule steps, with either one or
eight explicit PocketIC ticks per step. Ingress waits also execute rounds, so the
experiment does not establish a total round count or calls per second.

| Control | Waves | Initially accepted calls | E263 rejections | Peak batches | Final batches |
| --- | ---: | ---: | ---: | ---: | ---: |
| One tick per step | 13 | 100 | 4 | 64 | 27 |
| Eight ticks per step | 24 | 192 | 0 | 9 | 0 |

The first control reaches the batch bound with peak record and byte counts of
96 and 329,264. The second peaks at 14 records and 50,678 bytes. Both remain below
the other admission bounds. Append/retirement observations conserve exact debt:
`1 + 115 - 89 = 27` and `1 + 289 - 290 = 0`, respectively. The initial `1`
is outstanding fixture/enrolment debt at the measurement boundary.

All four rejected requests leave inventory and lockers unchanged. After a fixed
recovery schedule of 160 explicit ticks, those identical requests succeed. A
parity repair restores the interrupted transfer workload; eight exact replays
preserve revisions and inventory. The first control still retains **27 batches**
after recovery, retries and subsequent work; it is not a quiescence result.

Decision: retain backpressure and request identity on retry. Measure fold phases
and expose debt through the existing journal/diagnostic owner before considering
a scheduling change. Increasing the 64-batch cap would extend the burst without
demonstrating improved sustained service. These controls do not resolve the
downstream timed-service acceptance or qualify mainnet capacity. Journal logging
adds observer overhead, so its cycle totals are not production-cost comparisons.

## ICYDB-042: checkpoint field clones

The application already selects dirty checkpoint values, consolidates due work
into one structural transaction, retires preceding ActionDelta recovery rows,
and applies heap state after successful admission. Generated typed input encoding
consumes its owner; selected collection fields are still cloned before encoding.

Counters surround the eleven explicit clone sites in the Robot/Rocket checkpoint
builders. The eight inventory/walking trials exercise eight Robot fields:
action replays, carried/core inventories, installed equipment, learned patterns,
minions, NFT discoveries and recipe points. Across 512 actions, they perform
**400 field clones and consume 198,596 measured instructions**. Per 64-action
trial, the clone count ranges from 16 to 88 and instructions from 7,937 to 43,698.
That is below 0.0004% of the corresponding action-body instruction totals.

The counter windows include checkpoints and background activity; they are not an
exclusive decomposition of the action-body counter. The helper can alter compiler
optimisation. This establishes the small observed scale, not a prediction of
borrowed-encoder savings. Required structural allocation is outside these clone
counters. Rocket/journey clones and large payload extrema were not exercised.

Decision: defer the borrowed-input API. A realistic large Rocket/journey workload
is the remaining prerequisite if this item is revisited. No generated API or
additional mutation route is needed for the measured Robot workload.

## ICYDB-033: composed raw Wasm and entity growth

The uninstrumented comparison retains identical Candid interfaces for all roles.

| Role | Baseline raw bytes | Candidate raw bytes | Delta |
| --- | ---: | ---: | ---: |
| Game Hub | 4,064,411 | 4,059,785 | -4,626 |
| Game Shard | 10,639,850 | 10,633,865 | -5,985 |
| Translation | 8,721,910 | 8,704,904 | -17,006 |
| User Hub | 7,652,373 | 7,634,048 | -18,325 |
| User Shard | 7,401,965 | 7,385,628 | -16,337 |

Together the five roles shrink by **62,279 raw bytes (0.162%)**. These are fast
build results; release-profile size is unmeasured. Probe artifacts are excluded
from this comparison.

A separate matched probe adds one empty entity with Ulid ID, bounded Text label,
Nat64 value and a unique label index. Controller `icydb_schema` confirms the
accepted entity count increases from 30 to 31 and includes `UpstreamEntityProbe`.
Candid is identical. Game Shard raw size changes from 10,655,255 to 10,653,989
bytes: **-1,266 bytes**. Section bodies change by +272 data bytes, +14 function
bytes and -1,552 code bytes.

Metadata grows while generated code/layout changes offset it. An empty entity
without typed read/write consumers is not a universal per-entity cost estimate.
This is a fresh-composition ablation, not an entity-creation migration on populated
data. It provides no reason to impose a linear entity-count deployment limit.

## ICYDB-038: real reads, actions and opening work

Temporary query wrappers measure the maintained method bodies and encode the real
results after the counter window. All three repetitions per method and version
have identical instruction counts, and healthy encoded results match across
versions. Result serialization and transport are outside the counter window.

| Method | Baseline instructions | Candidate instructions | Delta |
| --- | ---: | ---: | ---: |
| `list_minerals` | 255,438,632 | 254,936,385 | -0.197% |
| `list_industry_catalog` | 265,648,323 | 265,265,125 | -0.144% |
| `get_my_robot` | 154,137,563 | 154,063,287 | -0.048% |

The uninstrumented 64-action stationary/walking trials use batch sizes 1, 8, 16
and 32. Both complete eight-trial reports repeat exactly in a second execution.
Requests match in canonical intent bytes and encoded Candid argument bytes;
conservation, replay and conflict rejection assertions pass. Candidate consumed
nominal cycles increase **0.21–0.97%**, while action-body instructions change
**-0.51% to +0.12%**. The maintained consumed-cycle owner sums nine management
metrics categories; settlement windows include background work. These totals are
not balance subtraction, exclusive method costs or a financial conversion.

Existing lifecycle counters in a freshly enrolled fixture give the following
instruction totals after a fixed 20-step service schedule:

| Phase | Baseline | Candidate | Samples in each |
| --- | ---: | ---: | ---: |
| Lowering | 177,894,429 | 180,734,450 | 1 |
| Publication | 239,548,331 | 242,668,476 | 1 |
| Runtime compilation | 165,994,831 | 167,352,696 | 1 |
| Cardinality | 3,733,751 | 3,735,098 | 4 |
| Startup recovery | 20,836,866,503 | 21,047,687,075 | 32 |

These spans overlap and must not be summed. They include fixture materialisation,
enrolment and online folds, so they do not isolate a cold-startup bill. The shared
recovery/fold span dominates observed opening work. Actual reads show small gains;
the comparison does not demonstrate a broad action-cycle reduction or justify
another cache or general query rewrite.

## Evidence, reproduction and validation

- [Measurements](artifacts/measurements.json) preserve build identities, artifact
  hashes, complete per-case counters, query parity and scope limits.
- [Journal events](artifacts/journal-events.csv) preserve unique indexed poststates
  for exact append/retirement conservation, including retained final debt.
- [Framework input](artifacts/framework-input.patch) reconstructs the eight frozen
  cleanup changes from the captured IcyDB HEAD, independently of later edits.
- [Probes](artifacts/probes.patch) preserve the application wrappers, journal
  observer and focused host tests against the captured source HEADs. Path prefixes
  distinguish disposable application, framework and host copies. The baseline
  journal observer is applied to a local copy of registry core 0.262.2.
- [Entity ablation](artifacts/extra-entity.patch) preserves the additional schema
  declaration and its module wiring.

Canonical builds use `canic --environment toko_miner build toko_miner --profile
fast` with offline cached dependencies. Probe dependency selectors use local
paths only in disposable copies. The host runs only the named direct-batch test,
`upstream_startup_and_reads`, and `upstream_controlled_debt`; the debt test selects
one or eight explicit ticks through `ICYDB_UPSTREAM_EXPLICIT_TICKS`. Build
identities distinguish uninstrumented baseline/candidate from matched probes.

Twelve focused probe executions pass: five action-cost runs (40 trials, 2,560
actions), five opening/read/schema runs and two debt controls. Eighteen healthy
read samples pass. Report evidence consistency, input patch reconstruction and
local documentation references are checked separately. Disposable PocketIC
fixtures were created and dropped; no existing network was restarted.

Setup failures were resolved before collecting final receipts: unavailable
registry DNS required offline builds; sandbox socket restrictions required
localhost-only execution permission; stale build dependency paths required a
fresh stable seal; prototype entity module ordering and tuple-result decoding
were corrected. One baseline build containing the ablation was cancelled and
replaced. Failed, rejected and cancelled artifacts are excluded from comparisons.
No seal policy or deployment limit was weakened.

Full repository suites, wall-clock benchmarks, mainnet runs and downstream release
adoption checks were not executed. Production runtime complexity is unchanged:
no new state, configuration, API or execution route. Only this report and its
five evidence files are added to IcyDB; disposable probe code is not shipped.

Follow-up priority: attribute shared recovery/fold instructions and inspect exact
backlog through its canonical owner. Live capacity and large checkpoint payloads
remain separate measurement questions; none of these feedback entries is declared
fully accepted downstream by this report.
