# Collection materialization — current-path baseline

## Result

The maintained structural query path has a measurable collection-read cost.
For the repeated 1,024-item observation, first-item membership uses 3,985,590
query instructions versus 1,119,198 for the scalar-only control. Explicit-call
cycles are 12,924,549 versus 10,178,932, a difference of 2,745,617 (+26.97%).
This is **not a measured optimization or an allocation-only attribution**.
Both queries run against the same populated row and project only its ID.

An owner-local experiment is justified for larger lists. Do not build a second
predicate decoder or replace full-value validation with early success. The
existing source preflight and corruption tests remain the semantic gate.
C1 is still in progress; no runtime candidate has been retained.

## Exact subject and artifact

HEAD: `4698cecb8dbba6786079b759b43494b020559997`; Cargo version 0.259.6,
0.260 planning/test work already dirty. The runtime is unchanged. The new
actor inputs are HEAD plus the modified actor root and schema and the new
collection-workload module, not HEAD alone. Previous dirty core tests are
`cfg(test)` and are not compiled into this actor. No dependency/version edits.

| Input | SHA-256 |
| --- | --- |
| Cargo.lock | `8024973c209ab9244ac24687f3a8d42e772f0c3c4454d7a5f57aeaaa0c34c658` |
| canisters/test/sql/src/lib.rs | `fcb472e4386176f5a9edfaedd13edd441065dce65a8ddbae5e0d024e91093068` |
| canisters/test/sql/src/collection_workload.rs | `d7e787fd6a31c4149c84dbb65d46869c7daea14b250cfc97bacbafb496ca588f` |
| schema/test/sql/src/sql.rs | `cc79dff96ef4b1f1b20081204efeb8b81726cdda1fc7d382e47809f7a28e9c82` |
| testing/integration/tests/sql_canister.rs | `8c5b1eb66ed60a804bf94443f313a311873aa81ec2a9688eb61d7cc78c952dfc` |
| testing/integration/tests/collection_workload/mod.rs | `eaf67011c3e2d740c24dbd4eaf1c027769f88f97ec37a75d83cb4046e82a1801` |

Rust 1.98.1 (`48a229cea`), PocketIC 16.0.0, pinned Binaryen 132. The existing
retained build owner builds `canister_test_sql`, defaults off, features
`candid-export,local-sql-query,test-admin-api`, LocalTest, SQL and Candid export
enabled. `wasm-release`: opt-level z, fat LTO, one codegen unit, abort panics,
stripped symbols. Existing Binaryen -Oz post-link flags remain unchanged.

Final raw Wasm: **4,255,817 bytes**, SHA-256
`c98541aa149b8d63af804003025514db60822a7719c5129de38b5cd8ff0a9af2`.
Defined functions: **10,133**. Code-section payload: **4,021,895 bytes**;
data-section payload: **218,386 bytes**, including section encoding overhead,
not just application payloads. Counts obtained with `wasm-objdump -h` against
the rechecked hash, consistent with the retained final module.

Artifact namespace:
`823dde03c7560a3664bd3b412f48ad81c265e1cc1d77fb12be6d3b9aad847199`;
entry `1c83c3fbeccc1c9c711059ce82eb7bea1da3a2115605976088677205a4ffa1df`,
`outputs/0000.artifact`. The retention handle lives through the initial byte
read; all three instances install clones of the same module. No new build
cache, lock, measurement runner or receipt protocol was introduced.

This actor includes SQL, the existing test endpoints, one new generated
collection entity and two test-admin-only endpoints. It is not a minimal typed
actor. The new entity is part of the shared SQL fixture schema in both build
profiles; only the probe endpoints are gated to test administration.
Do not subtract an older actor/report and call the difference a runtime
regression or optimization. Incremental fixture Wasm and gzip deltas are
unmeasured.

## Workload and independent checks

For each length 16, 256 and 1,024, install a fresh canister and seed three
rows: ID 1 has the list `0..length`, ID 2 an empty list, ID 3 null. All have
scalar marker 7. The accepted generated proposal uses Nat64 IDs, a Nat64
marker and a nullable list of Nat64; normal startup and writes provide the
accepted schema and persisted values.

Each query explicitly filters one ID and limits output to one row. No SQL is
parsed by the measured call. The scalar control filters marker 7; membership
uses 0, length-minus-one and length for early/late/absent results. Emptiness
uses the populated, empty and null rows. Output controls project the complete
list, with and without membership. These are different queries, not two
implementations of the same query. The host independently checks typed IDs,
all projected list elements and empty result sets; no rendered equality or
endpoint success flag substitutes for data assertions.

There are nine scenarios, each called twice in fixed order, at three lengths:
**54 measured update messages**. Observation 1 means the second call to that
scenario, not a fully isolated warm-cache experiment. Other scenarios and
allocator state precede it; observation 0 is not guaranteed globally cold.
All first and repeated observations are retained in [messages.txt](messages.txt).

The in-canister counter covers only `execute_trusted_live_page`: it includes
query admission/preparation, planning, row access, decode, predicate evaluation
and output construction. Session/query construction, request-scope teardown,
ingress decoding and reply encoding are outside that instruction interval.
No stage-level allocator, decode or comparison attribution was measured.

Cycles are the canister-balance difference around each explicit update and
include the broader message costs. Installation, startup, seed and 64 ticks
before each measurement are outside the interval. No time advancement is used
for those ticks. Any co-scheduled work inside the interval is not separately
attributed. These are not complete application-lifecycle costs.

## Selected repeated observations

| List length | Scalar query instructions | Early-match instructions | Scalar cycles | Early-match cycles |
| --- | ---: | ---: | ---: | ---: |
| 16 | 1,097,479 | 1,137,022 | 9,994,642 | 10,071,510 |
| 256 | 1,264,702 | 1,978,057 | 10,162,585 | 10,831,803 |
| 1,024 | 1,119,198 | 3,985,590 | 10,178,932 | 12,924,549 |

| 1,024-item fixture scenario | Query instructions | Explicit-call cycles | Result |
| --- | ---: | ---: | --- |
| Late match | 4,153,462 | 13,011,703 | ID 1 |
| Absent | 3,891,298 | 12,984,521 | No row |
| Nonempty | 3,970,486 | 12,908,677 | ID 1 |
| Empty | 1,089,453 | 10,184,421 | ID 2 |
| Null tested for emptiness | 1,073,161 | 10,170,488 | No row |
| Project list, scalar predicate | 4,420,737 | 14,548,744 | ID 1 and all items |
| Project list, early membership | 4,337,050 | 14,547,605 | ID 1 and all items |

The near-equal populated early-match/nonempty costs are consistent with the
source finding that both materialize the selected list. They do not establish
how much can safely be removed. Full-list output still needs ownership, and
two observations per case do not establish a distribution or universal cost.
No native timing, page-derived byte estimate, production ceiling, or
before/after speedup is claimed.

## Validation, scope and next step

The focused ordinary integration test passed: **1 passed, 0 failed, 0 ignored,
99 filtered out**, with all 54 typed-result checks. Actor feature check and
focused actor/integration Clippy gates passed with warnings denied. Formatting
and whitespace checks passed. Full suites and the whole-actor artifact matrix
remain user-owned. The initial actor check exposed a Candid collector import
scope error; qualifying the endpoint's error type fixed it before measurement.

Local lifecycle: the test started a PocketIC server, created and dropped three
disposable instances, and completed successfully. No deployed canister,
downstream repository, version, commit or publication was changed.

This measurement handoff adds five fixture/source files or edits, approximately
217 lines, plus evidence and active notes. Production query structure stays
unchanged: no decoder, runtime mode, cache or execution route was added.
Next, evaluate the smallest change at the accepted list decode/reader owner
against this frozen fixture, preserving whole-value corruption checks and
selected-list output reuse. Do not implement a general streaming framework.
