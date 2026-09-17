# Live allocation attribution — retained payload versus Wasm pages

Date: 2026-09-17. Published 0.257.20 versus the shared-snapshot handoff plus
borrowed fingerprint normalization in the current 0.257.21 candidate.
This is a diagnostic experiment, not another production change.

## Verdict

**The extra 192 KiB of allocated Wasm memory is not an increase in live Rust
allocation payload in this fixture.** After fixture loading, the candidate has
28,652 fewer live requested bytes, with the same number of live allocations.
Its peak requested bytes are also 28,652 lower. Nevertheless, linear memory
grows to 73 pages rather than 70. The page counts match the uninstrumented
[earlier measurements](../03/report.md).

This points to allocation layout/allocator high-water behavior rather than
more retained payload. It does not identify exact free-list fragmentation,
allocator overhead, internal realloc overlap, or touched memory pages. Do not
claim the warm-query instruction/cycle regression is eliminated, and do not
introduce padding or allocation-order hacks to recover these three pages.

## Measurements

Each observation is a query and repeats identically before continuing. Both
fresh canisters execute installation, the maintained startup driver, fixture
reset/load, and five identical primary-key updates with result parity. Zero-time
settling ticks discharge setup work between observations.

| Stage | Baseline live requested bytes | Candidate live requested bytes | Baseline → candidate Wasm pages |
| --- | ---: | ---: | ---: |
| Installed, before explicit startup delivery | 27,024 | 27,024 | 22 → 22 |
| Startup settled | 30,333 | 30,333 | 29 → 29 |
| Fixtures reset | 539,542 | 510,890 | 31 → 31 |
| Fixtures loaded | 687,140 | 658,488 | 70 → 73 |
| First warm update | 724,732 | 696,080 | 70 → 73 |
| Second through fifth warm updates | 739,249 | 710,597 | 70 → 73 |

The 28,652-byte reduction is already present after reset. After loading,
observed lifetime peaks are 2,732,315 → 2,703,663 requested bytes. At the final
sample both artifacts have 10,417 live blocks and a peak of 18,099 blocks.
Warm-up increases live storage equally in both; the last four observations are
flat. These samples do not establish absence of leaks in other workloads.

Cumulative successful requested bytes through the final sample are
86,426,810 → 85,903,704 (−523,106). This includes full successful realloc request
sizes even when resizing in place; it is allocation demand, not OS memory or
physical bytes copied. [Complete observations](allocations.csv).

## Scope and observer limits

The [temporary allocator probe](allocator-probe.rs.txt) forwards each allocation,
zeroed allocation, reallocation and deallocation unchanged to Rust's `System`
allocator. Non-allocating atomic counters record requested sizes and block
counts. Reallocation removes the old requested size and adds the new size after
success; it cannot observe temporary internal overlap. Failed allocations are
not counted. The returned observations include the probe query's live wrapper
allocations but are sampled before allocating its result vector.

This counts Rust global-allocator traffic, not allocator bookkeeping, free-list
capacity, stack/static memory, stable memory or allocations that bypass that
interface. The counters alter code and static layout, so no instrumented
instructions/cycles are used as production performance evidence. Equal page
counts with the original runs provide a useful cross-check, not proof that
every address is unchanged.

The first experimental build hit the workspace unsafe-code lint. Its required
`GlobalAlloc` implementation is explicitly permitted only in the isolated
probe module; the root policy was unchanged. No allocator instrumentation or
unsafe boundary was added to production. All temporary source wiring was removed.

## Remaining ownership opportunity

Source evidence still identifies one independent duplicate: the decoded bundle
owns each `PersistedSchemaSnapshot`, while catalog selection clones it into an
`AcceptedSchemaSnapshot` shared by runtime consumers. The earlier handoff
removed retained encoded bytes and downstream decoding; it did not share the
payload with its decoded bundle. These observations measure whole-canister
allocations, not a per-object byte attribution to that duplicate.

The next bounded preflight can test sharing the immutable snapshot payload at
its existing owner, preserving detached authority when a new schema is built.
Prefer that to a second cache, bundle-retaining lookup facade or per-field
reference graph. Need: the demonstrated bundle/selection copy. Simplest
alternative: keep it if the measured benefit does not justify changing ownership.
Canonical owner: `PersistedSchemaSnapshot`; no new configuration, format, query
route or authority is justified. Any copy-on-write implementation must prove
old/new root isolation and preserve encodings, mutation errors and hashes.

Measure both retained allocations and uninstrumented whole-query/cycle/Wasm
costs before landing it. Lower live bytes do not promise fewer allocated pages.
Do not expand this preflight into catalog-wide sharing or allocator redesign.

## Reproduction and validation

Use the separate source/artifact setup from receipts 02 and 04. Wire the same
probe module into each audit actor. Build with Rust 1.98.1, locked dependencies,
the same `wasm-release` profile and pinned Binaryen transform. Explicitly
invalidate the core crate when switching source copies with a shared target
directory; both core versions were rebuilt here. The candidate's release code
matches receipt 04's landed prototype before this instrumentation is added.

Temporarily wire the [native runner](runner.rs.txt) under `sql_perf_audit`, and
run only its named test with `ICYDB_ALLOCATION_MEASUREMENT_DIR` pointing to the
two instrumented artifacts and PocketIC 16.0.0. All 18 stage observations pass
their repeat equality and consistency assertions; ten updates preserve results.
Both disposable fixtures were released; shared networks were untouched.

Instrumented raw Wasm: 4,488,877 / 4,486,760 bytes. SHA-256 baseline
`d11072542d1d6b6b8ce4c7b330679cf71275a0c18ddbea0b556075238b3f5245`;
candidate `4459c1a2b16a0b1235e122a26bc522a4973a84e26bd1a863cccf60681cd12dd9`.
These sizes describe the diagnostic binaries, not an optimization in this turn.
Uninstrumented production remains 4,482,953 bytes; no new production cycle or
instruction delta is claimed. Formatting and diff checks pass. Full release
validation remains user-owned; runtime complexity and production source are unchanged.
