# IcyDB memory-bucket sizing experiment

Date: 2026-09-13. Source: `82b0d71c9` (published 0.257.10).
Dependency: published `ic-memory 0.13.2`, `ic-stable-structures 0.7.2`.
Production defaults, applications and persisted formats are unchanged.

## Finding

**16 pages is a strong general-purpose starting point for further application
qualification, not a universal optimum or a changed IcyDB default.** In this
fixture it reduces empty physical allocation from 64.06 to 22.06 MiB and final
allocation from 184.06 to 148.06 MiB, with +0.242% total write instructions.
The manager's theoretical capacity becomes 32 GiB instead of 256 GiB.

One-page buckets save little more than four-page buckets in the populated case
(0.75 MiB), while cutting manager capacity fourfold and increasing write work.
32 pages is also attractive: more headroom and essentially unchanged write
instructions, at the cost of 6 MiB more allocation than 16 pages in this fixture.

## Matched results

| Pages | Bucket | Empty MiB | Final MiB | Total write instructions vs 128 | Manager capacity |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 64 KiB | 17.06 | 144.56 | +1.044% | 2 GiB |
| 4 | 256 KiB | 18.06 | 145.31 | +0.523% | 8 GiB |
| 8 | 512 KiB | 19.56 | 146.56 | +0.507% | 16 GiB |
| 16 | 1024 KiB | 22.06 | 148.06 | +0.242% | 32 GiB |
| 32 | 2048 KiB | 28.06 | 154.06 | -0.010% | 64 GiB |
| 64 | 4096 KiB | 40.06 | 164.06 | -0.109% | 128 GiB |
| 128 | 8192 KiB | 64.06 | 184.06 | +0.000% | 256 GiB |

MiB/GiB are binary units. Capacity is the shared 32,768-entry bucket table's
allocation capacity, before platform/backing limits. It is **not application
payload capacity**, nor evidence of testing at that scale. Different IcyDB
stores and other users of the same manager share it.

Write percentages compare the sum of all four phases against 128 pages:
9,192,199,034 instructions at baseline. They are not averages of percentages.
Matched read samples vary from -0.57% to +1.07% across the entire sweep; no
uniform read improvement is established. Do not treat the tiny negative write
deltas at 32/64 pages as a general performance promise.

Raw Wasm is **2,916,837 bytes**, gzip 1,175,734 bytes, 7,474 defined functions.
The exact same binary is installed with every configuration: size/function
delta between bucket choices is zero. This is an experiment actor's absolute
size, not a new production-actor size or comparison against earlier releases.

## Suggested guidance profiles

These are documentation recipes for the existing setting, not new API modes.

| Guidance | Pages | Appropriate starting assumption |
| --- | ---: | --- |
| Compact | 4 | Many small canisters or tight initial footprint; measured growth stays comfortably below the 8 GiB manager ceiling. |
| General | 16 | Prefer low rounding overhead with 32 GiB manager capacity; this is the strongest initial candidate here. |
| High headroom | 128 | Keep the existing 256 GiB manager capacity when long-term growth is large or uncertain. |

For applications needing more than the general profile's headroom but not
256 GiB, the measured 32/64-page choices provide 64/128 GiB respectively.
Choose based on measured storage-region growth, including indexes and journals,
not just row counts or encoded payload bytes. Leave capacity headroom.

At the application's **single bootstrap owner**, before any IcyDB/default-runtime
access or lifecycle participant call, the existing API is:

```rust
let config = ic_memory::MemoryManagerConfig::new(16)?;
ic_memory::bootstrap_default_memory_manager_with_config(config, &policy)?;
```

Here `policy` is the application's existing allocation policy; map errors into
its normal bootstrap error type. Bucket size is one immutable manager-wide
setting, not per entity/store, and is not authorization to access memories.
Do not introduce another bootstrap owner or replace application policy with the
experiment's permissive test policy.

Existing memory must match an explicitly requested setting. Ordinary reopen
honors the persisted size. Changing an existing deployment's bucket size requires
recreation/reinstall; it is not a shrink operation. Deleting rows does not
reclaim manager buckets.

## Workload and measurement boundary

One SQL-free experiment actor links the maintained SQL fixture schema but uses
ordinary structural inserts and indexed dynamic reads, not SQL execution.
Each of seven fresh installations receives its page setting through init.
Generated lifecycle startup completes before the first sample.

Each installation runs these cumulative phases, in batches of 32 rows:

1. Empty, after startup.
2. Add 64 rows with 64-byte indexed names.
3. Add 2,048 rows with 1,024-byte indexed names.
4. Add another 2,048 rows with 1,024-byte indexed names.
5. Add 512 rows with 2,048-byte indexed names.

The final database contains 4,672 rows. Names are unique deterministic strings;
age/rank are deterministic numeric fields. The actor uses the fixture's
generated ULID primary keys. The simulated clock schedule is identical.
Eight PocketIC ticks and one second of simulated IC time are supplied between
write batches to deliver normal timer work. This is scheduler driving, **not**
a wall-clock benchmark.

Each phase reads 32 names at its beginning, end and midpoint, then rechecks
32 original small rows. The actor asserts each returned name equals its expected
value. The successful sweep contains 1,022 batch updates and 3,584 checked
point-read results. All writes succeed. These are sampled correctness checks,
not an exhaustive database-integrity or recovery qualification.

Instruction counters bracket each insert endpoint's request work, including
patch construction and the ordinary write pipeline, and each read endpoint's
32-query loop. Allocation snapshots run outside those intervals.
**Timer callback work, complete lifecycle cost and separate IC cycles were not
measured.** The profiles are therefore candidates, not an end-to-end cycle
qualification or a justification to change the production default immediately.

Independent query invocations and three different row positions are measured;
the read samples are not a cold/warm cache series. There is one complete sweep,
not statistical repetitions. The baseline pilot's first three valid phases
reproduced the corresponding final baseline instruction totals.

## What the memory numbers mean

For each phase, every bucket choice has exactly the same virtual extent,
including the same extent for each occupied region. Physical allocation obeys:

`physical = manager metadata + virtual extent + bucket rounding slack`

The final virtual extent is 144.5 MiB. Smaller buckets reduce rounding slack,
not that underlying extent. The index region alone occupies 126,877,696 virtual
bytes (121 MiB); the data region occupies 6,356,992 bytes. The fixed commit
region and other storage regions also contribute.

This fixture deliberately includes wide **indexed** text. Its index allocation
must not be extrapolated to an application with large unindexed payloads.
Neither virtual extent nor bucket slack measures live payload occupancy or
reusable capacity inside the storage structures. Index-node/internal allocator
efficiency is a separate question, not fixed by this setting.

## Reproduction and retained evidence

Raw phase records: [measurements.json](measurements.json).
Exact source snapshots: [actor.rs](actor.rs), [driver.rs](driver.rs),
[build.rs](build.rs), and the three adjacent TOML manifests.
These are isolated experimental sources, **not production application templates**:
they expose unauthenticated fixture endpoints and use assertions.

To reproduce, create an external temporary workspace: use `workspace.toml` as
its `Cargo.toml`; put `actor.toml`, `build.rs`, and `actor.rs` at
`actor/Cargo.toml`, `actor/build.rs`, and `actor/src/lib.rs`; put `driver.toml`
and `driver.rs` at `driver/Cargo.toml` and `driver/src/main.rs`.
The manifests record the original checkout path; adjust that prefix to a
checkout of the recorded source. They are not members of the IcyDB workspace.

Build the actor for `wasm32-unknown-unknown` using its `wasm-release` profile,
then apply Binaryen `-Oz --enable-bulk-memory --enable-sign-ext
--enable-nontrapping-float-to-int --one-caller-inline-max-function-size=0`.
Build the driver natively and pass it the optimized Wasm path and
`128,1,4,8,16,32,64`. Set `POCKET_IC_BIN` to PocketIC 16.0.0.
Rust was 1.98.1 and Binaryen 132. Native execution is only the driver;
all reported instruction samples execute in Wasm under PocketIC.

Local artifacts and the complete source/lockfile archive are retained at
`target/bucket-sizing-experiment/`. The archive includes the exact experiment
dependency resolution; rebuilding without that lockfile may resolve differently.
Wasm SHA-256:
`b8658f6ed4f7f75770d733f70f6901270188e8a6c393ada0fafcaeb6afbb0e1b`.
Experiment lockfile SHA-256:
`6319a5fb74204dc4630f48249f59a66a077d879e018a782fff6feea1a7b11490`.

The initial pilot fixed a missing request scope and an incorrectly inferred
Candid argument type in the temporary harness. An 8 KiB indexed-name pilot
correctly rejected at the existing 4 KiB index-key boundary; the successful
matrix uses 2 KiB instead. A build also encountered the user-owned release
version transition; the final matched artifact was rebuilt against 0.257.10.
None required a production fix.

The isolated PocketIC instances were dropped after their runs. No application
network/database was changed. Full repository suites and production-default
qualification were not run.

Handoff footprint: 11 documentation/evidence files, approximately +537 net
lines including standalone harness snapshots; zero production-code lines.
Runtime complexity is unchanged: no profile enum, bootstrap wrapper or allocator.

## Follow-up before changing the default

Qualify a real application's store count, key widths, payload placement,
long-running updates/deletes, convergence/recovery and growth headroom.
Include background/lifecycle instructions or appropriately separated cycle
costs. Investigate index-region allocation independently if its footprint is
important. Keep the current default until that decision is explicitly made.
