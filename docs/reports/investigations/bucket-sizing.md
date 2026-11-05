> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

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

## Follow-up before changing the default

Qualify a real application's store count, key widths, payload placement,
long-running updates/deletes, convergence/recovery and growth headroom.
Include background/lifecycle instructions or appropriately separated cycle
costs. Investigate index-region allocation independently if its footprint is
important. Keep the current default until that decision is explicitly made.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/13/bucket-sizing/01/report.md).
