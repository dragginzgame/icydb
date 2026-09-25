# C48 — Warm Read Instruction Qualification

## Decision

Retain C37's replicated accepted-root preparation. The current 0.261 source
reproduces the higher warm-query instruction counts, but removing preparation
restores roughly 34 million instructions to every cold query-only successor
read. The existing five-line driver call is the only source difference in the
matched experiment; the whole-query counts do not identify an independent
warm-path operation that can safely be removed. No runtime code, cache state,
execution route, or format changes are justified by this fixture.

## Matched Fixture

Measured 2026-09-25 with PocketIC 16.0.0 and the maintained two-case
`schema_migration_closeout` entity-rename target. Both cases passed serially
with identical input and projection hashes. The baseline was a disposable
source snapshot with only the five C37 driver lines removed. All other Rust
sources, Cargo manifests, and the lockfile matched the current tree; the
lockfile SHA-256 was
`2dd44559b40b8c03d503977f2baea28c753bcc6fb32d091cbcb63496fc3623c2`.
Baseline, retained, then baseline were built and run from the same temporary
source and target paths. The repeated baseline reproduced its raw Wasm hashes
and instruction counts exactly. Earlier shared-target runs were discarded
because they reused the wrong actor artifacts.

| Actor | Baseline raw Wasm bytes | Retained raw Wasm bytes | Delta |
| --- | ---: | ---: | ---: |
| Source | 8,956,758 | 8,956,611 | -147 |
| Successor | 8,987,193 | 8,987,291 | +98 |

Source Wasm BLAKE3: baseline
`cb37df4cdc5c237ea9685fc1c0e8cae84017d3bada9f071ceef38cc95e4a7a55`,
retained `0d0d3910c4fa3741956fdd62baaf10d1f66d55a166a5cd4ad09c7b59c2b4d1a9`.
Successor Wasm BLAKE3: baseline
`8e7a6fee9f56f1eb1717a84865995dc5cc9472e0309572013b31a8d1334bcef0`,
retained `cd5f991b77b5262376798f74b529cd17ccc7cecd6577fe041b5e2b782964254c`.

## IC Measurements

These are actor-local SQL-query instructions. The paired cycle values below
cover their named host-call envelopes and must not be added to instructions.

| Lifecycle and read | Baseline instructions | Retained instructions | Change |
| --- | ---: | ---: | ---: |
| Direct: first/repeated successor query | 34,241,694 | 2,024,990 | -94.09% |
| Direct: warm after updates | 2,067,815 | 2,148,790 | +3.92% |
| Direct: after final restart, before updates | 35,252,965 | 2,020,984 | -94.27% |
| Direct: final warm read | 1,897,739 | 2,057,441 | +8.42% |
| Restart before publication: first/repeated successor query | 34,335,548 | 1,943,529 | -94.34% |
| Restart before publication: warm after updates | 2,147,026 | 2,309,032 | +7.55% |
| Restart before publication: after final restart, before updates | 35,343,207 | 1,939,940 | -94.51% |
| Restart before publication: final warm read | 1,897,739 | 1,977,441 | +4.20% |

The source read after seeding fell from 1,819,265 to 1,738,607 instructions
(-4.43%). Thus the five-line startup call affects more than one lifecycle's
observed query cost; warm differences are not a per-function attribution.
For the direct case, the first successor-read delivery costs 457,044,624
baseline versus 489,206,444 retained cycles (+32,161,820). The source-to-
successor upgrade/watchdog envelope costs 52,350,529,015 versus
52,352,154,808 cycles (+1,625,793); the final restart/watchdog costs
20,400,211,733 versus 20,454,676,825 cycles (+54,465,092).

## Validation And Scope

Both focused PocketIC cases passed for both variants and the baseline repeat.
Eight disposable PocketIC servers were started and stopped across measurement
and artifact-isolation attempts; the first sandboxed attempt failed before
binding localhost. No persistent network was touched. This slice changes two
documentation files, adds no production code or tests, and leaves runtime
complexity unchanged. The qualification is limited to the two generated
rename lifecycles; whole-application savings and other query shapes remain
unmeasured. No wall-clock performance metric was used.
