> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Typed Binding Copy Removal — Landing Slice 6

## Source And Measurement Controls

Baseline: `4b11bf85f6c848e8944de7ef51572affa819d704` plus the pending test-only
boundary slice. Candidate: that source plus this copy-removal change. No Cargo
version or dependency changes. Cargo.lock SHA-256 remains
`20c9d8e610f00067d753f7fb4cda5a27843d8a7b49aca3d463230e37327a446e`.

Temporary probes live in `/tmp/icydb-batch-audit.bw3k0s`, not maintained source.
The measured schema store file matches the candidate byte-for-byte. The measured
typed-validation function matches after removing its test-only measurement guard.
Candidate production-owner file hashes:

- `crates/icydb-core/src/db/schema/store.rs`:
  `f09be354e7056732fed57ca269215f5c351e7e3e86977364495715e6b934256b`.

- `crates/icydb-core/src/db/session/write.rs`:
  `0012ab65eefd3ae6196c5131747499e76afbdc51e0c4ec7300a2e8cc07ac74a3`.

Native method matches run 01: Rust 1.97.1, optimized test profile, all features,
preconstructed requests/bindings, one warm-up write, fresh fixture threads,
two samples per size/shape, allocation-request counters and timed owner guards.
The 64-entity control adds 62 unused two-field Nat64 entities to the same store;
batch membership and row payloads stay unchanged. The small and wide catalogs
each cover general structural/typed same- and mixed-entity batches and the
same-entity typed terminal at 1, 8, 128 and 1,024 rows. The journaled nested-relation
control also retains sizes 1, 8 and 128. All measured batches succeeded.

## Native Results At 1,024 Rows

| Catalog entities | Terminal | Baseline host ms | Candidate host ms | Baseline requested bytes | Candidate requested bytes |
| --- | --- | ---: | ---: | ---: | ---: |
| 2 | General typed, one entity | 10.05–10.32 | 8.19–8.30 | 14,905,346 | 9,318,071 |
| 2 | General typed, two entities | 9.030–9.031 | 7.65–7.85 | 14,076,051 | 8,488,776 |
| 64 | General typed, one entity | 48.13–48.30 | 9.67–9.76 | 103,617,046 | 9,318,071 |
| 64 | General typed, two entities | 47.62–48.14 | 8.90–9.00 | 102,787,751 | 8,488,776 |
| 2 | Same-entity typed | 5.53–6.50 | 5.58–5.96 | 8,543,176 | 8,537,725 |
| 64 | Same-entity typed | 6.24–6.31 | 5.82–5.99 | 8,629,724 | 8,537,725 |

Allocation counts/bytes matched exactly across both samples. General homogeneous
typed allocation requests fell from 130,769 to 101,044 with the small catalog and
from 1,059,419 to 101,044 with the wide catalog. General mixed typed requests fell
from 126,199/1,054,849 to 96,474. Unrelated catalog copying is gone from these
intervals, but live validation still traverses catalog metadata; wide-catalog
execution is not claimed to have constant cost.

Structural allocation counts/bytes are unchanged: for example, the homogeneous
1,024-row terminal still requests 92,843 allocations and 8,843,430 bytes. The
nested-relation control is unchanged in allocation counts/bytes as well.

## Matched IC Results

A temporary method on the maintained `one_entity_typed_query` actor prepares
generated writes outside the measured interval, then measures the selected
terminal, result counting and destruction with performance counter 1. The fixture
has generated Ulid IDs, managed timestamps and empty profile collections. Each
sample uses a fresh installation and one warm-up write. There are two samples at
each size; both produced identical instruction counts for each binary.

Endpoint decoding, session/binding acquisition, input construction and generated
row decoding are excluded. The same-entity terminal includes its maintained row
projection/iteration. Compare each terminal with itself, not as a promise that
one terminal can replace every other application result surface.

| Terminal | Rows | Baseline instructions | Candidate instructions | Delta |
| --- | ---: | ---: | ---: | ---: |
| General typed | 1 | 3,461,532 | 3,369,333 | -2.664% |
| General typed | 8 | 7,808,244 | 7,432,461 | -4.813% |
| General typed | 128 | 87,736,504 | 82,406,646 | -6.075% |
| General typed | 1,024 | 716,568,738 | 674,753,117 | -5.836% |
| Same-entity typed | 1 | 3,035,732 | 2,991,248 | -1.465% |
| Same-entity typed | 8 | 5,841,776 | 5,788,588 | -0.910% |
| Same-entity typed | 128 | 56,481,707 | 56,414,701 | -0.119% |
| Same-entity typed | 1,024 | 466,568,524 | 466,789,853 | +0.047% |

The large same-entity terminal shows a small regression, not a speedup. Its
single validation already amortizes the old copy. The 64-entity result above is
native evidence only; no wide-catalog IC speedup is inferred from it.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/06/batch-preparation/02/report.md).
