# Typed Binding Copy Removal — Landing Slice 6

## Outcome

The approved follow-up to [the preparation audit](../01/report.md) removes the
owned accepted-schema bundle copy during typed-binding validation. General
typed batches use 2.66–6.07% fewer IC instructions in the measured actor. Native
cumulative requested allocation bytes fall 37–40% with two catalog entities and
91–92% with 64. No batch cache, skipped validation or new execution route is added.

`SchemaStore::borrow_current_accepted_schema_bundle` owns the validated borrow.
The existing owned accessor clones through that same owner. Both retain current
root verification and the per-call durable activation/job closure check,
including cache hits. The session checks the same incarnation, revision,
fingerprint, layout generation, entity source, field IDs and slots as before.
Its schema borrow ends before mutation preparation or commit.

The validation count remains N+1 for the general typed batch and one for the
same-entity terminal. This is copy removal, not permission or binding memoization.
The broader layout/decode context cache and insert-only result policy are unchanged.

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

These host figures are diagnostic samples with instrumentation and scheduling
noise, not production latency forecasts. Allocation bytes are cumulative
requests, including reallocations, not peak/live memory. Generated builder
`push`, initial binding issuance and final generated row decoding are excluded.

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

The unchanged `nested_relation_direct` structural actor repeats run 01's full
1/8/128/1,024-row insert, replacement-target insert, source insert/update/delete
matrix. All 40 measured phases succeeded, with instruction counts exactly equal
to the baseline. This is the non-typed shared-owner control.

## Wasm And Complexity

Both pairs use Rust 1.97.1, production features, SQL off, Candid metadata export
off, `wasm-release`, and the canonical Binaryen 132 post-link pipeline. PocketIC
is 16.0.0. Native instrumentation is compiled out. The typed actor's temporary
measurement method is identical in both binaries and is not added to the repo.

| Actor | Compiler raw bytes, baseline → candidate | Final raw bytes, baseline → candidate | Defined functions | Gzip bytes, baseline → candidate |
| --- | ---: | ---: | ---: | ---: |
| Typed measurement actor | 2,420,296 → 2,420,345 | 2,116,923 → 2,116,937 (**+14**) | 5,458 → 5,458 | 839,952 → 839,925 |
| Structural relation control | 2,374,616 → 2,374,625 | 2,075,549 → 2,075,565 (**+16**) | 5,340 → 5,340 | 820,852 → 820,854 |

Gzip uses `gzip -n`; raw deployable bytes are the decision metric. Typed Wasm
SHA-256, baseline then candidate:
`b39b5fd798f1e606c8375b42dd76f2c3c4b4fbc7c27996db3a7a383b16e0df46`,
`5bba3cb228f2a296e56cfa4500284b5fc5d9381ad2e007a1d2f782437e6667c5`.
Structural hashes:
`fef2d726adfb8aff6ef435409bdfcaf6b23251414b0c8ac0cb85770b4fde1522`,
`0b0122651b50ca3a7eecf87da0c69aa055857e5d8e0fd9cf53cda8805a3b5893`.

Slice 6 touches seven files: two runtime owners, their direct tests (the session
file also contains its new test), root/detailed notes, status tracker and this
receipt, approximately 300 net lines including documentation. Rust grows by 97
net lines: 16 production and 81 test/annotation lines.
The implementation shape is slightly simpler in ownership: one validated bundle
borrow supports both borrowing and owned consumers. There is no duplicate
validation policy, new cache, public API, persisted state or operational mode.
Earlier boundary-test and audit-report changes are separate pending work.

## Validation And Limits

- All 50 final focused batch, typed binding, schema authority/cache, relation
  and recovery checks passed. The new checks reject inconsistent durable
  jobs on a warm bundle and late field-ID/slot mismatches under current authority;
  a corrected batch succeeds after rejection.
- A test-length clippy failure was corrected with a narrowly justified fixture
  annotation. The focused all-feature lint rerun and complete `make clippy`
  rerun passed, including its SQL-only core gate.
- The temporary IC harness initially decoded the tuple response incorrectly.
  Its return envelope and decoder were aligned before accepting measurements;
  the failed attempts are not performance evidence. Passing samples use the
  identical corrected actor and test in both phases.
- Measurement servers were isolated and stopped by their wrappers, including
  failed attempts. No local ICP network was changed.
- Full repository/workspace tests remain user-owned. No Cargo versions,
  commits, pushes, maintained canister exports or batch response policies changed.
- Logs, temporary probes and baseline/candidate Wasm are retained under
  `/tmp/icydb-batch-audit.bw3k0s`; material results are preserved above.

The measured gain supports this narrow copy removal. Broader batching or schema
preparation work remains deferred until a separate demonstrated need.
