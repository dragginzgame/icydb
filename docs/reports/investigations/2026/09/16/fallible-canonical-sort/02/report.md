# Fallible merge sorting: not a general value-set replacement

Date: 2026-09-16. Authorized line: 0.257. Production code is unchanged.

## Verdict

Do not replace shared value-set sorting with this candidate. Reusing the
expression owner's bottom-up merge/permutation avoids inventing another sort
algorithm, but still trades substantially more integer instructions for less
Wasm. Long-string results improve; that does not establish a general query win.
Do not introduce a scalar-specific sorting mode or tune another hybrid here.

Recommendation requiring user approval: defer interruptible value-set sorting
for 0.257, retaining the standard sorter, current input admission and enclosing
preparation instruction accounting. This is not a completed hard-bound claim:
the standard comparator cannot return a resource error mid-sort. No deferral or
policy relaxation has been implemented by this investigation.

## Candidate and scope

[Prototype](prototype.rs.txt) extracts the index merge and final permutation
from `query/plan/expr/canonicalize/ordering.rs` into a typed fallible function.
It keeps stable ties and leaves input values untouched until comparisons finish.
It allocates two position vectors, reusing the second for final permutation.
Expression label/Debug key construction is deliberately not part of the kernel.

The [previous harness](../01/probe.rs.txt) is reused with its calls renamed from
`try_compact_sort_unstable_by` to `try_merge_sort_by`; its standard and compact
controls, fixtures, checksum and measurement interval are unchanged. The four
variants are standard, unchanged compact heapsort, fallible merge, and merge
with the same local surrogate comparison/payload/scratch admission.

These are u64, String and Vec<String> probes, not the complete Value enum or
production request dispatch. The admitted candidate does not include the
expression owner's per-destination work charges. Production Value equality,
deduplication representatives, canonical bytes and caller error propagation are
not qualified by scalar/checksum parity. Nested map comparison in Value itself
walks entries; predicate key encoding separately constructs sorted map views.
Changing the outer value-set sorter would not close all those obligations.

The shared value-set owner is also called by membership normalization, access
normalization, predicate key encoding and SQL schema binding, some infallible.
An extraction alone does not resolve their authority/error contracts. No optional
budget, unmetered adapter or expansion into deferred write/replay work is justified.

## Measurements

Rust 1.98.1 (48a229cea), wasm32-unknown-unknown, opt-level=z, fat LTO,
one codegen unit, panic=abort, symbols stripped; no wasm-opt. Function counts
exclude four imports. These are matched microcanisters, not production artifacts.

| Variant | Raw Wasm bytes | Defined functions |
| --- | ---: | ---: |
| Standard | 35,025 | 108 |
| Compact control | 22,854 | 88 |
| Fallible merge | 25,057 | 89 |
| Merge with surrogate admission | 26,231 | 91 |

Admitted merge versus standard: **8,794 fewer raw bytes (25.1%)** and 17 fewer
defined functions. Unsorted scratch payload remains 2*n*sizeof(usize): 512 bytes
at 64 values and 2,048 at 256 on wasm32, excluding allocation headers.

| Fixture | Standard instructions | Fallible merge | Admitted merge |
| --- | ---: | ---: | ---: |
| 16 shuffled integers | 2,748 | 14,910 | 16,067 |
| 64 sorted integers | 1,613 | 1,613 | 2,307 |
| 64 shuffled integers | 14,410 | 70,314 | 77,322 |
| 64 equal integers | 3,078 | 50,742 | 55,363 |
| 64 shuffled long strings | 11,434,020 | 10,335,859 | 10,360,444 |
| 256 shuffled integers | 87,760 | 329,810 | 365,645 |
| 256 shuffled long strings | 62,383,934 | 53,085,933 | 53,212,115 |

At 64 shuffled integers, admitted merge uses 62,912 extra instructions (5.37x
the standard total). The unadmitted merge already uses 70,314: removing budget
checks would not eliminate the algorithm trade-off. The equal-integer case is
about 18x the standard total. The 64/256 long-string cases use 9.4%/14.7% fewer
instructions. These ratios must not be extrapolated to whole queries.

Eight native prototype tests pass, covering stable ties, uneven lengths, owned
value preservation, canonical-set parity and failures during comparisons,
payload admission and scratch admission. No timings are used as cost evidence.
All 800 IC calls succeeded: two identical repeats for each of 100 fixture groups
across four variants. Lengths and checksums match between variants. The compact
and standard instruction controls reproduce the previous experiment.
[Samples](samples.csv) consolidate identical repeats and shared output evidence;
surrogate work units are not IC instructions. Production Wasm, full-query cycles
and real request-budget overhead remain unmeasured. Full-suite tests were not run.

## Reproduction and provenance

HEAD: `38d4067a04a2dc11421fc91fa06fa077d9732840`, with the prior schema-width and
order-match worktree edits preserved. Original local directory:
`/tmp/icydb-merge-sort.e1EDhH`. Reuse the previous report's baseline source and
[runner](../01/runner.rs.txt); its source hash is unchanged. Place this prototype
in `prototype.rs`, rename all corresponding calls in the previous probe as
above, and format with Rust 1.98.1. Compile the four configurations using the
previous report's exact flags: default, compact, candidate, candidate+admitted.
Native tests use --test --cfg candidate --cfg admitted.

PocketIC 16.0.0 sampling ran with authorized local binding in an isolated
instance, which the runner drops on exit. No shared ICP network was changed.

SHA-256:

- Probe: `a7a3c9076c6451fed2de5f84696999d5de0cc31c6d660c50508a45dda931ee39`
- Prototype: `d614463fd740f60d08672d3f7dcf9ede7fda4a43fe25d955f17d800ae0a70151`
- Standard Wasm: `4c7a89731e94f952198978061ba88464437bcb8e3df8d7388d782392833e5292`
- Compact Wasm: `afa4b5d3ee2e641e6461806fb2e9dd02f8f297125e20f983495022958029e0d6`
- Merge Wasm: `041a4a399083ce358c47af870d23ebd2013949445b63399148322c5453719cbe`
- Admitted merge Wasm: `5492a8ae99d830dac1e2cd1459b4261ac850402660f389e1059a9e9854a1c1ed`

Rust panic metadata embeds source paths; configured sizes can differ slightly
when reproduced elsewhere. No second maintained sorter or product benchmark is
added. The five evidence/tracker files add approximately 320 lines; production
lines and runtime complexity are unchanged. No release note is needed for this
evidence-only qualification.
