# Fallible canonical sorting: prototype rejected for general adoption

Date: 2026-09-16. Authorized line: 0.257. Production code is unchanged.

## Verdict

Do not replace shared value-set sorting with this prototype. It establishes that
the existing compact heapsort can propagate comparison errors without a second
sorting kernel, but the measured instruction trade-off is poor for general query
canonicalization. Its smaller Wasm does not establish an end-to-end query win.

This rejects the measured candidate, not bounded sorting as a requirement.
R3 sorting admission remains open. Do not silently add a raw membership cap,
a new sorting mode, or a blanket payload-size multiplier to close it.

## Experiment

Four isolated microcanisters share identical fixtures, reply encoding and build
flags. The standard variant mirrors the value-set owner's canonicality scan,
standard unstable sorting and deduplication. The compact control substitutes the
existing compact sorter only. The fallible variant reuses a modified copy of
that same erased heapsort and adds fallible canonicality/deduplication. The
admitted variant adds checked scalar counters for comparisons, operand extent
and scratch before those operations.

The prototype's erased comparator returns Option<Ordering> and retains the
original typed error in its typed adapter. Failure unwinds the heap traversal;
it does not fabricate an ordering. Positions are sorted before any permutation
of input values. Infallible compact calls use that same kernel. Canonicalization
retains the already-sorted fast path with no scratch, and deduplicates in place.

Important limits:

- Types are u64, String, and Vec<String>; these are representative comparator
  workloads, not the entire Value enum or a full fingerprint compatibility test.
- Admission uses a local counter surrogate, **not** the real request budget.
  Its increments are not IC instructions. Only the IC system performance counter
  supplies instruction results.
- Strings use common prefixes (16 or 1,024 bytes plus eight decimal digits).
  Nested fixtures contain two strings with 256-byte prefixes.
- Input construction and result checksum are outside the instruction interval;
  canonicality scan, sort, scratch allocation and deduplication are inside.
- Sizes are 0, 1, 16, 64 and 256. Shapes are sorted (0), reverse (1), shuffled
  (i * 37 + 11) % n (2), four repeated values (3), and all-equal values (4).
  Kinds are integers (0), short strings (1), long strings (2), nested strings (3).
- Two samples per variant/shape/type/size: 800 successful IC calls. Both repeats
  have identical instruction counts. Output lengths and checksums match across
  all four variants for all 100 fixture groups. See [samples.csv](samples.csv).
- Cycles, full query instructions and production-canister Wasm deltas are
  unmeasured. Native test durations are not performance evidence.

## Raw Wasm

Rust 1.98.1 (48a229cea), wasm32-unknown-unknown, opt-level=z, fat LTO,
one codegen unit, panic=abort, symbols stripped. No wasm-opt post-link step.
Both sides have the same configuration; these are not release/audit-canister
baseline artifacts. Defined function counts exclude four imported IC functions.

| Variant | Raw bytes | Defined functions |
| --- | ---: | ---: |
| Standard | 35,025 | 108 |
| Existing compact | 22,858 | 88 |
| Fallible compact | 23,419 | 93 |
| Fallible + surrogate admission | 24,352 | 95 |

Admitted versus standard: **−10,673 bytes (−30.5%)**, −13 defined functions.
Fallible versus existing compact: +561 bytes, +5 defined functions.
The candidate has 2 * n * sizeof(usize) scratch payload for unsorted input:
512 bytes at n=64 and 2,048 bytes at n=256 on wasm32. This is heap scratch,
not Wasm artifact size; allocation headers are excluded.

## IC instructions

| Fixture | Standard | Existing compact | Fallible | Admitted |
| --- | ---: | ---: | ---: | ---: |
| 16 shuffled integers | 2,748 | 14,052 | 16,230 | 22,304 |
| 64 sorted integers | 1,613 | 1,613 | 1,613 | 2,681 |
| 64 shuffled integers | 14,410 | 80,394 | 91,672 | 134,203 |
| 64 equal integers | 3,078 | 34,408 | 38,887 | 53,695 |
| 64 shuffled long strings | 11,434,020 | 17,400,611 | 17,414,353 | 17,463,880 |
| 256 shuffled integers | 87,760 | 422,425 | 476,629 | 715,895 |
| 256 shuffled long strings | 62,383,934 | 96,209,531 | 96,273,687 | 96,548,877 |

The compact-only control already costs 5.6x the standard path for 64 shuffled
integers. The admitted candidate costs 9.3x, or 119,793 additional instructions.
For the 64 long-string case the admitted increase is 52.7%. These are isolated
operation costs: neither ratios nor savings may be extrapolated to whole queries.

## Qualification and scope

Ten native prototype tests pass: current compact-sort parity, ownership across
permutation, set parity across eight widths/five shapes, failure at every sort
comparison, canonical comparison/payload exhaustion, and scratch rejection before
mutation. Failure tests cover the prototype, not production budget propagation.

The first sandboxed PocketIC startup could not bind locally; the attempts were
interrupted. Sampling then succeeded with authorized local binding using
PocketIC server 16.0.0. The runner creates isolated instances/canisters and its
PocketIC handle deletes the instance on drop. No shared ICP network was modified.
No production source or Cargo version was changed; full-suite validation was
not run. No benchmark path is added to the product.
Repository footprint: nine evidence/documentation files, approximately 840 net
lines; zero production lines. Runtime complexity is unchanged.

## Reproduction and provenance

Repository HEAD during experiment: f5bed19d6ec37b434de20aef4d662e36b4832eda,
with prior 0.257 worktree edits preserved. The unchanged compact source SHA-256
is 18af47f73fb2f28c72035a97e73e80c9463557ce36957e74364d99f4efd1ad09.
Recover it with git show at that HEAD, path crates/icydb-schema/src/compact_sort.rs.

Copy [probe.rs.txt](probe.rs.txt), [prototype.rs.txt](prototype.rs.txt), and
[runner.rs.txt](runner.rs.txt) to a fresh temporary directory without the .txt
suffix; place the recovered source at baseline.rs. These are experiment
artifacts, not a second maintained product sorter.

Compile probe.rs four times as a cdylib using the flags above:
no cfg = standard.wasm; --cfg compact = compact.wasm;
--cfg candidate = fallible.wasm; --cfg candidate --cfg admitted = admitted.wasm.
Use --edition=2024. Build and run native tests with --test --cfg candidate
--cfg admitted.

Compile runner.rs against the cached PocketIC 16 rlib and dependency directory
(or a standalone Cargo manifest pinned to pocket-ic 16.0.0). Run it with the
artifact directory argument and POCKET_IC_BIN pointing to server 16.0.0.
Its output is CSV; server startup logging can precede the CSV header. The retained
table consolidates identical repeats and checks equal length/checksum per fixture.
Original local artifacts: /tmp/icydb-sort-prototype.7nR8n9.

Final artifact SHA-256:

- standard: 4c7a89731e94f952198978061ba88464437bcb8e3df8d7388d782392833e5292
- compact: 00b8434b4bce5a5afa1209bff633013b2806eed7077b24c023f707910fe03a5f
- fallible: d2eabf89ae49cdde7cf35812f362a07f020f1bb9947ed6fb795c48cbfdf8a02f
- admitted: 7fa66aac0f05f1c470a62d5e48064face8c380e602610d5757a5972180ee0ada

Paths embedded in Rust panic metadata can change these hashes when reproducing
elsewhere. Compare configured sizes/instructions and semantic output, not hashes
alone. The final formatted artifacts were resampled after their metadata changed.
