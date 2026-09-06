# IcyDB 0.248 U256 Patch 1 Measurement

- **Predecessor:** `v0.247.0` (`904bd704800637f9f1172699d4cbf32751e57738`)
- **Candidate:** `ethnum 1.5.3`, default features disabled
- **Fixture:** `testing/integration/fixtures/u256/ethereum-workload-v1.json`
- **Decision:** build native unsigned `U256` with Candid `nat`
- **Production delta:** none; the candidate integration was disposable

## Scope And Method

The retained fixture freezes 2,048 rows, four unsigned numeric fields and eight
query shapes. It compares:

1. `NatBig(max_bytes = 37)`, which admits the complete `U256` domain;
2. the same `NatBig` with only a boundary conversion to canonical 32-byte
   big-endian form; and
3. an inline `ethnum::U256` candidate.

The disposable candidate compiled against the released IcyDB crates, mirrored
the exact current value variants and private owner layouts, and added one
inline candidate variant. Host allocation measurements use Valgrind
baseline-subtraction. Deterministic kernel counts are Callgrind IR per
operation. Wasm uses Rust's `wasm-release` shape and the checksum-pinned Canic
Binaryen 108 final pass. Raw non-gzipped Wasm and code-section bytes are the
authorities.

This is an admission measurement, not production qualification. It does not
claim exact IC instruction counts for SQL queries or a completed generated
actor integration; those are Patch 2 retention gates. No candidate source,
dependency, public variant, codec tag or runtime behavior is retained here.

## Frozen Workload

The fixture contains full-width unique `token_id` values and this `balance`
distribution: 410 zero, 205 one, 819 ordinary 64-bit, 410 128-bit, 184
full-width token-sized and 20 `U256::MAX` values. `allowance` is small-value
heavy; `total_supply` is 128-bit. Each field has exactly 2,048 values.

The eight frozen queries cover unique equality, indexed range, descending
pagination, checked projection arithmetic, tuple `DISTINCT`, grouped checked
`SUM`, extrema and global checked `SUM`.

`U256::MAX` needs 37 unsigned LEB128 bytes. A `NatBig(max_bytes = 32)` control
would reject part of the domain and is therefore not credible.

## Runtime Representation

`ethnum::U256` is `#[repr(transparent)]` over `[u128; 2]`, implements `Copy`,
`Eq` and `Hash`, and measures 32 bytes with 16-byte alignment.

| Hot owner | Current size / alignment | Candidate size / alignment |
| --- | ---: | ---: |
| `Value` | 64 / 16 | 64 / 16 |
| `InputValue` | 64 / 16 | 64 / 16 |
| `OutputValue` | 64 / 16 | 64 / 16 |
| `TypedScalarValue` | 64 / 16 | 64 / 16 |
| `ScalarLiteral` | 64 / 16 | 64 / 16 |
| `GroupKey` | 80 / 16 | 80 / 16 |
| `ValueReducerState` | 80 / 16 | 80 / 16 |

The existing `Account` and recursive variants already set the 64-byte value
enum envelope, so the 32-byte candidate adds no enum inflation or unrelated
copy-width increase. `PrimaryKeyComponent` is already 64 bytes and the index
component limit is 4 KiB, so a 33-byte tagged ordered payload fits the current
path without another key representation.

The native value itself copies 32 inline bytes. This can cost more collection
storage than a small heap-backed `NatBig`; the grouping controls below retain
that tradeoff rather than hiding it behind the unchanged enum size.

## Kernel Evidence

The full-width kernel uses values above 128 bits. Counts are deterministic
Callgrind IR per operation; allocation counts and bytes are incremental after
subtracting the zero-iteration process baseline.

| Kernel | Constrained `NatBig` IR | Native `U256` IR | Native speedup | `NatBig` allocations / bytes | Native allocations / bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Compare | 53.0 | 31.0 | 1.71× | 0 / 0 | 0 / 0 |
| Hash | 1,460.0 | 9.0 | 162× | 2 / 74 | 0 / 0 |
| Ordered index payload | 2,787.3 | 24.1 | 116× | 3 / 149 | 0 / 0 |
| Fixed-byte adapter | 452.0 | 8.0 | 56.4× | 1 / 32 | 0 / 0 |
| Checked add | 1,745.0 | 29.0 | 60.1× | 3 / 106 | 0 / 0 |
| Checked subtract | 2,619.0 | 23.0 | 114× | 3 / 106 | 0 / 0 |
| Checked multiply | 3,295.4 | 113.1 | 29.1× | 5 / 124 | 0 / 0 |
| Checked divide | 2,025.1 | 303.1 | 6.68× | 4 / 138 | 0 / 0 |
| Checked remainder | 814.0 | 414.0 | 1.97× | 2 / 40 | 0 / 0 |
| Warmed `SUM` step | 44.0 | 14.5 | 3.03× | 0 / 0 | 0 / 0 |

The `NatBig` remainder control uses its underlying `BigUint` because the public
wrapper does not currently implement remainder. The figures exclude the
additional final 256-bit range check that a constrained arbitrary-precision
result would require. Patch 3 still owns exhaustive arithmetic boundaries and
exact IC instruction gates.

Decimal parsing is an ingress exception to the runtime allocation contract.
Native parsing performs no measured heap allocation but uses 8,078 IR versus
6,361 for `NatBig`, a 27% instruction increase. Prepared values avoid paying
this text cost repeatedly.

## Value-Owner Container Workloads

| Workload | Constrained `NatBig` | Native `U256` | Result |
| --- | ---: | ---: | --- |
| Group 2,048 small keys, IR | 1,997,794 | 1,449,412 | native 1.38× faster |
| Group 2,048 small keys, allocations / bytes | 2,049 / 348,176 | 1 / 331,792 | native removes 2,048 payload allocations |
| Group 2,048 full-width keys, IR | 6,768,414 | 1,449,412 | native 4.67× faster |
| Group 2,048 full-width keys, allocations / bytes | 6,145 / 548,880 | 1 / 331,792 | native removes 6,144 payload allocations |
| Full-width `DISTINCT` key-payload control, IR | 6,768,414 | 1,449,412 | native 4.67× faster |
| Extrema, IR | 344,391 | 196,632 | native 1.75× faster |

These controls use the exact 64-byte candidate value envelope and 80-byte
cached-hash group-key layout. They exercise the current clone/copy, stable-hash
payload and hash-set admission shape; they do not claim a completed SQL
executor integration. The `DISTINCT` control isolates the fixed-width payload;
the frozen tuple query adds shared owner and list work to both representations.
The one native allocation in grouping and `DISTINCT` is collection growth, not
a per-value payload allocation. Because the value and group-key envelopes do
not grow, collection-slot bytes are identical for small and full-width native
keys; `NatBig` adds payload allocations in both cases.

## Row And Index Density

The row figures are canonical numeric leaf payloads. The index figures are
canonical ordered components. Shared row framing, raw index-key framing,
primary keys and one-byte index-entry witnesses are identical, so excluding
them preserves the exact delta without inventing a primary-key distribution.

| Sample | `NatBig` row B | `U256` row B | `NatBig` component B | `U256` component B |
| --- | ---: | ---: | ---: | ---: |
| Zero | 5 | 32 | 4 | 33 |
| One | 14 | 32 | 4 | 33 |
| 1e18 balance | 23 | 32 | 22 | 33 |
| 128-bit maximum | 41 | 32 | 42 | 33 |
| 2^255 token identifier | 77 | 32 | 80 | 33 |
| `U256::MAX` | 77 | 32 | 81 | 33 |

| Frozen field | `NatBig` row B | `U256` row B | `NatBig` indexed component B | `U256` indexed component B |
| --- | ---: | ---: | ---: | ---: |
| `token_id` | 157,696 | 65,536 | 163,840 | 67,584 |
| `balance` | 56,275 | 65,536 | 54,038 | 67,584 |
| `allowance` | 26,944 | 65,536 | — | — |
| `total_supply` | 83,968 | 65,536 | — | — |
| **Total** | **324,883** | **262,144** | **217,878** | **135,168** |

Across the measured numeric row and index-component payloads, native `U256`
uses 397,312 bytes versus 542,761 for `NatBig`, a 145,449-byte or 26.8%
reduction. That result comes from the fixture's deliberately representative
full-width token identifiers. The small-heavy `allowance` field alone is
38,592 bytes larger under fixed width.

The current index store does not turn the smaller component into higher node
density. `ic-stable-structures 0.7.2` fixes B-tree degree at six and maximum
entries per node at eleven. IcyDB's variable raw index key is bounded by the
4-KiB component ceiling, so the allocator page size is derived from that
maximum rather than actual key lengths. Adding a 33-byte component without
changing the shared bound leaves:

- maximum entries per node: 11 for both controls;
- tree height and nodes visited for the same insertion order: identical; and
- allocator pages touched by a lookup or range traversal: identical.

Native `U256` reduces bytes encoded, compared and copied within those nodes,
but it does not improve stable B-tree fan-out or page count. Patch 2 must not
claim an index-page saving unless it changes and separately justifies the
shared index representation; this design does not propose that change.

## Candid Carrier Decision

Single-value Candid messages, including their type table, measure:

| Sample | `nat` B | exact-length `blob` B | four-`nat64` B |
| --- | ---: | ---: | ---: |
| Zero | 8 | 42 | 65 |
| One | 8 | 42 | 65 |
| 1e18 balance | 16 | 42 | 65 |
| 128-bit maximum | 26 | 42 | 65 |
| 2^255 token identifier | 44 | 42 | 65 |
| `U256::MAX` | 44 | 42 | 65 |

Across the fixture's 8,192 numeric field values, carrier payloads use 145,724
bytes for `nat`, 270,336 for length-prefixed 32-byte blobs and 262,144 for four
limbs. `nat` wins the representative wire distribution despite being larger
than blob for a full-width single value.

Carrier encode/decode Valgrind measurements include generic Candid ownership,
not only the candidate payload:

| Carrier | Encode allocations / bytes | Decode allocations / bytes | Encode IR | Decode IR |
| --- | ---: | ---: | ---: | ---: |
| `nat` | 12.0 / 1,614 | 16.0 / 632 | 7,491 | 13,676 |
| `blob` | 19.0 / 2,021 | 20.0 / 1,847 | 9,686 | 11,818 |
| four `nat64` | 33.0 / 2,486 | 32.0 / 2,392 | 19,624 | 19,228 |

Blob decodes with 13.6% fewer IR than nat, but nat has lower allocation count,
lower allocated bytes, smaller workload wire payload and the natural Candid
`nat` / generated JavaScript `bigint` contract. Blob generates
`IDL.Vec(IDL.Nat8)` and needs conversion helpers plus exact 32-byte validation;
the limb carrier exposes four `IDL.Nat64` fields.

Inside the full recursive dynamic `InputValue` mirror, relative to the exact
baseline:

| Carrier | Compiler raw delta | Final raw delta | Compiler code delta | Final code delta |
| --- | ---: | ---: | ---: | ---: |
| `nat` | +1,318 | +1,083 | +1,202 | +1,047 |
| `blob` | +639 | +997 | +524 | +977 |
| four `nat64` | +4,910 | +4,627 | +4,793 | +4,541 |

Blob's final saving over nat is only 86 raw bytes and 70 code bytes. It does
not outweigh the wire, allocation and client-shape result. The Patch 2 carrier
recommendation is therefore Candid `nat`, validated to `0..=U256::MAX` at
ingress. There is no dual carrier or fallback.

The exact service diff adds only the `U256 : nat` case to the recursive input
and output value variants in the mirror. Because that is still a shared Candid
surface change, Patch 2 must capture exact generated-actor service diffs and
old-client decode behavior before retention.

## Wasm Evidence

The clean released-predecessor SQL-on baselines are:

| Maintained subject | Compiler raw / code B | Final raw / code B |
| --- | ---: | ---: |
| Empty | 1,988,631 / 1,882,387 | 1,742,169 / 1,637,829 |
| Empty + metrics | 2,194,509 / 2,072,469 | 1,921,420 / 1,801,701 |
| One entity, dynamic query | 3,130,314 / 2,961,096 | 2,750,602 / 2,584,933 |
| One entity, typed query | 2,175,044 / 2,060,388 | 1,904,445 / 1,791,834 |
| One entity, SQL query | 3,460,335 / 3,286,309 | 3,049,395 / 2,878,619 |
| Ten entities, typed query | 2,176,365 / 2,060,429 | 1,905,742 / 1,791,871 |
| SQL performance actor | 4,835,000 / 4,574,928 | 4,223,602 / 3,968,517 |
| SQL actor | 4,151,604 / 3,943,449 | 3,620,975 / 3,417,067 |

The disposable full dynamic-value mirror establishes a more sensitive A/B for
candidate retention:

| Mirror | Compiler raw / code B | Final raw / code B | Final functions |
| --- | ---: | ---: | ---: |
| Existing `InputValue` | 565,181 / 500,769 | 491,295 / 428,476 | 1,216 |
| Add Candid `nat` carrier | 566,499 / 501,971 | 492,378 / 429,523 | 1,212 |
| Carrier plus native kernels | 585,557 / 519,898 | 508,822 / 444,871 | 1,265 |
| Kernel delta from baseline | +20,376 / +19,129 | +17,527 / +16,395 | +49 |

This is below the representative-actor limits of 96 KiB final raw and 48 KiB
final code. It does not prove the unused-actor 8/4-KiB gate because the final
production integration may retain schema, parser, persistence and dispatch
arms absent from the mirror. Patch 2 must measure every maintained actor and
remove the candidate if an actor that does not use `U256` exceeds that gate.

The released predecessor's generated-entity slope remains 145 raw bytes per
entity, well inside the existing 2-KiB guard. Patch 2 must show that the new
type does not change that slope for schemas that do not use it.

## Dependency Audit

- `ethnum 1.5.3` is MIT OR Apache-2.0.
- With default features disabled, Cargo retains no normal dependency below
  `ethnum`; Serde and LLVM intrinsics remain optional and disabled.
- The type is a transparent, copyable two-`u128` value. IcyDB will own Candid,
  persistence, index encoding, errors and schema identity.
- The carrier-plus-kernel mirror passes `wasm32-unknown-unknown` checking on
  Rust 1.88.0, the workspace MSRV. It also builds under the active Rust 1.97.1
  toolchain.

## Decision And Patch 2 Gates

Build native unsigned `U256` and continue to defer `I256`.

The decision is justified by allocation-free fixed-width runtime kernels,
6.7×–162× gains in selected expensive value kernels, a 4.67× full-width
group-key / `DISTINCT` container gain, unchanged hot-owner sizes, a 26.8%
numeric-payload saving in the frozen workload and a 17.5-KiB final-raw
disposable kernel footprint.

It is not justified by index fan-out, universal small-value density or decimal
parsing. Those results are neutral or negative and remain explicit constraints.

Patch 2 may be authorized as one storage-and-query landing patch only if it:

- uses the existing schema, value, persistence, planner, index, grouping and
  cursor authorities with no alternate runtime route;
- selects Candid `nat` as the sole carrier and validates the fixed range;
- preserves current hot-owner sizes and allocation-free comparison, hashing,
  fixed-byte and index-key kernels;
- repeats raw/code/function Wasm A/Bs on exact generated actors, including the
  8/4-KiB unused-actor gate;
- measures exact IC instructions and physical work for the frozen non-arithmetic
  queries and unrelated boolean/narrow-integer controls;
- adds current version-1 tags and formats in place with no compatibility path;
  and
- leaves checked arithmetic and `SUM(U256)` to Patch 3.
