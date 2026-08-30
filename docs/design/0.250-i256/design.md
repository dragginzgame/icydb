# IcyDB 0.250 — Native Fixed-Width `I256`

- **Status:** Reserved design; implementation not authorized
- **Date:** 2026-08-29
- **Target line:** 0.250 after accepted 0.249 closeout and an explicit 0.250 start
- **Predecessor authority:** the final accepted 0.249 release, to be frozen by
  Patch 1
- **Audience:** applications with a concrete signed 256-bit persistence and
  query workload

## 1. Summary

IcyDB 0.250 reserves one independent investigation of native fixed-width
`I256`. It does not infer signed demand from the existence of `U256`, and it
does not start implementation while 0.249 is active.

IcyDB already supports arbitrary-precision `IntBig`. Native `I256` is justified
only if a real downstream signed schema benefits from one inline,
allocation-free, fixed-cost value representation while retaining the existing
schema, value, planner, executor, index and reducer authorities.

The central runtime hypothesis is:

> After ingress decode, ordinary `I256` comparison, hashing, checked scalar
> arithmetic and fixed-byte conversion require no per-value heap allocation.

Patch 1 must compare the smallest credible alternatives:

1. existing constrained `IntBig`;
2. constrained `IntBig` plus only generated or application boundary adapters;
3. one native `I256` candidate integrated through the maintained IcyDB value
   pipeline.

Functional capability alone is not sufficient: `IntBig` already stores and
queries the numeric domain. Conversely, a theoretically inline arithmetic type
is not sufficient if IcyDB's hot enums, grouping keys, reducers or projections
box it, allocate it or become materially larger.

## 2. Domain And Representative Workload

`I256` represents exactly:

```text
-2^255 ..= 2^255 - 1
```

It remains distinct from `U256`, `IntBig` and every smaller integer. The line
adds no implicit coercion between them.

Patch 1 requires a real downstream signed schema before production work can be
authorized. The frozen workload must include negative, zero and positive
values and exercise:

- inserts, updates, defaults and checked constraints;
- equality, unique and range indexes;
- ascending and descending ordering with pagination;
- grouping, tuple `DISTINCT`, `MIN` and `MAX`;
- checked scalar projection arithmetic; and
- lossless application-boundary conversion.

The value set must include `I256::MIN`, `-1`, `0`, `1`, representative signed
64-bit and 128-bit values, and `I256::MAX`. If no credible downstream schema
requires this domain, Patch 1 closes the line as a no-build.

## 3. Existing Authority And Candidate Delta

| Concern | Existing constrained `IntBig` | Candidate native `I256` |
| --- | --- | --- |
| Domain | Arbitrary precision with a schema byte bound | Exact signed 256-bit range |
| Overflow | May grow beyond 256 bits | Checked failure at the fixed-width boundary |
| Row payload | Canonical variable-length signed integer | Exactly 32 two's-complement bytes |
| Ordered index payload | Existing variable-length signed numeric encoding | Fixed 32-byte sign-biased big-endian encoding |
| Ordinary kernels | Big-integer payload may allocate | Must prove zero per-value allocation after decode |
| Planner and executor | Maintained authority | Reused without a parallel route |
| Primary keys | Existing eligibility policy | Admitted only through the current key contract |
| Candid | Existing `int` wrapper | Measured bounded `int`, exact blob or fixed limbs |
| Aggregation | Existing exact arbitrary-precision behavior where supported | `MIN`/`MAX`; `SUM` deferred unless exact final-range semantics are proven |

The simplest implementation candidate reuses the already-reviewed fixed-width
arithmetic dependency retained by `U256`; 0.250 does not add an Ethereum SDK
or a second numeric library merely for signed support. Dependency choice is
still a measured Patch 1 decision, not an implementation entitlement.

## 4. Representation And Ordering

The runtime value must be inline and copyable. Persisted row payloads use one
IcyDB-owned 32-byte two's-complement representation. A dependency's Serde or
internal memory layout is never persistence authority.

Ordered indexes and primary-key components use fixed-width big-endian bytes
with the high sign bit toggled:

```text
ordered[0] = twos_complement_be[0] XOR 0x80
ordered[1..32] = twos_complement_be[1..32]
```

This maps the signed order monotonically onto unsigned lexicographic byte
order: `I256::MIN` sorts first, then negative values, zero, positive values and
`I256::MAX`. Decode reverses the same bit transform and rejects every malformed
or wrong-width input through a typed error.

All changed pre-1.0 row, key, index, cursor and schema encodings replace their
current version-1 form in place. The line adds no predecessor decoder,
compatibility tag or repair bridge.

## 5. Candid Boundary

Patch 1 measures, rather than assumes, three public carriers:

- Candid `int`, with ingress validation of the exact signed 256-bit range;
- Candid `blob`, validated to exactly 32 two's-complement bytes; and
- a structural four-`nat64` two's-complement limb record.

Both `int` and `blob` are variable-sized Candid carriers. Their admitted values
are bounded by IcyDB validation; neither is intrinsically fixed-width at the
wire boundary. The comparison records wire bytes, encode/decode instructions,
allocations, final raw/code Wasm and generated JavaScript/TypeScript shape.

The expected ergonomic candidate is bounded Candid `int`, matching `U256`'s
`nat` boundary and generated `bigint` clients, but measurement owns the final
choice.

## 6. Hot-Value And Allocation Contract

Patch 1 records size and alignment before and after for every hot value
container reached by the candidate, including input, runtime, output,
expression, persisted scalar, group-key and reducer values.

It also records:

- heap allocation count and allocated bytes per ordinary operation;
- clone and copied-byte effects from inline values;
- non-`I256` scan, predicate and expression cost; and
- whether the existing 32-byte `U256` variant already fixes the relevant enum
  envelope size.

Comparison, hashing, checked scalar arithmetic, fixed-byte conversion,
borrowed index-key construction and reducer ingestion must not allocate per
`I256` value. Explicit ingress decoding and decimal rendering may allocate at
their public boundaries and must be reported separately.

## 7. Storage And Index Density

Fixed 32-byte storage is predictable but can be larger than variable `IntBig`
for common small values. Patch 1 measures complete stable row bytes, ordered
index-key bytes, entries per page and page touches for:

- `-1`, `0` and `1`;
- representative signed 64-bit and 128-bit values;
- one full-width negative value;
- one full-width positive value;
- `I256::MIN` and `I256::MAX`.

The decision reports density losses as well as fixed-cost kernel gains. A large
micro-kernel speedup does not excuse a material regression in the actual
downstream scan, projection, grouping or index workload.

## 8. Scalar Arithmetic

The candidate scalar operators are unary `-`, `+`, binary `-`, `*`, `/`, `%`
and signed comparisons. They reuse the existing expression typing,
constant-folding, preview and compiled-execution owners.

Arithmetic is checked. In particular:

- negating `I256::MIN` fails;
- `I256::MIN / -1` fails;
- division by zero fails; and
- overflow and underflow use the maintained typed numeric error family.

Division and remainder must match IcyDB's maintained signed-integer semantics;
0.250 does not introduce an alternative Euclidean or wrapping mode.

Mixed `I256`/`U256`, fixed/big-integer and fixed/smaller-integer arithmetic is
rejected unless an existing explicit conversion contract admits the pair. The
line does not add a numeric coercion lattice.

## 9. Signed Aggregate Contract

`MIN(I256)` and `MAX(I256)` follow the existing ordered scalar reducer route.
`AVG(I256)` is outside this line.

`SUM(I256)` is deferred by default. A same-width left-to-right accumulator is
not admissible because it makes success depend on reduction order:

```text
I256::MAX + 1 - 1
```

The mathematical result fits, but a same-width intermediate overflows. A
future `SUM(I256)` proposal must use an exact wider internal accumulator and
range-check only the final mathematical result, then prove bounded resource
cost and deterministic grouped/distinct behavior. If that proof is not part of
an explicitly reviewed 0.250 patch, `SUM(I256)` remains rejected alongside
`AVG(I256)`.

## 10. Admission Gates

Production implementation requires all of the following:

1. accepted 0.249 closeout and explicit authorization to start 0.250;
2. one exact released predecessor revision and tree;
3. one real downstream signed schema, query set and source fingerprint;
4. complete constrained-`IntBig`, adapter-only and native comparisons;
5. zero per-value heap allocation in ordinary native runtime kernels;
6. exact hot-enum size, alignment and copy evidence;
7. small/common/full-width stable and index density evidence;
8. identical existing planner, executor, schema and reducer authorities;
9. bounded Candid range validation and chosen-carrier evidence;
10. no material retained Wasm in actors whose schemas do not use `I256`; and
11. a neutral or simpler implementation shape with no new behavior mode.

Patch 1 freezes numerical Wasm, instruction, allocation and stable-density
retention thresholds before a production candidate is implemented. Failure of
any gate closes the line as a documented no-build or returns it to design
review.

## 11. Planned Landing Patches

The line contains at most four reviewable patches:

| Patch | Bounded outcome | Authorization |
| ---: | --- | --- |
| 1 | Freeze the real signed workload and compare constrained `IntBig`, adapter-only `IntBig`, representation/carrier candidates, enum inflation, allocations, density, instructions and Wasm | Not authorized |
| 2 | Add native `I256` storage, schema, keys, indexes, predicates, grouping, `DISTINCT` and extrema through existing owners if Patch 1 recommends build | Not authorized |
| 3 | Add checked scalar arithmetic; keep `SUM` deferred unless a separately reviewed exact wider-accumulator proof is part of this patch | Not authorized |
| 4 | Qualify Candid, upgrade/reinstall, query, instruction, allocation, stable-density, Wasm and downstream contracts | Not authorized |

Patch 1 changes no production behavior. A no-build decision ends the line
without opening Patches 2–4. Each later patch is separately reviewable and is
not authorized by this reservation.

## 12. Non-goals

0.250 does not add:

- a replacement for `IntBig` or `U256`;
- implicit numeric coercion;
- wrapping or saturating arithmetic;
- `AVG(I256)`;
- `SUM(I256)` with a same-width stepwise accumulator;
- bitwise SQL operators, shifts or bit tests;
- Ethereum ABI, EVM or SDK support;
- a public `I512` scalar;
- a second planner, executor, aggregate engine or value pipeline;
- a feature flag, size mode, runtime registry or schema fallback; or
- a legacy decoder, compatibility alias or migration shim.

## 13. Authority And State-Space Delta

| Concern | Canonical owner | Proposed behavior-axis delta |
| --- | --- | ---: |
| Accepted schema | Accepted schema snapshot | 0 |
| Values and expressions | Existing shared value/expression types | 0 |
| Planning and execution | Existing planner and executor | 0 |
| Grouping and extrema | Existing reducer pipeline | 0 |
| Rows, keys and indexes | Existing version-1 codecs | 0 |
| Candid | Existing generated value boundary | 0 |
| Native signed payload | One additional scalar domain inside existing owners | +1 type, 0 modes |

The demonstrated need must come from a downstream signed workload. The
simplest alternative remains constrained `IntBig` plus boundary adapters. The
candidate is retained only when its fixed-width semantics and measured runtime
shape materially improve that workload without penalizing unrelated values or
creating a parallel authority.
