# IcyDB 0.248 — Native Fixed-Width `U256`

- **Status:** Complete; native U256 retained through all four patches, pending user-owned release validation
- **Date:** 2026-08-28
- **Target line:** 0.248 after accepted 0.247 closeout
- **Audience:** Ethereum developers building native IC actors with IcyDB
- **Candidate arithmetic dependency:** `ethnum 1.5.3`, default features disabled

## 1. Summary

IcyDB 0.248 investigates one first-class fixed-width `U256` scalar type for
Ethereum-oriented applications. `I256` is deferred completely; it requires a
real downstream signed schema and a separate design before measurement or
implementation.

IcyDB already supports arbitrary-precision `NatBig` and `IntBig` values across
storage, ordering, secondary indexes, predicates, grouping, `DISTINCT`, output
values and Candid transport. The proposed fixed-width type is therefore not a
way to make large integers possible. They must demonstrate a narrower benefit:

- an exact Ethereum-compatible 256-bit domain;
- checked overflow and underflow instead of arbitrary-precision growth;
- canonical fixed 32-byte storage and index payloads;
- allocation-free ordinary runtime value kernels and predictable instruction
  costs;
- primary-key eligibility if the existing key contract admits it; and
- direct, lossless conversion at Ethereum application boundaries.

The central hypothesis is an execution-shape claim, not merely a type-domain
claim:

> Once decoded into the runtime value pipeline, ordinary `U256` comparison,
> hashing, checked arithmetic and fixed-byte conversion require no per-value
> heap allocation.

Patch 1 must prove this through IcyDB's actual input, runtime value, expression,
grouping, `DISTINCT`, index and reducer containers. An inline `ethnum::U256`
alone is insufficient if those owners subsequently box, clone or allocate it.

The candidate uses `ethnum` only as arithmetic machinery. IcyDB continues to
own scalar semantics, schema identities, persisted and index encodings, query
typing, errors, Candid representation and upgrade policy.

This document remains under `docs/design/` as the retained decision record.
Patch 1 compared native `U256` against constrained `NatBig`, constrained
`NatBig` plus application/generated Ethereum adapters, and the smallest real
IcyDB-integrated native candidate. It also compared three Candid carriers
rather than assuming that `nat` or `blob` wins.

The fact that constrained `NatBig` can express the workload is not itself a
no-build verdict. The line closes only when measurement shows no meaningful
native advantage, or when `U256` requires another runtime mode, value pipeline,
feature profile or aggregate engine.

## 2. Ethereum Problem Statement

Ethereum protocols use 256-bit values for balances, allowances, total supply,
token identifiers, prices, reserves and bit fields.
Applications can represent these with IcyDB's existing big integers, but that
does not by itself provide a fixed-width Ethereum domain or fixed-cost
representation.

The representative workload for admission contains:

- `balance`, `allowance`, `total_supply` and `token_id` as unsigned 256-bit
  values;
- equality, unique and range-index lookups;
- ascending and descending ordering with pagination;
- checked projection arithmetic;
- grouping, tuple `DISTINCT`, extrema and checked `SUM(U256)`; and
- lossless conversion to and from an Ethereum library at the application
  boundary.

Bitwise SQL operators, shifts and bit tests are outside this line. Applications
may store a `U256` bit field, but application code remains responsible for
interpreting its bits; the representative database workload does not claim
bit-field execution support.

`I256` is not admitted merely because the EVM defines a signed type. A concrete
signed accounting schema and query workload must authorize a later independent
design.

This scalar does not provide Solidity execution, an EVM, ABI handling or
Ethereum transaction semantics.

## 3. Existing Authority And Candidate Delta

Patch 1 must complete this comparison with measured artifacts:

| Concern | Existing constrained `NatBig` | Candidate native `U256` |
| --- | --- | --- |
| Numeric domain | Arbitrary precision with schema byte bounds | Exact 256-bit unsigned range |
| Overflow | Values may grow beyond 256 bits | Checked failure at the fixed-width boundary |
| Row payload | Canonical variable-length big integer | Exactly 32 bytes |
| Ordered index payload | Canonical variable-length numeric encoding | Fixed-width big-endian encoding |
| Ordinary value kernels | Big-integer representation may allocate | Must prove zero per-value heap allocation after ingress decode |
| Secondary ordering/predicates | Maintained | Must reuse the same planner/executor flow |
| Primary-key component | Currently not admitted for big integers | Candidate only; requires explicit key-bound proof |
| Candid | Existing `nat` wrapper | Measured choice: bounded `nat`, exactly validated 32-byte blob, or four-`nat64` record |
| Aggregate `SUM` / `AVG` | Full-domain exact reduction is not maintained | Checked `SUM(U256)`; `AVG` deferred |
| Ethereum conversion | Application-defined conversion | Direct fixed-width conversion at application boundary |

The candidate proceeds only when this table shows a meaningful semantic or
measured cost advantage for the representative workload. General Ethereum
ergonomics without a database-level advantage is insufficient.

## 4. Admission Criteria

Production implementation requires all of the following:

1. The accepted 0.247 predecessor has closed and 0.248 is rebased on its
   released production source.
2. The representative Ethereum schema and exact queries are checked in as a
   bounded measurement fixture.
3. `NatBig(max_bytes = 37)`, that same constrained `NatBig` plus only Ethereum
   conversion adapters, and native `U256` are captured as three complete
   controls. Thirty-seven is the smallest unsigned-LEB128 byte bound that
   admits every 256-bit value.
4. Clean empty, typed, SQL, one-entity and ten-entity actor baselines are
   captured before adding the dependency.
5. A real IcyDB integration spike measures `ethnum` representation, comparison,
   hashing, fixed-byte conversion, parsing and each checked arithmetic operator
   through the maintained value containers.
6. A carrier spike compares Candid `nat`, an exactly validated 32-byte `blob`,
   and a structural four-`nat64` record for wire bytes, Wasm bytes, code bytes,
   allocations, instructions and generated JavaScript ergonomics.
7. Review confirms that the admitted type uses the existing schema, value,
   planning, execution, grouping and persistence owners.
8. Patch 1 records `size_of`, alignment, allocation and copy effects for every
   hot value container before and after the candidate.
9. Storage and index density are measured for zero, one, representative 64-bit
   and 128-bit balances, a full-width token identifier and `U256::MAX`.
10. `I256` remains outside this line.

Failure of any criterion returns the line to design review or closes it as a
no-build.

## 5. Goals

- Add a distinct `U256` schema type when the measured comparison justifies it.
- Expose an IcyDB-owned `icydb::U256` facade type.
- Preserve one `InputValue`/`OutputValue` pipeline and one expression system.
- Provide canonical fixed-width storage and order-preserving index encodings.
- Support inserts, updates, defaults, constraints and prepared parameters.
- Support equality, ordering, `IN`, arithmetic, `CASE`, `GROUP BY`, `DISTINCT`,
  extrema and checked `SUM(U256)` through existing semantic owners.
- Preserve deterministic, bounded and fail-closed behavior.
- Avoid material retained code in actors whose schemas do not use the type.
- Avoid schema- or entity-proportional generated code.

## 6. Non-goals

0.248 does not add:

- a replacement for existing `NatBig` or `IntBig`;
- `I256`, signed 256-bit arithmetic or signed 256-bit aggregates;
- arbitrary-precision `Nat`, `Int`, decimal or `BigInt` database types;
- `AVG(U256)` without a separately accepted exact result contract;
- a public `U512` scalar;
- an EVM interpreter or Solidity compatibility layer;
- Ethereum addresses, hashes, ABI, RLP, SSZ, Keccak or signature verification;
- general fixed-point or floating-point financial arithmetic;
- implicit numeric coercion;
- bitwise SQL operators, shifts or bit tests;
- automatic migration from an existing integer or big-integer field;
- wrapping arithmetic as the database contract;
- a cache, runtime capability registry, second planner path or second aggregate
  engine;
- a Cargo feature or generated-actor mode used to hide unacceptable unused
  code; or
- Alloy or another Ethereum SDK dependency in `icydb-core`.

Optional ecosystem conversions belong in an integration crate or application
boundary.

## 7. Canonical Types And Arithmetic

### 7.1 Ranges

`U256` represents:

```text
0 ..= 2^256 - 1
```

`U256` remains distinct from all existing IcyDB numeric types. Its exact range
is part of schema identity, not an application-side convention. `I256` is not
part of the 0.248 type or range contract.

### 7.2 Dependency Ownership

The measurement candidate is:

```toml
ethnum = { version = "1.5.3", default-features = false }
```

The version exists under MIT OR Apache-2.0 licensing and declares no Rust
version, so Patch 1 must prove the workspace MSRV explicitly. The implementation
patch pins the reviewed compatible version under workspace dependency policy.

IcyDB does not use dependency-owned Serde as a persistence contract. Stable
formats use explicit fixed-width byte conversion. A future arithmetic-library
change cannot alter stored bytes, Candid, ordering or typed errors.

### 7.3 Arithmetic Contract

The initial operators are `+`, `-`, `*`, `/`, `%`, `=`, `!=`, `<`, `<=`, `>`
and `>=`.

Arithmetic is checked. Overflow, underflow and division by zero produce the
existing typed numeric execution error. Database expressions never silently
wrap modulo `2^256`.

Mixed-width arithmetic and comparison remain rejected unless an existing
explicit conversion contract already admits the pair. This line does not add a
general coercion lattice.

## 8. Schema And Value Pipeline

The existing route remains authoritative:

```text
Rust field / SQL parameter
    -> InputValue
    -> typed expression and predicate
    -> logical/access/executable plan
    -> field-level row view and kernels
    -> OutputValue
```

An admitted type adds variants only at these existing ownership points. It does
not add a crypto value enum, alternate planner expression or separate execution
route.

Type identity participates in:

- accepted schema field descriptors;
- defaults and constraints;
- prepared parameter validation and plan identity;
- projection and `CASE` result typing;
- predicate canonicalization;
- group and `DISTINCT` keys; and
- response conversion.

Existing row and schema bytes remain identical when they do not contain an
admitted type.

### 8.1 Allocation-Free Runtime Contract

Ingress decoding, decimal text rendering and the chosen Candid carrier may
allocate at their explicit public boundaries. After decode into the runtime
value pipeline, however, comparison, hashing, checked arithmetic, fixed-byte
conversion, index-key construction from a borrowed value and reducer ingestion
must perform zero heap allocations per `U256` value.

Patch 1 instruments allocation count and allocated bytes across point
comparison, range-key construction, projection arithmetic, grouping,
`DISTINCT`, extrema and `SUM`. It also records clone/copy traffic so a larger
inline value cannot hide its cost behind an allocation-only metric.

The zero-allocation contract applies to the `U256` value payload. Existing
owner collections may allocate when their capacity grows; Patch 1 reports that
container growth separately and rejects any additional per-value box, buffer
or big-integer allocation introduced by the candidate.

### 8.2 Enum Layout And Unrelated-Value Gate

An inline 32-byte variant can enlarge every instance of a shared enum. Patch 1
records `size_of` and alignment before and after for every hot container that
directly or transitively carries the candidate, including at least:

- `InputValue`, `OutputValue`, runtime `Value` and model `TypedScalarValue`;
- primary-key and persisted-slot value enums if the candidate reaches them;
- group, `DISTINCT`, order and expression values; and
- `ValueReducerState` and its containing aggregate state.

The audit must discover and include additional transitive containers rather
than treating this list as exhaustive. It measures moved/copied bytes and
instructions for non-256 scans, projections and expression evaluation using
boolean and narrow-integer controls. A native `U256` does not proceed when
enum inflation materially penalizes unrelated values.

## 9. SQL And Fluent Surfaces

SQL parsing must consume a 256-bit decimal literal directly from its source
lexeme. It must not first pass through `i64`, `u64`, `i128`, `u128` or floating
point.

The tentative typed form is:

```sql
U256 '1000000000000000000'
```

Patch 1 audits existing literal and cast grammar before accepting new syntax.
Prepared parameters are preferred where they avoid repeated decimal parsing.
Hexadecimal syntax is outside the initial contract.

The fluent surface accepts constructed IcyDB facade values. Input length is
bounded before decimal conversion, and malformed or out-of-range input fails
with a typed error.

## 10. Persistence And Pre-1.0 Policy

### 10.1 Row Leaves

The canonical `U256` scalar payload is exactly 32 bytes in unsigned big-endian
order.

Decoding accepts exactly 32 bytes and fails closed otherwise. The payload uses
the existing persisted-row and `SlotReader` path; it does not restore
entity-level deserialization or add a generic persistence codec.

### 10.2 Versioning

The first implementation patch audits free tagged values in the persisted-row,
schema, proposal and value codecs. No existing tag may be reinterpreted.

Before 1.0, every affected versioned representation remains version `1` and is
replaced in place. The implementation must not introduce version `2`, a
predecessor decoder, fallback tag, dual format or upgrade translator. If the
new current version-1 form is incompatible with existing persisted state, that
state requires reinstall, recreation or explicit regeneration.

Changing an existing integer or big-integer field to a fixed-width field is not
part of this line. New fields follow current nullable/default and schema
evolution rules.

### 10.3 Small-Value Density

Fixed width is predictable, but it may consume more stable storage and reduce
index-node fan-out for common balances that fit in 64 or 128 bits. Patch 1
measures these exact values for every candidate representation:

- zero and one;
- a representative 64-bit balance;
- a representative 128-bit value;
- a full-width token identifier; and
- `U256::MAX`.

For each distribution, the report includes encoded row bytes, total stable
bytes, encoded index-key bytes, entries per index page and pages touched—not
only encoding instructions. Native `U256` must earn any density loss through a
meaningful execution or semantic benefit.

The checked-in measurement fixture freezes field-specific value frequencies
for the bounded 2,048-row workload. Point samples alone are not accepted as a
claim about total row or index density.

## 11. Index Encoding

Index order must agree exactly with numeric order. `U256` uses 32-byte unsigned
big-endian encoding.

The existing index codec continues to own null discrimination, composite-key
framing, descending transformation, bounds, prefix behavior and cursor
continuation.

Required invariant:

```text
numeric_compare(a, b) == encoded_key_compare(a, b)
```

Property tests cover ascending, descending, nullable, unique and composite
indexes. Indexed and full-scan execution must return identical rows and order.
Primary-key eligibility is admitted only if the existing bounded component and
cursor contracts can encode the fixed 32-byte payload without another key path.

## 12. Expressions, Grouping And Reduction

The new value uses the existing expression evaluator and projected-value
streaming/reduction flow.

Required behavior includes:

- projection arithmetic;
- field-to-literal and field-to-field comparison;
- `IN` and negated predicates;
- `CASE` with type-compatible branches;
- `ORDER BY` and pagination;
- equality/hash identity for `GROUP BY` and tuple `DISTINCT`; and
- existing budget, fallback and typed-error behavior.

`COUNT`, `MIN` and `MAX` retain their existing contracts. Checked `SUM(U256)`
may add a fixed-width accumulator payload inside the one
canonical `ValueReducerState`; it must not add a second reducer or execution
route. The result is the same fixed-width type, and mathematical overflow is a
typed error. Because every input is non-negative, a same-width accumulator
cannot overflow temporarily and later return to range through cancellation.

`AVG(U256)` is deferred. The current aggregate owner accumulates and
returns bounded `Decimal`, which cannot represent the complete 256-bit domain.
Integer truncation would introduce different semantics, while an exact rational
or wider decimal would add a new public result contract. A later design may
admit `AVG` only after choosing one exact, bounded result and reusing the shared
reducer owner.

`SUM(I256)` is not part of this line. A future signed design must choose either
an exact wider internal accumulator with a final range check or an explicit
deterministic stepwise-overflow contract. Same-width accumulation is not
accepted because a sequence such as `I256::MAX + 1 - 1` has an in-range
mathematical result but overflows in left-to-right reduction.

## 13. Candid And Generated APIs

No Candid carrier is preferred before measurement. Patch 1 compares:

### Candidate A — Candid `nat`

- ergonomic Rust and JavaScript big-integer behavior;
- canonical variable-length LEB128 wire values;
- explicit `U256` range validation at ingress; and
- possible reuse of big-integer machinery already retained by dynamic IcyDB
  value surfaces.

### Candidate B — Candid `blob` with an exact-length contract

- Candid `blob` remains a variable-length `vec nat8`, not an intrinsically
  fixed-width carrier;
- IcyDB must reject every ingress value whose length is not exactly 32 bytes;
- an admitted value converts directly to the persisted/index representation;
  and
- application or generated JavaScript helpers for `bigint` conversion.

### Candidate C — Structural four-`nat64` record

- four fixed limbs in the Candid type rather than one variable-length carrier;
- potential decode without a byte-vector or `BigUint` allocation;
- explicit limb order and exact conversion to the canonical 32-byte value; and
- a less natural generated API that must be measured rather than dismissed.

Both `nat` and `blob` are variable-sized Candid carriers whose admitted values
become bounded through ingress validation. Only the four-limb record expresses
fixed cardinality structurally; that fact does not make it the automatic
winner.

The spike records encoded bytes for zero, one, a representative 64-bit balance,
a 128-bit value, a full-width token identifier and `U256::MAX`; compiler raw
bytes; pre- and post-Canic code sections; defined functions; ingress/egress
instructions; allocations; and generated client shape. A maximum 256-bit
Candid `nat` may require more than 32 payload bytes because LEB128 uses seven
value bits per byte, while small values may be more compact. The design does
not assume any carrier is cheaper overall.

One carrier is selected before production tags or public variants land. There
is no runtime fallback or dual wire representation.

Adding variants to shared `InputValue` and `OutputValue` changes Candid even for
some actors whose schemas do not use the new type. Patch 1 therefore captures
the exact service diff and checks upgrade compatibility for every affected
generated actor. An unacceptable shared-surface cost closes the design; it does
not create a feature profile.

JSON-facing documentation uses decimal strings and never JavaScript `number`.

## 14. Ethereum Integration Boundary

0.248 enables exact application-level conversion to an Ethereum numeric type. It
does not make Alloy or another Ethereum SDK part of IcyDB core.

A later independent adapter may provide:

```text
alloy_primitives::U256 <-> icydb::U256
```

The adapter preserves exact bits and rejects out-of-contract values. Address,
hash, ABI and authentication work remains independently scoped.

## 15. Landing Plan

The line contains four substantive patches. Patch 1 measured the candidate
after the accepted 0.247 predecessor closed. Patches 2–4 proceeded only after
that evidence recorded an explicit build decision and the user accepted each
bounded outcome.

### Patch 1 — Existing-authority, integration and carrier spike

- Check in the bounded Ethereum workload and exact queries.
- Compare constrained `NatBig`, constrained `NatBig` plus only Ethereum
  conversion adapters, and native `U256` semantics and costs.
- Capture clean actor baselines.
- Measure `ethnum` with default features disabled through the real IcyDB value,
  expression, grouping, index and reducer containers.
- Record hot-enum size/alignment, allocation count/bytes, copied bytes and
  unrelated-value scan/expression effects.
- Compare the common-value stable-storage and index-density distributions.
- Compare Candid `nat`, exactly validated `blob` and four-`nat64` carriers.
- Audit license, MSRV, dependency features and IC/Wasm compatibility.
- Record one `U256` build/no-build recommendation. `I256` remains deferred.
- Change no production IcyDB behavior.

The spike may use a disposable experimental integration to measure real hot
containers. Patch 1 does not retain public tags, variants or production
behavior at handoff.

### Patch 2 — Complete `U256` storage and query contract

- Add facade, schema, input/output and runtime identities.
- Add exact row, schema and index encodings.
- Add DML, defaults, constraints, prepared parameters and generated bindings.
- Add equality, ordering, ranges, grouping, `DISTINCT`, extrema and cursor
  behavior through existing flows.
- Complete upgrade, replay, Candid and unused-actor gates.

This patch is a coherent fixed-width storage/query outcome even if later
arithmetic work is rejected.

### Patch 3 — Checked `U256` arithmetic and `SUM`

- Add checked expression operators.
- Extend the shared reducer with one fixed-width accumulator payload.
- Add SQL/fluent/prepared result and error equivalence.
- Retain only if the footprint and instruction gates pass.

### Patch 4 — Downstream qualification and closeout

- Run the complete correctness, persistence, footprint and instruction
  matrices.
- Verify generated Candid compatibility and client representations.
- Run the representative Ethereum fixture and read-only downstream gate.
- Remove any candidate that does not retain a net measured benefit.

Each retained patch is reviewable and leaves one coherent maintained boundary.
The minor line is published only after the complete accepted contract passes.

## 16. Validation Matrix

Correctness coverage includes:

- `U256::{MIN, MAX}`, zero, one and the common-value density set;
- every overflow and underflow boundary;
- division and remainder by zero;
- decimal parse bounds and malformed input;
- row, index and accepted-schema round trips;
- insert, update, default, constraint and unique-conflict behavior;
- ascending/descending, nullable and composite index ordering;
- range bounds and cursor continuation;
- full-scan/indexed result equivalence;
- SQL/fluent/prepared result and error equivalence;
- `CASE`, grouping, tuple `DISTINCT`, extrema, checked `SUM(U256)` and
  pagination;
- upgrade, reinstall, recovery and accepted-schema replay;
- exact Candid range rejection and chosen-carrier round trips;
- existing schemas and queries with no fixed-width fields;
- zero per-value allocations in the admitted runtime kernels; and
- exact before/after enum size, alignment, allocation and copy reports.

Property tests compare encoded ordering with independent reference numeric
ordering over generated 256-bit values.

## 17. Footprint And Performance Gates

Patch 1 confirms or tightens these tentative rejection gates from clean
baselines:

- actor not using the type: at most 8 KiB final raw growth and 4 KiB final
  code-section growth;
- representative actor using `U256`: at most 96 KiB final raw growth and 48 KiB
  final code-section growth;
- ten-entity actor: no more than 2 KiB final raw growth per added entity caused
  by the new scalar machinery;
- existing non-256 queries: no regression that adds both more than 250,000
  instructions and more than 3%;
- fixed-width equality and range queries: identical logical and physical work
  to the equivalent existing indexed route;
- comparison, hashing, key encoding and common arithmetic: zero per-value heap
  allocations after ingress decode;
- fixed-width kernels: measured against `Nat128`, constrained `NatBig`, and
  constrained `NatBig` plus application/generated conversion adapters;
- common-value row and index measurements: total stable bytes, encoded key
  bytes, entries per page and pages touched for all three representations;
- every hot enum: exact before/after size and alignment, with copied bytes and
  non-256 scan/expression instruction effects;
- no default dependency feature, duplicate big-integer runtime or retained
  Ethereum utility code; and
- exact export/service diffs, with no unrelated Candid change.

For every spike and candidate, raw non-gzipped bytes and code-section bytes are
reported before and after Canic's final optimisation, together with defined
functions. Gzip is secondary context only.

The ceilings are rejection limits, not expected growth. Patch 1 reports type
machinery, carrier conversion, parsing, arithmetic and IcyDB integration
separately. If the unused-actor gate fails, the design does not add a Cargo
feature or generated capability mode; it closes or returns for architectural
review.

A persuasive build recommendation should show approximately 5–20× improvement
in representative value kernels and a meaningful improvement in complete
scan, projection or grouping workloads without damaging index density or
unrelated values. If native `U256` saves only a few percent end to end while
approaching the 96-KiB representative-actor ceiling, constrained `NatBig` plus
adapters is the preferred no-build outcome.

## 18. Risks

### Duplicate Numeric Authority

The largest risk is adding a type whose real behavior is already satisfied by
constrained `NatBig` plus narrow adapters. The three-way Patch 1 comparison is
a release blocker.

### Shared-Enum Inflation

An inline 32-byte variant may enlarge hot containers holding unrelated small
values. Layout, copied-byte and non-256 workload measurements are release
blockers; allocation-free `U256` kernels do not excuse a broader regression.

### Wasm Growth

Decimal formatting, division, Candid conversion and exhaustive value dispatch
may retain substantially more code than the 32-byte representation suggests.

### Candid Efficiency

Variable-length `nat` and `blob` carriers may add allocation, validation or
conversion cost, while a four-limb record may reduce ergonomics. None is
selected by taste; the representative wire and instruction measurements
decide.

### Ordering Defects

An unsigned encoding or framing error could make an index return plausible but
incorrect range results. Property and full-scan parity tests are release
blockers.

### Semantic Mismatch

Ethereum developers may expect modulo arithmetic, while IcyDB specifies
checked database arithmetic. Public documentation must make this explicit.

### Accidental Scope Expansion

Addresses, hashes, decimals, ABI codecs and `U512` intermediates are related but
outside this line.

### Dependency Leakage

Dependency-owned formats or traits must not become IcyDB persistence, Candid or
error contracts.

## 19. Reversion And Closeout

Before release, the implementation is reversible because no published schema
may use the new tags.

If admission, correctness, compatibility, footprint or performance fails:

- remove all production type, codec, query and generated-surface changes;
- remove release-facing changelog/version claims;
- retain this active design and measurements as a 0.248 no-build report;
- preserve unrelated predecessor and lockfile changes; and
- report zero attributable stable-format or public-API change after reversion.

## 20. Open Review Questions

1. Does native `U256` beat both constrained `NatBig` controls on kernels and
   complete Ethereum-shaped workloads after storage density is included?
2. Which of Candid `nat`, exactly validated `blob`, or four-`nat64` record wins
   on final Wasm, instructions, allocations, wire bytes and generated-client
   ergonomics?
3. Can actors that do not use the type remain under the 4 KiB final-code gate
   without a new feature or build mode, and do hot enums avoid material
   inflation for unrelated values?
4. Can checked `SUM(U256)` remain one payload inside the shared reducer without a
   material query-cost regression?
5. Does the existing primary-key component budget admit a 32-byte value without
   another key encoding path?

## 21. Final Verdict

All four patches are complete. IcyDB retains native `U256` with Candid `nat`
through its existing schema, value, persistence, planner, executor, index and
reducer authorities. Fixed-width kernels are allocation-free and materially
faster, hot enum and reducer sizes do not grow, the representative actor and
unrelated actor remain inside their Wasm gates, and exact live-actor arithmetic
stays inside the instruction gate.

The conclusion remains deliberately narrower than a universal performance
claim: small values use more row and index bytes, and the fixed-degree stable
B-tree gains no fan-out or page-touch reduction. `I256`, `AVG(U256)`, bitwise
SQL, Ethereum SDK dependencies and compatibility paths remain deferred. Full
repository release validation remains user-owned.
