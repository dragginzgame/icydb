# Query comparison, guarded SQL and identity checkpoint corrections

Status: hard cut authorized; numeric implementation in progress, not qualified.
The maintainer selected these three investigations on 7 November 2026.
Tracking authority remains [#110](https://github.com/dragginzgame/icydb/issues/110),
[#335](https://github.com/dragginzgame/icydb/issues/335) and
[#102](https://github.com/dragginzgame/icydb/issues/102).

## 1. One numeric comparison domain (#110)

Current predicate/database comparison converts mixed values to Decimal, while
Value comparison also has a separate float fallback. SQL canonical ordering
combines numeric equality with variant-rank fallback. Consequently repairing a
single pair can preserve or create cycles involving a third value or a nonnumeric
rank. Converge these consumers before changing admitted mixed comparisons.

Proposed policy: compare the exact represented finite numeric value. Binary
floats retain their binary value; Decimal retains its decimal value. Thus a
Float32 decimal spelling is not automatically equal to the same Float64 or
Decimal spelling. Preserve time-value comparison units and numerical zero ties.
Arithmetic and deduplication identity retain their separate existing contracts.
U256 remains outside the broad numeric coercion domain.

Use the existing representation owner in value::ops::numeric. Represent magnitude
as an unsigned coefficient times a power of two divided by a power of five.
An i128 Decimal with scale at most 28 needs only a 128-bit coefficient, exponent
minus scale and denominator 5^scale. Finite IEEE floats need at most 53 coefficient
bits and a bounded signed binary exponent. Integers need at most 128 bits.
Cross-multiplication by the opposite denominator needs fewer than 194 bits, so
existing 256-bit arithmetic is sufficient: no arbitrary-precision dependency,
text roundtrip or heap allocation is needed. Compare leading-bit exponents first,
then normalized significands; never shift a large exponent into a finite word.
Validate this bound against all maintained Decimal constructors before coding.

SQL sorting must place all broad numeric values in one rank block relative to
nonnumeric values. Within that block use exact numeric order and compatible ties.
Do not change raw Value identity ranks, collection encoding or index key bytes
incidentally. Review planner range proofs and cursor-domain binding explicitly;
changed query ordering must not silently admit a continuation from old semantics.

Deliver as one semantic change: shared comparison, predicate/projection/ORDER BY
consumers, planner legality, cursor implications, docs and direct tests. Preserve
same-type native paths. Qualify the two reported transitivity triples, numeric
and nonnumeric ties, extreme/subnormal/adjacent values, signed zero, full integer
ranges, Decimal scales, explicit SQL rows and indexed/residual agreement. Add
interaction cases for distinct risks, not a Cartesian test matrix.

## 2. Guarded SQL through public paging (#335)

The generated guarded handler and controller handler currently converge on the
trusted collect-all SQL dispatch. Authorization succeeds before execution, but
it does not select public admission or a public page envelope. SqlQueryResult
cannot convey scalar-page continuation, so changing the lane alone is incomplete.

Reuse the structural public pager and its route, liveness, envelope and cursor
owners. Guarded SELECT needs a bounded page request and response carrying its
continuation; controller-only and explicitly trusted in-process dispatch retain
their operator contract. Do not add a second pager or truncate collect-all output.
Authorization still precedes request construction.

Selected hard-cut direction: initially reject SELECT shapes without a maintained
public paging route. Aggregate, DISTINCT, expression/sort and introspection routes
must not silently escape through trusted dispatch. No unsupported route may fall back to trusted execution.

Qualification must exercise an actual generated guarded endpoint, indexed route
admission, more than one page, sparse scans, continuation/exhaustion and denied
callers. Keep controller behavior as an independent control.

## 3. Separate immutable checkpoint and mutable identity state (#102)

The existing exact-replay shortcut is a local compatible correction. New advances
still load the entire candidate bundle and rewrite its checkpoint. Caching that
bundle would add invalidation authority without eliminating the durable rewrite.

Investigate a bounded identity record per existing identity owner under the
existing control store, bound to database/store/owner authority. Reuse the
IdentityState codec and marker-owned range replay; do not add a second allocator,
journal or retry state machine. Ordinary commits should read/write only affected
identity states. Accepted schema publication remains the owner of immutable
candidate bytes and active/retired owner reconciliation.

Before implementation resolve publication atomicity, inventory discovery, owner
retirement and control-store record limits. The current maximum record count
assumes application receipts plus store checkpoints, lineage and migration; it
cannot simply be reused for per-owner records. Startup/integrity must validate
ownership without trusting an unverified cached schema.

This is a retained-format hard cut. Select a new database format discriminator,
reject earlier images without mutation, and retain same-format interrupted-commit
recovery. No dual reader, legacy fallback or automatic deletion. Qualify multiple
owners, unchanged candidate bytes, marker interruption/replay, retirement,
conflicting advance identity and corrupt/missing identity records. Measure actual
IC instructions/cycles and total/code-section Wasm before claiming cost savings.

## Delivery boundaries

These are three independent outcomes, not one combined release gate or mega-slice.
Implement numeric comparison first; guarded SQL waits for the support decision;
checkpoint layout follows its retained-data design review. Existing pending
supplier adoption, compatibility decisions and package versions are preserved.
No current source or test evidence establishes completion of these designs.
