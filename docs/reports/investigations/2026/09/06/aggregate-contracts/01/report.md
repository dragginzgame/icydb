# Aggregate Result Contracts — Current-Surface Audit

Date: 2026-09-06

Source: `v0.254.1`, HEAD `ba336f4c0c83107bb18fb03e213657924a0e38e6`.
The accompanying worktree changes are documentation housekeeping only.
No aggregate implementation or new minor-version line is authorized by this report.

## Verdict

Prioritize narrow numeric-domain diagnostics and aggregate type-contract work.
Do not promote arbitrary-precision aggregation merely because the intake names
it. The current engine already has shared reducers and exact `SUM(U256)`;
the gaps below do not require a second aggregate pipeline.

## Findings

### 1. Numeric inputs outside Decimal report an invariant failure — medium

An accepted `Nat128` or `NatBig` operand equal to `u128::MAX`, or an `IntBig`
operand equal to `-340282366920938463463374607431768211455`, stores and supports
`MIN`, `MAX` and `COUNT`. `SUM` and `AVG` instead return
`RuntimeInvariantViolation` (19). This reproduces in global SQL, grouped SQL,
compiled SQL execution, and structural grouped queries.

The conversion does not admit these values in the current Decimal domain.
Correction during implementation: the original log also shows failures for
small NatBig/IntBig values. These families do not participate in Decimal
coercion at any magnitude; this is not solely a magnitude limit.
That is a data-dependent numeric limitation, not evidence of corrupt storage or
an impossible executor state. The canonical numeric taxonomy already has
`QueryNumericNotRepresentable` (9), distinct from `QueryNumericOverflow` (8).

Owner: `crates/icydb-core/src/db/executor/aggregate/value_reducer.rs`.
`SumAccumulator::from_value` returns `None` on failed Decimal coercion;
`ingest_sum_value` maps it to an executor invariant. Both borrowed and owned
AVG ingestion perform the same invariant mapping. Preserve true nonnumeric
internal-state failures while classifying admitted out-of-domain numeric values.
No accumulator widening is required to correct this diagnostic.

Minimal reproduction: publish a `NatBig { max_bytes: 64 }` accepted field
`operand`, insert `u128::MAX` through `InputValue::nat_big`, then execute
`SELECT SUM(operand) FROM Singleton`. Replacing SUM with AVG has the same error.
`WHERE FALSE` returns NULL without consuming the out-of-domain operand.

### 2. Mixed-domain aggregate HAVING reaches an invariant — medium

With an accepted U256 operand containing `U256::MAX`, this query returns
`RuntimeInvariantViolation` (19):

```sql
SELECT operand, SUM(operand)
FROM Singleton
GROUP BY operand
HAVING SUM(operand) = 2
```

The plain numeric literal is not a U256 operand. Implicit mixed-width U256
coercion is intentionally unsupported; the failure should be a typed admission
or comparison error, not an internal invariant. This is distinct from the
successful U256 SUM itself. The maintained typed-U256 HAVING control remains
covered by the existing SQL binding tests.

Audit the shared grouped-HAVING admission/evaluation boundary before correcting
this. Do not enable implicit coercion or add a SQL-local aggregate classifier.
The probe establishes the observable failure, not a complete fix or a claim
that every mixed-domain predicate follows this route.

### 3. Inference describes aggregate inputs rather than results — low

Against current accepted schema, `infer_expr_type` reports `Numeric(Integer)`
for SUM/AVG of integer fields, including NatBig and IntBig. Runtime results in
the admitted domain are Decimal; the reducer's `[1, 2]` AVG is Decimal `1.5`.
U256 SUM correctly infers U256 and returns U256. U256 AVG is rejected.

Owner: `crates/icydb-core/src/db/query/plan/expr/type_inference/aggregate.rs`.
`infer_sum_aggregate_type` validates the input and returns that same type.
The result subtype also participates in CASE/COALESCE unification and numeric
scale-argument admission, so it should not be treated as an exact result contract.

The measured ordinary numeric HAVING comparisons succeed; this audit does not
establish a wrong-result query caused by the subtype discrepancy. Specify and
test a shared result contract before changing inference. Keep input admission,
empty-result NULL behavior and U256's separate domain explicit.

## Precision And Boundedness: Contract Decisions, Not Automatic Fixes

| Input/control | Observed SUM | Observed AVG |
| --- | --- | --- |
| Nat64 `[1, 2]`, reducer control | Decimal `3` | Decimal `1.5` |
| Single NatBig `i128::MAX` | Invariant failure | Invariant failure |
| Single NatBig `u128::MAX` | Invariant failure | Invariant failure |
| Single Decimal `10^-28`, stored-query control | Decimal `10^-28` | Decimal `0` |
| `[i128::MAX, 1]`, reducer control | Numeric overflow | Numeric overflow |
| `[i128::MAX, i128::MAX]`, reducer control | Numeric overflow | Numeric overflow despite a representable mathematical mean |
| Single U256 maximum | Exact U256 value | Rejected at query admission |
| Empty or all-NULL admitted reducer input | NULL | NULL |

Decimal has an i128 mantissa and supports scales through 28. Query AVG uses
checked sum/count division. The Decimal division owner starts at 18 fractional
places, rounds half away from zero, and retries at lower precision when
intermediate scaling cannot fit. Thus the tiny single-value AVG is consistent
with that existing arithmetic policy, but is not exact at every stored scale.
The policy has maintained Decimal tests; an exact-average promise would require
a deliberate semantic change, not a covert aggregate fix.

The current SUM and AVG accumulators have fixed-size payloads. AVG can reject
on its intermediate sum even when the final mean would fit. Signed cancellation
and arbitrary-precision accumulation need their own final-range, intermediate
work, memory and encoded-result contracts if a workload justifies them.
No new accumulator, resource counter, cache, result family or persisted state
is proposed here. Full request-budget stress testing is outside this audit.

## Evidence And Reproduction

Temporary sources and complete corrected output are retained locally in
`/tmp/icydb-aggregate-audit.lpJYuE` as `session-probe.rs`, `reducer-probe.rs`
and `probes.log`. They are not maintained regression tests or production code.
To rerun on this source, wire the session probe as a child of
`db::session::tests::unit_ordering::bindings_parity` and the reducer probe as
a child of `db::executor::aggregate::value_reducer::tests`, then select
`aggregate_audit_probe` in the all-feature icydb-core library test target.

- Two temporary probe tests passed. The session probe covers ten accepted
  value cases across SUM/AVG/MIN/MAX/COUNT, global/grouped/empty SQL shapes:
  150 direct cases with corresponding compile/prepared attempts. Successful
  compiled execution compares result values or typed diagnostics with direct
  execution; compile rejection requires direct rejection.
- It also records twenty structural grouped calls, twenty literal HAVING
  calls, and twenty accepted-schema inference results. Structural parity is
  inspected from the recorded values/diagnostics, not a separate automatic
  cross-frontend equality assertion.
- The reducer probe covers nine sequences through SUM and AVG, including
  NULLs, fractional results, out-of-domain input, overflow and precision.
  Its deliberate direct AVG(U256) call bypasses admission and is not evidence
  of an exposed U256 AVG execution route.
- Initial SQL group controls used Unit, which SQL deliberately does not admit
  as a grouping key. The final probe groups by the admitted numeric operand;
  only this corrected run supports grouped-query findings.
- All 25 selected maintained numeric, U256, grouped reducer, planner, typed
  HAVING control and SQL/prepared/fluent convergence tests passed using the freshly built native
  test binary. No full repository/workspace suite ran.
- All temporary wiring and probe sources were removed from the repository.
  The final diff has no Rust, Cargo version or dependency changes. Formatting,
  whitespace, remaining idea links and deleted-note reference checks passed.
  No clippy, Wasm, PocketIC or performance gate was run for this docs-only
  handoff. No local network lifecycle was changed.

Corrected log SHA-256:
`81b7c6d1a430342add274585e2d536ac1387c1a104761668fb2c98530259d9ed`.
Production reducer SHA-256:
`a55e6cd734175ad5025197ecf7f1c9dfec0f39389a6602d1f4012ac51b57b802`.
Aggregate inference SHA-256:
`67fafbdfc7ef67d6305980f6ea718ea5f962cf292722f9da8787abcf607a764b`.

## Housekeeping And Follow-Up

Removed five obsolete ideas: the capability roadmap, completed programme map,
delivered DISTINCT and panic-lint proposals, and superseded physical-migration
sketch. The streaming idea retains only the unresolved collection-allocation
question. Incoming references are repaired; no replacement rolling roadmap is
introduced. Historical release entries are unchanged. Deleted text remains
recoverable from Git.

This is a documentation-only reduction across 25 files, approximately 2,200
net lines removed including this receipt. Runtime complexity is unchanged;
planning-document duplication is reduced. No performance or Wasm delta is
claimed without measurement.

Recommended next action: approve the bounded diagnostic corrections and their
direct regression proofs, with result typing reviewed at its shared owner.
Changing Decimal precision or adding exact NatBig accumulation remains a
separate decision. No correction was implemented during this audit.
