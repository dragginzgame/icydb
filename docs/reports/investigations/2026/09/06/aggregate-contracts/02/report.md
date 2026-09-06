# Aggregate Contract Corrections

Date: 2026-09-06

Baseline: `v0.254.1`, HEAD `ba336f4c0c83107bb18fb03e213657924a0e38e6`.
Approved follow-up: 0.254 tracker slice 7; release notes remain in active 0.254.2.
The pre-existing ideas housekeeping remains separate and intact.

## Outcome And Owners

- The shared value reducer returns `QueryNumericNotRepresentable` for numeric
  operands outside its Decimal coercion domain. SUM and borrowed/owned AVG
  reuse that check; grouped AVG no longer owns a duplicate coercion step.
- `ProjectionEvalError::into_internal_error` owns operand-error translation.
  Preview, grouped evaluation, global HAVING and global post-aggregate
  projection use it. The obsolete preview mapper and unused numeric query-error
  wrapper are removed. Reader-origin/class and genuine internal failures remain
  distinct from unsupported operands.
- Shared aggregate inference reports Decimal for broad numeric SUM/AVG results,
  and U256 for SUM(U256), without admitting AVG(U256) or changing input rules.

No new execution route, numeric family, accumulator, cache, resource counter,
public signature or persisted format is added. Decimal precision, rounding,
running-sum overflow and NULL behavior are unchanged. Bound HAVING is deferred.

## Audit Evidence Correction

The original audit table incorrectly described NatBig `i128::MAX` aggregation
as successful. Its retained log actually records invariant failures for that
case and even small NatBig/IntBig values. The table and finding are corrected
in [the audit](../01/report.md); its baseline log is unchanged.

Big integers deliberately do not participate in Decimal coercion at any
magnitude. These corrections report their existing limitation as a numeric
domain error, rather than adding conversions or exact accumulation. Tests cover
small and large big integers so mathematical representability is not confused
with the maintained coercion policy.

## Validation

- Six new focused tests pass, including three stored-query contract tests.
  The initial end-to-end tests failed against the pre-correction owners.
- The broader focused aggregate, expression, numeric and SQL convergence
  selection passes all 114 tests. Global/grouped SQL and compiled execution
  agree on domain/HAVING diagnostics; structural grouped queries use the same
  domain failure. Accepted-schema inference is checked against actual Decimal
  and U256 outputs. Empty inputs, NULLs, fractional AVG, overflow, invalid
  reducer operands and reader corruption remain covered.
- Initial implementation failures exposed the separate grouped AVG coercion
  and the audit's big-integer assumption. A temporary missing error conversion
  was repaired. Lint exposed an unused wrapper after error-map consolidation;
  the wrapper was removed instead of suppressed, and its test-only caller was
  updated to use the maintained conversion. The repository clippy workflow
  and focused all-feature core test lint both pass after those corrections.
- The final 114-test rerun, SQL-disabled library check, formatting and whitespace
  checks pass. No remaining validation failure is known in these selected gates.
- Full repository tests, IC instruction/performance measurements and raw/gzip
  Wasm/function-count comparisons are not run for this correction. No local
  network lifecycle is changed. No performance or Wasm-size improvement is
  claimed; prior slice measurements do not measure this correction.

## Complexity

Twelve Rust files change, approximately +303 net lines: about +314 test lines
and -11 production lines. Eight production-owner files are touched; four are
test-only files. Seven documentation/receipt paths accompany the correction.
These figures exclude the pre-existing ideas housekeeping.

The implementation shape is simpler: one aggregate coercion/error owner and
one expression-error translation replace duplicate paths. Success-path Decimal
coercion is unchanged; the numeric-family distinction runs only on failed
coercion. This is structural reasoning, not a measured performance result.

Follow-up remains workload-driven: exact big-integer accumulation or a different
AVG precision contract requires a separate decision, not another error-path fix.
