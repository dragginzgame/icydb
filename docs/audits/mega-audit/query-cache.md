# Prepared-query cache closeout investigation

Investigation and authorized implementation after relation preparation,
2026-11-11. This inspects
[cache resolution](../../../crates/icydb-core/src/db/session/query/cache/mod.rs) and
[template ownership](../../../crates/icydb-core/src/db/session/query/cache/template.rs).
It is evidence for a bounded next outcome, not a separate follow-up queue.

## Implemented: one parameterized preparation/publication tail

`resolve_parameterized_query_plan_for_authority` first checks for an exactly
reusable bound plan and its current cardinality lifecycle. On a miss there are
two construction paths: rebind an existing template, or compile and create one.
Both independently finish with `SharedPreparedExecutionPlan::from_plan`, remember
the bound fingerprint/plan, wrap the template, insert the artifact, and return it.

Keep exact-bound reuse as an early return. For remaining work, charge compilation
only when no template exists, then retain one preparation interval containing the
two distinct logical construction branches. Each branch can yield its logical
plan and template; share final prepared-plan construction within that interval.
After successful interval completion, share memo replacement, weighted insertion
and return. Retain the explicit Hit/Miss result: rebinding an existing template
is a Hit, whereas first construction is a Miss.

Preserve the precise miss order: visible indexes, logical planning, cardinality
tiebreak, template allocation, prepared execution construction. Preserve the
rebind order: bind against retained indexes, current cardinality tiebreak, then
prepared construction. Do not hoist template allocation ahead of planning or
move publication inside `PreparationWork::run`. Its final instruction charge
must succeed before the checked-out template replaces any cache resident.

Expected complexity reduction: one final preparation/publication implementation
instead of two, with no new helper, cache, type or generic framework. Actual
non-test/test LOC reduction and IC/Wasm effects remain unmeasured. This is local
control-flow consolidation, not evidence of duplicate query-planning engines.

## Boundaries to retain

- Filtered requests normalize and fingerprint current operands before cache
  reuse. Template shape is insufficient to validate new values; existing range
  tests reject wrong operand types before reuse.
- The recent-bound memo retains at most one concrete plan and is checked against
  the normalized predicate fingerprint. Candidate index arrays are Rc-shared;
  cloning a template is not a deep copy of that metadata.
- Non-parameterized filtered lookup currently follows visible-index projection.
  Moving it earlier might avoid work on hits, but changes resource charging and
  failure order. That is not part of this equivalent consolidation.
- Filterless hits already bypass schema projection. Continuation route pins
  deliberately avoid ordinary cache replacement and current cardinality choice.
- Weighted insertion checks retention eligibility and completes sizing before
  mutation. A failed rebind must leave the previously cached artifact intact.

## Qualification

Select existing parameterized dynamic/SQL reuse, changed-value/range/IN-list
rebinding, retained-cache accounting, failed-preparation non-publication,
planner-watermark insertion and verbose Hit/Miss/cardinality-lifecycle tests.
Review the selected tests for empty cache, exact-bound hit, rebind and stale
cardinality paths; add only missing behavioral coverage.

The two logical construction branches now return their plan and template into
one final prepared-plan constructor, followed by one memo/publication tail.
Exact-bound hits retain their early return; miss-only charging and allocation
order are unchanged. No new helper, type or cache is added.

Actual delta: **-9 non-test / 0 test code lines**. Runtime policy duplication is
reduced to one finalization/publication owner. Cumulative audit delta:
**-293 non-test, +80 test, net -213 code lines**. IC cycles/instructions and
uncompressed Wasm total/code-section effects remain unmeasured.

Strict Core lib/tests Clippy with SQL/migration/metrics and 18 focused tests pass:
eight planner-watermark, three template-accounting, three parameterized-query,
retained-cache accounting, bound-SQL reuse, verbose Hit/Miss and cardinality
lifecycle checks. Formatting, documentation and diff checks pass. Broad
workspace/release gates remain skipped user-owned validation. No dependency,
package-version or delivery changes are made by this cleanup.
