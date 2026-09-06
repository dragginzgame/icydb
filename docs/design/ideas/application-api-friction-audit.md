# Application API Friction Audit

Status: idea intake only; no implementation or release authority

Recorded: 2026-09-06

## Problem And Candidate Outcome

Routine application code may repeat schema binding, value conversion, result
decoding, and error handling even when IcyDB already has the information or
ownership needed to perform the operation directly. Some of that code carries
necessary authorization or correctness; some may be avoidable ceremony.

Audit real call sites to distinguish the two. The outcome is a short,
evidence-backed list of steps to delete, simplify, document, or leave intact.
It is not a proposal for another facade, builder family, or convenience-wrapper
layer, and it does not presume that the current API is deficient.

## Maintained Starting Point

- The [public facade guide](../../guides/public-facade-api.md) documents request
  scope, generated adapters, reads, projections, and atomic mutation batches.
- The [read-intent guide](../../guides/read-intent.md) distinguishes maintained
  query and continuation contracts.
- The [transaction contract](../../contracts/TRANSACTION_SEMANTICS.md) and
  [write validation rules](../../contracts/WRITE_ADMISSION.md) own mutation
  boundaries and accepted-schema validation.
- The [diagnostics guide](../../guides/diagnostics.md) describes typed errors
  and supported field context.

Write validation means the checks IcyDB performs before accepting a write:
field/value compatibility with the accepted schema, required values and declared
constraints, uniqueness and relation integrity, and resource limits. The
technical contract calls this "write admission." Application code separately
decides whether the caller may perform the operation and whether it satisfies
business rules. The audit should simplify submitting data while preserving
these database checks in the existing write pipeline.

Check current code and examples before calling a capability missing. Include
relevant completed ownership/decoding improvements from the
[0.255 tracker](../0.255-owned-value-handoff/0.255-status.md) in the baseline;
record pending application or IcyDB changes separately from released behavior.

## Three Initial Workflows

| Workflow | Representative task | Questions to trace |
| --- | --- | --- |
| Lookup | Read one application entity by its maintained key contract. | Are bindings, conversions, not-found handling, or decoding repeated unnecessarily? Does an existing terminal already express the intent? |
| Paginated projection | Return selected fields with bounded continuation. | Is the application loading full rows or rebuilding projection/result metadata? Is continuation handling using the correct existing read contract? |
| Validated atomic write | Read current state, apply application rules, and publish one bounded batch. | Which preparation steps are repeated, and which enforce current authority, atomicity, or business meaning? Does a maintained same-entity or mixed-entity terminal already fit? |

Freeze one actual call site per workflow with its source revision and relevant
schema. Start with one application; seek a second independent example before
generalizing an application-specific helper into a widely consumed API.
Typed/structural Rust is the default scope. SQL transport or language expansion
is not part of this audit.

## Audit Method

For each workflow, record the smallest complete current application code and
trace every step to its existing owner. Classify the work as application
authorization/business logic, required database semantics, avoidable conversion
or copying, missing documentation, or a demonstrated API gap.

Compare in this order:

1. Use the maintained API correctly, including an existing specialized terminal.
2. Improve the example or remove a redundant application step.
3. Simplify an existing owner/signature or transfer an already-owned value.
4. Consider a new public surface only when the preceding options cannot express
   the repeated intent without meaningful application boilerplate or cost.

Do not use line count alone as an ergonomics measure. Record repeated metadata,
intermediate representations, error conversions, and ownership transfers. Note
whether a shorter expression hides authorization, expensive work, or a changed
result/continuation contract.

## Boundaries To Preserve

- Application authorization and business validation remain explicit and owned
  by the application; do not infer them from schema or query shape.
- Accepted schema remains runtime authority. Reducing binding ceremony must not
  remove freshness checks or reconstruct missing authority from generated types.
- Nested helpers reuse the existing request budget; convenience must not reset
  counters or create a separate execution route.
- Atomic publication does not make a calculation from stale reads current.
  Preserve required rereads after asynchronous work and do not add hidden retry,
  compare-and-set, or cross-store transaction semantics.
- Preserve typed errors, missing-row behavior, result ordering, response bounds,
  managed fields, and the selected continuation contract.
- Renamed or removed public surfaces hard-cut before 1.0. Do not keep aliases
  or compatibility wrappers merely to make the candidate diff smaller.

## Deliverable And Complexity Decision

Produce one finding table with the concrete call site, current owner, user
impact, simplest alternative, proposed disposition, and supporting evidence.
Include before/after examples only where a real simplification is available.
An audit that finds documentation or application usage issues only is useful;
it need not result in an engine change.

For proposed code changes, report application and library line deltas separately,
public surface additions/removals, and the state-space delta. Measure allocations,
instructions, and raw Wasm when the proposal claims a runtime improvement or
changes generated specialization. Fewer application lines alone is not proof
of lower execution cost. Tests should exercise maintained outcomes and boundaries,
not freeze a preferred spelling without a behavioral reason.

The default complexity budget is no new runtime state, cache, configuration,
persisted format, or semantic route. One existing owner should become easier
to use. A widely consumed API addition requires demonstrated repeated need and
an explicit explanation of why changing the existing surface is insufficient.

## Promotion Gate

Complete the three-workflow audit and report findings before implementation.
Promote only a selected, independently useful correction with its direct
examples, documentation, and focused validation. Independent API improvements
remain separate landing slices; this note assigns no minor line and does not
authorize a general facade rewrite or changes to downstream applications.
