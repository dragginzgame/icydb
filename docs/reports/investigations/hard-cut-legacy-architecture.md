> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# IcyDB Hard-Cut Legacy Architecture Audit

Date: 2026-07-13

> Scope: repository-wide architectural-authority and legacy-sediment audit
>
> Compared baseline report: N/A. No earlier report uses this scope and method;
> `docs/audits/reports/2026-07/2026-07-04/technical-debt.md` was consulted as
> historical context only.
>
> Code snapshot: `e1928d349e9294cfc9cee3d6a78d2eb831da0bec`, plus the recorded dirty
> worktree described below.
>
> Method: Hard-Cut Architecture Audit V1
>
> Comparability: non-comparable — this is the first deletion-proof and
> single-authority audit using the A–F hard-cut classification.

## Executive assessment

Architectural sediment is **high**, but not critical.

The production read spine is substantially more coherent than the repository's
surface area suggests. Typed/fluent and reduced SQL reads both lower to
`StructuralQuery`, use the shared query-plan cache, produce an
`AccessPlannedQuery`/`SharedPreparedExecutionPlan`, and execute through the same
route planner and executor contracts. Accepted schema snapshots are the runtime
authority. Cursor decoding is one bounded, version-gated implementation. The
executor has one effective residual-filter contract and one route-selection
artifact.

The surviving old design families are:

- a public-but-hidden direct `Query<E>` surface beside the fluent facade;

- two inert `PlannedQuery<E>` / `CompiledQuery<E>` stages over the same logical
  plan;

- a test-only public-SQL admission architecture even though production SQL is
  trusted/controller-only;

- public admission DTOs that expose an internal policy vocabulary;

- an obsolete projection-response facade and explicit SQL compatibility aliases;

- diagnostics that stringify typed planner facts and reconstruct them later;

### Authority sources

The audit treated sources in this order:

1. Active designs `0.200`, `0.201`, and `0.202`, especially their accepted-schema
   authority and pre-1.0 hard-cut rules.
2. The `0.198` closeout and `0.199` boundary/authority artifacts now under
   `docs/design/archive/`.
3. Root and detailed changelogs through `0.201`, plus the current `Unreleased`
   worktree.
4. Normative contracts under `docs/contracts/` and the public facade guide.
5. Current facade, generator, planner, executor, admission, cursor, persistence,
   metrics, and error implementations.
6. Unit, integration, generated-code, compile-fail, and snapshot tests.
7. Targeted Git history for conflicting or compatibility-shaped surfaces.

The latest query/read-intent design families are archived because their release
lines closed; their closeout and boundary matrices remain the most recent
architecture evidence for those subsystems. Active `0.200`–`0.202` documents do
not replace that query design. They reinforce its two relevant invariants:
accepted snapshots are runtime authority and pre-1.0 removals are hard cuts.

### Methods and constructors

- `DbSession::execute_sql_query_with_read_admission_policy`.

- `DbSession::execute_compiled_sql_context_with_read_admission_policy`.

- `DbSession::execute_global_aggregate_compiled_statement_ref_with_read_admission_policy`.

- Test-only SQL select methods whose only caller is that dispatch.

- `QueryAdmissionPolicy::admin_ad_hoc`.

- All public/core `.planned()` and `.plan()` terminals listed in HCLA-004.

- `DbSession::planned_query_with_visible_indexes`.

- `DbSession::compile_query_with_visible_indexes`.

- Facade `DbSession::execute_query_result_with_attribution` that accepts raw
  `Query<E>`.

- Facade `DbSession::trace_query` that accepts raw `Query<E>`.

- `ProjectionResponse::from_core` and its iteration/cardinality methods.

- `ExplainRejectedIndexV1::from_rejection` in its string-parsing form.

- `parse_rejected_index_label`.

## Change and cost readout

- Production files changed by this audit: **0**.

- Audit artifacts added: **1 Markdown report, approximately 1,400 lines**.

- Runtime implementation complexity: **unchanged**.

- Runtime performance delta: **none; report-only change**.

- Raw non-gzipped wasm delta: **0 bytes by construction; no wasm-producing
  source changed**. No wasm artifact was rebuilt.

- Gzip wasm context: not applicable.

- Changelog: not changed because this is a governance/audit-only artifact.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/07/13/hard-cut-legacy-architecture/01/report.md).
