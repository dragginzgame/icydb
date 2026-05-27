# 0.165 Residual Vocabulary Sweep

## Status

Complete.

## Scope

This sweep checks the concrete old-vocabulary lists produced by the completed
0.165 families and records remaining hits. It does not accept new renames by
itself; it verifies that accepted hard-cut renames did not leave old live-code
aliases, forwarding helpers, or active instructional drift.

## Concrete Old-Name Sweep

Command:

```bash
rg -n "LoadOrderRouteContract|GroupedExecutionModeProjection|RouteCapabilities|route::capability\\b|route::contracts::capabilities|CoveringProjectionContext|CoveringAccessMetadata|PreparedExecutionPlanCoreShared|PreparedExecutionInputParts|GroupedPathRuntimeCore|PreparedScalarRuntimeParts|PreparedGroupedRuntimeParts|PreparedAccessPlanParts|PreparedAggregateStreamingPlanParts|SharedPreparedProjectionRuntimeParts|from_valid_shared_parts|into_scalar_runtime_parts|cloned_grouped_runtime_parts|into_access_plan_parts|into_streaming_parts|into_projection_runtime_parts|execute_initial_scalar_retained_slot_page_from_runtime_parts|prepared_execution_plan::parts|from_scalar_runtime_parts|from_stream_runtime_parts|PlannedCursor|GroupedPlannedCursor|GroupedWindowProjection|ScalarTokenParts|GroupedTokenParts|GroupedContinuationToken::into_parts|PersistedRelationDescriptionParts|persisted_relation_description_parts|SqlProjectionPayloadParts|SqlProjectionPayload::into_parts|SqlProjectionContract::into_parts|PreparedSqlScalarAggregateDescriptorShape|SqlGlobalAggregateCommandCore|compile_sql_global_aggregate_command_core_from_prepared_with_schema|AggregateTerminalSemantics|resolve_or_insert_global_aggregate_terminal|resolve_having_global_aggregate_terminal_index" crates docs CHANGELOG.md
```

Remaining hits classify as:

- current `CHANGELOG.md` and `docs/changelog/0.165.md`: active release notes
  that intentionally map old names to accepted 0.165 names
- 0.165 family notes: accepted-rename history, role proof, and scan terms
- older `docs/changelog/*`: historical release notes
- `docs/design/archive/*`: historical design state
- `docs/audits/reports/*`: generated or retained audit artifacts

No live-code hit remains under `crates/**` for the accepted 0.165 old names.

## Generic Role-Term Sweep

Command:

```bash
rg -n "\b(Contract|Decision|Facts|Context|Shape|Identity|Semantics|Analysis)\b" crates/icydb-core/src/db crates/icydb/src docs/design/0.165-naming-audit-and-role-alignment
```

Representative retained hits classify as:

- `executor::Context`: current executor-local runtime context; matches
  `*Context` policy
- route `Contract:` comments and route contract modules: current route proof
  and invariant vocabulary, not the removed ordered-load route mode
- predicate semantics helpers: current predicate behavior classification
- `ErrorOrigin::Identity`: current error-origin vocabulary for identity/key
  failures
- EXPLAIN `Decision:` rendering text: user-facing route explanation text
- 0.165 family notes and policy docs: active naming policy and role proof

No generic role-term hit from this sweep requires a new 0.165 rename.

## Public Surface Sweep

Command:

```bash
rg -n "QueryResponse|ProjectionResponse|MutationResult|MutationMode|SqlProjectionRows|SqlQueryRowsOutput|SqlGroupedRowsOutput|SqlQueryResult|QueryExecutionAttribution|SqlQueryExecutionAttribution|ExplainExecutionDescriptor|ExplainPropertyMap" crates docs
```

Remaining live-code hits are intentional public/facade vocabulary. Public
surface names are kept with proof in `public-surface.md`.

## Deferred Items

None from this sweep.

Future families may still audit narrower concepts, but this residual pass did
not find unclassified live old-name drift from the accepted 0.165 renames.
