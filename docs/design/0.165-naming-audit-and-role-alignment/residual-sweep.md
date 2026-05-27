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
rg -n "LoadOrderRouteContract|GroupedExecutionModeProjection|RouteCapabilities|route::capability\\b|route::contracts::capabilities|CoveringProjectionContext|CoveringAccessMetadata|PreparedExecutionPlanCoreShared|PreparedExecutionInputParts|GroupedPathRuntimeCore|PreparedScalarRuntimeParts|PreparedGroupedRuntimeParts|PreparedAccessPlanParts|PreparedAggregateStreamingPlanParts|SharedPreparedProjectionRuntimeParts|from_valid_shared_parts|into_scalar_runtime_parts|cloned_grouped_runtime_parts|into_access_plan_parts|into_streaming_parts|into_projection_runtime_parts|execute_initial_scalar_retained_slot_page_from_runtime_parts|prepared_execution_plan::parts|from_scalar_runtime_parts|from_stream_runtime_parts|LoweredAccess::into_parts|lowered\\.into_parts\\(|prepare_scalar_route_runtime_from_parts|compile_grouped_row_slot_layout_from_parts|into_runtime_parts|from_validated_parts|PlannedCursor|GroupedPlannedCursor|GroupedWindowProjection|ScalarTokenParts|GroupedTokenParts|GroupedContinuationToken::into_parts|PersistedRelationDescriptionParts|persisted_relation_description_parts|SqlProjectionPayloadParts|SqlProjectionPayload::into_parts|SqlProjectionContract::into_parts|describe_entity_model_with_parts|PreparedSqlScalarAggregateDescriptorShape|SqlGlobalAggregateCommandCore|compile_sql_global_aggregate_command_core_from_prepared_with_schema|into_execution_parts|into_aggregate_plan_parts|AggregateTerminalSemantics|resolve_or_insert_global_aggregate_terminal|resolve_having_global_aggregate_terminal_index|derive_single_path_access_shape_facts_from_parts|from_authority_parts|PlannedAccessSelection::into_parts|from_parts_with_projection|from_planned_parts_with_projection|AggregateIdentity::from_parts|AggregateSemanticKey::into_parts|from_semantic_parts|from_uncompiled_parts|from_planner_parts|PreparedAggregateSemantics::from_parts|PreparedAggregateSemantics::into_executor_parts|AggregateTerminalSemanticKey::into_parts|PreparedAggregateTarget::into_executor_parts|RuntimeGroupedRow::into_parts|StructuralGroupedProjectionResult::into_parts|GroupedContinuationWindow::into_parts|CompiledGroupedProjectionPlan::from_parts_for_test|FieldSlot::from_parts_for_test|from_parts_for_test|ResolvedExecutionKeyStream::into_parts|StructuralCursorPage::into_parts|GroupedStreamStage::parts_mut|IndexDecodedKeyScanChunk::into_parts|SqlCompiledCommandCacheContext::into_parts|DeleteProjection::into_parts|Self::from_parts|PreparedProjectionShape::from_test_parts|Row::into_parts|ProjectedRow::into_parts|PagedLoadExecution::into_parts|PagedGroupedExecution::into_parts|SqlProjectionRows::into_parts|MetricRatio::into_parts|AcceptedGeneratedCompatibleRowShape|generated_compatible_row_shape_for_model|RelationDescriptor|RelationDescriptorCardinality|relation_descriptors_for_model_iter|accepted_relation_target_descriptor_from_kind|AcceptedRelationTargetDescriptor|IndexName::try_from_parts|IndexName::try_unique_from_parts|try_from_parts_with_prefix|query::fingerprint::hash_parts|mod hash_parts|hash_parts::" crates docs CHANGELOG.md
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
