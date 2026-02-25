use crate::{
    db::{
        Context,
        executor::{
            ExecutablePlan, ExecutionKernel, IndexPredicateCompileMode, LoweredIndexPrefixSpec,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, apply_aggregate_direction,
                compare_entities_by_orderable_field, compare_entities_for_field_extrema,
            },
            fold::{AggregateFoldMode, AggregateKind, AggregateOutput, AggregateSpec},
            load::{AggregateExecutionDescriptor, AggregateFastPathInputs, LoadExecutor},
            plan::record_rows_scanned,
            route::{ExecutionMode, RoutedKeyStreamRequest},
        },
        index::predicate::IndexPredicateExecution,
        query::{
            ReadConsistency,
            plan::{AccessPlannedQuery, Direction},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};
use std::cmp::Ordering;

///
/// AggregateReducerDispatch
///
/// AggregateReducerDispatch maps one aggregate descriptor to one kernel-owned
/// reducer execution adapter. This keeps orchestration declarative and avoids
/// re-deriving execution-mode semantics at call sites.
///

enum AggregateReducerDispatch<'a> {
    Materialized {
        spec: &'a AggregateSpec,
    },
    FieldExtremaStreaming {
        kind: AggregateKind,
        target_field: &'a str,
        direction: Direction,
        route_plan: &'a crate::db::executor::ExecutionPlan,
    },
    StreamingFold,
}

///
/// AggregateReducerSelection
///
/// AggregateReducerSelection carries either a completed aggregate output or a
/// plan that must continue through canonical streaming fold execution.
///

/// LLM TODO - fix this please
#[expect(clippy::large_enum_variant)]
enum AggregateReducerSelection<E: EntityKind + EntityValue> {
    Completed(AggregateOutput<E>),
    Streaming(ExecutablePlan<E>),
}

impl<'a> AggregateReducerDispatch<'a> {
    // Derive one reducer adapter from a validated aggregate descriptor.
    fn from_descriptor(descriptor: &'a AggregateExecutionDescriptor) -> Self {
        if matches!(
            descriptor.route_plan.execution_mode,
            ExecutionMode::Materialized
        ) {
            return Self::Materialized {
                spec: &descriptor.spec,
            };
        }
        if let Some(target_field) = descriptor.spec.target_field() {
            return Self::FieldExtremaStreaming {
                kind: descriptor.spec.kind(),
                target_field,
                direction: descriptor.direction,
                route_plan: &descriptor.route_plan,
            };
        }

        Self::StreamingFold
    }

    // Execute eager reducers immediately, or defer to canonical streaming fold.
    fn execute_or_stream<E>(
        self,
        executor: &LoadExecutor<E>,
        plan: ExecutablePlan<E>,
    ) -> Result<AggregateReducerSelection<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match self {
            Self::Materialized { spec } => Ok(AggregateReducerSelection::Completed(
                executor.execute_materialized_aggregate_spec(plan, spec)?,
            )),
            Self::FieldExtremaStreaming {
                kind,
                target_field,
                direction,
                route_plan,
            } => Ok(AggregateReducerSelection::Completed(
                executor.execute_field_target_extrema_aggregate(
                    plan,
                    kind,
                    target_field,
                    direction,
                    route_plan,
                )?,
            )),
            Self::StreamingFold => Ok(AggregateReducerSelection::Streaming(plan)),
        }
    }
}

impl ExecutionKernel {
    // Resolve one routed key stream request, then fold one aggregate terminal
    // over the resolved stream using canonical aggregate fold behavior.
    pub(in crate::db::executor) fn fold_aggregate_from_routed_stream_request<E>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        direction: Direction,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
        stream_request: RoutedKeyStreamRequest<'_, E::Key>,
    ) -> Result<(AggregateOutput<E>, usize), InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let mut key_stream = LoadExecutor::<E>::resolve_routed_key_stream(ctx, stream_request)?;

        LoadExecutor::<E>::fold_aggregate_over_key_stream(
            ctx,
            plan,
            direction,
            kind,
            fold_mode,
            key_stream.as_mut(),
        )
    }

    // Resolve one secondary index order stream attempt and fold one aggregate
    // terminal from it, preserving rows-scanned accounting from the fast path.
    #[expect(clippy::too_many_arguments)]
    pub(in crate::db::executor) fn try_fold_secondary_index_aggregate<E>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        index_prefix_spec: Option<&LoweredIndexPrefixSpec>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        direction: Direction,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let Some(fast) = LoadExecutor::<E>::try_execute_secondary_index_order_stream(
            ctx,
            plan,
            index_prefix_spec,
            physical_fetch_hint,
            index_predicate_execution,
        )?
        else {
            return Ok(None);
        };
        let (aggregate_output, rows_scanned) =
            LoadExecutor::<E>::fold_aggregate_from_fast_path_result(
                ctx, plan, direction, kind, fold_mode, fast,
            )?;
        if let Some(fetch) = physical_fetch_hint {
            debug_assert!(
                rows_scanned <= fetch,
                "secondary extrema probe rows_scanned must not exceed bounded fetch",
            );
        }

        Ok(Some((aggregate_output, rows_scanned)))
    }

    // Execute one aggregate spec through kernel-owned orchestration while
    // preserving route-owned execution-mode and fast-path behavior.
    pub(in crate::db::executor) fn execute_aggregate_spec<E>(
        executor: &LoadExecutor<E>,
        plan: ExecutablePlan<E>,
        spec: AggregateSpec,
    ) -> Result<AggregateOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let descriptor = LoadExecutor::<E>::build_aggregate_execution_descriptor(&plan, spec)?;
        let kind = descriptor.spec.kind();

        // Kernel-owned reducer adapter selection. Eager reducers return
        // immediately; streaming reducers continue through canonical key-stream
        // preparation and fold.
        let plan = match AggregateReducerDispatch::from_descriptor(&descriptor)
            .execute_or_stream(executor, plan)?
        {
            AggregateReducerSelection::Completed(aggregate_output) => return Ok(aggregate_output),
            AggregateReducerSelection::Streaming(plan) => plan,
        };
        let fold_mode = descriptor.route_plan.aggregate_fold_mode;
        let physical_fetch_hint = descriptor.route_plan.scan_hints.physical_fetch_hint;
        let prepared = executor.prepare_aggregate_streaming_inputs(plan)?;

        let fast_path_inputs = AggregateFastPathInputs {
            ctx: &prepared.ctx,
            logical_plan: &prepared.logical_plan,
            route_plan: &descriptor.route_plan,
            index_prefix_specs: prepared.index_prefix_specs.as_slice(),
            index_range_specs: prepared.index_range_specs.as_slice(),
            index_predicate_program: descriptor.strict_index_predicate_program.as_ref(),
            direction: descriptor.direction,
            physical_fetch_hint,
            kind,
            fold_mode,
        };
        // Policy boundary: all aggregate optimizations must dispatch through the
        // route-owned fast-path order below (no ad-hoc kind-specialized branches
        // in executor call sites).
        if let Some((aggregate_output, rows_scanned)) =
            LoadExecutor::<E>::try_fast_path_aggregate(&fast_path_inputs)?
        {
            record_rows_scanned::<E>(rows_scanned);
            return Ok(aggregate_output);
        }

        // Build canonical execution inputs. This must match the load executor
        // path exactly to preserve ordering and DISTINCT behavior.
        let execution_inputs = prepared.execution_inputs(descriptor.direction);

        // Resolve the ordered key stream using canonical routing logic.
        let mut resolved = Self::resolve_execution_key_stream(
            &execution_inputs,
            &descriptor.route_plan,
            IndexPredicateCompileMode::StrictAllOrNone,
        )?;

        // Fold via one streaming engine. COUNT pushdown uses key-only mode;
        // other terminals use row-existence mode.
        let (aggregate_output, keys_scanned) = LoadExecutor::<E>::fold_aggregate_over_key_stream(
            &prepared.ctx,
            &prepared.logical_plan,
            descriptor.direction,
            kind,
            fold_mode,
            resolved.key_stream.as_mut(),
        )?;

        // Preserve row-scan metrics semantics.
        // If a fast-path overrides scan accounting, honor it.
        let rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);
        record_rows_scanned::<E>(rows_scanned);

        Ok(aggregate_output)
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Streaming reducer for index-leading field extrema. This keeps execution in
    // key-stream mode and stops once the first non-tie worse field value appears.
    pub(in crate::db::executor) fn fold_streaming_field_extrema(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        key_stream: &mut dyn crate::db::executor::OrderedKeyStream,
        target_field: &str,
        field_slot: FieldSlot,
        kind: AggregateKind,
        direction: Direction,
    ) -> Result<(AggregateOutput<E>, usize), InternalError> {
        if direction != Self::field_extrema_aggregate_direction(kind)? {
            return Err(InternalError::query_executor_invariant(
                "field-extrema fold direction must match aggregate terminal semantics",
            ));
        }

        let mut keys_scanned = 0usize;
        let mut selected: Option<(Id<E>, E)> = None;

        while let Some(data_key) = key_stream.next_key()? {
            keys_scanned = keys_scanned.saturating_add(1);
            let Some(entity) = Self::read_entity_for_field_extrema(ctx, consistency, &data_key)?
            else {
                continue;
            };
            let id = Id::from_key(data_key.try_key::<E>()?);
            let selected_was_empty = selected.is_none();
            let candidate_replaces = match selected.as_ref() {
                Some((_, current)) => {
                    compare_entities_for_field_extrema(
                        &entity,
                        current,
                        target_field,
                        field_slot,
                        direction,
                    )
                    .map_err(AggregateFieldValueError::into_internal_error)?
                        == Ordering::Less
                }
                None => true,
            };
            if candidate_replaces {
                selected = Some((id, entity));
                if selected_was_empty && matches!(kind, AggregateKind::Min) {
                    // MIN(field) under ascending index-leading traversal is resolved
                    // by the first in-window existing row.
                    break;
                }
                continue;
            }

            let Some((_, current)) = selected.as_ref() else {
                continue;
            };
            let field_order =
                compare_entities_by_orderable_field(&entity, current, target_field, field_slot)
                    .map_err(AggregateFieldValueError::into_internal_error)?;
            let directional_field_order = apply_aggregate_direction(field_order, direction);

            // Once traversal leaves the winning field-value group, the ordered
            // stream cannot produce a better extrema candidate.
            if directional_field_order == Ordering::Greater {
                break;
            }
        }

        let selected_id = selected.map(|(id, _)| id);
        let output = match kind {
            AggregateKind::Min => AggregateOutput::Min(selected_id),
            AggregateKind::Max => AggregateOutput::Max(selected_id),
            AggregateKind::Count
            | AggregateKind::Exists
            | AggregateKind::First
            | AggregateKind::Last => {
                return Err(InternalError::query_executor_invariant(
                    "field-extrema fold reached non-extrema terminal",
                ));
            }
        };

        Ok((output, keys_scanned))
    }
}
