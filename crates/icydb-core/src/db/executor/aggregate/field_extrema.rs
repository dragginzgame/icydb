use crate::{
    db::{
        Context,
        direction::Direction,
        executor::{
            AccessStreamBindings, ExecutablePlan, ExecutionKernel, ExecutionPreparation,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, apply_aggregate_direction,
                compare_entities_by_orderable_field, compare_entities_for_field_extrema,
                resolve_orderable_aggregate_target_slot,
            },
            aggregate::{AggregateKind, AggregateOutput},
            load::{ExecutionInputs, LoadExecutor},
            plan_metrics::record_rows_scanned,
        },
        index::IndexCompilePolicy,
        predicate::MissingRowPolicy,
        response::Response,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};
use std::cmp::Ordering;

impl ExecutionKernel {
    // Reduce one materialized response into a field-target extrema id with the
    // deterministic tie-break contract `(field_value, primary_key_asc)`.
    pub(in crate::db::executor::aggregate) fn aggregate_field_extrema_from_materialized<E>(
        response: Response<E>,
        kind: AggregateKind,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<AggregateOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        if !kind.is_extrema() {
            return Err(InternalError::query_executor_invariant(
                "materialized field-extrema reduction requires MIN/MAX terminal",
            ));
        }
        let compare_direction = kind.extrema_direction().ok_or_else(|| {
            InternalError::query_executor_invariant(
                "materialized field-extrema reduction reached non-extrema terminal",
            )
        })?;

        let mut selected: Option<(Id<E>, E)> = None;
        for (id, entity) in response {
            let should_replace = match selected.as_ref() {
                Some((_, current)) => {
                    compare_entities_for_field_extrema(
                        &entity,
                        current,
                        target_field,
                        field_slot,
                        compare_direction,
                    )
                    .map_err(AggregateFieldValueError::into_internal_error)?
                        == Ordering::Less
                }
                None => true,
            };
            if should_replace {
                selected = Some((id, entity));
            }
        }

        let selected_id = selected.map(|(id, _)| id);

        kind.extrema_output(selected_id).ok_or_else(|| {
            InternalError::query_executor_invariant(
                "materialized field-extrema reduction reached non-extrema terminal",
            )
        })
    }

    // Execute one route-eligible field-target extrema aggregate through kernel-
    // owned streaming setup, stream resolution, and fold orchestration.
    pub(in crate::db::executor::aggregate) fn execute_field_target_extrema_aggregate<E>(
        executor: &LoadExecutor<E>,
        plan: ExecutablePlan<E>,
        kind: AggregateKind,
        target_field: &str,
        direction: Direction,
        route_plan: &crate::db::executor::ExecutionPlan,
    ) -> Result<AggregateOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let field_fast_path_eligible = if kind == AggregateKind::Min {
            route_plan.field_min_fast_path_eligible()
        } else if kind == AggregateKind::Max {
            route_plan.field_max_fast_path_eligible()
        } else {
            return Err(InternalError::query_executor_invariant(
                "field-target aggregate execution requires MIN/MAX terminal",
            ));
        };
        if !field_fast_path_eligible {
            return Err(InternalError::query_executor_invariant(
                "field-target aggregate streaming requires route-eligible field-extrema fast path",
            ));
        }

        // Validate the field target before any stream execution work so
        // unsupported targets fail without scan-budget consumption.
        let field_slot = resolve_orderable_aggregate_target_slot::<E>(target_field)
            .map_err(AggregateFieldValueError::into_internal_error)?;
        let execution_preparation = ExecutionPreparation::for_plan::<E>(plan.as_inner());

        // Reuse shared aggregate streaming setup and route-owned stream resolution.
        let prepared = Self::prepare_aggregate_streaming_inputs(executor, plan)?;
        let execution_inputs = ExecutionInputs {
            ctx: &prepared.ctx,
            plan: &prepared.logical_plan,
            stream_bindings: AccessStreamBindings {
                index_prefix_specs: prepared.index_prefix_specs.as_slice(),
                index_range_specs: prepared.index_range_specs.as_slice(),
                index_range_anchor: None,
                direction,
            },
            execution_preparation: &execution_preparation,
        };
        let mut resolved = Self::resolve_execution_key_stream(
            &execution_inputs,
            route_plan,
            IndexCompilePolicy::StrictAllOrNone,
        )?;
        let (aggregate_output, keys_scanned) = LoadExecutor::<E>::fold_streaming_field_extrema(
            &prepared.ctx,
            prepared.logical_plan.scalar_plan().consistency,
            resolved.key_stream.as_mut(),
            target_field,
            field_slot,
            kind,
            direction,
        )?;

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
        consistency: MissingRowPolicy,
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
        let output = kind.extrema_output(selected_id).ok_or_else(|| {
            InternalError::query_executor_invariant(
                "field-extrema fold reached non-extrema terminal",
            )
        })?;

        Ok((output, keys_scanned))
    }
}
