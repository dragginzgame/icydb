//! Module: executor::aggregate::field_extrema
//! Responsibility: field-target extrema aggregate execution helpers.
//! Does not own: field capability derivation or planner aggregate semantics.
//! Boundary: materialized and streaming extrema execution for aggregate kernels.

use crate::{
    db::{
        Context,
        data::{DataKey, DataRow},
        direction::Direction,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutionKernel, ExecutionPlan,
            ExecutionPreparation, KeyStreamLoopControl,
            aggregate::{
                AggregateKind, PreparedAggregateStreamingInputs, ScalarAggregateOutput,
                field::{
                    AggregateFieldValueError, FieldSlot, apply_aggregate_direction,
                    compare_orderable_field_values, extract_orderable_field_value_with_slot_reader,
                    resolve_orderable_aggregate_target_slot,
                },
            },
            pipeline::contracts::{ExecutionInputs, ExecutionRuntimeAdapter, LoadExecutor},
            plan_metrics::record_rows_scanned,
            route::aggregate_extrema_direction,
            terminal::{RowDecoder, RowLayout},
        },
        index::IndexCompilePolicy,
        predicate::MissingRowPolicy,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::{StorageKey, Value},
};
use std::cmp::Ordering;

impl ExecutionKernel {
    // Reduce one materialized response into a field-target extrema id with the
    // deterministic tie-break contract `(field_value, primary_key_asc)`.
    pub(in crate::db::executor::aggregate) fn aggregate_field_extrema_from_materialized<E>(
        rows: Vec<DataRow>,
        kind: AggregateKind,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<ScalarAggregateOutput, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        if !kind.is_extrema() {
            return Err(crate::db::error::query_executor_invariant(
                "materialized field-extrema reduction requires MIN/MAX terminal",
            ));
        }
        let compare_direction = aggregate_extrema_direction(kind).ok_or_else(|| {
            crate::db::error::query_executor_invariant(
                "materialized field-extrema reduction reached non-extrema terminal",
            )
        })?;

        let row_layout = RowLayout::from_model(E::MODEL);
        let row_decoder = RowDecoder::structural();
        let mut selected: Option<(StorageKey, Value)> = None;
        for (data_key, raw_row) in rows {
            let candidate_key = data_key.storage_key();
            let row = row_decoder.decode(&row_layout, (data_key, raw_row))?;
            let candidate_value = extract_orderable_field_value_with_slot_reader(
                target_field,
                field_slot,
                &mut |index| row.slot(index),
            )
            .map_err(AggregateFieldValueError::into_internal_error)?;
            let should_replace = match selected.as_ref() {
                Some((current_key, current_value)) => {
                    let field_order = compare_orderable_field_values(
                        target_field,
                        &candidate_value,
                        current_value,
                    )
                    .map_err(AggregateFieldValueError::into_internal_error)?;
                    let directional_field_order =
                        apply_aggregate_direction(field_order, compare_direction);

                    directional_field_order == Ordering::Less
                        || (directional_field_order == Ordering::Equal
                            && candidate_key < *current_key)
                }
                None => true,
            };
            if should_replace {
                selected = Some((candidate_key, candidate_value));
            }
        }

        let selected_key = selected.map(|(key, _)| key);

        kind.extrema_output(selected_key).ok_or_else(|| {
            crate::db::error::query_executor_invariant(
                "materialized field-extrema reduction reached non-extrema terminal",
            )
        })
    }

    // Execute one route-eligible field-target extrema aggregate through kernel-
    // owned streaming setup, stream resolution, and fold orchestration.
    pub(in crate::db::executor::aggregate) fn execute_field_target_extrema_aggregate<E>(
        prepared: &PreparedAggregateStreamingInputs<'_, E>,
        kind: AggregateKind,
        target_field: &str,
        direction: Direction,
        route_plan: &crate::db::executor::ExecutionPlan,
    ) -> Result<ScalarAggregateOutput, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let field_fast_path_eligible = if kind == AggregateKind::Min {
            route_plan.field_min_fast_path_eligible()
        } else if kind == AggregateKind::Max {
            route_plan.field_max_fast_path_eligible()
        } else {
            return Err(crate::db::error::query_executor_invariant(
                "field-target aggregate execution requires MIN/MAX terminal",
            ));
        };
        if !field_fast_path_eligible {
            return Err(crate::db::error::query_executor_invariant(
                "field-target aggregate streaming requires route-eligible field-extrema fast path",
            ));
        }

        // Validate the field target before any stream execution work so
        // unsupported targets fail without scan-budget consumption.
        let field_slot = resolve_orderable_aggregate_target_slot::<E>(target_field)
            .map_err(AggregateFieldValueError::into_internal_error)?;

        // Reuse shared aggregate streaming setup and route-owned stream resolution.
        let consistency = prepared.consistency();
        let (probe_output, probe_rows_scanned) = Self::fold_field_target_extrema_for_route_plan(
            prepared,
            consistency,
            direction,
            route_plan,
            target_field,
            field_slot,
            kind,
        )?;
        if !Self::field_extrema_probe_may_be_inconclusive(
            consistency,
            kind,
            route_plan.aggregate_seek_fetch_hint(),
            &probe_output,
            probe_rows_scanned,
        ) {
            record_rows_scanned::<E>(probe_rows_scanned);
            return Ok(probe_output);
        }

        // Ignore + bounded field-extrema probe can under-fetch when leading
        // index entries are stale. Retry unbounded to preserve parity.
        let mut fallback_route_plan = route_plan.clone();
        fallback_route_plan.scan_hints.physical_fetch_hint = None;
        fallback_route_plan.index_range_limit_spec = None;
        fallback_route_plan.aggregate_seek_spec = None;
        let (fallback_output, fallback_rows_scanned) =
            Self::fold_field_target_extrema_for_route_plan(
                prepared,
                consistency,
                direction,
                &fallback_route_plan,
                target_field,
                field_slot,
                kind,
            )?;
        let total_rows_scanned = probe_rows_scanned.saturating_add(fallback_rows_scanned);
        record_rows_scanned::<E>(total_rows_scanned);

        Ok(fallback_output)
    }

    // Run one field-target extrema streaming attempt for one route plan and
    // return the aggregate output plus scan-accounting rows.
    fn fold_field_target_extrema_for_route_plan<E>(
        prepared: &PreparedAggregateStreamingInputs<'_, E>,
        consistency: MissingRowPolicy,
        direction: Direction,
        route_plan: &ExecutionPlan,
        target_field: &str,
        field_slot: FieldSlot,
        kind: AggregateKind,
    ) -> Result<(ScalarAggregateOutput, usize), InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let runtime = ExecutionRuntimeAdapter::new(&prepared.ctx, &prepared.logical_plan.access)?;
        let execution_preparation = ExecutionPreparation::from_plan(
            E::MODEL,
            &prepared.logical_plan,
            runtime.slot_map().map(<[usize]>::to_vec),
        );
        let execution_inputs = ExecutionInputs::new(
            &runtime,
            &prepared.logical_plan,
            AccessStreamBindings {
                index_prefix_specs: prepared.index_prefix_specs.as_slice(),
                index_range_specs: prepared.index_range_specs.as_slice(),
                continuation: AccessScanContinuationInput::new(None, direction),
            },
            &execution_preparation,
        );
        let mut resolved = Self::resolve_execution_key_stream(
            &execution_inputs,
            route_plan,
            IndexCompilePolicy::StrictAllOrNone,
        )?;
        let (aggregate_output, keys_scanned) = LoadExecutor::<E>::fold_streaming_field_extrema(
            &prepared.ctx,
            consistency,
            resolved.key_stream_mut(),
            target_field,
            field_slot,
            kind,
            direction,
        )?;
        let rows_scanned = resolved.rows_scanned_override().unwrap_or(keys_scanned);

        Ok((aggregate_output, rows_scanned))
    }

    // Ignore can skip stale leading index entries. If a bounded field-extrema
    // probe returns None exactly at the fetch boundary, the outcome is
    // inconclusive and must retry unbounded.
    const fn field_extrema_probe_may_be_inconclusive(
        consistency: MissingRowPolicy,
        kind: AggregateKind,
        probe_fetch_hint: Option<usize>,
        probe_output: &ScalarAggregateOutput,
        probe_rows_scanned: usize,
    ) -> bool {
        if !matches!(consistency, MissingRowPolicy::Ignore) {
            return false;
        }
        if !kind.is_extrema() {
            return false;
        }

        let Some(fetch) = probe_fetch_hint else {
            return false;
        };
        if fetch == 0 || probe_rows_scanned < fetch {
            return false;
        }

        kind.is_unresolved_extrema_output(probe_output)
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
    ) -> Result<(ScalarAggregateOutput, usize), InternalError> {
        if direction != Self::field_extrema_aggregate_direction(kind)? {
            return Err(crate::db::error::query_executor_invariant(
                "field-extrema fold direction must match aggregate terminal semantics",
            ));
        }

        let mut keys_scanned = 0usize;
        let mut selected: Option<(StorageKey, Value)> = None;
        let mut pre_key = || KeyStreamLoopControl::Emit;
        let mut on_key = |data_key: DataKey,
                          row: Option<crate::db::executor::terminal::page::KernelRow>|
         -> Result<KeyStreamLoopControl, InternalError> {
            keys_scanned = keys_scanned.saturating_add(1);
            let Some(row) = row else {
                return Ok(KeyStreamLoopControl::Emit);
            };
            let key = data_key.storage_key();
            let value = extract_orderable_field_value_with_slot_reader(
                target_field,
                field_slot,
                &mut |index| row.slot(index),
            )
            .map_err(AggregateFieldValueError::into_internal_error)?;
            let selected_was_empty = selected.is_none();
            let candidate_replaces = match selected.as_ref() {
                Some((current_key, current_value)) => {
                    let field_order =
                        compare_orderable_field_values(target_field, &value, current_value)
                            .map_err(AggregateFieldValueError::into_internal_error)?;
                    let directional_field_order = apply_aggregate_direction(field_order, direction);

                    directional_field_order == Ordering::Less
                        || (directional_field_order == Ordering::Equal && key < *current_key)
                }
                None => true,
            };
            if candidate_replaces {
                selected = Some((key, value));
                if selected_was_empty && matches!(kind, AggregateKind::Min) {
                    // MIN(field) under ascending index-leading traversal is resolved
                    // by the first in-window existing row.
                    return Ok(KeyStreamLoopControl::Stop);
                }

                return Ok(KeyStreamLoopControl::Emit);
            }

            let Some((_, current_value)) = selected.as_ref() else {
                return Ok(KeyStreamLoopControl::Emit);
            };
            let field_order = compare_orderable_field_values(target_field, &value, current_value)
                .map_err(AggregateFieldValueError::into_internal_error)?;
            let directional_field_order = apply_aggregate_direction(field_order, direction);

            // Once traversal leaves the winning field-value group, the ordered
            // stream cannot produce a better extrema candidate.
            if directional_field_order == Ordering::Greater {
                return Ok(KeyStreamLoopControl::Stop);
            }

            Ok(KeyStreamLoopControl::Emit)
        };
        Self::drive_field_row_stream(ctx, consistency, key_stream, &mut pre_key, &mut on_key)?;

        let selected_key = selected.map(|(key, _)| key);
        let output = kind.extrema_output(selected_key).ok_or_else(|| {
            crate::db::error::query_executor_invariant(
                "field-extrema fold reached non-extrema terminal",
            )
        })?;

        Ok((output, keys_scanned))
    }
}
