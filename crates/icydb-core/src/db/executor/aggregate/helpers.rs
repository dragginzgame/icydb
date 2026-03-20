//! Module: executor::aggregate::helpers
//! Responsibility: helper terminals for ranked and projected field aggregates.
//! Does not own: core aggregate route planning or key-stream folding contracts.
//! Boundary: materialized helper projections used by aggregate terminal APIs.

use crate::{
    db::{
        data::{DataKey, DataRow},
        direction::Direction,
        executor::{
            KeyStreamLoopControl, OrderedKeyStream,
            aggregate::AggregateKind,
            aggregate::PreparedAggregateStreamingInputs,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, compare_orderable_field_values,
                extract_orderable_field_value_with_slot_reader,
            },
            drive_key_stream_with_control_flow,
            pipeline::contracts::LoadExecutor,
            read_data_row_with_consistency_from_store,
            route::aggregate_extrema_direction,
            terminal::{RowDecoder, RowLayout, page::KernelRow},
        },
        predicate::MissingRowPolicy,
        registry::StoreHandle,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
    value::Value,
};
use std::cmp::Ordering;

type MinMaxByIds<E> = Option<(Id<E>, Id<E>)>;

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Canonical precedence predicate for field projections under deterministic
    // field ordering with primary-key ascending tie-break.
    fn field_projection_candidate_precedes(
        target_field: &str,
        candidate_id: &Id<E>,
        candidate_value: &Value,
        current_id: &Id<E>,
        current_value: &Value,
        field_preference: Ordering,
    ) -> Result<bool, InternalError> {
        let field_order =
            compare_orderable_field_values(target_field, candidate_value, current_value)
                .map_err(Self::map_aggregate_field_value_error)?;
        if field_order == field_preference {
            return Ok(true);
        }

        Ok(field_order == Ordering::Equal && candidate_id.key() < current_id.key())
    }

    // Execute one field-target nth aggregate (`nth(field, n)`) via canonical
    // materialized fallback semantics using one planner-resolved field slot.
    pub(in crate::db::executor::aggregate) fn execute_nth_field_aggregate_with_slot(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
        field_slot: FieldSlot,
        nth: usize,
    ) -> Result<Option<Id<E>>, InternalError> {
        let page = self.execute_scalar_materialized_page_stage(prepared)?;
        let (rows, _) = page.into_parts();

        Self::aggregate_nth_field_from_materialized(rows, target_field, field_slot, nth)
    }

    // Execute one field-target median aggregate (`median(field)`) via
    // canonical materialized fallback semantics using one planner-resolved
    // field slot.
    pub(in crate::db::executor::aggregate) fn execute_median_field_aggregate_with_slot(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Option<Id<E>>, InternalError> {
        let page = self.execute_scalar_materialized_page_stage(prepared)?;
        let (rows, _) = page.into_parts();

        Self::aggregate_median_field_from_materialized(rows, target_field, field_slot)
    }

    // Execute one field-target paired extrema aggregate (`min_max(field)`)
    // via canonical materialized fallback semantics using one
    // planner-resolved field slot.
    pub(in crate::db::executor::aggregate) fn execute_min_max_field_aggregate_with_slot(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<MinMaxByIds<E>, InternalError> {
        let page = self.execute_scalar_materialized_page_stage(prepared)?;
        let (rows, _) = page.into_parts();

        Self::aggregate_min_max_field_from_materialized(rows, target_field, field_slot)
    }

    // Reduce one materialized response into `nth(field, n)` using deterministic
    // ordering `(field_value_asc, primary_key_asc)`.
    fn aggregate_nth_field_from_materialized(
        rows: Vec<DataRow>,
        target_field: &str,
        field_slot: FieldSlot,
        nth: usize,
    ) -> Result<Option<Id<E>>, InternalError> {
        let ordered_rows =
            Self::ordered_field_projection_from_materialized(rows, target_field, field_slot)?;

        // Phase 2: project the requested ordinal position.
        if nth >= ordered_rows.len() {
            return Ok(None);
        }

        Ok(ordered_rows.into_iter().nth(nth).map(|(id, _)| id))
    }

    // Reduce one materialized response into `median(field)` using deterministic
    // ordering `(field_value_asc, primary_key_asc)`.
    // Even-length windows select the lower median for type-agnostic stability.
    fn aggregate_median_field_from_materialized(
        rows: Vec<DataRow>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Option<Id<E>>, InternalError> {
        let ordered_rows =
            Self::ordered_field_projection_from_materialized(rows, target_field, field_slot)?;
        if ordered_rows.is_empty() {
            return Ok(None);
        }

        let median_index = if ordered_rows.len() % 2 == 0 {
            ordered_rows.len() / 2 - 1
        } else {
            ordered_rows.len() / 2
        };

        Ok(ordered_rows.into_iter().nth(median_index).map(|(id, _)| id))
    }

    // Reduce one materialized response into `(min_by(field), max_by(field))`
    // using one pass over the response window.
    fn aggregate_min_max_field_from_materialized(
        rows: Vec<DataRow>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<MinMaxByIds<E>, InternalError> {
        let mut min_candidate: Option<(Id<E>, Value)> = None;
        let mut max_candidate: Option<(Id<E>, Value)> = None;
        for (id, value) in Self::field_projection_from_materialized(rows, target_field, field_slot)?
        {
            let replace_min = match min_candidate.as_ref() {
                Some((current_id, current_value)) => Self::field_projection_candidate_precedes(
                    target_field,
                    &id,
                    &value,
                    current_id,
                    current_value,
                    Ordering::Less,
                )?,
                None => true,
            };
            if replace_min {
                min_candidate = Some((id, value.clone()));
            }

            let replace_max = match max_candidate.as_ref() {
                Some((current_id, current_value)) => Self::field_projection_candidate_precedes(
                    target_field,
                    &id,
                    &value,
                    current_id,
                    current_value,
                    Ordering::Greater,
                )?,
                None => true,
            };
            if replace_max {
                max_candidate = Some((id, value));
            }
        }

        let Some((min_id, _)) = min_candidate else {
            return Ok(None);
        };
        let Some((max_id, _)) = max_candidate else {
            return Err(crate::db::error::query_executor_invariant(
                "min_max(field) reduction produced a min id without a max id",
            ));
        };

        Ok(Some((min_id, max_id)))
    }

    // Project one response window into deterministic field ordering
    // `(field_value_asc, primary_key_asc)`.
    fn ordered_field_projection_from_materialized(
        rows: Vec<DataRow>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let mut ordered_rows: Vec<(Id<E>, Value)> = Vec::new();
        for (id, value) in Self::field_projection_from_materialized(rows, target_field, field_slot)?
        {
            let mut insert_index = ordered_rows.len();
            for (index, (current_id, current_value)) in ordered_rows.iter().enumerate() {
                let candidate_precedes = Self::field_projection_candidate_precedes(
                    target_field,
                    &id,
                    &value,
                    current_id,
                    current_value,
                    Ordering::Less,
                )?;
                if candidate_precedes {
                    insert_index = index;
                    break;
                }
            }

            ordered_rows.insert(insert_index, (id, value));
        }

        Ok(ordered_rows)
    }

    // Project materialized scalar rows into `(id, field_value)` pairs through
    // structural row decoding rather than full entity reconstruction.
    fn field_projection_from_materialized(
        rows: Vec<DataRow>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let row_layout = RowLayout::from_model(E::MODEL);
        let row_decoder = RowDecoder::structural();
        let mut projected = Vec::with_capacity(rows.len());

        for (data_key, raw_row) in rows {
            let id = Id::from_key(data_key.try_key::<E>()?);
            let kernel_row = row_decoder.decode(&row_layout, (data_key, raw_row))?;
            let value = extract_orderable_field_value_with_slot_reader(
                target_field,
                field_slot,
                &mut |index| kernel_row.slot(index),
            )
            .map_err(AggregateFieldValueError::into_internal_error)?;
            projected.push((id, value));
        }

        Ok(projected)
    }

    // Load one structural row for field aggregates while preserving read
    // consistency classification behavior.
    pub(in crate::db::executor) fn read_kernel_row_for_field_aggregate(
        store: StoreHandle,
        row_layout: &RowLayout,
        row_decoder: RowDecoder,
        consistency: MissingRowPolicy,
        key: &DataKey,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(row) = read_data_row_with_consistency_from_store(store, key, consistency)? else {
            return Ok(None);
        };

        row_decoder.decode(row_layout, row).map(Some)
    }

    // Load one projected field value from one persisted row while preserving
    // read consistency classification behavior at the outer aggregate edge.
    pub(in crate::db::executor) fn read_field_value_for_aggregate(
        store: StoreHandle,
        consistency: MissingRowPolicy,
        key: &DataKey,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Option<Value>, InternalError> {
        let row_layout = RowLayout::from_model(E::MODEL);
        let row_decoder = RowDecoder::structural();
        let Some(row) = Self::read_kernel_row_for_field_aggregate(
            store,
            &row_layout,
            row_decoder,
            consistency,
            key,
        )?
        else {
            return Ok(None);
        };
        let value = extract_orderable_field_value_with_slot_reader(
            target_field,
            field_slot,
            &mut |index| row.slot(index),
        )
        .map_err(Self::map_aggregate_field_value_error)?;

        Ok(Some(value))
    }

    // Drive one canonical key stream and decode rows with field-aggregate read
    // consistency contracts while delegating row-level behavior to callbacks.
    // This keeps stream control-flow ownership in one helper so aggregate
    // terminals do not duplicate key-stream/read scaffolding.
    pub(in crate::db::executor) fn drive_field_row_stream(
        store: StoreHandle,
        consistency: MissingRowPolicy,
        key_stream: &mut dyn OrderedKeyStream,
        pre_key: &mut dyn FnMut() -> KeyStreamLoopControl,
        on_key: &mut dyn FnMut(
            DataKey,
            Option<KernelRow>,
        ) -> Result<KeyStreamLoopControl, InternalError>,
    ) -> Result<(), InternalError> {
        let row_layout = RowLayout::from_model(E::MODEL);
        let row_decoder = RowDecoder::structural();

        drive_key_stream_with_control_flow(key_stream, &mut || pre_key(), &mut |data_key| {
            let row = Self::read_kernel_row_for_field_aggregate(
                store,
                &row_layout,
                row_decoder,
                consistency,
                &data_key,
            )?;

            on_key(data_key, row)
        })
    }

    pub(in crate::db::executor) fn field_extrema_aggregate_direction(
        kind: AggregateKind,
    ) -> Result<Direction, InternalError> {
        aggregate_extrema_direction(kind).ok_or_else(|| {
            crate::db::error::query_executor_invariant(
                "field-target aggregate direction requires MIN/MAX terminal",
            )
        })
    }

    // Adapter so aggregate submodules keep one internal mapping entrypoint while
    // taxonomy mapping ownership remains centralized in aggregate field semantics.
    pub(in crate::db::executor::aggregate) fn map_aggregate_field_value_error(
        err: AggregateFieldValueError,
    ) -> InternalError {
        err.into_internal_error()
    }
}
