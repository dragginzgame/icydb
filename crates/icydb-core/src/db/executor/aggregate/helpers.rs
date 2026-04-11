//! Module: executor::aggregate::helpers
//! Responsibility: helper terminals for ranked and projected field aggregates.
//! Does not own: core aggregate route planning or key-stream folding contracts.
//! Boundary: materialized helper projections used by aggregate terminal APIs.

use crate::{
    db::{
        data::{DataKey, DataRow},
        executor::{
            aggregate::PreparedAggregateStreamingInputs,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, compare_orderable_field_values,
                extract_orderable_field_value_from_decoded_slot,
            },
            pipeline::contracts::LoadExecutor,
            read_data_row_with_consistency_from_store,
            terminal::{RowDecoder, RowLayout, page::KernelRow},
        },
        predicate::MissingRowPolicy,
        registry::StoreHandle,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::{StorageKey, Value},
};
use std::cmp::Ordering;

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Canonical precedence predicate for field projections under deterministic
    // field ordering with primary-key ascending tie-break.
    fn field_projection_candidate_precedes(
        target_field: &str,
        candidate_key: &StorageKey,
        candidate_value: &Value,
        current_key: &StorageKey,
        current_value: &Value,
        field_preference: Ordering,
    ) -> Result<bool, InternalError> {
        let field_order =
            compare_orderable_field_values(target_field, candidate_value, current_value)
                .map_err(AggregateFieldValueError::into_internal_error)?;
        if field_order == field_preference {
            return Ok(true);
        }

        Ok(field_order == Ordering::Equal && candidate_key < current_key)
    }

    // Execute one field-target nth aggregate (`nth(field, n)`) via canonical
    // materialized fallback semantics using one planner-resolved field slot.
    pub(in crate::db::executor::aggregate) fn execute_nth_field_aggregate_with_slot(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
        field_slot: FieldSlot,
        nth: usize,
    ) -> Result<Option<StorageKey>, InternalError> {
        let row_layout = prepared.authority.row_layout();
        let page = self.execute_scalar_materialized_page_stage(prepared)?;
        let (rows, _) = page.into_parts();

        Self::aggregate_nth_field_from_materialized(
            rows,
            &row_layout,
            target_field,
            field_slot,
            nth,
        )
    }

    // Execute one field-target median aggregate (`median(field)`) via
    // canonical materialized fallback semantics using one planner-resolved
    // field slot.
    pub(in crate::db::executor::aggregate) fn execute_median_field_aggregate_with_slot(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Option<StorageKey>, InternalError> {
        let row_layout = prepared.authority.row_layout();
        let page = self.execute_scalar_materialized_page_stage(prepared)?;
        let (rows, _) = page.into_parts();

        Self::aggregate_median_field_from_materialized(rows, &row_layout, target_field, field_slot)
    }

    // Execute one field-target paired extrema aggregate (`min_max(field)`)
    // via canonical materialized fallback semantics using one
    // planner-resolved field slot.
    pub(in crate::db::executor::aggregate) fn execute_min_max_field_aggregate_with_slot(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Option<(StorageKey, StorageKey)>, InternalError> {
        let row_layout = prepared.authority.row_layout();
        let page = self.execute_scalar_materialized_page_stage(prepared)?;
        let (rows, _) = page.into_parts();

        Self::aggregate_min_max_field_from_materialized(rows, &row_layout, target_field, field_slot)
    }

    // Reduce one materialized response into `nth(field, n)` using deterministic
    // ordering `(field_value_asc, primary_key_asc)`.
    fn aggregate_nth_field_from_materialized(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
        nth: usize,
    ) -> Result<Option<StorageKey>, InternalError> {
        let ordered_rows = Self::ordered_field_projection_from_materialized(
            rows,
            row_layout,
            target_field,
            field_slot,
        )?;

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
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Option<StorageKey>, InternalError> {
        let ordered_rows = Self::ordered_field_projection_from_materialized(
            rows,
            row_layout,
            target_field,
            field_slot,
        )?;
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
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Option<(StorageKey, StorageKey)>, InternalError> {
        let mut min_candidate: Option<(StorageKey, Value)> = None;
        let mut max_candidate: Option<(StorageKey, Value)> = None;
        for (key, value) in
            Self::field_projection_from_materialized(rows, row_layout, target_field, field_slot)?
        {
            let replace_min = match min_candidate.as_ref() {
                Some((current_key, current_value)) => Self::field_projection_candidate_precedes(
                    target_field,
                    &key,
                    &value,
                    current_key,
                    current_value,
                    Ordering::Less,
                )?,
                None => true,
            };
            if replace_min {
                min_candidate = Some((key, value.clone()));
            }

            let replace_max = match max_candidate.as_ref() {
                Some((current_key, current_value)) => Self::field_projection_candidate_precedes(
                    target_field,
                    &key,
                    &value,
                    current_key,
                    current_value,
                    Ordering::Greater,
                )?,
                None => true,
            };
            if replace_max {
                max_candidate = Some((key, value));
            }
        }

        let Some((min_key, _)) = min_candidate else {
            return Ok(None);
        };
        let Some((max_key, _)) = max_candidate else {
            return Err(InternalError::query_executor_invariant(
                "min_max(field) reduction produced a min id without a max id",
            ));
        };

        Ok(Some((min_key, max_key)))
    }

    // Project one response window into deterministic field ordering
    // `(field_value_asc, primary_key_asc)`.
    fn ordered_field_projection_from_materialized(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<(StorageKey, Value)>, InternalError> {
        let mut ordered_rows: Vec<(StorageKey, Value)> = Vec::new();
        for (key, value) in
            Self::field_projection_from_materialized(rows, row_layout, target_field, field_slot)?
        {
            let mut insert_index = ordered_rows.len();
            for (index, (current_key, current_value)) in ordered_rows.iter().enumerate() {
                let candidate_precedes = Self::field_projection_candidate_precedes(
                    target_field,
                    &key,
                    &value,
                    current_key,
                    current_value,
                    Ordering::Less,
                )?;
                if candidate_precedes {
                    insert_index = index;
                    break;
                }
            }

            ordered_rows.insert(insert_index, (key, value));
        }

        Ok(ordered_rows)
    }

    // Project materialized scalar rows into `(id, field_value)` pairs through
    // structural row decoding rather than full entity reconstruction.
    fn field_projection_from_materialized(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<(StorageKey, Value)>, InternalError> {
        let mut projected = Vec::with_capacity(rows.len());

        for (data_key, raw_row) in rows {
            let storage_key = data_key.storage_key();
            let value = RowDecoder::decode_required_slot_value(
                row_layout,
                storage_key,
                &raw_row,
                field_slot.index,
            )?;
            let value =
                extract_orderable_field_value_from_decoded_slot(target_field, field_slot, value)
                    .map_err(AggregateFieldValueError::into_internal_error)?;
            projected.push((storage_key, value));
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
        row_layout: &RowLayout,
        consistency: MissingRowPolicy,
        key: &DataKey,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Option<Value>, InternalError> {
        let Some(row) = read_data_row_with_consistency_from_store(store, key, consistency)? else {
            return Ok(None);
        };
        let value = RowDecoder::decode_required_slot_value(
            row_layout,
            key.storage_key(),
            &row.1,
            field_slot.index,
        )?;
        let value =
            extract_orderable_field_value_from_decoded_slot(target_field, field_slot, value)
                .map_err(AggregateFieldValueError::into_internal_error)?;

        Ok(Some(value))
    }
}
