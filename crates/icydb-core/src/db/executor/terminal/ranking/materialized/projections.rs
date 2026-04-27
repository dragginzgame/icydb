//! Module: db::executor::terminal::ranking::materialized::projections
//! Defines projection helpers for ranking over already materialized row data.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        PersistedRow,
        data::{DataKey, DataRow},
        executor::{
            aggregate::field::FieldSlot,
            pipeline::contracts::LoadExecutor,
            terminal::{RowLayout, decode_data_rows_into_entity_response},
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::EntityValue,
    value::Value,
};

impl<E> LoadExecutor<E>
where
    E: PersistedRow + EntityValue,
{
    // Reduce one materialized response into a deterministic top-k response
    // ordered by `(field_value_desc, primary_key_asc)`.
    pub(in crate::db::executor::terminal::ranking) fn top_k_field_from_materialized(
        row_layout: RowLayout,
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let ordered_rows = Self::top_k_ranked_rows_from_materialized(
            row_layout,
            rows,
            target_field,
            field_slot,
            take_count,
        )?;
        entity_response_from_ranked_rows(rows, ordered_rows)
    }

    // Reduce one materialized response into top-k projected field values under
    // deterministic `(field_value_desc, primary_key_asc)` ranking.
    pub(in crate::db::executor::terminal::ranking) fn top_k_field_values_from_materialized(
        row_layout: RowLayout,
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, InternalError> {
        let ordered_rows = Self::top_k_ranked_rows_from_materialized(
            row_layout,
            rows,
            target_field,
            field_slot,
            take_count,
        )?;
        Ok(field_values_from_ranked_rows(ordered_rows))
    }

    // Reduce one materialized response into top-k projected field values with
    // ids under deterministic `(field_value_desc, primary_key_asc)` ranking.
    pub(in crate::db::executor::terminal::ranking) fn top_k_field_values_with_ids_from_materialized(
        row_layout: RowLayout,
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(DataKey, Value)>, InternalError> {
        Ok(field_values_with_data_keys_from_ranked_rows(
            rows,
            Self::top_k_ranked_rows_from_materialized(
                row_layout,
                rows,
                target_field,
                field_slot,
                take_count,
            )?,
        ))
    }

    // Reduce one materialized response into a deterministic bottom-k response
    // ordered by `(field_value_asc, primary_key_asc)`.
    pub(in crate::db::executor::terminal::ranking) fn bottom_k_field_from_materialized(
        row_layout: RowLayout,
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let ordered_rows = Self::bottom_k_ranked_rows_from_materialized(
            row_layout,
            rows,
            target_field,
            field_slot,
            take_count,
        )?;
        entity_response_from_ranked_rows(rows, ordered_rows)
    }

    // Reduce one materialized response into bottom-k projected field values
    // under deterministic `(field_value_asc, primary_key_asc)` ranking.
    pub(in crate::db::executor::terminal::ranking) fn bottom_k_field_values_from_materialized(
        row_layout: RowLayout,
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, InternalError> {
        let ordered_rows = Self::bottom_k_ranked_rows_from_materialized(
            row_layout,
            rows,
            target_field,
            field_slot,
            take_count,
        )?;
        Ok(field_values_from_ranked_rows(ordered_rows))
    }

    // Reduce one materialized response into bottom-k projected field values
    // with ids under deterministic `(field_value_asc, primary_key_asc)` ranking.
    pub(in crate::db::executor::terminal::ranking) fn bottom_k_field_values_with_ids_from_materialized(
        row_layout: RowLayout,
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(DataKey, Value)>, InternalError> {
        Ok(field_values_with_data_keys_from_ranked_rows(
            rows,
            Self::bottom_k_ranked_rows_from_materialized(
                row_layout,
                rows,
                target_field,
                field_slot,
                take_count,
            )?,
        ))
    }
}

// Convert ranked row indices back into the entity response surface after the
// top-k/bottom-k policy has already selected row order.
fn entity_response_from_ranked_rows<E>(
    rows: &[DataRow],
    ordered_rows: Vec<(usize, Value)>,
) -> Result<EntityResponse<E>, InternalError>
where
    E: PersistedRow + EntityValue,
{
    let mut output_rows = Vec::with_capacity(ordered_rows.len());
    for (row_index, _) in ordered_rows {
        output_rows.push(rows[row_index].clone());
    }

    decode_data_rows_into_entity_response::<E>(output_rows)
}

// Drop row-index metadata once callers only need the ranked values.
fn field_values_from_ranked_rows(ordered_rows: Vec<(usize, Value)>) -> Vec<Value> {
    let mut projected_values = Vec::with_capacity(ordered_rows.len());
    for (_, value) in ordered_rows {
        projected_values.push(value);
    }

    projected_values
}

fn field_values_with_data_keys_from_ranked_rows(
    rows: &[DataRow],
    ordered_rows: Vec<(usize, Value)>,
) -> Vec<(DataKey, Value)> {
    let mut values_with_keys = Vec::with_capacity(ordered_rows.len());

    for (row_index, value) in ordered_rows {
        values_with_keys.push((rows[row_index].0.clone(), value));
    }

    values_with_keys
}
