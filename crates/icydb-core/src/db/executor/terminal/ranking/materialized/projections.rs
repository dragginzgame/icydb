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
use std::cmp::Reverse;

impl<E> LoadExecutor<E>
where
    E: PersistedRow + EntityValue,
{
    // Reduce one materialized response into a deterministic top-k response
    // ordered by `(field_value_desc, primary_key_asc)`.
    pub(in crate::db::executor::terminal::ranking) fn top_k_field_from_materialized(
        row_layout: RowLayout,
        rows: Vec<DataRow>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let ordered_rows = Self::top_k_ranked_rows_from_materialized(
            row_layout.clone(),
            &rows,
            target_field,
            field_slot,
            take_count,
        )?;
        entity_response_from_ranked_rows(&row_layout, rows, ordered_rows)
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
        Self::top_k_ranked_values_from_materialized(
            row_layout,
            rows,
            target_field,
            field_slot,
            take_count,
        )
    }

    // Reduce one materialized response into top-k projected field values with
    // ids under deterministic `(field_value_desc, primary_key_asc)` ranking.
    pub(in crate::db::executor::terminal::ranking) fn top_k_field_values_with_ids_from_materialized(
        row_layout: RowLayout,
        rows: Vec<DataRow>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(DataKey, Value)>, InternalError> {
        let ordered_rows = Self::top_k_ranked_rows_from_materialized(
            row_layout,
            &rows,
            target_field,
            field_slot,
            take_count,
        )?;
        field_values_with_data_keys_from_ranked_rows(rows, ordered_rows)
    }

    // Reduce one materialized response into a deterministic bottom-k response
    // ordered by `(field_value_asc, primary_key_asc)`.
    pub(in crate::db::executor::terminal::ranking) fn bottom_k_field_from_materialized(
        row_layout: RowLayout,
        rows: Vec<DataRow>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let ordered_rows = Self::bottom_k_ranked_rows_from_materialized(
            row_layout.clone(),
            &rows,
            target_field,
            field_slot,
            take_count,
        )?;
        entity_response_from_ranked_rows(&row_layout, rows, ordered_rows)
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
        Self::bottom_k_ranked_values_from_materialized(
            row_layout,
            rows,
            target_field,
            field_slot,
            take_count,
        )
    }

    // Reduce one materialized response into bottom-k projected field values
    // with ids under deterministic `(field_value_asc, primary_key_asc)` ranking.
    pub(in crate::db::executor::terminal::ranking) fn bottom_k_field_values_with_ids_from_materialized(
        row_layout: RowLayout,
        rows: Vec<DataRow>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(DataKey, Value)>, InternalError> {
        let ordered_rows = Self::bottom_k_ranked_rows_from_materialized(
            row_layout,
            &rows,
            target_field,
            field_slot,
            take_count,
        )?;
        field_values_with_data_keys_from_ranked_rows(rows, ordered_rows)
    }
}

// Convert ranked row indices back into the entity response surface after the
// top-k/bottom-k policy has already selected row order.
fn entity_response_from_ranked_rows<E>(
    row_layout: &RowLayout,
    rows: Vec<DataRow>,
    ordered_rows: Vec<(usize, Value)>,
) -> Result<EntityResponse<E>, InternalError>
where
    E: PersistedRow + EntityValue,
{
    let output_rows = move_selected_ranked_rows(
        rows,
        ordered_rows,
        "ranked row terminal selected an invalid materialized row index",
    )?
    .into_iter()
    .map(|(row, _)| row)
    .collect();

    decode_data_rows_into_entity_response::<E>(row_layout, output_rows)
}

fn field_values_with_data_keys_from_ranked_rows(
    rows: Vec<DataRow>,
    ordered_rows: Vec<(usize, Value)>,
) -> Result<Vec<(DataKey, Value)>, InternalError> {
    move_selected_ranked_rows(
        rows,
        ordered_rows,
        "ranked values-with-ids terminal selected an invalid materialized row index",
    )
    .map(|rows| {
        rows.into_iter()
            .map(|((data_key, _raw_row), value)| (data_key, value))
            .collect()
    })
}

// Move ranked winners out of the materialized response without wrapping every
// source row in `Option`. Selected row indices are removed in descending source
// order so earlier `swap_remove` calls cannot shift the remaining selected
// positions.
fn move_selected_ranked_rows(
    mut rows: Vec<DataRow>,
    ordered_rows: Vec<(usize, Value)>,
    invalid_index_message: &'static str,
) -> Result<Vec<(DataRow, Value)>, InternalError> {
    let mut selected_indices = Vec::with_capacity(ordered_rows.len());
    for (output_index, (row_index, value)) in ordered_rows.into_iter().enumerate() {
        selected_indices.push((row_index, output_index, value));
    }
    selected_indices.sort_unstable_by_key(|(row_index, _, _)| Reverse(*row_index));

    let mut output_rows = Vec::with_capacity(selected_indices.len());
    output_rows.resize_with(selected_indices.len(), || None);
    let mut previous_row_index = None;
    for (row_index, output_index, value) in selected_indices {
        if previous_row_index == Some(row_index) || row_index >= rows.len() {
            return Err(InternalError::query_executor_invariant(
                invalid_index_message,
            ));
        }
        previous_row_index = Some(row_index);

        output_rows[output_index] = Some((rows.swap_remove(row_index), value));
    }

    let mut ranked_rows = Vec::with_capacity(output_rows.len());
    for row in output_rows {
        let row =
            row.ok_or_else(|| InternalError::query_executor_invariant(invalid_index_message))?;
        ranked_rows.push(row);
    }

    Ok(ranked_rows)
}
