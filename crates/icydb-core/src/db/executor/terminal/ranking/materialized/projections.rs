//! Module: db::executor::terminal::ranking::materialized::projections
//! Responsibility: module-local ownership and contracts for db::executor::terminal::ranking::materialized::projections.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        data::DataRow,
        executor::{aggregate::field::FieldSlot, pipeline::contracts::LoadExecutor},
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
    value::Value,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Reduce one materialized response into a deterministic top-k response
    // ordered by `(field_value_desc, primary_key_asc)`.
    pub(in crate::db::executor::terminal::ranking) fn top_k_field_from_materialized(
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let ordered_rows =
            Self::top_k_ranked_rows_from_materialized(rows, target_field, field_slot, take_count)?;
        let output_rows = ordered_rows
            .into_iter()
            .map(|(row, _)| row)
            .collect::<Vec<_>>();

        EntityResponse::from_data_rows(output_rows)
    }

    // Reduce one materialized response into top-k projected field values under
    // deterministic `(field_value_desc, primary_key_asc)` ranking.
    pub(in crate::db::executor::terminal::ranking) fn top_k_field_values_from_materialized(
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, InternalError> {
        let ordered_rows =
            Self::top_k_ranked_rows_from_materialized(rows, target_field, field_slot, take_count)?;
        let projected_values = ordered_rows.into_iter().map(|(_, value)| value).collect();

        Ok(projected_values)
    }

    // Reduce one materialized response into top-k projected field values with
    // ids under deterministic `(field_value_desc, primary_key_asc)` ranking.
    pub(in crate::db::executor::terminal::ranking) fn top_k_field_values_with_ids_from_materialized(
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let ordered_rows =
            Self::top_k_ranked_rows_from_materialized(rows, target_field, field_slot, take_count)?;
        let projected_values = ordered_rows
            .into_iter()
            .map(|((data_key, _), value)| Ok((Id::from_key(data_key.try_key::<E>()?), value)))
            .collect::<Result<Vec<_>, InternalError>>()?;

        Ok(projected_values)
    }

    // Reduce one materialized response into a deterministic bottom-k response
    // ordered by `(field_value_asc, primary_key_asc)`.
    pub(in crate::db::executor::terminal::ranking) fn bottom_k_field_from_materialized(
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let ordered_rows = Self::bottom_k_ranked_rows_from_materialized(
            rows,
            target_field,
            field_slot,
            take_count,
        )?;
        let output_rows = ordered_rows
            .into_iter()
            .map(|(row, _)| row)
            .collect::<Vec<_>>();

        EntityResponse::from_data_rows(output_rows)
    }

    // Reduce one materialized response into bottom-k projected field values
    // under deterministic `(field_value_asc, primary_key_asc)` ranking.
    pub(in crate::db::executor::terminal::ranking) fn bottom_k_field_values_from_materialized(
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, InternalError> {
        let ordered_rows = Self::bottom_k_ranked_rows_from_materialized(
            rows,
            target_field,
            field_slot,
            take_count,
        )?;
        let projected_values = ordered_rows.into_iter().map(|(_, value)| value).collect();

        Ok(projected_values)
    }

    // Reduce one materialized response into bottom-k projected field values
    // with ids under deterministic `(field_value_asc, primary_key_asc)` ranking.
    pub(in crate::db::executor::terminal::ranking) fn bottom_k_field_values_with_ids_from_materialized(
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let ordered_rows = Self::bottom_k_ranked_rows_from_materialized(
            rows,
            target_field,
            field_slot,
            take_count,
        )?;
        let projected_values = ordered_rows
            .into_iter()
            .map(|((data_key, _), value)| Ok((Id::from_key(data_key.try_key::<E>()?), value)))
            .collect::<Result<Vec<_>, InternalError>>()?;

        Ok(projected_values)
    }
}
