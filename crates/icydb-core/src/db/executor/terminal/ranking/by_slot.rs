//! Module: db::executor::terminal::ranking::by_slot
//! Defines ranking helpers that read order values from slot-based row layouts.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        PersistedRow,
        executor::{
            ExecutablePlan,
            pipeline::contracts::LoadExecutor,
            terminal::ranking::{RankedFieldBoundaryDirection, RankedFieldBoundaryProjection},
        },
        query::plan::FieldSlot as PlannedFieldSlot,
        response::EntityResponse,
    },
    error::InternalError,
    traits::EntityValue,
    types::Id,
    value::Value,
};

impl<E> LoadExecutor<E>
where
    E: PersistedRow + EntityValue,
{
    /// Execute one `top_k_by(field, k)` terminal over materialized load rows
    /// using one planner-resolved field slot.
    pub(in crate::db) fn top_k_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        self.execute_ranked_field_terminal_boundary(
            plan.into_prepared_load_plan(),
            target_field,
            take_count,
            RankedFieldBoundaryDirection::Top,
            RankedFieldBoundaryProjection::Rows,
        )?
        .into_rows()
    }

    /// Execute one `bottom_k_by(field, k)` terminal over materialized load rows
    /// using one planner-resolved field slot.
    pub(in crate::db) fn bottom_k_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        self.execute_ranked_field_terminal_boundary(
            plan.into_prepared_load_plan(),
            target_field,
            take_count,
            RankedFieldBoundaryDirection::Bottom,
            RankedFieldBoundaryProjection::Rows,
        )?
        .into_rows()
    }

    /// Execute one `top_k_by_values(field, k)` terminal and return ranked values
    /// using one planner-resolved field slot.
    pub(in crate::db) fn top_k_by_values_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, InternalError> {
        self.execute_ranked_field_terminal_boundary(
            plan.into_prepared_load_plan(),
            target_field,
            take_count,
            RankedFieldBoundaryDirection::Top,
            RankedFieldBoundaryProjection::Values,
        )?
        .into_values()
    }

    /// Execute one `bottom_k_by_values(field, k)` terminal and return ranked
    /// values using one planner-resolved field slot.
    pub(in crate::db) fn bottom_k_by_values_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, InternalError> {
        self.execute_ranked_field_terminal_boundary(
            plan.into_prepared_load_plan(),
            target_field,
            take_count,
            RankedFieldBoundaryDirection::Bottom,
            RankedFieldBoundaryProjection::Values,
        )?
        .into_values()
    }

    /// Execute one `top_k_by_with_ids(field, k)` terminal and return `(id, value)`
    /// rows using one planner-resolved field slot.
    pub(in crate::db) fn top_k_by_with_ids_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        self.execute_ranked_field_terminal_boundary(
            plan.into_prepared_load_plan(),
            target_field,
            take_count,
            RankedFieldBoundaryDirection::Top,
            RankedFieldBoundaryProjection::ValuesWithIds,
        )?
        .into_values_with_ids()
    }

    /// Execute one `bottom_k_by_with_ids(field, k)` terminal and return
    /// `(id, value)` rows using one planner-resolved field slot.
    pub(in crate::db) fn bottom_k_by_with_ids_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        self.execute_ranked_field_terminal_boundary(
            plan.into_prepared_load_plan(),
            target_field,
            take_count,
            RankedFieldBoundaryDirection::Bottom,
            RankedFieldBoundaryProjection::ValuesWithIds,
        )?
        .into_values_with_ids()
    }
}
