//! Module: db::executor::terminal::ranking::by_slot
//! Responsibility: module-local ownership and contracts for db::executor::terminal::ranking::by_slot.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::{
            ExecutablePlan, pipeline::contracts::LoadExecutor,
            terminal::ranking::RankingTerminalBoundaryRequest,
        },
        query::plan::FieldSlot as PlannedFieldSlot,
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
    /// Execute one `top_k_by(field, k)` terminal over materialized load rows
    /// using one planner-resolved field slot.
    pub(in crate::db) fn top_k_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        self.execute_ranking_terminal_boundary(
            plan,
            RankingTerminalBoundaryRequest::TopKRows {
                target_field,
                take_count,
            },
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
        self.execute_ranking_terminal_boundary(
            plan,
            RankingTerminalBoundaryRequest::BottomKRows {
                target_field,
                take_count,
            },
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
        self.execute_ranking_terminal_boundary(
            plan,
            RankingTerminalBoundaryRequest::TopKValues {
                target_field,
                take_count,
            },
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
        self.execute_ranking_terminal_boundary(
            plan,
            RankingTerminalBoundaryRequest::BottomKValues {
                target_field,
                take_count,
            },
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
        self.execute_ranking_terminal_boundary(
            plan,
            RankingTerminalBoundaryRequest::TopKValuesWithIds {
                target_field,
                take_count,
            },
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
        self.execute_ranking_terminal_boundary(
            plan,
            RankingTerminalBoundaryRequest::BottomKValuesWithIds {
                target_field,
                take_count,
            },
        )?
        .into_values_with_ids()
    }
}
