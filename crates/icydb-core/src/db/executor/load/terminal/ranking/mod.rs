//! Module: executor::load::terminal::ranking
//! Responsibility: ranking terminal selection (`min/max` and `*_by`) for load execution.
//! Does not own: planner aggregate semantics or projection-expression evaluation.
//! Boundary: consumes planned slots and returns entity response terminals.

mod materialized;

use crate::{
    db::{
        executor::{
            ExecutablePlan,
            aggregate::field::{
                AggregateFieldValueError, resolve_orderable_aggregate_target_slot_from_planner_slot,
            },
            load::LoadExecutor,
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
    /// Execute one `take(k)` terminal over the canonical load response.
    pub(in crate::db) fn take(
        &self,
        plan: ExecutablePlan<E>,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        self.execute_take_terminal(plan, take_count)
    }

    /// Execute one `top_k_by(field, k)` terminal over materialized load rows
    /// using one planner-resolved field slot.
    pub(in crate::db) fn top_k_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(AggregateFieldValueError::into_internal_error)?;
        let response = self.execute(plan)?;

        Self::top_k_field_from_materialized(response, target_field.field(), field_slot, take_count)
    }

    /// Execute one `bottom_k_by(field, k)` terminal over materialized load rows
    /// using one planner-resolved field slot.
    pub(in crate::db) fn bottom_k_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(AggregateFieldValueError::into_internal_error)?;
        let response = self.execute(plan)?;

        Self::bottom_k_field_from_materialized(
            response,
            target_field.field(),
            field_slot,
            take_count,
        )
    }

    /// Execute one `top_k_by_values(field, k)` terminal and return ranked values
    /// using one planner-resolved field slot.
    pub(in crate::db) fn top_k_by_values_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(AggregateFieldValueError::into_internal_error)?;
        let response = self.execute(plan)?;

        Self::top_k_field_values_from_materialized(
            response,
            target_field.field(),
            field_slot,
            take_count,
        )
    }

    /// Execute one `bottom_k_by_values(field, k)` terminal and return ranked
    /// values using one planner-resolved field slot.
    pub(in crate::db) fn bottom_k_by_values_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(AggregateFieldValueError::into_internal_error)?;
        let response = self.execute(plan)?;

        Self::bottom_k_field_values_from_materialized(
            response,
            target_field.field(),
            field_slot,
            take_count,
        )
    }

    /// Execute one `top_k_by_with_ids(field, k)` terminal and return `(id, value)`
    /// rows using one planner-resolved field slot.
    pub(in crate::db) fn top_k_by_with_ids_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(AggregateFieldValueError::into_internal_error)?;
        let response = self.execute(plan)?;

        Self::top_k_field_values_with_ids_from_materialized(
            response,
            target_field.field(),
            field_slot,
            take_count,
        )
    }

    /// Execute one `bottom_k_by_with_ids(field, k)` terminal and return
    /// `(id, value)` rows using one planner-resolved field slot.
    pub(in crate::db) fn bottom_k_by_with_ids_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(AggregateFieldValueError::into_internal_error)?;
        let response = self.execute(plan)?;

        Self::bottom_k_field_values_with_ids_from_materialized(
            response,
            target_field.field(),
            field_slot,
            take_count,
        )
    }

    // Execute one row-terminal take (`take(k)`) via canonical materialized
    // response semantics.
    fn execute_take_terminal(
        &self,
        plan: ExecutablePlan<E>,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let response = self.execute(plan)?;
        let mut rows = response.rows();
        let take_len = usize::try_from(take_count).unwrap_or(usize::MAX);
        if rows.len() > take_len {
            rows.truncate(take_len);
        }

        Ok(EntityResponse::new(rows))
    }
}
