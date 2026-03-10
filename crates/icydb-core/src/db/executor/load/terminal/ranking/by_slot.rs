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
}
