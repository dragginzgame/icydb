//! Module: executor::aggregate::terminals
//! Responsibility: aggregate terminal API adapters over kernel aggregate execution.
//! Does not own: aggregate dispatch internals or fast-path eligibility derivation.
//! Boundary: user-facing aggregate terminal helpers on `LoadExecutor`.

use crate::{
    db::query::plan::FieldSlot as PlannedFieldSlot,
    db::{
        executor::{
            ExecutablePlan, ExecutionKernel,
            aggregate::{
                AggregateOutput, field::resolve_orderable_aggregate_target_slot_from_planner_slot,
            },
            load::LoadExecutor,
        },
        query::builder::aggregate::{count, exists, first, last, max, max_by, min, min_by},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute `count()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_count(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<u32, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(self, plan, count())? {
            AggregateOutput::Count(value) => Ok(value),
            _ => Err(invariant("aggregate COUNT result kind mismatch")),
        }
    }

    /// Execute `exists()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_exists(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<bool, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(self, plan, exists())? {
            AggregateOutput::Exists(value) => Ok(value),
            _ => Err(invariant("aggregate EXISTS result kind mismatch")),
        }
    }

    /// Execute `min()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_min(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(self, plan, min())? {
            AggregateOutput::Min(value) => Ok(value),
            _ => Err(invariant("aggregate MIN result kind mismatch")),
        }
    }

    /// Execute `max()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_max(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(self, plan, max())? {
            AggregateOutput::Max(value) => Ok(value),
            _ => Err(invariant("aggregate MAX result kind mismatch")),
        }
    }

    /// Execute `min(field)` over the effective aggregate window using one
    /// planner-resolved field slot.
    pub(in crate::db) fn aggregate_min_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Id<E>>, InternalError> {
        resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        match ExecutionKernel::execute_aggregate_spec(self, plan, min_by(target_field.field()))? {
            AggregateOutput::Min(value) => Ok(value),
            _ => Err(invariant("aggregate MIN(field) result kind mismatch")),
        }
    }

    /// Execute `max(field)` over the effective aggregate window using one
    /// planner-resolved field slot.
    pub(in crate::db) fn aggregate_max_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Id<E>>, InternalError> {
        resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        match ExecutionKernel::execute_aggregate_spec(self, plan, max_by(target_field.field()))? {
            AggregateOutput::Max(value) => Ok(value),
            _ => Err(invariant("aggregate MAX(field) result kind mismatch")),
        }
    }

    /// Execute `nth(field, n)` over the effective aggregate window using one
    /// planner-resolved field slot.
    pub(in crate::db) fn aggregate_nth_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        nth: usize,
    ) -> Result<Option<Id<E>>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(Self::map_aggregate_field_value_error)?;

        self.execute_nth_field_aggregate_with_slot(plan, target_field.field(), field_slot, nth)
    }

    /// Execute `median(field)` over the effective aggregate window using one
    /// planner-resolved field slot.
    pub(in crate::db) fn aggregate_median_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Id<E>>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(Self::map_aggregate_field_value_error)?;

        self.execute_median_field_aggregate_with_slot(plan, target_field.field(), field_slot)
    }

    #[expect(clippy::type_complexity)]
    /// Execute paired extrema `min_max(field)` over the effective aggregate
    /// window using one planner-resolved field slot.
    pub(in crate::db) fn aggregate_min_max_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<(Id<E>, Id<E>)>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(Self::map_aggregate_field_value_error)?;

        self.execute_min_max_field_aggregate_with_slot(plan, target_field.field(), field_slot)
    }

    /// Execute `first()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_first(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(self, plan, first())? {
            AggregateOutput::First(value) => Ok(value),
            _ => Err(invariant("aggregate FIRST result kind mismatch")),
        }
    }

    /// Execute `last()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_last(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(self, plan, last())? {
            AggregateOutput::Last(value) => Ok(value),
            _ => Err(invariant("aggregate LAST result kind mismatch")),
        }
    }
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
