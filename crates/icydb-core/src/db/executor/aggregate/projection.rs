//! Module: executor::aggregate::projection
//! Responsibility: field-value projection terminals over materialized responses.
//! Does not own: grouped key canonicalization internals or route planning logic.
//! Boundary: projection terminal helpers (`values`, `distinct_values`, `first/last value`).
//!
//! `distinct_values_by(field)` here is a non-grouped effective-window helper.
//! Grouped Class B DISTINCT accounting is enforced only through grouped
//! execution context boundaries.

use crate::{
    db::{
        data::DataKey,
        executor::{
            ExecutablePlan, ExecutionKernel,
            aggregate::field::{
                FieldSlot, extract_orderable_field_value,
                resolve_any_aggregate_target_slot_from_planner_slot,
            },
            aggregate::materialized_distinct::insert_materialized_distinct_value,
            aggregate::{AggregateKind, AggregateOutput},
            group::GroupKeySet,
            load::LoadExecutor,
        },
        query::builder::{
            AggregateExpr,
            aggregate::{count, exists, first, last, max, min},
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
    /// Execute `values_by(field)` over the effective response window using one
    /// planner-resolved field slot.
    pub(in crate::db) fn values_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        let response = self.execute(plan)?;

        Self::project_field_values_from_materialized(response, target_field.field(), field_slot)
    }

    /// Execute `distinct_values_by(field)` over the effective response window
    /// using one planner-resolved field slot.
    pub(in crate::db) fn distinct_values_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        let response = self.execute(plan)?;

        Self::project_distinct_field_values_from_materialized(
            response,
            target_field.field(),
            field_slot,
        )
    }

    /// Execute `values_by_with_ids(field)` over the effective response window
    /// using one planner-resolved field slot.
    pub(in crate::db) fn values_by_with_ids_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        let response = self.execute(plan)?;

        Self::project_field_values_with_ids_from_materialized(
            response,
            target_field.field(),
            field_slot,
        )
    }

    /// Execute `first_value_by(field)` using one planner-resolved field slot.
    pub(in crate::db) fn first_value_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Value>, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;

        self.execute_terminal_value_field_projection_with_slot(
            plan,
            target_field.field(),
            field_slot,
            AggregateKind::First,
        )
    }

    /// Execute `last_value_by(field)` using one planner-resolved field slot.
    pub(in crate::db) fn last_value_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Value>, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;

        self.execute_terminal_value_field_projection_with_slot(
            plan,
            target_field.field(),
            field_slot,
            AggregateKind::Last,
        )
    }

    // Execute one field-target scalar terminal projection (`first_value_by` /
    // `last_value_by`) using a planner-validated slot and route-owned
    // first/last row selection semantics.
    fn execute_terminal_value_field_projection_with_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: &str,
        field_slot: FieldSlot,
        terminal_kind: AggregateKind,
    ) -> Result<Option<Value>, InternalError> {
        if !terminal_kind.supports_terminal_value_projection() {
            return Err(invariant(
                "terminal value projection requires FIRST/LAST aggregate kind",
            ));
        }

        let consistency = plan.consistency();
        let (AggregateOutput::First(selected_id) | AggregateOutput::Last(selected_id)) =
            ExecutionKernel::execute_aggregate_spec(
                self,
                plan,
                terminal_aggregate_expr(terminal_kind),
            )?
        else {
            return Err(invariant("terminal value projection result kind mismatch"));
        };
        let Some(selected_id) = selected_id else {
            return Ok(None);
        };

        let ctx = self.recovered_context()?;
        let key = DataKey::try_new::<E>(selected_id.key())?;
        let Some(entity) = Self::read_entity_for_field_extrema(&ctx, consistency, &key)? else {
            return Ok(None);
        };
        extract_orderable_field_value(&entity, target_field, field_slot)
            .map_err(Self::map_aggregate_field_value_error)
            .map(Some)
    }

    // Project one materialized response into one field value vector while
    // preserving the effective response row order.
    fn project_field_values_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        let mut projected_values = Vec::new();
        for row in response {
            let value = extract_orderable_field_value(row.entity_ref(), target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            projected_values.push(value);
        }

        Ok(projected_values)
    }

    // Project one materialized response into distinct field values while
    // preserving first-observed order within the effective response window.
    // This is value DISTINCT semantics via canonical `GroupKey` equality.
    fn project_distinct_field_values_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        let mut distinct_values = GroupKeySet::default();
        let mut projected_values = Vec::new();
        for row in response {
            let value = extract_orderable_field_value(row.entity_ref(), target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            if !insert_materialized_distinct_value(&mut distinct_values, &value)? {
                continue;
            }
            projected_values.push(value);
        }

        Ok(projected_values)
    }

    // Project one materialized response into id/value pairs while preserving
    // the effective response row order.
    fn project_field_values_with_ids_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let mut projected_values = Vec::new();
        for row in response {
            let (id, entity) = row.into_parts();
            let value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            projected_values.push((id, value));
        }

        Ok(projected_values)
    }
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}

fn terminal_aggregate_expr(kind: AggregateKind) -> AggregateExpr {
    match kind {
        AggregateKind::Count => count(),
        AggregateKind::Sum => {
            unreachable!("terminal aggregate expression helper must not be used for SUM(field)")
        }
        AggregateKind::Exists => exists(),
        AggregateKind::Min => min(),
        AggregateKind::Max => max(),
        AggregateKind::First => first(),
        AggregateKind::Last => last(),
    }
}
