//! Module: executor::aggregate::projection
//! Responsibility: field-value projection terminals over materialized responses.
//! Does not own: grouped key canonicalization internals or route planning logic.
//! Boundary: projection terminal helpers (`values`, `distinct_values`, `first/last value`).

use crate::{
    db::{
        data::DataKey,
        executor::{
            ExecutablePlan, ExecutionKernel,
            aggregate::field::{FieldSlot, extract_orderable_field_value},
            aggregate::{AggregateKind, AggregateOutput, AggregateSpec},
            group::{GroupKeySet, KeyCanonicalError},
            load::LoadExecutor,
        },
        response::Response,
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
    /// Execute `values_by(field)` over the effective response window.
    pub(in crate::db) fn values_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Vec<Value>, InternalError> {
        let target_field = target_field.into();

        self.execute_values_field_projection(plan, target_field.as_str())
    }

    /// Execute `distinct_values_by(field)` over the effective response window.
    pub(in crate::db) fn distinct_values_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Vec<Value>, InternalError> {
        let target_field = target_field.into();

        self.execute_distinct_values_field_projection(plan, target_field.as_str())
    }

    /// Execute `values_by_with_ids(field)` over the effective response window.
    pub(in crate::db) fn values_by_with_ids(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let target_field = target_field.into();

        self.execute_values_with_ids_field_projection(plan, target_field.as_str())
    }

    /// Execute `first_value_by(field)` using canonical FIRST terminal semantics.
    pub(in crate::db) fn first_value_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Option<Value>, InternalError> {
        let target_field = target_field.into();

        self.execute_terminal_value_field_projection(
            plan,
            target_field.as_str(),
            AggregateKind::First,
        )
    }

    /// Execute `last_value_by(field)` using canonical LAST terminal semantics.
    pub(in crate::db) fn last_value_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Option<Value>, InternalError> {
        let target_field = target_field.into();

        self.execute_terminal_value_field_projection(
            plan,
            target_field.as_str(),
            AggregateKind::Last,
        )
    }

    // Execute one field-target value projection (`values_by(field)`) via
    // canonical materialized fallback semantics.
    fn execute_values_field_projection(
        &self,
        plan: ExecutablePlan<E>,
        target_field: &str,
    ) -> Result<Vec<Value>, InternalError> {
        let field_slot = Self::resolve_any_field_slot(target_field)?;
        let response = self.execute(plan)?;

        Self::project_field_values_from_materialized(response, target_field, field_slot)
    }

    // Execute one field-target distinct value projection
    // (`distinct_values_by(field)`) via canonical materialized fallback semantics.
    fn execute_distinct_values_field_projection(
        &self,
        plan: ExecutablePlan<E>,
        target_field: &str,
    ) -> Result<Vec<Value>, InternalError> {
        let field_slot = Self::resolve_any_field_slot(target_field)?;
        let response = self.execute(plan)?;

        Self::project_distinct_field_values_from_materialized(response, target_field, field_slot)
    }

    // Execute one field-target id/value paired projection (`values_by_with_ids(field)`)
    // via canonical materialized fallback semantics.
    fn execute_values_with_ids_field_projection(
        &self,
        plan: ExecutablePlan<E>,
        target_field: &str,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let field_slot = Self::resolve_any_field_slot(target_field)?;
        let response = self.execute(plan)?;

        Self::project_field_values_with_ids_from_materialized(response, target_field, field_slot)
    }

    // Execute one field-target scalar terminal projection (`first_value_by` /
    // `last_value_by`) using route-owned first/last row selection semantics.
    fn execute_terminal_value_field_projection(
        &self,
        plan: ExecutablePlan<E>,
        target_field: &str,
        terminal_kind: AggregateKind,
    ) -> Result<Option<Value>, InternalError> {
        if !terminal_kind.supports_terminal_value_projection() {
            return Err(InternalError::query_executor_invariant(
                "terminal value projection requires FIRST/LAST aggregate kind",
            ));
        }

        let field_slot = Self::resolve_any_field_slot(target_field)?;
        let consistency = plan.as_inner().scalar_plan().consistency;
        let (AggregateOutput::First(selected_id) | AggregateOutput::Last(selected_id)) =
            ExecutionKernel::execute_aggregate_spec(
                self,
                plan,
                AggregateSpec::for_terminal(terminal_kind),
            )?
        else {
            return Err(InternalError::query_executor_invariant(
                "terminal value projection result kind mismatch",
            ));
        };
        let Some(selected_id) = selected_id else {
            return Ok(None);
        };

        let ctx = self.recovered_context()?;
        let key = DataKey::try_new::<E>(selected_id.key())?;
        let Some(entity) = Self::read_entity_for_field_extrema(&ctx, consistency, &key)? else {
            return Ok(None);
        };
        let value = extract_orderable_field_value(&entity, target_field, field_slot)
            .map_err(Self::map_aggregate_field_value_error)?;

        Ok(Some(value))
    }

    // Project one materialized response into one field value vector while
    // preserving the effective response row order.
    fn project_field_values_from_materialized(
        response: Response<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        let mut projected_values = Vec::new();
        for (_, entity) in response {
            let value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            projected_values.push(value);
        }

        Ok(projected_values)
    }

    // Project one materialized response into distinct field values while
    // preserving first-observed order within the effective response window.
    // This is value DISTINCT semantics via canonical `GroupKey` equality.
    fn project_distinct_field_values_from_materialized(
        response: Response<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        let mut distinct_values = GroupKeySet::default();
        let mut projected_values = Vec::new();
        for (_, entity) in response {
            let value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            if !distinct_values
                .insert_value(&value)
                .map_err(KeyCanonicalError::into_internal_error)?
            {
                continue;
            }
            projected_values.push(value);
        }

        Ok(projected_values)
    }

    // Project one materialized response into id/value pairs while preserving
    // the effective response row order.
    fn project_field_values_with_ids_from_materialized(
        response: Response<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let mut projected_values = Vec::new();
        for (id, entity) in response {
            let value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            projected_values.push((id, value));
        }

        Ok(projected_values)
    }
}
