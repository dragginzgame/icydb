//! Module: executor::load::terminal::ranking
//! Responsibility: ranking terminal selection (`min/max` and `*_by`) for load execution.
//! Does not own: planner aggregate semantics or projection-expression evaluation.
//! Boundary: consumes planned slots and returns entity response terminals.

use crate::{
    db::{
        executor::{
            ExecutablePlan,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, compare_orderable_field_values,
                extract_orderable_field_value,
                resolve_orderable_aggregate_target_slot_from_planner_slot,
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
use std::cmp::Ordering;

// Field ranking direction for k-selection terminals.
#[derive(Clone, Copy)]
enum RankedFieldDirection {
    Descending,
    Ascending,
}

impl RankedFieldDirection {
    // Determine whether the candidate value outranks the current value under
    // the selected direction contract.
    const fn candidate_precedes(self, candidate_vs_current: Ordering) -> bool {
        match self {
            Self::Descending => matches!(candidate_vs_current, Ordering::Greater),
            Self::Ascending => matches!(candidate_vs_current, Ordering::Less),
        }
    }
}

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

    // Reduce one materialized response into deterministic top-k ranked rows
    // ordered by `(field_value_desc, primary_key_asc)`.
    fn top_k_ranked_rows_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, E, Value)>, InternalError> {
        Self::rank_k_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
            RankedFieldDirection::Descending,
        )
    }

    // Reduce one materialized response into deterministic bottom-k ranked rows
    // ordered by `(field_value_asc, primary_key_asc)`.
    fn bottom_k_ranked_rows_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, E, Value)>, InternalError> {
        Self::rank_k_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
            RankedFieldDirection::Ascending,
        )
    }

    // Shared ranked-row helper for all top/bottom k terminal families.
    // Memory contract:
    // - Ranking is applied to the materialized effective response window only.
    // - Memory growth is bounded by the effective execute() response size.
    // - No streaming heap optimization is used in 0.29 by design.
    fn rank_k_rows_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
        direction: RankedFieldDirection,
    ) -> Result<Vec<(Id<E>, E, Value)>, InternalError> {
        let mut ordered_rows: Vec<(Id<E>, E, Value)> = Vec::new();
        for row in response {
            let (id, entity) = row.into_parts();
            let value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(AggregateFieldValueError::into_internal_error)?;
            let mut insert_index = ordered_rows.len();
            for (index, (current_id, _, current_value)) in ordered_rows.iter().enumerate() {
                let ordering = compare_orderable_field_values(target_field, &value, current_value)
                    .map_err(AggregateFieldValueError::into_internal_error)?;
                let outranks_current = direction.candidate_precedes(ordering);
                let tie_breaks_by_pk = ordering == Ordering::Equal && id.key() < current_id.key();
                if outranks_current || tie_breaks_by_pk {
                    insert_index = index;
                    break;
                }
            }
            ordered_rows.insert(insert_index, (id, entity, value));
        }
        let take_len = usize::try_from(take_count).unwrap_or(usize::MAX);
        if ordered_rows.len() > take_len {
            ordered_rows.truncate(take_len);
        }

        Ok(ordered_rows)
    }

    // Reduce one materialized response into a deterministic top-k response
    // ordered by `(field_value_desc, primary_key_asc)`.
    fn top_k_field_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let ordered_rows = Self::top_k_ranked_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
        )?;
        let output_rows = ordered_rows
            .into_iter()
            .map(|(id, entity, _)| (id, entity))
            .collect();

        Ok(EntityResponse::from_rows(output_rows))
    }

    // Reduce one materialized response into top-k projected field values under
    // deterministic `(field_value_desc, primary_key_asc)` ranking.
    fn top_k_field_values_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, InternalError> {
        let ordered_rows = Self::top_k_ranked_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
        )?;
        let projected_values = ordered_rows
            .into_iter()
            .map(|(_, _, value)| value)
            .collect();

        Ok(projected_values)
    }

    // Reduce one materialized response into top-k projected field values with
    // ids under deterministic `(field_value_desc, primary_key_asc)` ranking.
    fn top_k_field_values_with_ids_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let ordered_rows = Self::top_k_ranked_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
        )?;
        let projected_values = ordered_rows
            .into_iter()
            .map(|(id, _, value)| (id, value))
            .collect();

        Ok(projected_values)
    }

    // Reduce one materialized response into a deterministic bottom-k response
    // ordered by `(field_value_asc, primary_key_asc)`.
    fn bottom_k_field_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let ordered_rows = Self::bottom_k_ranked_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
        )?;
        let output_rows = ordered_rows
            .into_iter()
            .map(|(id, entity, _)| (id, entity))
            .collect();

        Ok(EntityResponse::from_rows(output_rows))
    }

    // Reduce one materialized response into bottom-k projected field values
    // under deterministic `(field_value_asc, primary_key_asc)` ranking.
    fn bottom_k_field_values_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, InternalError> {
        let ordered_rows = Self::bottom_k_ranked_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
        )?;
        let projected_values = ordered_rows
            .into_iter()
            .map(|(_, _, value)| value)
            .collect();

        Ok(projected_values)
    }

    // Reduce one materialized response into bottom-k projected field values
    // with ids under deterministic `(field_value_asc, primary_key_asc)` ranking.
    fn bottom_k_field_values_with_ids_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let ordered_rows = Self::bottom_k_ranked_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
        )?;
        let projected_values = ordered_rows
            .into_iter()
            .map(|(id, _, value)| (id, value))
            .collect();

        Ok(projected_values)
    }
}
