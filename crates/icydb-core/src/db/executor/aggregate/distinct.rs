//! Module: executor::aggregate::distinct
//! Responsibility: DISTINCT aggregate terminal helpers over materialized responses.
//! Does not own: grouped key canonicalization internals or route planning policy.
//! Boundary: value-DISTINCT aggregate adapters for load executor terminals.
//!
//! Non-grouped field-target DISTINCT helpers in this module are effective-window
//! materialized terminals. Grouped Class B DISTINCT accounting remains owned by
//! `ExecutionContext` and grouped executor paths.

use crate::{
    db::{
        cursor::GroupedPlannedCursor,
        executor::{
            ExecutablePlan,
            aggregate::{
                AggregateKind,
                field::{
                    extract_orderable_field_value,
                    resolve_any_aggregate_target_slot_from_planner_slot,
                },
                materialized_distinct::insert_materialized_distinct_value,
            },
            group::GroupKeySet,
            load::LoadExecutor,
        },
        query::plan::{
            FieldSlot as PlannedFieldSlot, GroupedExecutionConfig,
            global_distinct_group_spec_for_semantic_aggregate,
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

const GLOBAL_DISTINCT_GROUPED_MAX_GROUPS: u64 = 1;
const GLOBAL_DISTINCT_GROUPED_MAX_GROUP_BYTES: u64 = 16 * 1024 * 1024;

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute `count_distinct(field)` over the effective response window using
    /// one planner-resolved field slot.
    pub(in crate::db) fn aggregate_count_distinct_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<u32, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        let response = self.execute(plan)?;

        Self::count_distinct_field_values_from_materialized(
            response,
            target_field.field(),
            field_slot,
        )
    }

    // Execute one global DISTINCT field-target grouped aggregate by lowering
    // into grouped logical shape with zero group keys.
    pub(in crate::db::executor) fn execute_global_distinct_field_grouped_aggregate(
        &self,
        plan: ExecutablePlan<E>,
        kind: AggregateKind,
        target_field: &str,
    ) -> Result<Option<Value>, InternalError> {
        // Build global DISTINCT grouped shape via query semantic authority.
        let grouped_shape = global_distinct_group_spec_for_semantic_aggregate(
            kind,
            target_field,
            GroupedExecutionConfig::with_hard_limits(
                GLOBAL_DISTINCT_GROUPED_MAX_GROUPS,
                GLOBAL_DISTINCT_GROUPED_MAX_GROUP_BYTES,
            ),
        )
        .map_err(|reason| {
            crate::db::error::executor_invariant(format!(
                "{}: found {kind:?}",
                reason.invariant_message(),
            ))
        })?;
        let grouped_plan = plan.into_inner().into_grouped(grouped_shape);
        let grouped_plan = ExecutablePlan::new(grouped_plan);
        let (page, _) = self
            .execute_grouped_paged_with_cursor_traced(grouped_plan, GroupedPlannedCursor::none())?;

        if page.next_cursor.is_some() {
            return Err(crate::db::error::executor_invariant(
                "global DISTINCT grouped aggregate must not emit continuation cursor",
            ));
        }
        if page.rows.len() > 1 {
            return Err(crate::db::error::executor_invariant(
                "global DISTINCT grouped aggregate must emit at most one grouped row",
            ));
        }
        let Some(row) = page.rows.first() else {
            return Ok(None);
        };
        if !row.group_key().is_empty() {
            return Err(crate::db::error::executor_invariant(
                "global DISTINCT grouped aggregate row must have empty grouped key",
            ));
        }
        if row.aggregate_values().len() != 1 {
            return Err(crate::db::error::executor_invariant(format!(
                "global DISTINCT grouped aggregate row must have one aggregate value, found {}",
                row.aggregate_values().len()
            )));
        }

        Ok(row.aggregate_values().first().cloned())
    }

    // Count distinct field values from one materialized response while preserving
    // value DISTINCT semantics via canonical GroupKey equality.
    fn count_distinct_field_values_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: crate::db::executor::aggregate::field::FieldSlot,
    ) -> Result<u32, InternalError> {
        let mut distinct_values = GroupKeySet::default();
        let mut distinct_count = 0u32;
        for row in response {
            let value = extract_orderable_field_value(row.entity_ref(), target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            if !insert_materialized_distinct_value(&mut distinct_values, &value)? {
                continue;
            }
            distinct_count = distinct_count.saturating_add(1);
        }

        Ok(distinct_count)
    }
}
