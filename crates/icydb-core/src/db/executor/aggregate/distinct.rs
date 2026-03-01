//! Module: executor::aggregate::distinct
//! Responsibility: DISTINCT aggregate terminal helpers over materialized responses.
//! Does not own: grouped key canonicalization internals or route planning policy.
//! Boundary: value-DISTINCT aggregate adapters for load executor terminals.

use crate::{
    db::{
        cursor::GroupedPlannedCursor,
        executor::{
            ExecutablePlan,
            aggregate::{AggregateKind, field::extract_orderable_field_value},
            group::{GroupKeySet, KeyCanonicalError},
            load::LoadExecutor,
        },
        query::plan::{GroupAggregateSpec, GroupSpec, GroupedExecutionConfig},
        response::Response,
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
    /// Execute `count_distinct(field)` over the effective response window.
    pub(in crate::db) fn aggregate_count_distinct_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<u32, InternalError> {
        let target_field = target_field.into();
        let field_slot = Self::resolve_any_field_slot(target_field.as_str())?;
        let response = self.execute(plan)?;

        Self::count_distinct_field_values_from_materialized(
            response,
            target_field.as_str(),
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
        let grouped_plan = plan.into_inner().into_grouped(GroupSpec {
            group_fields: Vec::new(),
            aggregates: vec![GroupAggregateSpec {
                kind,
                target_field: Some(target_field.to_string()),
                distinct: true,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(
                GLOBAL_DISTINCT_GROUPED_MAX_GROUPS,
                GLOBAL_DISTINCT_GROUPED_MAX_GROUP_BYTES,
            ),
        });
        let grouped_plan = ExecutablePlan::new(grouped_plan);
        let (page, _) = self
            .execute_grouped_paged_with_cursor_traced(grouped_plan, GroupedPlannedCursor::none())?;

        if page.next_cursor.is_some() {
            return Err(InternalError::query_executor_invariant(
                "global DISTINCT grouped aggregate must not emit continuation cursor",
            ));
        }
        if page.rows.len() > 1 {
            return Err(InternalError::query_executor_invariant(
                "global DISTINCT grouped aggregate must emit at most one grouped row",
            ));
        }
        let Some(row) = page.rows.first() else {
            return Ok(None);
        };
        if !row.group_key().is_empty() {
            return Err(InternalError::query_executor_invariant(
                "global DISTINCT grouped aggregate row must have empty grouped key",
            ));
        }
        if row.aggregate_values().len() != 1 {
            return Err(InternalError::query_executor_invariant(format!(
                "global DISTINCT grouped aggregate row must have one aggregate value, found {}",
                row.aggregate_values().len()
            )));
        }

        Ok(row.aggregate_values().first().cloned())
    }

    // Count distinct field values from one materialized response while preserving
    // value DISTINCT semantics via canonical GroupKey equality.
    fn count_distinct_field_values_from_materialized(
        response: Response<E>,
        target_field: &str,
        field_slot: crate::db::executor::aggregate::field::FieldSlot,
    ) -> Result<u32, InternalError> {
        let mut distinct_values = GroupKeySet::default();
        let mut distinct_count = 0u32;
        for (_, entity) in response {
            let value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            if !distinct_values
                .insert_value(&value)
                .map_err(KeyCanonicalError::into_internal_error)?
            {
                continue;
            }
            distinct_count = distinct_count.saturating_add(1);
        }

        Ok(distinct_count)
    }
}
