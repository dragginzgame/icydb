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
        GroupedRow,
        cursor::GroupedPlannedCursor,
        executor::{
            ExecutablePlan,
            aggregate::AggregateKind,
            pipeline::contracts::{GroupedCursorPage, GroupedRouteStage, LoadExecutor},
        },
        query::plan::{GroupedExecutionConfig, global_distinct_group_spec_for_semantic_aggregate},
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
    // Lower one scalar DISTINCT aggregate request into one prepared grouped
    // route stage used by global DISTINCT terminals.
    pub(in crate::db::executor::aggregate) fn prepare_global_distinct_grouped_route(
        &self,
        plan: ExecutablePlan<E>,
        kind: AggregateKind,
        target_field: &str,
    ) -> Result<GroupedRouteStage, InternalError> {
        let grouped_shape = global_distinct_group_spec_for_semantic_aggregate(
            kind,
            target_field,
            GroupedExecutionConfig::with_hard_limits(
                GLOBAL_DISTINCT_GROUPED_MAX_GROUPS,
                GLOBAL_DISTINCT_GROUPED_MAX_GROUP_BYTES,
            ),
        )
        .map_err(|reason| {
            crate::db::error::query_executor_invariant(format!(
                "{}: found {kind:?}",
                reason.invariant_message(),
            ))
        })?;
        let plan = plan.into_plan();
        let grouped_plan = ExecutablePlan::new(plan.into_grouped(grouped_shape));

        Self::resolve_grouped_route(grouped_plan, GroupedPlannedCursor::none(), self.debug)
    }

    // Decode one grouped zero-key DISTINCT aggregate page back into one scalar
    // aggregate value while preserving grouped-output invariants explicitly.
    fn decode_global_distinct_grouped_output(
        page: GroupedCursorPage,
    ) -> Result<Option<Value>, InternalError> {
        if page.next_cursor.is_some() {
            return Err(crate::db::error::query_executor_invariant(
                "global DISTINCT grouped aggregate must not emit continuation cursor",
            ));
        }
        if page.rows.len() > 1 {
            return Err(crate::db::error::query_executor_invariant(
                "global DISTINCT grouped aggregate must emit at most one grouped row",
            ));
        }
        let Some(row) = page.rows.first() else {
            return Ok(None);
        };

        Self::decode_global_distinct_grouped_row(row)
    }

    // Decode one grouped zero-key DISTINCT aggregate row into one scalar value.
    fn decode_global_distinct_grouped_row(
        row: &GroupedRow,
    ) -> Result<Option<Value>, InternalError> {
        if !row.group_key().is_empty() {
            return Err(crate::db::error::query_executor_invariant(
                "global DISTINCT grouped aggregate row must have empty grouped key",
            ));
        }
        if row.aggregate_values().len() != 1 {
            return Err(crate::db::error::query_executor_invariant(format!(
                "global DISTINCT grouped aggregate row must have one aggregate value, found {}",
                row.aggregate_values().len()
            )));
        }

        Ok(row.aggregate_values().first().cloned())
    }

    // Execute one global DISTINCT field-target grouped aggregate by lowering
    // into grouped logical shape with zero group keys.
    pub(in crate::db::executor::aggregate) fn execute_prepared_global_distinct_grouped_aggregate(
        &self,
        route: GroupedRouteStage,
    ) -> Result<Option<Value>, InternalError> {
        let (page, _) = self.execute_prepared_grouped_route(route)?;

        Self::decode_global_distinct_grouped_output(page)
    }
}
