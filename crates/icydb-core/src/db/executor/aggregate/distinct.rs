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
            PreparedAggregatePlan,
            aggregate::AggregateKind,
            pipeline::{
                contracts::{GroupedCursorPage, GroupedRouteStage, LoadExecutor},
                entrypoints::execute_prepared_grouped_route_runtime,
            },
        },
        query::plan::{GroupedExecutionConfig, global_distinct_group_spec_for_semantic_aggregate},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

const GLOBAL_DISTINCT_GROUPED_MAX_GROUPS: u64 = 1;
const GLOBAL_DISTINCT_GROUPED_MAX_GROUP_BYTES: u64 = 16 * 1024 * 1024;

///
/// GlobalDistinctGroupedOutputContract
///
/// GlobalDistinctGroupedOutputContract owns the zero-key grouped-output shape
/// required by global DISTINCT aggregate lowering before that grouped page is
/// decoded back into one scalar terminal value.
///

struct GlobalDistinctGroupedOutputContract;

impl GlobalDistinctGroupedOutputContract {
    // Build the canonical invariant for unexpected continuation output.
    fn continuation_cursor_forbidden() -> InternalError {
        InternalError::query_executor_invariant(
            "global DISTINCT grouped aggregate must not emit continuation cursor",
        )
    }

    // Build the canonical invariant for grouped pages that exceed the zero-key singleton shape.
    fn grouped_row_count_invalid(found: usize) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "global DISTINCT grouped aggregate must emit at most one grouped row, found {found}"
        ))
    }

    // Build the canonical invariant for grouped rows that retain grouping keys.
    fn grouped_key_must_be_empty() -> InternalError {
        InternalError::query_executor_invariant(
            "global DISTINCT grouped aggregate row must have empty grouped key",
        )
    }

    // Build the canonical invariant for grouped rows with unexpected aggregate width.
    fn aggregate_value_count_invalid(found: usize) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "global DISTINCT grouped aggregate row must have one aggregate value, found {found}"
        ))
    }

    // Decode one grouped zero-key DISTINCT aggregate page back into one scalar
    // aggregate value while preserving grouped-output invariants explicitly.
    fn decode_page(page: GroupedCursorPage) -> Result<Option<Value>, InternalError> {
        if page.next_cursor.is_some() {
            return Err(Self::continuation_cursor_forbidden());
        }
        if page.rows.len() > 1 {
            return Err(Self::grouped_row_count_invalid(page.rows.len()));
        }
        let Some(row) = page.rows.first() else {
            return Ok(None);
        };

        Self::decode_row(row)
    }

    // Decode one grouped zero-key DISTINCT aggregate row into one scalar value.
    fn decode_row(row: &GroupedRow) -> Result<Option<Value>, InternalError> {
        if !row.group_key().is_empty() {
            return Err(Self::grouped_key_must_be_empty());
        }
        if row.aggregate_values().len() != 1 {
            return Err(Self::aggregate_value_count_invalid(
                row.aggregate_values().len(),
            ));
        }

        Ok(row.aggregate_values().first().cloned())
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Lower one scalar DISTINCT aggregate request into one prepared grouped
    // route stage used by global DISTINCT terminals.
    pub(in crate::db::executor::aggregate) fn prepare_global_distinct_grouped_route(
        &self,
        plan: PreparedAggregatePlan,
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
        .map_err(|reason| reason.into_global_distinct_prepare_internal_error(kind))?;

        Self::resolve_grouped_route(
            plan.into_grouped_load_plan(grouped_shape),
            GroupedPlannedCursor::none(),
            self.debug,
        )
    }

    // Execute one global DISTINCT field-target grouped aggregate by lowering
    // into grouped logical shape with zero group keys.
    pub(in crate::db::executor::aggregate) fn execute_prepared_global_distinct_grouped_aggregate(
        &self,
        route: GroupedRouteStage,
    ) -> Result<Option<Value>, InternalError> {
        let (page, _) =
            execute_prepared_grouped_route_runtime(self.prepare_grouped_route_runtime(route)?)?;

        GlobalDistinctGroupedOutputContract::decode_page(page)
    }
}
