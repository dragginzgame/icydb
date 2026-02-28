//! Module: query::plan::group
//! Responsibility: grouped-plan handoff contract between query planning and executor.
//! Does not own: grouped runtime execution logic.
//! Boundary: explicit grouped query-to-executor transfer surface.

use crate::{
    db::query::plan::{AccessPlannedQuery, FieldSlot, GroupAggregateSpec, GroupedExecutionConfig},
    error::InternalError,
};

///
/// GroupedExecutorHandoff
///
/// Borrowed grouped planning handoff consumed at the query->executor boundary.
/// This contract keeps grouped execution routing input explicit while grouped
/// runtime entry remains explicit at query->executor boundaries.
///

#[derive(Clone, Copy)]
pub(in crate::db) struct GroupedExecutorHandoff<'a, K> {
    base: &'a AccessPlannedQuery<K>,
    group_fields: &'a [FieldSlot],
    aggregates: &'a [GroupAggregateSpec],
    execution: GroupedExecutionConfig,
}

impl<'a, K> GroupedExecutorHandoff<'a, K> {
    /// Borrow the grouped query base plan.
    #[must_use]
    pub(in crate::db) const fn base(&self) -> &'a AccessPlannedQuery<K> {
        self.base
    }

    /// Borrow declared grouped key fields.
    #[must_use]
    pub(in crate::db) const fn group_fields(&self) -> &'a [FieldSlot] {
        self.group_fields
    }

    /// Borrow grouped aggregate terminals.
    #[must_use]
    pub(in crate::db) const fn aggregates(&self) -> &'a [GroupAggregateSpec] {
        self.aggregates
    }

    /// Borrow grouped execution hard-limit policy selected by planning.
    #[must_use]
    pub(in crate::db) const fn execution(&self) -> GroupedExecutionConfig {
        self.execution
    }
}

/// Build one grouped executor handoff from one grouped logical plan.
pub(in crate::db) fn grouped_executor_handoff<K>(
    plan: &AccessPlannedQuery<K>,
) -> Result<GroupedExecutorHandoff<'_, K>, InternalError> {
    // Grouped handoff is valid only for plans with grouped execution payload.
    let Some(grouped) = plan.grouped_plan() else {
        return Err(InternalError::query_executor_invariant(
            "grouped executor handoff requires grouped logical plans",
        ));
    };

    Ok(GroupedExecutorHandoff {
        base: plan,
        group_fields: grouped.group.group_fields.as_slice(),
        aggregates: grouped.group.aggregates.as_slice(),
        execution: grouped.group.execution,
    })
}
