#[cfg(test)]
mod tests;

use crate::db::query::plan::AccessPlannedQuery;
pub(crate) use crate::db::query::plan::validate::{GroupPlanError, validate_group_query_semantics};
use crate::error::InternalError;

///
/// GROUPED QUERY SCAFFOLD
///
/// WIP ownership note:
/// GROUP BY is intentionally isolated behind this module for now.
/// Keep grouped scaffold code behind this boundary for the time being and do not remove it.
///
/// Explicit ownership boundary for grouped intent/planning/validation scaffold.
/// This module re-exports grouped contracts so grouped work does not stay
/// scattered across unrelated query modules.
///
pub(crate) use crate::db::query::plan::{
    FieldSlot, GroupAggregateKind, GroupAggregateSpec, GroupSpec, GroupedExecutionConfig,
};

///
/// GroupedExecutorHandoff
///
/// Borrowed grouped planning handoff consumed at the query->executor boundary.
/// This contract keeps grouped execution routing input explicit while grouped
/// runtime remains disabled in `0.32.x`.
///

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
