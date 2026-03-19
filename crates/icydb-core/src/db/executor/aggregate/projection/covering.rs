//! Module: db::executor::aggregate::projection::covering
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::projection::covering.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::AccessPlan,
        executor::aggregate::materialized_distinct::insert_materialized_distinct_value,
        executor::group::GroupKeySet,
        query::plan::{
            CoveringProjectionContext, OrderSpec, PageSpec,
            covering_index_adjacent_distinct_eligible as plan_adjacent,
            covering_index_projection_context as plan_covering_context,
        },
    },
    error::InternalError,
    value::Value,
};

///
/// CoveringProjectionValues
///
/// Covering projection decoded values emitted by one prepared index-only
/// projection attempt.
/// Distinct policy is now resolved earlier during boundary preparation, so
/// execution only needs the decoded values here.
///

pub(super) struct CoveringProjectionValues {
    pub(super) values: Vec<Value>,
}

// Derive one planner-owned covering projection context from executor plan
// contracts without duplicating order-shape interpretation in executor code.
pub(super) fn covering_index_projection_context<K>(
    access: &AccessPlan<K>,
    order: Option<&OrderSpec>,
    target_field: &str,
    primary_key_name: &'static str,
) -> Option<CoveringProjectionContext> {
    plan_covering_context(access, order, target_field, primary_key_name)
}

// Return whether adjacent dedupe is safe for one covering context.
pub(super) const fn covering_index_adjacent_distinct_eligible(
    context: CoveringProjectionContext,
) -> bool {
    plan_adjacent(context)
}

pub(super) fn scalar_window_for_covering_projection(
    page: Option<&PageSpec>,
) -> (usize, Option<usize>) {
    let Some(page) = page else {
        return (0, None);
    };

    let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);
    let limit = page
        .limit
        .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

    (offset, limit)
}

pub(super) fn dedup_values_preserving_first(
    values: Vec<Value>,
) -> Result<Vec<Value>, InternalError> {
    let mut seen = GroupKeySet::default();
    let mut out = Vec::new();
    for value in values {
        if !insert_materialized_distinct_value(&mut seen, &value)? {
            continue;
        }
        out.push(value);
    }

    Ok(out)
}

pub(super) fn dedup_adjacent_values(values: Vec<Value>) -> Vec<Value> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        if out.last().is_some_and(|previous| previous == &value) {
            continue;
        }
        out.push(value);
    }

    out
}
