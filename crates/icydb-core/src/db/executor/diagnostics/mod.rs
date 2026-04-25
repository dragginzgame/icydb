//! Module: db::executor::diagnostics
//! Responsibility: executor-scoped diagnostics contracts for node/counter correlation.
//! Does not own: explain rendering, metrics sink persistence, or route behavior.
//! Boundary: additive observability types consumed by executor-local diagnostics paths.

#[cfg(test)]
pub(crate) mod counters;
#[cfg(test)]
pub(crate) mod node;

use crate::db::{
    access::{AccessPathKind, AccessPlan},
    diagnostics::ExecutionAccessPathVariant,
    direction::Direction,
    query::plan::OrderDirection,
};

pub(in crate::db) use crate::db::diagnostics::ExecutionOptimization;
pub(in crate::db::executor) use crate::db::diagnostics::ExecutionTrace;

/// Build one execution trace from executor-owned access and route state.
#[must_use]
pub(in crate::db::executor) fn execution_trace_for_access<K>(
    access: &AccessPlan<K>,
    direction: Direction,
    continuation_applied: bool,
) -> ExecutionTrace {
    ExecutionTrace::new_from_variant(
        execution_access_path_variant(access),
        execution_order_direction(direction),
        continuation_applied,
    )
}

// Keep planner/executor access-shape interpretation on the executor side of
// the diagnostics boundary; diagnostics only stores the projected variant.
fn execution_access_path_variant<K>(access: &AccessPlan<K>) -> ExecutionAccessPathVariant {
    match access {
        AccessPlan::Path(path) => match path.kind() {
            AccessPathKind::ByKey => ExecutionAccessPathVariant::ByKey,
            AccessPathKind::ByKeys => ExecutionAccessPathVariant::ByKeys,
            AccessPathKind::KeyRange => ExecutionAccessPathVariant::KeyRange,
            AccessPathKind::IndexPrefix => ExecutionAccessPathVariant::IndexPrefix,
            AccessPathKind::IndexMultiLookup => ExecutionAccessPathVariant::IndexMultiLookup,
            AccessPathKind::IndexRange => ExecutionAccessPathVariant::IndexRange,
            AccessPathKind::FullScan => ExecutionAccessPathVariant::FullScan,
        },
        AccessPlan::Union(_) => ExecutionAccessPathVariant::Union,
        AccessPlan::Intersection(_) => ExecutionAccessPathVariant::Intersection,
    }
}

// Runtime scan direction and diagnostic order direction are distinct enum
// surfaces, so the executor performs the mechanical projection before tracing.
const fn execution_order_direction(direction: Direction) -> OrderDirection {
    match direction {
        Direction::Asc => OrderDirection::Asc,
        Direction::Desc => OrderDirection::Desc,
    }
}
