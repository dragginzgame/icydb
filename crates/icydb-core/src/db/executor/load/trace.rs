//! Module: executor::load::trace
//! Responsibility: map load-path runtime inputs into trace-surface enum projections.
//! Does not own: execution routing decisions or observability storage policy.
//! Boundary: pure projection helpers used by `ExecutionTrace`.

use crate::db::{
    access::{AccessPath, AccessPlan},
    direction::Direction,
    executor::load::ExecutionAccessPathVariant,
    query::plan::OrderDirection,
};

/// Project access-plan shape into trace-level access-path variant.
pub(super) fn access_path_variant<K>(access: &AccessPlan<K>) -> ExecutionAccessPathVariant {
    match access {
        AccessPlan::Path(path) => match path.as_ref() {
            AccessPath::ByKey(_) => ExecutionAccessPathVariant::ByKey,
            AccessPath::ByKeys(_) => ExecutionAccessPathVariant::ByKeys,
            AccessPath::KeyRange { .. } => ExecutionAccessPathVariant::KeyRange,
            AccessPath::IndexPrefix { .. } => ExecutionAccessPathVariant::IndexPrefix,
            AccessPath::IndexRange { .. } => ExecutionAccessPathVariant::IndexRange,
            AccessPath::FullScan => ExecutionAccessPathVariant::FullScan,
        },
        AccessPlan::Union(_) => ExecutionAccessPathVariant::Union,
        AccessPlan::Intersection(_) => ExecutionAccessPathVariant::Intersection,
    }
}

/// Project runtime direction into trace-level order direction.
pub(super) const fn execution_order_direction(direction: Direction) -> OrderDirection {
    OrderDirection::from_direction(direction)
}
