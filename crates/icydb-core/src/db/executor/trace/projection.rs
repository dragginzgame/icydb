//! Module: executor::trace::projection
//! Responsibility: map load-path runtime inputs into trace-surface enum projections.
//! Does not own: execution routing decisions or observability storage policy.
//! Boundary: pure projection helpers used by `ExecutionTrace`.

use crate::db::{
    access::AccessPlan,
    direction::Direction,
    executor::route::order_direction_from_direction,
    executor::{dispatch_access_plan_kind, trace::ExecutionAccessPathVariant},
    query::plan::{OrderDirection, lower_executable_access_plan},
};

/// Project access-plan shape into trace-level access-path variant.
pub(super) fn access_path_variant<K>(access: &AccessPlan<K>) -> ExecutionAccessPathVariant {
    let executable = lower_executable_access_plan(access);

    dispatch_access_plan_kind(&executable).execution_access_path_variant()
}

/// Project runtime direction into trace-level order direction.
pub(super) const fn execution_order_direction(direction: Direction) -> OrderDirection {
    order_direction_from_direction(direction)
}
