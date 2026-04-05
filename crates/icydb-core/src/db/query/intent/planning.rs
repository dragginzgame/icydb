//! Module: db::query::intent::planning
//! Responsibility: module-local ownership and contracts for db::query::intent::planning.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::query::{
        intent::{build_access_plan_from_keys, state::QueryIntent},
        plan::{AccessPlanningInputs, LogicalPlanningInputs},
    },
    traits::FieldValue,
};

impl<K> QueryIntent<K> {
    /// Project logical-planning inputs from intent-owned query state.
    #[must_use]
    pub(in crate::db::query::intent) fn planning_logical_inputs(&self) -> LogicalPlanningInputs {
        let (group, having) = match self.grouped() {
            Some(grouped) => (Some(grouped.group.clone()), grouped.having.clone()),
            None => (None, None),
        };

        LogicalPlanningInputs::new(
            self.mode(),
            self.scalar().order.clone(),
            self.scalar().distinct,
            group,
            having,
        )
    }
}

impl<K: FieldValue> QueryIntent<K> {
    /// Project access-planning inputs from intent-owned scalar state.
    #[must_use]
    pub(in crate::db::query::intent) fn planning_access_inputs(&self) -> AccessPlanningInputs<'_> {
        let scalar = self.scalar();
        let key_access_override = scalar
            .key_access
            .as_ref()
            .map(|state| build_access_plan_from_keys(&state.access));

        AccessPlanningInputs::new(
            scalar.predicate.as_ref(),
            scalar.order.as_ref(),
            key_access_override,
        )
    }
}
