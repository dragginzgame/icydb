//! Module: db::executor::pipeline::contracts::post_access
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::contracts::post_access.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::query::plan::{AccessPlannedQuery, DeleteLimitSpec, OrderSpec, QueryMode};
use core::marker::PhantomData;

///
/// PostAccessContract
///
/// Planner-to-pipeline projection seam for post-access execution.
/// Exposes only the scalar-plan fields and access payload needed by
/// post-access coordinator/runtime phases.
///

pub(in crate::db::executor) struct PostAccessContract<'a, K> {
    plan: &'a AccessPlannedQuery,
    marker: PhantomData<K>,
}

impl<'a, K> PostAccessContract<'a, K> {
    /// Build a post-access contract projection from one planned access query.
    #[must_use]
    pub(in crate::db::executor) const fn new(plan: &'a AccessPlannedQuery) -> Self {
        Self {
            plan,
            marker: PhantomData,
        }
    }

    /// Project query mode for post-access phase gating.
    #[must_use]
    pub(in crate::db::executor) const fn mode(&self) -> QueryMode {
        self.plan.scalar_plan().mode
    }

    /// Project ORDER BY contract for post-access ordering.
    #[must_use]
    pub(in crate::db::executor) const fn order_spec(&self) -> Option<&OrderSpec> {
        self.plan.scalar_plan().order.as_ref()
    }

    /// Project delete-limit contract for post-access delete limiting.
    #[must_use]
    pub(in crate::db::executor) const fn delete_limit_spec(&self) -> Option<&DeleteLimitSpec> {
        self.plan.scalar_plan().delete_limit.as_ref()
    }

    /// Project residual predicate-presence bit for filter-phase gating.
    #[must_use]
    pub(in crate::db::executor) const fn has_predicate(&self) -> bool {
        self.plan.scalar_plan().predicate.is_some()
    }

    /// Borrow the planned access query for post-access runtime helpers.
    #[must_use]
    pub(in crate::db::executor) const fn plan(&self) -> &AccessPlannedQuery {
        self.plan
    }
}
