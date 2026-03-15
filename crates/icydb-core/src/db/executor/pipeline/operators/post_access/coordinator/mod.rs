//! Module: executor::pipeline::operators::post_access::coordinator
//! Responsibility: plan coordination seam for post-access execution phases.
//! Does not own: terminal phase operator mechanics or executor entrypoint wiring.
//! Boundary: exposes one plan-owned coordinator consumed by post-access wrappers.

mod runtime;
#[cfg(test)]
mod safety;

use crate::db::query::plan::{AccessPlannedQuery, DeleteLimitSpec, OrderSpec, PageSpec, QueryMode};

///
/// PostAccessPlan
///
/// Executor-owned post-access operation wrapper over one plan contract.
///

pub(super) struct PostAccessPlan<'a, K> {
    plan: &'a AccessPlannedQuery<K>,
}

impl<'a, K> PostAccessPlan<'a, K> {
    pub(super) const fn new(plan: &'a AccessPlannedQuery<K>) -> Self {
        Self { plan }
    }

    // Project the plan mode through one post-access boundary accessor.
    const fn mode(&self) -> QueryMode {
        self.plan.scalar_plan().mode
    }

    // Project ORDER BY semantics through one post-access boundary accessor.
    const fn order_spec(&self) -> Option<&OrderSpec> {
        self.plan.scalar_plan().order.as_ref()
    }

    // Project page-window semantics through one post-access boundary accessor.
    const fn page_spec(&self) -> Option<&PageSpec> {
        self.plan.scalar_plan().page.as_ref()
    }

    // Project delete-limit semantics through one post-access boundary accessor.
    const fn delete_limit_spec(&self) -> Option<&DeleteLimitSpec> {
        self.plan.scalar_plan().delete_limit.as_ref()
    }

    // Project residual predicate presence through one post-access boundary accessor.
    const fn has_predicate(&self) -> bool {
        self.plan.scalar_plan().predicate.is_some()
    }
}
