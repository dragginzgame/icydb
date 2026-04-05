//! Module: query::plan::semantics::logical
//! Responsibility: logical-plan semantic lowering from planner contracts to access-planned queries.
//! Does not own: access-path index selection internals or runtime execution behavior.
//! Boundary: derives planner-owned execution semantics, shape signatures, and continuation policy.

use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        query::plan::{
            AccessPlannedQuery, ContinuationPolicy, DistinctExecutionStrategy,
            ExecutionShapeSignature, GroupPlan, LogicalPlan, PlannerRouteProfile, QueryMode,
            ScalarPlan, derive_logical_pushdown_eligibility, expr::ProjectionSpec,
            filtered_index_predicate_satisfies_query, grouped_cursor_policy_violation,
            lower_projection_identity, lower_projection_intent,
        },
    },
    model::entity::EntityModel,
};

impl QueryMode {
    /// True if this mode represents a load intent.
    #[must_use]
    pub const fn is_load(&self) -> bool {
        match self {
            Self::Load(_) => true,
            Self::Delete(_) => false,
        }
    }

    /// True if this mode represents a delete intent.
    #[must_use]
    pub const fn is_delete(&self) -> bool {
        match self {
            Self::Delete(_) => true,
            Self::Load(_) => false,
        }
    }
}

impl LogicalPlan {
    /// Borrow scalar semantic fields shared by scalar/grouped logical variants.
    #[must_use]
    pub(in crate::db) const fn scalar_semantics(&self) -> &ScalarPlan {
        match self {
            Self::Scalar(plan) => plan,
            Self::Grouped(plan) => &plan.scalar,
        }
    }

    /// Borrow scalar semantic fields mutably across logical variants for tests.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar_semantics_mut(&mut self) -> &mut ScalarPlan {
        match self {
            Self::Scalar(plan) => plan,
            Self::Grouped(plan) => &mut plan.scalar,
        }
    }

    /// Test-only shorthand for explicit scalar semantic borrowing.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar(&self) -> &ScalarPlan {
        self.scalar_semantics()
    }

    /// Test-only shorthand for explicit mutable scalar semantic borrowing.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar_mut(&mut self) -> &mut ScalarPlan {
        self.scalar_semantics_mut()
    }
}

impl AccessPlannedQuery {
    /// Borrow scalar semantic fields shared by scalar/grouped logical variants.
    #[must_use]
    pub(in crate::db) const fn scalar_plan(&self) -> &ScalarPlan {
        self.logical.scalar_semantics()
    }

    /// Borrow scalar semantic fields mutably across logical variants for tests.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar_plan_mut(&mut self) -> &mut ScalarPlan {
        self.logical.scalar_semantics_mut()
    }

    /// Test-only shorthand for explicit scalar plan borrowing.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar(&self) -> &ScalarPlan {
        self.scalar_plan()
    }

    /// Test-only shorthand for explicit mutable scalar plan borrowing.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar_mut(&mut self) -> &mut ScalarPlan {
        self.scalar_plan_mut()
    }

    /// Borrow grouped semantic fields when this plan is grouped.
    #[must_use]
    pub(in crate::db) const fn grouped_plan(&self) -> Option<&GroupPlan> {
        match &self.logical {
            LogicalPlan::Scalar(_) => None,
            LogicalPlan::Grouped(plan) => Some(plan),
        }
    }

    /// Lower this plan into one canonical planner-owned projection semantic spec.
    #[must_use]
    pub(in crate::db) fn projection_spec(&self, model: &EntityModel) -> ProjectionSpec {
        lower_projection_intent(model, &self.logical, &self.projection_selection)
    }

    /// Lower this plan into one projection semantic shape for identity hashing.
    #[must_use]
    pub(in crate::db::query) fn projection_spec_for_identity(&self) -> ProjectionSpec {
        lower_projection_identity(&self.logical)
    }

    /// Lower scalar DISTINCT semantics into one executor-facing execution strategy.
    #[must_use]
    pub(in crate::db) fn distinct_execution_strategy(&self) -> DistinctExecutionStrategy {
        if !self.scalar_plan().distinct {
            return DistinctExecutionStrategy::None;
        }

        // DISTINCT on duplicate-safe single-path access shapes is a planner
        // no-op for runtime dedup mechanics. Composite shapes can surface
        // duplicate keys and therefore retain explicit dedup execution.
        match distinct_runtime_dedup_strategy(&self.access) {
            Some(strategy) => strategy,
            None => DistinctExecutionStrategy::None,
        }
    }

    /// Project one planner-owned route profile for executor route planning.
    #[must_use]
    pub(in crate::db) fn planner_route_profile(&self, model: &EntityModel) -> PlannerRouteProfile {
        PlannerRouteProfile::new(
            derive_continuation_policy_validated(self),
            derive_logical_pushdown_eligibility(model, self),
        )
    }

    /// Build one immutable execution-shape signature contract for runtime layers.
    #[must_use]
    pub(in crate::db) fn execution_shape_signature(
        &self,
        entity_path: &'static str,
    ) -> ExecutionShapeSignature {
        ExecutionShapeSignature::new(self.continuation_signature(entity_path))
    }

    /// Return whether one filtered index predicate fully satisfies the current
    /// scalar query predicate without any additional post-access filtering.
    #[must_use]
    pub(in crate::db) fn predicate_fully_satisfied_by_filtered_access(&self) -> bool {
        let Some(query_predicate) = self.scalar_plan().predicate.as_ref() else {
            return false;
        };
        let Some(path) = self.access.as_path() else {
            return false;
        };

        let index = match path {
            AccessPath::IndexPrefix { index, .. } | AccessPath::IndexMultiLookup { index, .. } => {
                index
            }
            AccessPath::IndexRange { spec } => spec.index(),
            AccessPath::ByKey(_)
            | AccessPath::ByKeys(_)
            | AccessPath::KeyRange { .. }
            | AccessPath::FullScan => return false,
        };

        filtered_index_predicate_satisfies_query(index, query_predicate)
    }

    /// Return whether the scalar logical predicate still requires post-access
    /// filtering after accounting for filtered-index guard predicates.
    #[must_use]
    pub(in crate::db) fn has_residual_predicate(&self) -> bool {
        self.scalar_plan().predicate.is_some()
            && !self.predicate_fully_satisfied_by_filtered_access()
    }
}

fn distinct_runtime_dedup_strategy<K>(access: &AccessPlan<K>) -> Option<DistinctExecutionStrategy> {
    match access {
        AccessPlan::Union(_) | AccessPlan::Intersection(_) => {
            Some(DistinctExecutionStrategy::PreOrdered)
        }
        AccessPlan::Path(path) if path.as_ref().is_index_multi_lookup() => {
            Some(DistinctExecutionStrategy::HashMaterialize)
        }
        AccessPlan::Path(_) => None,
    }
}

fn derive_continuation_policy_validated(plan: &AccessPlannedQuery) -> ContinuationPolicy {
    let is_grouped_safe = plan
        .grouped_plan()
        .is_none_or(|grouped| grouped_cursor_policy_violation(grouped, true).is_none());

    ContinuationPolicy::new(
        true, // Continuation resume windows require anchor semantics for pushdown-safe replay.
        true, // Continuation resumes must advance strictly to prevent replay/regression loops.
        is_grouped_safe,
    )
}
