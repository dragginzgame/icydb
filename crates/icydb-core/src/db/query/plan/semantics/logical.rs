use crate::{
    db::{
        access::AccessPlan,
        query::plan::{
            AccessPlannedQuery, ContinuationPolicy, DistinctExecutionStrategy,
            ExecutionShapeSignature, GroupPlan, LogicalPlan, PlannerRouteProfile, QueryMode,
            ScalarPlan, derive_logical_pushdown_eligibility, expr::ProjectionSpec,
            grouped_cursor_policy_violation_for_continuation, lower_projection_identity,
            lower_projection_intent,
        },
    },
    model::entity::EntityModel,
    traits::FieldValue,
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

    /// Borrow scalar semantic fields mutably across logical variants.
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

impl<K> AccessPlannedQuery<K> {
    /// Borrow scalar semantic fields shared by scalar/grouped logical variants.
    #[must_use]
    pub(in crate::db) const fn scalar_plan(&self) -> &ScalarPlan {
        self.logical.scalar_semantics()
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
        lower_projection_intent(model, &self.logical)
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

        if access_shape_requires_distinct_materialization(&self.access) {
            DistinctExecutionStrategy::HashMaterialize
        } else {
            DistinctExecutionStrategy::PreOrdered
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
    ) -> ExecutionShapeSignature
    where
        K: FieldValue,
    {
        ExecutionShapeSignature::new(self.continuation_signature(entity_path))
    }

    /// Borrow scalar semantic fields mutably across logical variants.
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
}

fn access_shape_requires_distinct_materialization<K>(access: &AccessPlan<K>) -> bool {
    match access {
        AccessPlan::Union(_) | AccessPlan::Intersection(_) => true,
        AccessPlan::Path(path) => path.as_ref().is_index_multi_lookup(),
    }
}

fn derive_continuation_policy_validated<K>(plan: &AccessPlannedQuery<K>) -> ContinuationPolicy {
    let is_grouped_safe = plan.grouped_plan().is_none_or(|grouped| {
        grouped_cursor_policy_violation_for_continuation(grouped, true).is_none()
    });

    ContinuationPolicy::new(
        true, // Continuation resume windows require anchor semantics for pushdown-safe replay.
        true, // Continuation resumes must advance strictly to prevent replay/regression loops.
        is_grouped_safe,
    )
}
