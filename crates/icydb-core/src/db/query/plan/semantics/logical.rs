use crate::{
    db::{
        access::AccessPlan,
        query::plan::{
            AccessPlannedQuery, DistinctExecutionStrategy, GroupPlan, LogicalPlan, QueryMode,
            ScalarPlan, expr::ProjectionSpec, lower_projection_identity, lower_projection_intent,
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
    pub(in crate::db) const fn distinct_execution_strategy(&self) -> DistinctExecutionStrategy {
        if !self.scalar_plan().distinct {
            return DistinctExecutionStrategy::None;
        }

        if access_shape_requires_distinct_materialization(&self.access) {
            DistinctExecutionStrategy::HashMaterialize
        } else {
            DistinctExecutionStrategy::PreOrdered
        }
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

const fn access_shape_requires_distinct_materialization<K>(access: &AccessPlan<K>) -> bool {
    matches!(access, AccessPlan::Union(_) | AccessPlan::Intersection(_))
}
