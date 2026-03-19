use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, LoadSpec, LogicalPlan, QueryMode, ScalarPlan,
            expr::{ProjectionField, ProjectionSelection, ProjectionSpec},
        },
    },
    value::Value,
};

impl LogicalPlan {
    /// Borrow scalar semantic fields mutably across logical variants for tests.
    #[must_use]
    pub(in crate::db) const fn scalar_semantics_mut(&mut self) -> &mut ScalarPlan {
        match self {
            Self::Scalar(plan) => plan,
            Self::Grouped(plan) => &mut plan.scalar,
        }
    }

    /// Test-only shorthand for explicit scalar semantic borrowing.
    #[must_use]
    pub(in crate::db) const fn scalar(&self) -> &ScalarPlan {
        self.scalar_semantics()
    }

    /// Test-only shorthand for explicit mutable scalar semantic borrowing.
    #[must_use]
    pub(in crate::db) const fn scalar_mut(&mut self) -> &mut ScalarPlan {
        self.scalar_semantics_mut()
    }
}

impl AccessPlannedQuery {
    /// Construct a minimal access-planned query with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[must_use]
    pub(crate) fn new(access: AccessPath<Value>, consistency: MissingRowPolicy) -> Self {
        Self {
            logical: LogicalPlan::Scalar(ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: None,
                distinct: false,
                delete_limit: None,
                page: None,
                consistency,
            }),
            access: AccessPlan::path(access),
            projection_selection: ProjectionSelection::All,
        }
    }

    /// Borrow scalar semantic fields mutably across logical variants for tests.
    #[must_use]
    pub(in crate::db) const fn scalar_plan_mut(&mut self) -> &mut ScalarPlan {
        self.logical.scalar_semantics_mut()
    }

    /// Test-only shorthand for explicit scalar plan borrowing.
    #[must_use]
    pub(in crate::db) const fn scalar(&self) -> &ScalarPlan {
        self.scalar_plan()
    }

    /// Test-only shorthand for explicit mutable scalar plan borrowing.
    #[must_use]
    pub(in crate::db) const fn scalar_mut(&mut self) -> &mut ScalarPlan {
        self.scalar_plan_mut()
    }
}

impl ProjectionSpec {
    /// Build one projection semantic contract for tests outside planner modules.
    #[must_use]
    pub(in crate::db) const fn from_fields_for_test(fields: Vec<ProjectionField>) -> Self {
        Self::new(fields)
    }
}
