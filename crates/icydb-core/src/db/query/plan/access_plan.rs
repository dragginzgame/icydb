//! Module: query::plan::access
//! Responsibility: post-planning logical+access composite contracts and builders.
//! Does not own: pure logical plan model definitions or semantic interpretation.
//! Boundary: glue between logical plan semantics and selected access paths.

#[cfg(test)]
use crate::db::access::AccessPath;
use crate::db::{
    access::{AccessPlan, AccessStrategy},
    query::plan::{GroupHavingSpec, GroupPlan, GroupSpec, LogicalPlan, expr::ProjectionSelection},
};
#[cfg(test)]
use crate::db::{
    predicate::MissingRowPolicy,
    query::plan::{LoadSpec, QueryMode, ScalarPlan},
};

///
/// AccessPlannedQuery
///
/// Access-planned query produced after access-path selection.
/// Binds one pure `LogicalPlan` to one chosen `AccessPlan`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AccessPlannedQuery<K> {
    pub(crate) logical: LogicalPlan,
    pub(crate) access: AccessPlan<K>,
    pub(crate) projection_selection: ProjectionSelection,
}

impl<K> AccessPlannedQuery<K> {
    /// Construct an access-planned query from logical + access stages.
    #[must_use]
    pub(crate) const fn from_parts(logical: LogicalPlan, access: AccessPlan<K>) -> Self {
        Self {
            logical,
            access,
            projection_selection: ProjectionSelection::All,
        }
    }

    /// Construct an access-planned query from logical + access + projection stages.
    #[must_use]
    pub(crate) fn from_parts_with_projection(
        logical: LogicalPlan,
        access: AccessPlan<K>,
        projection_selection: ProjectionSelection,
    ) -> Self {
        let mut plan = Self::from_parts(logical, access);
        plan.projection_selection = projection_selection;

        plan
    }

    /// Decompose into logical + access stages.
    #[must_use]
    pub(crate) fn into_parts(self) -> (LogicalPlan, AccessPlan<K>, ProjectionSelection) {
        (self.logical, self.access, self.projection_selection)
    }

    /// Convert this plan into grouped logical form with one explicit group spec.
    #[must_use]
    pub(in crate::db) fn into_grouped(self, group: GroupSpec) -> Self {
        self.into_grouped_with_having(group, None)
    }

    /// Convert this plan into grouped logical form with explicit HAVING shape.
    #[must_use]
    pub(in crate::db) fn into_grouped_with_having(
        self,
        group: GroupSpec,
        having: Option<GroupHavingSpec>,
    ) -> Self {
        let Self {
            logical,
            access,
            projection_selection,
        } = self;
        let scalar = match logical {
            LogicalPlan::Scalar(plan) => plan,
            LogicalPlan::Grouped(plan) => plan.scalar,
        };

        Self {
            logical: LogicalPlan::Grouped(GroupPlan {
                scalar,
                group,
                having,
            }),
            access,
            projection_selection,
        }
    }

    /// Lower the chosen access plan into an access-owned normalized contract.
    #[must_use]
    pub(in crate::db) fn access_strategy(&self) -> AccessStrategy<'_, K> {
        self.access.resolve_strategy()
    }

    /// Construct a minimal access-planned query with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[cfg(test)]
    pub(crate) fn new(access: AccessPath<K>, consistency: MissingRowPolicy) -> Self {
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
}
