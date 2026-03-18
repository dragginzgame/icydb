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
use crate::{traits::FieldValue, value::Value};

///
/// AccessPlannedQuery
///
/// Access-planned query produced after access-path selection.
/// Binds one pure `LogicalPlan` to one chosen structural `AccessPlan<Value>`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AccessPlannedQuery {
    pub(crate) logical: LogicalPlan,
    pub(crate) access: AccessPlan<Value>,
    pub(crate) projection_selection: ProjectionSelection,
}

impl AccessPlannedQuery {
    /// Construct an access-planned query from logical + access stages.
    #[must_use]
    pub(crate) fn from_parts<K>(logical: LogicalPlan, access: AccessPlan<K>) -> Self
    where
        K: FieldValue,
    {
        Self {
            logical,
            access: access.into_value_plan(),
            projection_selection: ProjectionSelection::All,
        }
    }

    /// Construct an access-planned query from logical + access + projection stages.
    #[must_use]
    pub(crate) fn from_parts_with_projection<K>(
        logical: LogicalPlan,
        access: AccessPlan<K>,
        projection_selection: ProjectionSelection,
    ) -> Self
    where
        K: FieldValue,
    {
        let mut plan = Self::from_parts(logical, access);
        plan.projection_selection = projection_selection;

        plan
    }

    /// Decompose the canonical structural plan into its stable planner-owned parts.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn into_parts(self) -> (LogicalPlan, AccessPlan<Value>, ProjectionSelection) {
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
    pub(in crate::db) fn access_strategy(&self) -> AccessStrategy<'_, Value> {
        self.access.resolve_strategy()
    }

    /// Construct a minimal access-planned query with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[cfg(test)]
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

    /// Construct one minimal access-planned query from one typed access path.
    ///
    /// This is transitional boundary glue for tests and typed planner call sites
    /// that still express primary-key access using one concrete key type.
    #[cfg(test)]
    pub(crate) fn new_typed<K>(access: AccessPath<K>, consistency: MissingRowPolicy) -> Self
    where
        K: FieldValue,
    {
        Self::new(access.into_value_path(), consistency)
    }
}
