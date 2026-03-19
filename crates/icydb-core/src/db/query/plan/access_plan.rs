//! Module: query::plan::access
//! Responsibility: post-planning logical+access composite contracts and builders.
//! Does not own: pure logical plan model definitions or semantic interpretation.
//! Boundary: glue between logical plan semantics and selected access paths.

use crate::db::{
    access::{AccessPlan, AccessStrategy},
    query::plan::{GroupHavingSpec, GroupPlan, GroupSpec, LogicalPlan, expr::ProjectionSelection},
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
}
