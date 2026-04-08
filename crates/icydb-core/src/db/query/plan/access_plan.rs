//! Module: query::plan::access
//! Responsibility: post-planning logical+access composite contracts and builders.
//! Does not own: pure logical plan model definitions or semantic interpretation.
//! Boundary: glue between logical plan semantics and selected access paths.

use crate::db::{
    access::{AccessPlan, AccessStrategy},
    query::plan::{
        AccessChoiceExplainSnapshot, GroupHavingSpec, GroupPlan, GroupSpec, LogicalPlan,
        PlannerRouteProfile, expr::ProjectionSelection,
    },
};
use crate::{traits::FieldValue, value::Value};

#[cfg(test)]
use crate::db::{
    access::AccessPath,
    predicate::MissingRowPolicy,
    query::plan::{LoadSpec, QueryMode, ScalarPlan},
};

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
    pub(in crate::db) access_choice: AccessChoiceExplainSnapshot,
    pub(in crate::db) planner_route_profile: PlannerRouteProfile,
}

impl AccessPlannedQuery {
    /// Construct a minimal access-planned query with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn new(access: AccessPath<Value>, consistency: MissingRowPolicy) -> Self {
        let access = AccessPlan::path(access);

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
            access_choice: seeded_access_choice_snapshot(&access),
            planner_route_profile: PlannerRouteProfile::seeded_unfinalized(false),
            access,
            projection_selection: ProjectionSelection::All,
        }
    }

    /// Construct an access-planned query from logical + access stages.
    #[must_use]
    pub(crate) fn from_parts<K>(logical: LogicalPlan, access: AccessPlan<K>) -> Self
    where
        K: FieldValue,
    {
        let access = access.into_value_plan();
        let mut plan = Self {
            logical,
            access_choice: seeded_access_choice_snapshot(&access),
            planner_route_profile: PlannerRouteProfile::seeded_unfinalized(false),
            access,
            projection_selection: ProjectionSelection::All,
        };
        plan.planner_route_profile = seeded_planner_route_profile(&plan);

        plan
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
            access_choice,
            planner_route_profile: _planner_route_profile,
        } = self;
        let scalar = match logical {
            LogicalPlan::Scalar(plan) => plan,
            LogicalPlan::Grouped(plan) => plan.scalar,
        };
        let mut plan = Self {
            logical: LogicalPlan::Grouped(GroupPlan {
                scalar,
                group,
                having,
            }),
            access,
            projection_selection,
            access_choice,
            planner_route_profile: PlannerRouteProfile::seeded_unfinalized(true),
        };
        plan.planner_route_profile = seeded_planner_route_profile(&plan);

        plan
    }

    /// Lower the chosen access plan into an access-owned normalized contract.
    #[must_use]
    pub(in crate::db) fn access_strategy(&self) -> AccessStrategy<'_, Value> {
        self.access.resolve_strategy()
    }

    /// Borrow the planner-owned access-choice diagnostics snapshot.
    #[must_use]
    pub(in crate::db) const fn access_choice(&self) -> &AccessChoiceExplainSnapshot {
        &self.access_choice
    }

    /// Attach one planner-owned access-choice diagnostics snapshot.
    pub(in crate::db) fn set_access_choice(&mut self, access_choice: AccessChoiceExplainSnapshot) {
        self.access_choice = access_choice;
    }

    /// Borrow the frozen planner-owned route profile.
    #[must_use]
    pub(in crate::db) const fn planner_route_profile(&self) -> &PlannerRouteProfile {
        &self.planner_route_profile
    }

    /// Attach one frozen planner-owned route profile.
    pub(in crate::db) fn set_planner_route_profile(
        &mut self,
        planner_route_profile: PlannerRouteProfile,
    ) {
        self.planner_route_profile = planner_route_profile;
    }
}

fn seeded_access_choice_snapshot(access: &AccessPlan<Value>) -> AccessChoiceExplainSnapshot {
    if access.selected_index_model().is_some() {
        AccessChoiceExplainSnapshot::selected_index_unavailable()
    } else {
        AccessChoiceExplainSnapshot::non_index_access()
    }
}

fn seeded_planner_route_profile(plan: &AccessPlannedQuery) -> PlannerRouteProfile {
    PlannerRouteProfile::seeded_unfinalized(plan.grouped_plan().is_some())
}
