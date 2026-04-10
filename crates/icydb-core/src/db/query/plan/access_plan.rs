//! Module: query::plan::access
//! Responsibility: post-planning logical+access composite contracts and builders.
//! Does not own: pure logical plan model definitions or semantic interpretation.
//! Boundary: glue between logical plan semantics and selected access paths.

use crate::db::{
    access::{AccessPlan, AccessStrategy},
    predicate::{IndexCompileTarget, PredicateProgram},
    query::plan::{
        AccessChoiceExplainSnapshot, GroupHavingSpec, GroupPlan, GroupSpec,
        GroupedAggregateExecutionSpec, GroupedDistinctExecutionStrategy, LogicalPlan,
        PlannerRouteProfile,
        expr::{ProjectionSelection, ProjectionSpec, ScalarProjectionExpr},
        model::OrderDirection,
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

///
/// ResolvedOrderValueSource
///
/// Planner-resolved structural ORDER BY source for one canonical order term.
/// Executor consumers read this frozen source directly instead of re-parsing
/// field names against the model during sort or cursor evaluation.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ResolvedOrderValueSource {
    DirectField(usize),
    ExpressionLower(usize),
    ExpressionUpper(usize),
}

impl ResolvedOrderValueSource {
    /// Construct one direct field-slot order source.
    #[must_use]
    pub(in crate::db) const fn direct_field(slot: usize) -> Self {
        Self::DirectField(slot)
    }

    /// Construct one lower-cased expression order source.
    #[must_use]
    pub(in crate::db) const fn expression_lower(slot: usize) -> Self {
        Self::ExpressionLower(slot)
    }

    /// Construct one upper-cased expression order source.
    #[must_use]
    pub(in crate::db) const fn expression_upper(slot: usize) -> Self {
        Self::ExpressionUpper(slot)
    }

    /// Return the canonical source slot already resolved during planning.
    #[must_use]
    pub(in crate::db) const fn slot(self) -> usize {
        match self {
            Self::DirectField(slot) | Self::ExpressionLower(slot) | Self::ExpressionUpper(slot) => {
                slot
            }
        }
    }
}

///
/// ResolvedOrderField
///
/// ResolvedOrderField freezes one planner-validated ORDER BY term.
/// Each field already carries its structural row source and final direction,
/// so executor ordering paths can stay purely consumptive.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ResolvedOrderField {
    source: ResolvedOrderValueSource,
    direction: OrderDirection,
}

impl ResolvedOrderField {
    /// Construct one planner-resolved order field contract.
    #[must_use]
    pub(in crate::db) const fn new(
        source: ResolvedOrderValueSource,
        direction: OrderDirection,
    ) -> Self {
        Self { source, direction }
    }

    /// Borrow the planner-resolved structural row source.
    #[must_use]
    pub(in crate::db) const fn source(self) -> ResolvedOrderValueSource {
        self.source
    }

    /// Borrow the final executor-facing direction for this order term.
    #[must_use]
    pub(in crate::db) const fn direction(self) -> OrderDirection {
        self.direction
    }
}

///
/// ResolvedOrder
///
/// ResolvedOrder freezes the fully resolved structural ORDER BY program.
/// Executor sort and cursor helpers consume this immutable contract without
/// field-name parsing, slot lookup, or model validation at runtime.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ResolvedOrder {
    fields: Vec<ResolvedOrderField>,
}

impl ResolvedOrder {
    /// Construct one planner-owned resolved order program.
    #[must_use]
    pub(in crate::db) const fn new(fields: Vec<ResolvedOrderField>) -> Self {
        Self { fields }
    }

    /// Borrow the frozen order fields in canonical evaluation order.
    #[must_use]
    pub(in crate::db) const fn fields(&self) -> &[ResolvedOrderField] {
        self.fields.as_slice()
    }
}

///
/// StaticPlanningShape
///
/// StaticPlanningShape freezes planner-derived executor metadata that must not
/// be rediscovered from `EntityModel` once execution begins.
/// This keeps projection/order slot reachability and index compile targeting
/// under planner ownership instead of executor-local model scans.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct StaticPlanningShape {
    pub(in crate::db) primary_key_name: &'static str,
    pub(in crate::db) projection_spec: ProjectionSpec,
    pub(in crate::db) execution_preparation_compiled_predicate: Option<PredicateProgram>,
    pub(in crate::db) effective_runtime_compiled_predicate: Option<PredicateProgram>,
    pub(in crate::db) scalar_projection_plan: Option<Vec<ScalarProjectionExpr>>,
    pub(in crate::db) grouped_aggregate_execution_specs: Option<Vec<GroupedAggregateExecutionSpec>>,
    pub(in crate::db) grouped_distinct_execution_strategy: Option<GroupedDistinctExecutionStrategy>,
    pub(in crate::db) projection_direct_slots: Option<Vec<usize>>,
    pub(in crate::db) projection_referenced_slots: Vec<usize>,
    pub(in crate::db) projected_slot_mask: Vec<bool>,
    pub(in crate::db) projection_is_model_identity: bool,
    pub(in crate::db) resolved_order: Option<ResolvedOrder>,
    pub(in crate::db) order_referenced_slots: Option<Vec<usize>>,
    pub(in crate::db) slot_map: Option<Vec<usize>>,
    pub(in crate::db) index_compile_targets: Option<Vec<IndexCompileTarget>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AccessPlannedQuery {
    pub(crate) logical: LogicalPlan,
    pub(crate) access: AccessPlan<Value>,
    pub(crate) projection_selection: ProjectionSelection,
    pub(in crate::db) access_choice: AccessChoiceExplainSnapshot,
    pub(in crate::db) planner_route_profile: PlannerRouteProfile,
    pub(in crate::db) static_planning_shape: Option<StaticPlanningShape>,
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
            static_planning_shape: None,
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
            static_planning_shape: None,
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
            static_planning_shape: _static_planning_shape,
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
            static_planning_shape: None,
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

const fn seeded_planner_route_profile(plan: &AccessPlannedQuery) -> PlannerRouteProfile {
    PlannerRouteProfile::seeded_unfinalized(plan.grouped_plan().is_some())
}
