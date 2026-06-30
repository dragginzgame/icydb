//! Module: query::plan::access
//! Responsibility: post-planning logical+access composite contracts and builders.
//! Does not own: pure logical plan model definitions or semantic interpretation.
//! Boundary: glue between logical plan semantics and selected access paths.

use crate::db::{
    access::{AccessPath, AccessPlan, AccessShapeFacts, SemanticIndexAccessContract},
    direction::Direction,
    predicate::{CoercionId, CompareOp, IndexCompileTarget, Predicate, PredicateProgram},
    query::plan::{
        AccessChoiceExplainSnapshot, GroupPlan, GroupSpec, GroupedAggregateExecutionSpec,
        GroupedDistinctExecutionStrategy, LogicalPlan, PlannerRouteProfile,
        access_choice::{
            non_index_access_choice_snapshot_for_access_plan,
            project_access_choice_explain_snapshot_with_indexes_and_schema,
            project_access_choice_explain_snapshot_with_semantic_indexes_and_schema,
        },
        expr::{CompiledExpr, Expr, ProjectionSelection, ProjectionSpec},
        model::OrderDirection,
    },
    schema::SchemaInfo,
};
use crate::{
    error::InternalError,
    model::{entity::EntityModel, index::IndexModel},
    traits::KeyValueCodec,
    value::Value,
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
/// Binds one pure `LogicalPlan` to one chosen structural `AccessPlan<Value>`.
///

///
/// ResolvedOrderValueSource
///
/// Planner-resolved structural ORDER BY source for one canonical order term.
/// Executor consumers read this frozen source directly instead of re-parsing
/// field names against the model during sort or cursor evaluation.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ResolvedOrderValueSource {
    DirectField(usize),
    Expression(CompiledExpr),
}

impl ResolvedOrderValueSource {
    /// Construct one direct field-slot order source.
    #[must_use]
    pub(in crate::db) const fn direct_field(slot: usize) -> Self {
        Self::DirectField(slot)
    }

    /// Construct one compiled expression order source.
    #[must_use]
    pub(in crate::db) const fn expression(expr: CompiledExpr) -> Self {
        Self::Expression(expr)
    }

    /// Extend one slot list with every field slot this order source touches.
    pub(in crate::db) fn extend_referenced_slots(&self, referenced: &mut Vec<usize>) {
        match self {
            Self::DirectField(slot) => {
                if !referenced.contains(slot) {
                    referenced.push(*slot);
                }
            }
            Self::Expression(expr) => expr.extend_referenced_slots(referenced),
        }
    }

    /// Return the direct field slot when this frozen order source stays on one
    /// plain field reference.
    #[must_use]
    pub(in crate::db) const fn direct_field_slot(&self) -> Option<usize> {
        match self {
            Self::DirectField(slot) => Some(*slot),
            Self::Expression(_) => None,
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
    pub(in crate::db) const fn source(&self) -> &ResolvedOrderValueSource {
        &self.source
    }

    /// Borrow the final executor-facing direction for this order term.
    #[must_use]
    pub(in crate::db) const fn direction(&self) -> OrderDirection {
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

    /// Return the stable referenced-slot set touched anywhere by this frozen
    /// resolved order contract.
    #[must_use]
    pub(in crate::db) fn referenced_slots(&self) -> Vec<usize> {
        let mut referenced = Vec::new();

        for field in self.fields() {
            field.source().extend_referenced_slots(&mut referenced);
        }

        referenced
    }

    /// Return the direct field-slot list when every order term stays on one
    /// plain field source, preserving canonical term order and duplicates.
    #[must_use]
    pub(in crate::db) fn direct_field_slots(&self) -> Option<Vec<usize>> {
        let mut slots = Vec::with_capacity(self.fields().len());

        for field in self.fields() {
            slots.push(field.source().direct_field_slot()?);
        }

        Some(slots)
    }
}

///
/// StaticExecutionPlanningContract
///
/// StaticExecutionPlanningContract freezes planner-derived executor metadata that must not
/// be rediscovered from `EntityModel` once execution begins.
/// This keeps projection/order slot reachability and index compile targeting
/// under planner ownership instead of executor-local model scans.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct StaticExecutionPlanningContract {
    pub(in crate::db) primary_key_names: Vec<String>,
    pub(in crate::db) projection_spec: ProjectionSpec,
    pub(in crate::db) execution_preparation_predicate: Option<Predicate>,
    pub(in crate::db) execution_preparation_compiled_predicate: Option<PredicateProgram>,
    pub(in crate::db) residual_filter_contract: ResidualFilterContract,
    pub(in crate::db) predicate_pushdown_diagnostics: PredicatePushdownDiagnostics,
    pub(in crate::db) scalar_projection_plan: Option<Vec<CompiledExpr>>,
    pub(in crate::db) grouped_aggregate_execution_specs: Option<Vec<GroupedAggregateExecutionSpec>>,
    pub(in crate::db) grouped_distinct_execution_strategy: Option<GroupedDistinctExecutionStrategy>,
    pub(in crate::db) projection_direct_slots: Option<Vec<usize>>,
    pub(in crate::db) projection_data_row_direct_slots: Option<Vec<usize>>,
    pub(in crate::db) projection_referenced_slots: Vec<usize>,
    pub(in crate::db) projected_slot_mask: Vec<bool>,
    pub(in crate::db) projection_is_model_identity: bool,
    pub(in crate::db) resolved_order: Option<ResolvedOrder>,
    pub(in crate::db) order_referenced_slots: Option<Vec<usize>>,
    pub(in crate::db) slot_map: Option<Vec<usize>>,
    pub(in crate::db) index_compile_targets: Option<Vec<IndexCompileTarget>>,
}

///
/// PlannedNonIndexAccessReason
///
/// PlannedNonIndexAccessReason freezes the planner-owned non-index winner
/// family chosen during access planning before explain rendering begins.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PlannedNonIndexAccessReason {
    IntentKeyAccessOverride,
    PlannerPrimaryKeyLookup,
    PlannerKeySetAccess,
    PlannerPrimaryKeyRange,
    EmptyChildAccessPreferred,
    ConflictingPrimaryKeyChildrenAccessPreferred,
    SingletonPrimaryKeyChildAccessPreferred,
    RequiredOrderPrimaryKeyRangePreferred,
    LimitZeroWindow,
    ConstantFalsePredicate,
    PlannerFullScanFallback,
    PlannerCompositeNonIndex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EffectiveRuntimeFilterKind {
    Predicate(PredicateProgram),
    Expr(CompiledExpr),
}

///
/// EffectiveRuntimeFilterProgram
///
/// EffectiveRuntimeFilterProgram is the single compiled predicate surface used
/// by executor row loops.
/// It hides whether planning produced a predicate-native program or an
/// expression-backed TRUE-only predicate wrapper, so executors only ask the
/// compiled filter to evaluate one row.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct EffectiveRuntimeFilterProgram {
    kind: EffectiveRuntimeFilterKind,
}

impl EffectiveRuntimeFilterProgram {
    /// Wrap one predicate-native runtime filter program.
    #[must_use]
    pub(in crate::db) const fn predicate(program: PredicateProgram) -> Self {
        Self {
            kind: EffectiveRuntimeFilterKind::Predicate(program),
        }
    }

    /// Wrap one expression-backed runtime filter as a TRUE-only predicate.
    #[must_use]
    pub(in crate::db) const fn expression(expr: CompiledExpr) -> Self {
        Self {
            kind: EffectiveRuntimeFilterKind::Expr(expr),
        }
    }

    /// Borrow the predicate-native runtime program when this filter has one.
    #[must_use]
    pub(in crate::db) const fn predicate_program(&self) -> Option<&PredicateProgram> {
        match &self.kind {
            EffectiveRuntimeFilterKind::Predicate(program) => Some(program),
            EffectiveRuntimeFilterKind::Expr(_) => None,
        }
    }

    /// Borrow the expression-backed runtime filter when this filter has one.
    #[must_use]
    pub(in crate::db) const fn expression_filter(&self) -> Option<&CompiledExpr> {
        match &self.kind {
            EffectiveRuntimeFilterKind::Expr(expr) => Some(expr),
            EffectiveRuntimeFilterKind::Predicate(_) => None,
        }
    }

    /// Mark every structural slot referenced by this compiled runtime filter.
    pub(in crate::db) fn mark_referenced_slots(&self, required_slots: &mut [bool]) {
        match &self.kind {
            EffectiveRuntimeFilterKind::Predicate(predicate_program) => {
                predicate_program.mark_referenced_slots(required_slots);
            }
            EffectiveRuntimeFilterKind::Expr(filter_expr) => {
                filter_expr.mark_referenced_slots(required_slots);
            }
        }
    }
}

///
/// ResidualFilterContract
///
/// ResidualFilterContract freezes the planner-facing post-access filter
/// contract. It keeps the visible residual expression, the residual predicate
/// subset, and the compiled runtime filter program together so downstream
/// layers consume one residual-filter shape instead of rejoining loose fields.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ResidualFilterContract {
    residual_filter_expr: Option<Expr>,
    residual_filter_predicate: Option<Predicate>,
    effective_runtime_filter_program: Option<EffectiveRuntimeFilterProgram>,
}

///
/// PredicatePushdownDiagnostics
///
/// PredicatePushdownDiagnostics freezes the planner-owned predicate pushdown
/// label facts consumed by verbose EXPLAIN. This keeps fallback labels tied to
/// the same predicate/access facts that finalized planning used, instead of
/// deriving them later from rendered EXPLAIN predicate trees.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct PredicatePushdownDiagnostics {
    outcome: PredicatePushdownOutcome,
    reason: PredicatePushdownReason,
    access_label: &'static str,
}

///
/// PredicatePushdownOutcome
///
/// Coarse planner-owned predicate pushdown outcome.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PredicatePushdownOutcome {
    None,
    Full,
    Partial,
    Fallback,
}

///
/// PredicatePushdownReason
///
/// Stable planner-owned reason vocabulary for predicate pushdown diagnostics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PredicatePushdownReason {
    NoFilter,
    NoPredicateSubset,
    PredicateSubsetDoesNotCoverExpression,
    AccessPathApplied,
    ResidualAfterAccess,
    NonStrictCompareCoercion,
    StartsWithEmptyPrefix,
    IsNullFullScan,
    TextOperatorFullScan,
    FullScanAccess,
}

impl PredicatePushdownDiagnostics {
    /// Derive one predicate pushdown diagnostics contract from finalized
    /// planner facts without changing route selection or residual execution.
    #[must_use]
    pub(in crate::db) fn from_plan(
        filter_expr_present: bool,
        predicate_covers_filter_expr: bool,
        predicate: Option<&Predicate>,
        access: &AccessPlan<Value>,
        residual_filter_shape: ResidualFilterShape,
    ) -> Self {
        let Some(predicate) = predicate else {
            if filter_expr_present {
                return Self {
                    outcome: PredicatePushdownOutcome::Fallback,
                    reason: PredicatePushdownReason::NoPredicateSubset,
                    access_label: "none",
                };
            }

            return Self::none();
        };

        let access_label = predicate_pushdown_access_label(access);
        if access_label == "full_scan" {
            return Self {
                outcome: PredicatePushdownOutcome::Fallback,
                reason: predicate_pushdown_fallback_reason(predicate),
                access_label,
            };
        }

        let (outcome, reason) = if residual_filter_shape.is_absent() {
            (
                PredicatePushdownOutcome::Full,
                PredicatePushdownReason::AccessPathApplied,
            )
        } else if filter_expr_present && !predicate_covers_filter_expr {
            (
                PredicatePushdownOutcome::Partial,
                PredicatePushdownReason::PredicateSubsetDoesNotCoverExpression,
            )
        } else {
            (
                PredicatePushdownOutcome::Partial,
                PredicatePushdownReason::ResidualAfterAccess,
            )
        };

        Self {
            outcome,
            reason,
            access_label,
        }
    }

    /// Build the no-filter diagnostics contract.
    #[must_use]
    pub(in crate::db) const fn none() -> Self {
        Self {
            outcome: PredicatePushdownOutcome::None,
            reason: PredicatePushdownReason::NoFilter,
            access_label: "none",
        }
    }

    /// Render the stable verbose EXPLAIN label for this diagnostics contract.
    #[must_use]
    pub(in crate::db) fn label(self) -> String {
        match self.outcome {
            PredicatePushdownOutcome::None => "none".to_string(),
            PredicatePushdownOutcome::Full | PredicatePushdownOutcome::Partial => {
                format!("applied({})", self.access_label)
            }
            PredicatePushdownOutcome::Fallback
                if self.reason == PredicatePushdownReason::NoPredicateSubset =>
            {
                "none".to_string()
            }
            PredicatePushdownOutcome::Fallback => {
                format!("fallback({})", self.reason.label())
            }
        }
    }

    /// Return the planner-owned coarse outcome label.
    #[must_use]
    pub(in crate::db) const fn outcome_label(self) -> &'static str {
        self.outcome.label()
    }

    /// Return the planner-owned reason label.
    #[must_use]
    pub(in crate::db) const fn reason_label(self) -> &'static str {
        self.reason.label()
    }

    /// Return whether the selected access path fully proves the predicate.
    #[must_use]
    pub(in crate::db) const fn access_path_fully_applied(self) -> bool {
        matches!(self.outcome, PredicatePushdownOutcome::Full)
            && matches!(self.reason, PredicatePushdownReason::AccessPathApplied)
    }
}

impl PredicatePushdownOutcome {
    /// Stable verbose EXPLAIN outcome label.
    #[must_use]
    pub(in crate::db) const fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Full => "full",
            Self::Partial => "partial",
            Self::Fallback => "fallback",
        }
    }
}

impl PredicatePushdownReason {
    /// Stable verbose EXPLAIN reason label.
    #[must_use]
    pub(in crate::db) const fn label(self) -> &'static str {
        match self {
            Self::NoFilter => "no_filter",
            Self::NoPredicateSubset => "no_predicate_subset",
            Self::PredicateSubsetDoesNotCoverExpression => {
                "predicate_subset_does_not_cover_expression"
            }
            Self::AccessPathApplied => "access_path_applied",
            Self::ResidualAfterAccess => "residual_after_access",
            Self::NonStrictCompareCoercion => "non_strict_compare_coercion",
            Self::StartsWithEmptyPrefix => "starts_with_empty_prefix",
            Self::IsNullFullScan => "is_null_full_scan",
            Self::TextOperatorFullScan => "text_operator_full_scan",
            Self::FullScanAccess => "full_scan",
        }
    }
}

fn predicate_pushdown_access_label(access: &AccessPlan<Value>) -> &'static str {
    match access {
        AccessPlan::Path(path) => predicate_pushdown_access_path_label(path),
        AccessPlan::Union(_) => "union",
        AccessPlan::Intersection(_) => "intersection",
    }
}

const fn predicate_pushdown_access_path_label(path: &AccessPath<Value>) -> &'static str {
    match path {
        AccessPath::ByKey(_) => "by_key",
        AccessPath::ByKeys(keys) if keys.is_empty() => "empty_access_contract",
        AccessPath::ByKeys(_) => "by_keys",
        AccessPath::KeyRange { .. } => "key_range",
        AccessPath::IndexPrefix { .. } => "index_prefix",
        AccessPath::IndexMultiLookup { .. } => "index_multi_lookup",
        AccessPath::IndexBranchSet { .. } => "index_branch_set",
        AccessPath::IndexRange { .. } => "index_range",
        AccessPath::FullScan => "full_scan",
    }
}

fn predicate_pushdown_fallback_reason(predicate: &Predicate) -> PredicatePushdownReason {
    if predicate_contains_non_strict_compare(predicate) {
        return PredicatePushdownReason::NonStrictCompareCoercion;
    }
    if predicate_contains_empty_prefix_starts_with(predicate) {
        return PredicatePushdownReason::StartsWithEmptyPrefix;
    }
    if predicate_contains_is_null(predicate) {
        return PredicatePushdownReason::IsNullFullScan;
    }
    if predicate_contains_text_scan_operator(predicate) {
        return PredicatePushdownReason::TextOperatorFullScan;
    }

    PredicatePushdownReason::FullScanAccess
}

fn predicate_contains(predicate: &Predicate, leaf_matches: fn(&Predicate) -> bool) -> bool {
    if leaf_matches(predicate) {
        return true;
    }

    match predicate {
        Predicate::And(children) | Predicate::Or(children) => children
            .iter()
            .any(|child| predicate_contains(child, leaf_matches)),
        Predicate::Not(inner) => predicate_contains(inner, leaf_matches),
        _ => false,
    }
}

fn predicate_contains_non_strict_compare(predicate: &Predicate) -> bool {
    predicate_contains(predicate, predicate_is_non_strict_compare)
}

fn predicate_is_non_strict_compare(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::Compare(compare) => compare.coercion().id() != CoercionId::Strict,
        Predicate::CompareFields(compare) => compare.coercion().id() != CoercionId::Strict,
        _ => false,
    }
}

fn predicate_contains_empty_prefix_starts_with(predicate: &Predicate) -> bool {
    predicate_contains(predicate, predicate_is_empty_prefix_starts_with)
}

fn predicate_is_empty_prefix_starts_with(predicate: &Predicate) -> bool {
    matches!(
        predicate,
        Predicate::Compare(compare)
            if compare.op() == CompareOp::StartsWith
                && matches!(compare.value(), Value::Text(prefix) if prefix.is_empty())
    )
}

fn predicate_contains_is_null(predicate: &Predicate) -> bool {
    predicate_contains(predicate, predicate_is_null_leaf)
}

const fn predicate_is_null_leaf(predicate: &Predicate) -> bool {
    matches!(predicate, Predicate::IsNull { .. })
}

fn predicate_contains_text_scan_operator(predicate: &Predicate) -> bool {
    predicate_contains(predicate, predicate_is_text_scan_operator)
}

fn predicate_is_text_scan_operator(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::Compare(compare) if compare.op() == CompareOp::EndsWith => true,
        Predicate::TextContains { .. } | Predicate::TextContainsCi { .. } => true,
        _ => false,
    }
}

///
/// ResidualFilterShape
///
/// ResidualFilterShape is the compact diagnostics-facing classification of
/// one finalized residual filter contract. It is derived from planner-owned
/// residual artifacts, not from EXPLAIN rendering strings.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ResidualFilterShape {
    Absent,
    Predicate,
    Expression,
    ExpressionAndPredicate,
}

impl ResidualFilterShape {
    /// Classify one residual filter from expression/predicate presence.
    #[must_use]
    pub(in crate::db) const fn from_presence(has_expr: bool, has_predicate: bool) -> Self {
        match (has_expr, has_predicate) {
            (false, false) => Self::Absent,
            (false, true) => Self::Predicate,
            (true, false) => Self::Expression,
            (true, true) => Self::ExpressionAndPredicate,
        }
    }

    /// Stable diagnostics label for this residual-filter shape.
    #[must_use]
    pub(in crate::db) const fn label(self) -> &'static str {
        match self {
            Self::Absent => "none",
            Self::Predicate => "predicate",
            Self::Expression => "expression",
            Self::ExpressionAndPredicate => "expression_and_predicate",
        }
    }

    /// Return whether no post-access residual filter survives.
    #[must_use]
    pub(in crate::db) const fn is_absent(self) -> bool {
        matches!(self, Self::Absent)
    }
}

impl ResidualFilterContract {
    /// Freeze one post-access residual filter contract.
    #[must_use]
    pub(in crate::db) const fn new(
        residual_filter_expr: Option<Expr>,
        residual_filter_predicate: Option<Predicate>,
        effective_runtime_filter_program: Option<EffectiveRuntimeFilterProgram>,
    ) -> Self {
        Self {
            residual_filter_expr,
            residual_filter_predicate,
            effective_runtime_filter_program,
        }
    }

    /// Borrow the residual semantic expression, when runtime filtering still
    /// needs expression evaluation.
    #[must_use]
    pub(in crate::db) const fn residual_filter_expr(&self) -> Option<&Expr> {
        self.residual_filter_expr.as_ref()
    }

    /// Borrow the residual predicate subset, when runtime filtering can stay
    /// on the predicate-native lane.
    #[must_use]
    pub(in crate::db) const fn residual_filter_predicate(&self) -> Option<&Predicate> {
        self.residual_filter_predicate.as_ref()
    }

    /// Borrow the compiled runtime filter program derived from the residual
    /// expression/predicate shape.
    #[must_use]
    pub(in crate::db) const fn effective_runtime_filter_program(
        &self,
    ) -> Option<&EffectiveRuntimeFilterProgram> {
        self.effective_runtime_filter_program.as_ref()
    }

    /// Return whether any post-access residual filtering survives.
    #[must_use]
    pub(in crate::db) const fn has_residual_filter(&self) -> bool {
        !self.shape().is_absent()
    }

    /// Return the diagnostics-facing residual-filter shape.
    #[must_use]
    pub(in crate::db) const fn shape(&self) -> ResidualFilterShape {
        ResidualFilterShape::from_presence(
            self.residual_filter_expr.is_some(),
            self.residual_filter_predicate.is_some(),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AccessPlannedQuery {
    pub(in crate::db) logical: LogicalPlan,
    pub(in crate::db) access: AccessPlan<Value>,
    pub(in crate::db) projection_selection: ProjectionSelection,
    pub(in crate::db) access_choice: AccessChoiceExplainSnapshot,
    pub(in crate::db) planner_route_profile: PlannerRouteProfile,
    pub(in crate::db) static_execution_planning_contract: Option<StaticExecutionPlanningContract>,
}

impl AccessPlannedQuery {
    /// Construct a minimal access-planned query with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn new(access: AccessPath<Value>, consistency: MissingRowPolicy) -> Self {
        let access = AccessPlan::path(access);
        let logical = LogicalPlan::Scalar(ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency,
        });

        Self::seeded_unfinalized(
            logical,
            access.clone(),
            ProjectionSelection::All,
            if access.has_selected_index_access_path() {
                AccessChoiceExplainSnapshot::selected_index_not_projected()
            } else {
                non_index_access_choice_snapshot_for_access_plan(&access)
            },
        )
    }

    /// Construct one minimal full-scan access-planned query under the access
    /// boundary for runtime tests that only need grouped execution shells.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn full_scan_for_test(consistency: MissingRowPolicy) -> Self {
        Self::new(AccessPath::<Value>::FullScan, consistency)
    }

    // Construct one seeded, unfinalized access-planned query shell so the
    // planner-owned access-choice seed and grouped/scalar route-profile seed
    // are initialized under one local authority.
    const fn seeded_unfinalized(
        logical: LogicalPlan,
        access: AccessPlan<Value>,
        projection_selection: ProjectionSelection,
        access_choice: AccessChoiceExplainSnapshot,
    ) -> Self {
        let planner_route_profile =
            PlannerRouteProfile::seeded_unfinalized(matches!(logical, LogicalPlan::Grouped(_)));

        Self {
            logical,
            access,
            projection_selection,
            access_choice,
            planner_route_profile,
            static_execution_planning_contract: None,
        }
    }

    // Construct one planner-owned seeded query shell when access planning has
    // already frozen a concrete non-index winner reason for the selected route.
    fn seeded_from_planned_selection(
        logical: LogicalPlan,
        access: AccessPlan<Value>,
        projection_selection: ProjectionSelection,
        planned_non_index_reason: Option<PlannedNonIndexAccessReason>,
    ) -> Self {
        let access_choice = if access.has_selected_index_access_path() {
            AccessChoiceExplainSnapshot::selected_index_not_projected()
        } else if let Some(reason) = planned_non_index_reason {
            AccessChoiceExplainSnapshot::from_planned_non_index_reason(reason)
        } else {
            non_index_access_choice_snapshot_for_access_plan(&access)
        };

        Self::seeded_unfinalized(logical, access, projection_selection, access_choice)
    }

    /// Construct an access-planned query from logical + access + projection stages.
    #[must_use]
    pub(in crate::db) fn from_logical_access_and_projection<K>(
        logical: LogicalPlan,
        access: AccessPlan<K>,
        projection_selection: ProjectionSelection,
    ) -> Self
    where
        K: KeyValueCodec,
    {
        let access = access.into_value_plan();

        Self::seeded_unfinalized(
            logical,
            access.clone(),
            projection_selection,
            if access.has_selected_index_access_path() {
                AccessChoiceExplainSnapshot::selected_index_not_projected()
            } else {
                non_index_access_choice_snapshot_for_access_plan(&access)
            },
        )
    }

    /// Construct an access-planned query from planner-owned access selection.
    #[must_use]
    pub(in crate::db::query) fn from_planned_access_with_projection<K>(
        logical: LogicalPlan,
        access: AccessPlan<K>,
        projection_selection: ProjectionSelection,
        planned_non_index_reason: Option<PlannedNonIndexAccessReason>,
    ) -> Self
    where
        K: KeyValueCodec,
    {
        let access = access.into_value_plan();

        Self::seeded_from_planned_selection(
            logical,
            access,
            projection_selection,
            planned_non_index_reason,
        )
    }

    /// Convert this plan into grouped logical form with one explicit group spec.
    #[must_use]
    pub(in crate::db) fn into_grouped(self, group: GroupSpec) -> Self {
        self.into_grouped_with_having_expr(group, None)
    }

    /// Convert this plan into grouped logical form with explicit grouped HAVING expression.
    #[must_use]
    pub(in crate::db) fn into_grouped_with_having_expr(
        self,
        group: GroupSpec,
        having_expr: Option<crate::db::query::plan::expr::Expr>,
    ) -> Self {
        let Self {
            logical,
            access,
            projection_selection,
            access_choice,
            planner_route_profile: _planner_route_profile,
            static_execution_planning_contract: _static_execution_planning_contract,
        } = self;
        let scalar = match logical {
            LogicalPlan::Scalar(plan) => plan,
            LogicalPlan::Grouped(plan) => plan.scalar,
        };

        Self::seeded_unfinalized(
            LogicalPlan::Grouped(GroupPlan {
                scalar,
                group,
                having_expr,
            }),
            access,
            projection_selection,
            access_choice,
        )
    }

    /// Return a copy with grouped execution caps replaced when the plan is grouped.
    #[must_use]
    #[cfg(all(test, feature = "sql"))]
    pub(in crate::db) fn with_grouped_execution_config(
        mut self,
        execution: crate::db::query::plan::GroupedExecutionConfig,
    ) -> Self {
        if let LogicalPlan::Grouped(grouped) = &mut self.logical {
            grouped.group.execution = execution;
            self.static_execution_planning_contract = None;
        }

        self
    }

    /// Project route-facing access-shape facts directly from the chosen access plan.
    #[must_use]
    pub(in crate::db) fn access_shape_facts(&self) -> AccessShapeFacts {
        self.access.shape_facts()
    }

    /// Borrow the planner-owned access-choice diagnostics snapshot.
    #[must_use]
    pub(in crate::db) const fn access_choice(&self) -> &AccessChoiceExplainSnapshot {
        &self.access_choice
    }

    /// Freeze one standalone model-only explain access-choice snapshot for the
    /// caller-visible index slice after normal planning has already selected
    /// the winner.
    pub(in crate::db) fn finalize_access_choice_for_model_only_with_indexes(
        &mut self,
        model: &EntityModel,
        generated_model_only_indexes: &[&'static IndexModel],
    ) {
        self.finalize_access_choice_for_model_with_indexes_and_schema(
            model,
            generated_model_only_indexes,
            SchemaInfo::cached_for_generated_entity_model(model),
        );
    }

    /// Freeze one explain-only access-choice snapshot with explicit schema
    /// authority.
    pub(in crate::db) fn finalize_access_choice_for_model_with_indexes_and_schema(
        &mut self,
        model: &EntityModel,
        generated_model_only_indexes: &[&'static IndexModel],
        schema_info: &SchemaInfo,
    ) {
        if !self.access.has_selected_index_access_path() {
            return;
        }

        self.access_choice = project_access_choice_explain_snapshot_with_indexes_and_schema(
            model,
            generated_model_only_indexes,
            schema_info,
            self,
        );
    }

    /// Freeze one explain-only access-choice snapshot using already-projected
    /// semantic index contracts from the visible-index boundary.
    pub(in crate::db) fn finalize_access_choice_for_model_with_semantic_indexes_and_schema(
        &mut self,
        model: &EntityModel,
        semantic_indexes: &[SemanticIndexAccessContract],
        schema_info: &SchemaInfo,
    ) {
        if !self.access.has_selected_index_access_path() {
            return;
        }

        self.access_choice =
            project_access_choice_explain_snapshot_with_semantic_indexes_and_schema(
                model,
                semantic_indexes,
                schema_info,
                self,
            );
    }

    /// Borrow the frozen planner-owned route profile.
    #[must_use]
    pub(in crate::db) const fn planner_route_profile(&self) -> &PlannerRouteProfile {
        &self.planner_route_profile
    }

    /// Return whether any residual predicate or residual expression survives access planning.
    #[must_use]
    pub(in crate::db) fn has_any_residual_filter(&self) -> bool {
        !self.residual_filter_shape().is_absent()
    }

    /// Return whether the scalar plan carries no DISTINCT execution gate.
    #[must_use]
    pub(in crate::db) const fn has_no_distinct(&self) -> bool {
        !self.scalar_plan().distinct
    }

    /// Clone this access-planned query while removing only scalar pagination.
    ///
    /// Projection DISTINCT execution uses this shape to scan the full ordered
    /// candidate stream, then applies the original page window after projected
    /// row deduplication.
    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) fn clone_without_scalar_page(&self) -> Self {
        let mut plan = self.clone();
        match &mut plan.logical {
            LogicalPlan::Scalar(scalar) => scalar.page = None,
            LogicalPlan::Grouped(grouped) => grouped.scalar.page = None,
        }

        plan
    }

    /// Return the canonical scan direction for unordered plans or primary-key-only ordering.
    #[must_use]
    pub(in crate::db) fn unordered_or_primary_key_order_direction(&self) -> Option<Direction> {
        let Some(order) = self.scalar_plan().order.as_ref() else {
            return Some(Direction::Asc);
        };

        let primary_key_names = self.primary_key_names().ok()?;
        order
            .primary_key_only_direction_fields(primary_key_names.as_slice())
            .map(|direction| match direction {
                OrderDirection::Asc => Direction::Asc,
                OrderDirection::Desc => Direction::Desc,
            })
    }

    /// Return the maximum number of direct data rows worth staging before the
    /// final cursorless page window runs.
    #[must_use]
    pub(in crate::db) fn direct_data_row_keep_cap(&self) -> Option<usize> {
        let page = self.scalar_plan().page.as_ref()?;
        let limit = page.limit?;
        let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);

        Some(offset.saturating_add(limit))
    }

    /// Borrow the planner-frozen resolved ORDER BY program or return one executor invariant error.
    pub(in crate::db) fn require_resolved_order(&self) -> Result<&ResolvedOrder, InternalError> {
        self.resolved_order()
            .ok_or_else(InternalError::query_executor_invariant)
    }

    /// Attach one frozen planner-owned route profile.
    pub(in crate::db) fn set_planner_route_profile(
        &mut self,
        planner_route_profile: PlannerRouteProfile,
    ) {
        self.planner_route_profile = planner_route_profile;
    }
}
