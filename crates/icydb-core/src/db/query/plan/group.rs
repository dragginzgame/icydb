//! Module: query::plan::group
//! Responsibility: grouped-plan handoff contract between query planning and executor.
//! Does not own: grouped runtime execution logic.
//! Boundary: explicit grouped query-to-executor transfer surface.

use crate::{
    db::query::{
        builder::AggregateExpr,
        plan::{
            AccessPlannedQuery, AggregateKind, FieldSlot, GroupAggregateSpec,
            GroupDistinctAdmissibility, GroupDistinctPolicyReason, GroupHavingSpec,
            GroupedExecutionConfig, GroupedPlanStrategy,
            expr::{Expr, ProjectionField, ProjectionSpec},
            grouped_distinct_admissibility, grouped_plan_strategy,
            resolve_aggregate_target_field_slot, resolve_global_distinct_field_aggregate,
            validate_grouped_projection_layout,
        },
    },
    error::InternalError,
    model::entity::EntityModel,
};

///
/// PlannedProjectionLayout
///
/// Planner-owned grouped projection position layout transferred to executor.
/// Positions are derived only from `ProjectionSpec` semantic shape and preserve
/// projection declaration order for grouped fields and aggregates.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PlannedProjectionLayout {
    pub(in crate::db) group_field_positions: Vec<usize>,
    pub(in crate::db) aggregate_positions: Vec<usize>,
}

///
/// GroupedAggregateExecutionSpec
///
/// GroupedAggregateExecutionSpec carries one planner-lowered grouped aggregate
/// execution contract into grouped route/runtime stages.
/// This keeps grouped target-slot resolution structural so grouped execution
/// does not rediscover field-target inputs inside runtime loops.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct GroupedAggregateExecutionSpec {
    kind: AggregateKind,
    target_field: Option<FieldSlot>,
    projection_target_field: Option<String>,
    distinct: bool,
}

///
/// GroupedAggregateProjectionSpec
///
/// Planner-owned grouped aggregate projection contract.
/// This carries only the semantic grouped aggregate identity that grouped
/// output projection needs to match finalized aggregate values back onto
/// projection expressions, without retaining full builder-layer
/// `AggregateExpr` payloads in grouped runtime carriage.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct GroupedAggregateProjectionSpec {
    kind: AggregateKind,
    target_field: Option<String>,
    distinct: bool,
}

impl GroupedAggregateProjectionSpec {
    /// Build one grouped aggregate projection spec from one semantic aggregate expression.
    #[must_use]
    pub(in crate::db) fn from_aggregate_expr(aggregate_expr: &AggregateExpr) -> Self {
        Self {
            kind: aggregate_expr.kind(),
            target_field: aggregate_expr.target_field().map(str::to_string),
            distinct: aggregate_expr.is_distinct(),
        }
    }

    /// Return the grouped aggregate kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> AggregateKind {
        self.kind
    }

    /// Borrow the optional grouped aggregate target field label.
    #[must_use]
    pub(in crate::db) fn target_field(&self) -> Option<&str> {
        self.target_field.as_deref()
    }

    /// Return whether the grouped aggregate uses DISTINCT semantics.
    #[must_use]
    pub(in crate::db) const fn distinct(&self) -> bool {
        self.distinct
    }
}

impl GroupedAggregateExecutionSpec {
    /// Build one grouped aggregate execution spec from one grouped aggregate
    /// projection spec and one structural model context.
    pub(in crate::db) fn from_projection_spec_with_model(
        model: &'static EntityModel,
        aggregate_projection_spec: &GroupedAggregateProjectionSpec,
    ) -> Result<Self, InternalError> {
        let target_field = aggregate_projection_spec
            .target_field()
            .map(|field| {
                resolve_aggregate_target_field_slot(model, field).map_err(|err| {
                    InternalError::planner_executor_invariant(format!(
                        "grouped aggregate execution target slot resolution failed: field='{field}', error={err}",
                    ))
                })
            })
            .transpose()?;

        Ok(Self {
            kind: aggregate_projection_spec.kind(),
            target_field,
            projection_target_field: aggregate_projection_spec.target_field().map(str::to_string),
            distinct: aggregate_projection_spec.distinct(),
        })
    }

    /// Return the grouped aggregate kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> AggregateKind {
        self.kind
    }

    /// Borrow the optional grouped aggregate target slot.
    #[must_use]
    pub(in crate::db) const fn target_field(&self) -> Option<&FieldSlot> {
        self.target_field.as_ref()
    }

    /// Borrow the optional grouped aggregate target field label used for
    /// grouped projection/expression matching.
    #[must_use]
    pub(in crate::db) fn projection_target_field(&self) -> Option<&str> {
        self.projection_target_field.as_deref()
    }

    /// Return whether the grouped aggregate uses DISTINCT semantics.
    #[must_use]
    pub(in crate::db) const fn distinct(&self) -> bool {
        self.distinct
    }

    /// Return whether one aggregate expression matches this grouped execution spec semantically.
    #[must_use]
    pub(in crate::db) fn matches_aggregate_expr(&self, aggregate_expr: &AggregateExpr) -> bool {
        self.kind == aggregate_expr.kind()
            && self.projection_target_field() == aggregate_expr.target_field()
            && self.distinct == aggregate_expr.is_distinct()
    }

    /// Build one grouped aggregate execution spec directly for tests that do
    /// not carry a model-owned grouped lowering context.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn from_parts_for_test(
        kind: AggregateKind,
        target_field: Option<FieldSlot>,
        projection_target_field: Option<&str>,
        distinct: bool,
    ) -> Self {
        Self {
            kind,
            target_field,
            projection_target_field: projection_target_field.map(str::to_string),
            distinct,
        }
    }
}

impl PlannedProjectionLayout {
    /// Borrow grouped field positions in projection declaration order.
    #[must_use]
    pub(in crate::db) const fn group_field_positions(&self) -> &[usize] {
        self.group_field_positions.as_slice()
    }

    /// Borrow aggregate positions in projection declaration order.
    #[must_use]
    pub(in crate::db) const fn aggregate_positions(&self) -> &[usize] {
        self.aggregate_positions.as_slice()
    }

    /// Construct one grouped layout invariant for mismatched grouped-field counts.
    pub(in crate::db) fn group_field_count_mismatch(
        layout_count: usize,
        handoff_count: usize,
    ) -> InternalError {
        InternalError::planner_executor_invariant(format!(
            "grouped projection layout group-field count mismatch: layout={layout_count}, handoff={handoff_count}",
        ))
    }

    /// Construct one grouped layout invariant for mismatched aggregate counts.
    pub(in crate::db) fn aggregate_count_mismatch(
        layout_count: usize,
        handoff_count: usize,
    ) -> InternalError {
        InternalError::planner_executor_invariant(format!(
            "grouped projection layout aggregate count mismatch: layout={layout_count}, handoff={handoff_count}",
        ))
    }

    /// Construct one grouped layout invariant for non-monotonic grouped-field positions.
    pub(in crate::db) fn group_field_positions_not_strictly_increasing() -> InternalError {
        InternalError::planner_executor_invariant(
            "grouped projection layout group-field positions must be strictly increasing",
        )
    }

    /// Construct one grouped layout invariant for non-monotonic aggregate positions.
    pub(in crate::db) fn aggregate_positions_not_strictly_increasing() -> InternalError {
        InternalError::planner_executor_invariant(
            "grouped projection layout aggregate positions must be strictly increasing",
        )
    }

    /// Construct one grouped layout invariant for mixed field/aggregate ordering.
    pub(in crate::db) fn group_fields_must_precede_aggregates() -> InternalError {
        InternalError::planner_executor_invariant(
            "grouped projection layout must keep group fields before aggregate terminals",
        )
    }

    /// Construct one grouped layout invariant for runtime projection splits
    /// that reference a layout position outside the projected value buffer.
    pub(in crate::db) fn projected_position_out_of_bounds(
        position_kind: &str,
        position: usize,
        projected_len: usize,
    ) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped projection layout {position_kind} position out of bounds: position={position}, projected_len={projected_len}",
        ))
    }
}

///
/// GroupedExecutorHandoff
///
/// Borrowed grouped planning handoff consumed at the query->executor boundary.
/// This contract keeps grouped execution routing input explicit while grouped
/// runtime entry remains explicit at query->executor boundaries.
///

#[derive(Clone)]
pub(in crate::db) struct GroupedExecutorHandoff<'a> {
    base: &'a AccessPlannedQuery,
    group_fields: &'a [FieldSlot],
    aggregate_projection_specs: Vec<GroupedAggregateProjectionSpec>,
    projection_layout: PlannedProjectionLayout,
    projection_layout_valid: bool,
    grouped_plan_strategy: GroupedPlanStrategy,
    grouped_distinct_policy_contract: GroupedDistinctPolicyContract,
    having: Option<&'a GroupHavingSpec>,
    execution: GroupedExecutionConfig,
}

impl<'a> GroupedExecutorHandoff<'a> {
    /// Borrow the grouped query base plan.
    #[must_use]
    pub(in crate::db) const fn base(&self) -> &'a AccessPlannedQuery {
        self.base
    }

    /// Borrow declared grouped key fields.
    #[must_use]
    pub(in crate::db) const fn group_fields(&self) -> &'a [FieldSlot] {
        self.group_fields
    }

    /// Borrow grouped aggregate projection specs derived from planner projection semantics.
    #[must_use]
    pub(in crate::db) const fn aggregate_projection_specs(
        &self,
    ) -> &[GroupedAggregateProjectionSpec] {
        self.aggregate_projection_specs.as_slice()
    }

    /// Borrow grouped projection position layout derived by planner.
    #[must_use]
    pub(in crate::db) const fn projection_layout(&self) -> &PlannedProjectionLayout {
        &self.projection_layout
    }

    /// Return whether planner already validated grouped projection layout invariants.
    #[must_use]
    pub(in crate::db) const fn projection_layout_valid(&self) -> bool {
        self.projection_layout_valid
    }

    /// Borrow grouped execution strategy projected by planner semantics.
    #[must_use]
    pub(in crate::db) const fn grouped_plan_strategy(&self) -> GroupedPlanStrategy {
        self.grouped_plan_strategy
    }

    /// Borrow grouped DISTINCT execution strategy lowered by planner.
    #[must_use]
    pub(in crate::db) const fn distinct_execution_strategy(
        &self,
    ) -> &GroupedDistinctExecutionStrategy {
        self.grouped_distinct_policy_contract.execution_strategy()
    }

    /// Borrow grouped DISTINCT policy violation reason for executor boundaries.
    #[must_use]
    pub(in crate::db) const fn distinct_policy_violation_for_executor(
        &self,
    ) -> Option<GroupDistinctPolicyReason> {
        self.grouped_distinct_policy_contract
            .violation_for_executor()
    }

    /// Borrow grouped HAVING clause specification when present.
    #[must_use]
    pub(in crate::db) const fn having(&self) -> Option<&'a GroupHavingSpec> {
        self.having
    }

    /// Borrow grouped execution hard-limit policy selected by planning.
    #[must_use]
    pub(in crate::db) const fn execution(&self) -> GroupedExecutionConfig {
        self.execution
    }
}

/// Build one grouped executor handoff from one grouped logical plan.
pub(in crate::db) fn grouped_executor_handoff(
    plan: &AccessPlannedQuery,
) -> Result<GroupedExecutorHandoff<'_>, InternalError> {
    // Grouped handoff is valid only for plans with grouped execution payload.
    let Some(grouped) = plan.grouped_plan() else {
        return Err(InternalError::planner_executor_invariant(
            "grouped executor handoff requires grouped logical plans",
        ));
    };
    let projection_spec = plan.projection_spec_for_identity();
    let (projection_layout, aggregate_projection_specs) =
        planned_projection_layout_and_aggregate_projection_specs_from_spec(&projection_spec)?;
    let projection_layout_valid = validate_grouped_projection_layout(
        &projection_layout,
        grouped.group.group_fields.len(),
        aggregate_projection_specs.len(),
    )
    .map(|()| true)?;
    let grouped_plan_strategy = grouped_plan_strategy(plan).ok_or_else(|| {
        InternalError::planner_executor_invariant(
            "grouped executor handoff must carry grouped strategy for grouped plans",
        )
    })?;
    let grouped_distinct_policy_contract = grouped_distinct_policy_contract(
        grouped.scalar.distinct,
        grouped.having.is_some(),
        grouped.group.group_fields.as_slice(),
        grouped.group.aggregates.as_slice(),
        grouped.having.as_ref(),
    )?;

    Ok(GroupedExecutorHandoff {
        base: plan,
        group_fields: grouped.group.group_fields.as_slice(),
        aggregate_projection_specs,
        projection_layout,
        projection_layout_valid,
        grouped_plan_strategy,
        grouped_distinct_policy_contract,
        having: grouped.having.as_ref(),
        execution: grouped.group.execution,
    })
}

/// Build grouped aggregate execution specs from planner-owned aggregate
/// projection specs and one structural model context.
pub(in crate::db) fn grouped_aggregate_execution_specs_with_model(
    model: &'static EntityModel,
    aggregate_projection_specs: &[GroupedAggregateProjectionSpec],
) -> Result<Vec<GroupedAggregateExecutionSpec>, InternalError> {
    aggregate_projection_specs
        .iter()
        .map(|aggregate_projection_spec| {
            GroupedAggregateExecutionSpec::from_projection_spec_with_model(
                model,
                aggregate_projection_spec,
            )
        })
        .collect()
}

///
/// GroupedDistinctPolicyContract
///
/// Planner-projected grouped DISTINCT policy contract for executor boundaries.
/// Route/load layers consume this contract directly instead of re-deriving
/// grouped DISTINCT policy from logical plan internals.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct GroupedDistinctPolicyContract {
    violation_for_executor: Option<GroupDistinctPolicyReason>,
    execution_strategy: GroupedDistinctExecutionStrategy,
}

impl GroupedDistinctPolicyContract {
    /// Construct one grouped DISTINCT policy contract.
    #[must_use]
    const fn new(
        violation_for_executor: Option<GroupDistinctPolicyReason>,
        execution_strategy: GroupedDistinctExecutionStrategy,
    ) -> Self {
        Self {
            violation_for_executor,
            execution_strategy,
        }
    }

    /// Borrow grouped DISTINCT execution strategy lowered by planner.
    #[must_use]
    pub(in crate::db) const fn execution_strategy(&self) -> &GroupedDistinctExecutionStrategy {
        &self.execution_strategy
    }

    /// Borrow grouped DISTINCT policy violation reason for executor boundaries.
    #[must_use]
    pub(in crate::db) const fn violation_for_executor(&self) -> Option<GroupDistinctPolicyReason> {
        self.violation_for_executor
    }
}

///
/// GroupedDistinctExecutionStrategy
///
/// Planner-owned grouped DISTINCT execution strategy lowered for executor consumption.
/// This strategy is mechanical-only and must not be re-derived by executor policy checks.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum GroupedDistinctExecutionStrategy {
    None,
    GlobalDistinctFieldCount { target_field: String },
    GlobalDistinctFieldSum { target_field: String },
    GlobalDistinctFieldAvg { target_field: String },
}

// Lower grouped DISTINCT execution strategy from validated grouped planner semantics.
fn grouped_distinct_execution_strategy(
    group_fields: &[FieldSlot],
    aggregates: &[GroupAggregateSpec],
    having: Option<&GroupHavingSpec>,
) -> Result<GroupedDistinctExecutionStrategy, InternalError> {
    match resolve_global_distinct_field_aggregate(group_fields, aggregates, having) {
        Ok(Some(aggregate)) => match aggregate.kind() {
            AggregateKind::Count => {
                Ok(GroupedDistinctExecutionStrategy::GlobalDistinctFieldCount {
                    target_field: aggregate.target_field().to_string(),
                })
            }
            AggregateKind::Sum => Ok(GroupedDistinctExecutionStrategy::GlobalDistinctFieldSum {
                target_field: aggregate.target_field().to_string(),
            }),
            AggregateKind::Avg => Ok(GroupedDistinctExecutionStrategy::GlobalDistinctFieldAvg {
                target_field: aggregate.target_field().to_string(),
            }),
            AggregateKind::Exists
            | AggregateKind::Min
            | AggregateKind::Max
            | AggregateKind::First
            | AggregateKind::Last => Err(InternalError::planner_executor_invariant(
                "planner grouped DISTINCT strategy handoff must lower only COUNT/SUM/AVG field-target aggregates",
            )),
        },
        Ok(None) => Ok(GroupedDistinctExecutionStrategy::None),
        Err(reason) => Err(reason.into_planner_handoff_internal_error()),
    }
}

// Build grouped DISTINCT executor policy contract from validated grouped semantics.
fn grouped_distinct_policy_contract(
    scalar_distinct: bool,
    has_having: bool,
    group_fields: &[FieldSlot],
    aggregates: &[GroupAggregateSpec],
    having: Option<&GroupHavingSpec>,
) -> Result<GroupedDistinctPolicyContract, InternalError> {
    let violation_for_executor = match grouped_distinct_admissibility(scalar_distinct, has_having) {
        GroupDistinctAdmissibility::Allowed => None,
        GroupDistinctAdmissibility::Disallowed(reason) => Some(reason),
    };
    let execution_strategy = grouped_distinct_execution_strategy(group_fields, aggregates, having)?;

    Ok(GroupedDistinctPolicyContract::new(
        violation_for_executor,
        execution_strategy,
    ))
}

// Derive grouped field/aggregate projection slots and grouped aggregate
// projection specs from canonical projection semantics.
fn planned_projection_layout_and_aggregate_projection_specs_from_spec(
    projection_spec: &ProjectionSpec,
) -> Result<(PlannedProjectionLayout, Vec<GroupedAggregateProjectionSpec>), InternalError> {
    let mut group_field_positions = Vec::new();
    let mut aggregate_positions = Vec::new();
    let mut aggregate_projection_specs = Vec::new();
    for (index, field) in projection_spec.fields().enumerate() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                let root_expr = expression_without_alias(expr);
                match root_expr {
                    Expr::Field(_) => group_field_positions.push(index),
                    Expr::Aggregate(aggregate_expr) => {
                        aggregate_positions.push(index);
                        aggregate_projection_specs.push(
                            GroupedAggregateProjectionSpec::from_aggregate_expr(aggregate_expr),
                        );
                    }
                    Expr::Literal(_) | Expr::Unary { .. } | Expr::Binary { .. } => {
                        return Err(InternalError::planner_executor_invariant(format!(
                            "grouped projection layout expects only field/aggregate expressions; found non-grouped projection expression at index={index}",
                        )));
                    }
                    Expr::Alias { .. } => {
                        return Err(InternalError::planner_executor_invariant(
                            "grouped projection layout alias normalization must remove alias wrappers",
                        ));
                    }
                }
            }
        }
    }

    Ok((
        PlannedProjectionLayout {
            group_field_positions,
            aggregate_positions,
        },
        aggregate_projection_specs,
    ))
}

// Strip alias wrappers so layout classification uses semantic expression roots.
fn expression_without_alias(mut expr: &Expr) -> &Expr {
    while let Expr::Alias { expr: inner, .. } = expr {
        expr = inner.as_ref();
    }

    expr
}
