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
            GroupedExecutionConfig, GroupedPlanStrategyHint,
            expr::{Expr, ProjectionField, ProjectionSpec},
            grouped_distinct_admissibility, grouped_plan_strategy_hint,
            resolve_global_distinct_field_aggregate, validate_grouped_projection_layout,
        },
    },
    error::InternalError,
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
}

///
/// GroupedExecutorHandoff
///
/// Borrowed grouped planning handoff consumed at the query->executor boundary.
/// This contract keeps grouped execution routing input explicit while grouped
/// runtime entry remains explicit at query->executor boundaries.
///

#[derive(Clone)]
pub(in crate::db) struct GroupedExecutorHandoff<'a, K> {
    base: &'a AccessPlannedQuery<K>,
    group_fields: &'a [FieldSlot],
    aggregate_exprs: Vec<AggregateExpr>,
    projection_layout: PlannedProjectionLayout,
    projection_layout_valid: bool,
    grouped_plan_strategy_hint: GroupedPlanStrategyHint,
    grouped_distinct_policy_contract: GroupedDistinctPolicyContract,
    having: Option<&'a GroupHavingSpec>,
    execution: GroupedExecutionConfig,
}

impl<'a, K> GroupedExecutorHandoff<'a, K> {
    /// Borrow the grouped query base plan.
    #[must_use]
    pub(in crate::db) const fn base(&self) -> &'a AccessPlannedQuery<K> {
        self.base
    }

    /// Borrow declared grouped key fields.
    #[must_use]
    pub(in crate::db) const fn group_fields(&self) -> &'a [FieldSlot] {
        self.group_fields
    }

    /// Borrow grouped aggregate expressions derived from planner projection semantics.
    #[must_use]
    pub(in crate::db) const fn aggregate_exprs(&self) -> &[AggregateExpr] {
        self.aggregate_exprs.as_slice()
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

    /// Borrow grouped execution strategy hint projected by planner semantics.
    #[must_use]
    pub(in crate::db) const fn grouped_plan_strategy_hint(&self) -> GroupedPlanStrategyHint {
        self.grouped_plan_strategy_hint
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
pub(in crate::db) fn grouped_executor_handoff<K>(
    plan: &AccessPlannedQuery<K>,
) -> Result<GroupedExecutorHandoff<'_, K>, InternalError> {
    // Grouped handoff is valid only for plans with grouped execution payload.
    let Some(grouped) = plan.grouped_plan() else {
        return Err(InternalError::planner_invariant(
            InternalError::executor_invariant_message(
                "grouped executor handoff requires grouped logical plans",
            ),
        ));
    };
    let projection_spec = plan.projection_spec_for_identity();
    let (projection_layout, aggregate_exprs) =
        planned_projection_layout_and_aggregate_exprs_from_spec(&projection_spec)?;
    let projection_layout_valid = validate_grouped_projection_layout(
        &projection_layout,
        grouped.group.group_fields.len(),
        aggregate_exprs.len(),
    )
    .map(|()| true)?;
    let grouped_plan_strategy_hint = grouped_plan_strategy_hint(plan).ok_or_else(|| {
        InternalError::planner_invariant(InternalError::executor_invariant_message(
            "grouped executor handoff must carry grouped strategy hint for grouped plans",
        ))
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
        aggregate_exprs,
        projection_layout,
        projection_layout_valid,
        grouped_plan_strategy_hint,
        grouped_distinct_policy_contract,
        having: grouped.having.as_ref(),
        execution: grouped.group.execution,
    })
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
            AggregateKind::Exists
            | AggregateKind::Min
            | AggregateKind::Max
            | AggregateKind::First
            | AggregateKind::Last => Err(InternalError::planner_invariant(
                InternalError::executor_invariant_message(
                    "planner grouped DISTINCT strategy handoff must lower only COUNT/SUM field-target aggregates",
                ),
            )),
        },
        Ok(None) => Ok(GroupedDistinctExecutionStrategy::None),
        Err(reason) => Err(InternalError::planner_invariant(
            InternalError::executor_invariant_message(format!(
                "planner grouped DISTINCT strategy handoff must be validated before executor handoff: {}",
                reason.invariant_message()
            )),
        )),
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

// Derive grouped field/aggregate projection slots and aggregate expressions from
// canonical projection semantics.
fn planned_projection_layout_and_aggregate_exprs_from_spec(
    projection_spec: &ProjectionSpec,
) -> Result<(PlannedProjectionLayout, Vec<AggregateExpr>), InternalError> {
    let mut group_field_positions = Vec::new();
    let mut aggregate_positions = Vec::new();
    let mut aggregate_exprs = Vec::new();
    for (index, field) in projection_spec.fields().enumerate() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                let root_expr = expression_without_alias(expr);
                match root_expr {
                    Expr::Field(_) => group_field_positions.push(index),
                    Expr::Aggregate(aggregate_expr) => {
                        aggregate_positions.push(index);
                        aggregate_exprs.push(aggregate_expr.clone());
                    }
                    Expr::Literal(_) | Expr::Unary { .. } | Expr::Binary { .. } => {
                        return Err(InternalError::planner_invariant(
                            InternalError::executor_invariant_message(format!(
                                "grouped projection layout expects only field/aggregate expressions; found non-grouped projection expression at index={index}"
                            )),
                        ));
                    }
                    Expr::Alias { .. } => {
                        return Err(InternalError::planner_invariant(
                            InternalError::executor_invariant_message(
                                "grouped projection layout alias normalization must remove alias wrappers",
                            ),
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
        aggregate_exprs,
    ))
}

// Strip alias wrappers so layout classification uses semantic expression roots.
fn expression_without_alias(mut expr: &Expr) -> &Expr {
    while let Expr::Alias { expr: inner, .. } = expr {
        expr = inner.as_ref();
    }

    expr
}
