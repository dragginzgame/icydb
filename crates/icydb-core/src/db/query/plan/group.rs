//! Module: query::plan::group
//! Responsibility: grouped-plan handoff contract between query planning and executor.
//! Does not own: grouped runtime execution logic.
//! Boundary: explicit grouped query-to-executor transfer surface.

use crate::{
    db::query::{
        builder::AggregateExpr,
        plan::{
            AccessPlannedQuery, AggregateKind, FieldSlot, GroupAggregateSpec, GroupHavingSpec,
            GroupedExecutionConfig,
            expr::{Expr, ProjectionField, ProjectionSpec},
            resolve_global_distinct_field_aggregate,
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
    pub group_field_positions: Vec<usize>,
    pub aggregate_positions: Vec<usize>,
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
    distinct_execution_strategy: GroupedDistinctExecutionStrategy,
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

    /// Borrow grouped DISTINCT execution strategy lowered by planner.
    #[must_use]
    pub(in crate::db) const fn distinct_execution_strategy(
        &self,
    ) -> &GroupedDistinctExecutionStrategy {
        &self.distinct_execution_strategy
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
        return Err(invariant(
            "grouped executor handoff requires grouped logical plans",
        ));
    };
    let projection_spec = plan.projection_spec_for_identity();
    let (projection_layout, aggregate_exprs) =
        planned_projection_layout_and_aggregate_exprs_from_spec(&projection_spec)?;
    let distinct_execution_strategy = grouped_distinct_execution_strategy(
        grouped.group.group_fields.as_slice(),
        grouped.group.aggregates.as_slice(),
        grouped.having.as_ref(),
    )?;

    Ok(GroupedExecutorHandoff {
        base: plan,
        group_fields: grouped.group.group_fields.as_slice(),
        aggregate_exprs,
        projection_layout,
        distinct_execution_strategy,
        having: grouped.having.as_ref(),
        execution: grouped.group.execution,
    })
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
    GlobalDistinctFieldAggregate {
        kind: AggregateKind,
        target_field: String,
    },
}

// Lower grouped DISTINCT execution strategy from validated grouped planner semantics.
fn grouped_distinct_execution_strategy(
    group_fields: &[FieldSlot],
    aggregates: &[GroupAggregateSpec],
    having: Option<&GroupHavingSpec>,
) -> Result<GroupedDistinctExecutionStrategy, InternalError> {
    match resolve_global_distinct_field_aggregate(group_fields, aggregates, having) {
        Ok(Some(aggregate)) => Ok(
            GroupedDistinctExecutionStrategy::GlobalDistinctFieldAggregate {
                kind: aggregate.kind(),
                target_field: aggregate.target_field().to_string(),
            },
        ),
        Ok(None) => Ok(GroupedDistinctExecutionStrategy::None),
        Err(reason) => Err(invariant(format!(
            "planner grouped DISTINCT strategy handoff must be validated before executor handoff: {}",
            reason.invariant_message()
        ))),
    }
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
                        return Err(invariant(format!(
                            "grouped projection layout expects only field/aggregate expressions; found non-grouped projection expression at index={index}"
                        )));
                    }
                    Expr::Alias { .. } => {
                        return Err(invariant(
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

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::planner_invariant(InternalError::executor_invariant_message(message))
}
