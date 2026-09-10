//! Module: query::plan::logical_builder
//! Responsibility: construct logical planning inputs and logical plan contracts from query intent.
//! Does not own: access-path planning heuristics or runtime executor routing.
//! Boundary: emits planner-domain logical plan structures prior to access planning.

#[cfg(test)]
mod tests;

use crate::db::{
    QueryError,
    predicate::{MissingRowPolicy, Predicate},
    query::plan::{
        DeleteLimitSpec, GroupAggregateSpec, GroupPlan, GroupSpec, LogicalPlan, OrderDirection,
        OrderSpec, OrderTerm, PageSpec, QueryMode, ScalarPlan, expr::Expr,
    },
    query::preparation::PreparationWork,
    schema::SchemaInfo,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

///
/// LogicalPlanningInputs
///
/// Borrowed logical-planning input contract projected from query intent.
/// Carries mode and shape declarations independent of access-path selection.
/// Logical planning consumes this contract together with normalized predicates.
///

#[derive(Debug)]
pub(in crate::db::query) struct LogicalPlanningInputs<'a> {
    mode: QueryMode,
    filter_expr: Option<&'a Expr>,
    filter_predicate_covers_expr: bool,
    order: Option<&'a OrderSpec>,
    distinct: bool,
    group: Option<&'a GroupSpec>,
    having_expr: Option<&'a Expr>,
}

impl<'a> LogicalPlanningInputs<'a> {
    /// Build logical-planning inputs from intent-projected shape values.
    #[must_use]
    pub(in crate::db::query) const fn new(
        mode: QueryMode,
        filter_expr: Option<&'a Expr>,
        filter_predicate_covers_expr: bool,
        order: Option<&'a OrderSpec>,
        distinct: bool,
        group: Option<&'a GroupSpec>,
        having_expr: Option<&'a Expr>,
    ) -> Self {
        Self {
            mode,
            filter_expr,
            filter_predicate_covers_expr,
            order,
            distinct,
            group,
            having_expr,
        }
    }

    /// Drop the semantic scalar filter expression when a stronger access
    /// contract already proves the same exact primary-key semantics.
    #[must_use]
    pub(in crate::db::query) const fn without_filter_expr(mut self) -> Self {
        self.filter_expr = None;
        self.filter_predicate_covers_expr = false;
        self
    }

    #[must_use]
    pub(in crate::db::query) const fn has_filter_expr(&self) -> bool {
        self.filter_expr.is_some()
    }

    #[must_use]
    pub(in crate::db::query) const fn filter_predicate_covers_expr(&self) -> bool {
        self.filter_predicate_covers_expr
    }

    #[must_use]
    pub(in crate::db::query) const fn distinct(&self) -> bool {
        self.distinct
    }

    #[must_use]
    pub(in crate::db::query) const fn has_group(&self) -> bool {
        self.group.is_some()
    }

    #[must_use]
    pub(in crate::db::query) const fn has_having_expr(&self) -> bool {
        self.having_expr.is_some()
    }
}

///
/// LogicalQuery
///
/// Plan-owned normalized logical query contract assembled from query intent.
/// This DTO captures logical query semantics before access-path selection is
/// coupled into one `AccessPlannedQuery`.
///

#[derive(Clone, Debug)]
pub(in crate::db::query) struct LogicalQuery {
    pub(in crate::db::query) mode: QueryMode,
    pub(in crate::db::query) filter_expr: Option<Expr>,
    pub(in crate::db::query) filter_predicate_covers_expr: bool,
    pub(in crate::db::query) normalized_predicate: Option<Predicate>,
    pub(in crate::db::query) order: Option<OrderSpec>,
    pub(in crate::db::query) distinct: bool,
    pub(in crate::db::query) group: Option<GroupSpec>,
    pub(in crate::db::query) having_expr: Option<Expr>,
    pub(in crate::db::query) consistency: MissingRowPolicy,
}

/// Materialize borrowed clauses only when a plan needs them, under the current
/// request. Shape-only inspections and stripped filters never allocate copies.
pub(in crate::db::query) fn logical_query_from_logical_inputs(
    inputs: LogicalPlanningInputs<'_>,
    normalized_predicate: Option<Predicate>,
    consistency: MissingRowPolicy,
    work: &PreparationWork<'_>,
) -> Result<LogicalQuery, QueryError> {
    let LogicalPlanningInputs {
        mode,
        filter_expr,
        filter_predicate_covers_expr,
        order,
        distinct,
        group,
        having_expr,
    } = inputs;

    Ok(LogicalQuery {
        mode,
        filter_expr: filter_expr.map(|expr| work.copy_expr(expr)).transpose()?,
        filter_predicate_covers_expr,
        normalized_predicate,
        order: order.map(|order| work.copy_order_spec(order)).transpose()?,
        distinct,
        group: group
            .map(|group| {
                Ok::<_, QueryError>(GroupSpec {
                    group_fields: group.group_fields.copy_for_preparation(work)?,
                    aggregates: work.copy_slice(&group.aggregates, |aggregate| {
                        Ok(GroupAggregateSpec::from_shape(
                            aggregate.shape().copy_for_preparation(work)?,
                        ))
                    })?,
                    execution: group.execution,
                })
            })
            .transpose()?,
        having_expr: having_expr.map(|expr| work.copy_expr(expr)).transpose()?,
        consistency,
    })
}

/// Build a logical plan from intent-owned scalar and grouped plan inputs.
pub(in crate::db::query) fn build_logical_plan(
    schema: &SchemaInfo,
    query: LogicalQuery,
    work: &PreparationWork<'_>,
) -> Result<LogicalPlan, QueryError> {
    let LogicalQuery {
        mode,
        filter_expr,
        filter_predicate_covers_expr,
        normalized_predicate,
        order,
        distinct,
        group,
        having_expr,
        consistency,
    } = query;
    let grouped_order = group.is_some();
    let predicate_covers_filter_expr = filter_predicate_covers_expr && filter_expr.is_some();

    // Build scalar shape first so grouped/non-grouped plans share one scalar contract.
    let scalar = ScalarPlan {
        mode,
        filter_expr,
        predicate_covers_filter_expr,
        predicate: normalized_predicate,
        order: canonicalize_order_spec_for_grouping(
            schema.primary_key_names(),
            order,
            grouped_order,
            work,
        )?,
        distinct,
        delete_limit: match mode {
            QueryMode::Delete(spec) if spec.limit.is_some() || spec.offset() > 0 => {
                Some(DeleteLimitSpec {
                    limit: spec.limit(),
                    offset: spec.offset(),
                })
            }
            QueryMode::Load(_) | QueryMode::Delete(_) => None,
        },
        page: match mode {
            QueryMode::Load(spec) if spec.limit.is_some() || spec.offset > 0 => Some(PageSpec {
                limit: spec.limit,
                offset: spec.offset,
            }),
            QueryMode::Load(_) | QueryMode::Delete(_) => None,
        },
        consistency,
    };

    // Grouped shape wraps scalar shape; HAVING without GROUP BY is invalid and
    // should be rejected by intent validation before reaching this boundary.
    if let Some(group) = group {
        Ok(LogicalPlan::Grouped(GroupPlan {
            scalar,
            group,
            having_expr,
        }))
    } else {
        debug_assert!(
            having_expr.is_none(),
            "HAVING clauses require grouped shape before logical plan assembly"
        );

        Ok(LogicalPlan::Scalar(scalar))
    }
}

/// Normalize one ORDER BY shape while respecting the grouped/scalar
/// determinism split.
///
/// Scalar row ordering still requires primary-key tie-break components so
/// result order stays total and resumable. Grouped ordering does not use the
/// row-level primary key contract, so explicit grouped `ORDER BY` terms must
/// remain unchanged.
pub(in crate::db::query) fn canonicalize_order_spec_for_grouping(
    primary_key_names: &[String],
    order: Option<OrderSpec>,
    grouped: bool,
    work: &PreparationWork<'_>,
) -> Result<Option<OrderSpec>, QueryError> {
    let Some(mut order) = order else {
        return Ok(None);
    };
    if grouped {
        return Ok(Some(order));
    }

    let appended_direction = order.fields.last().map_or(
        OrderDirection::Asc,
        crate::db::query::plan::OrderTerm::direction,
    );
    // Callers borrow these names from accepted schema authority. Charge visits,
    // comparisons and backing before work; no independent sizing pass or set
    // allocation is needed to preserve authored order and exact-name matching.
    for primary_key_name in primary_key_names {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        let mut already_ordered = false;
        for term in &order.fields {
            work.charge(Resource::PredicateExpressionSteps, 1)?;
            let Some(field) = term.direct_field() else {
                continue;
            };
            // String equality reads payload bytes only for equal lengths.
            if field.len() == primary_key_name.len() {
                work.charge(Resource::PredicateExpressionSteps, field.len() as u64)?;
                if field == primary_key_name {
                    already_ordered = true;
                    break;
                }
            }
        }
        if already_ordered {
            continue;
        }

        work.reserve_vec(&mut order.fields, 1)?;
        order.fields.push(OrderTerm::field(
            work.copy_text(primary_key_name)?,
            appended_direction,
        ));
    }

    Ok(Some(order))
}
