//! Module: query::plan::validate::grouped::cursor
//! Responsibility: grouped cursor-order feasibility checks for planner validation.
//! Does not own: runtime grouped cursor continuation behavior or token decoding.
//! Boundary: validates grouped order/paging alignment before plan admission.

#[cfg(test)]
mod tests;

use crate::db::{
    QueryError,
    query::{
        builder::scalar_projection::write_scalar_projection_expr_plan_label,
        plan::{
            GroupFieldSet, GroupSpec, OrderSpec, ScalarPlan,
            expr::{
                GroupedOrderTermAdmissibility, GroupedTopKOrderTermAdmissibility,
                try_classify_grouped_order_term_for_field, try_classify_grouped_top_k_order_term,
                try_grouped_top_k_order_term_requires_heap,
            },
            validate::{GroupPlanError, PlanError},
        },
        preparation::PreparationWork,
    },
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

///
/// GroupedOrderCursorLane
///
/// Planner-local grouped cursor lane chosen from the declared grouped ORDER BY
/// terms. Canonical keeps the grouped-key ordered contract. TopK reserves the
/// bounded aggregate-order lane that still requires LIMIT.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupedOrderCursorLane {
    Canonical,
    TopK,
}

// Validate grouped cursor-order constraints in one dedicated gate.
pub(in crate::db::query) fn validate_group_cursor_constraints(
    logical: &ScalarPlan,
    group: &GroupSpec,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    // Grouped pagination/order constraints are cursor-domain policy. A finite
    // canonical group-key order may return its complete bounded group set
    // without a row LIMIT. Aggregate-driven ordering still needs a finite
    // Top-K window because its retained candidate bound comes from LIMIT.
    let Some(order) = logical.order.as_ref() else {
        return Ok(());
    };

    let lane = validate_order_lane(order, &group.group_fields, work)?;
    let has_limit = logical.page.as_ref().and_then(|page| page.limit).is_some();
    if matches!(lane, GroupedOrderCursorLane::TopK) && !has_limit {
        return Err(PlanError::from(GroupPlanError::order_requires_limit()).into());
    }
    if matches!(lane, GroupedOrderCursorLane::Canonical)
        && !has_limit
        && !group.execution.is_finite_bounded()
    {
        return Err(PlanError::from(GroupPlanError::order_requires_limit()).into());
    }

    Ok(())
}

// Validate that grouped ORDER BY terms stay on one supported planner lane.
//
// Canonical grouped ordering still requires grouped-key prefix alignment.
// Aggregate-driven grouped ordering may reserve the bounded Top-K lane instead,
// but only when every term is admissible under the grouped post-aggregate
// expression model.
fn validate_order_lane(
    order: &OrderSpec,
    group_fields: &GroupFieldSet,
    work: &PreparationWork<'_>,
) -> Result<GroupedOrderCursorLane, QueryError> {
    // Keep the short-circuit lane search before term validation: an aggregate
    // later in ORDER BY changes the admissibility of earlier scalar terms.
    for term in &order.fields {
        if try_grouped_top_k_order_term_requires_heap(term.expr(), &mut |steps| {
            work.charge(Resource::PredicateExpressionSteps, steps)
        })? {
            return validate_top_k_order_lane(order, group_fields, work);
        }
    }

    validate_canonical_order_lane(order, group_fields, work)
}

// Validate one aggregate-free grouped ORDER BY list against the canonical
// grouped-key cursor contract that still powers resumable grouped ordering.
fn validate_canonical_order_lane(
    order: &OrderSpec,
    group_fields: &GroupFieldSet,
    work: &PreparationWork<'_>,
) -> Result<GroupedOrderCursorLane, QueryError> {
    if order.fields.len() < group_fields.len() {
        return Err(
            PlanError::from(GroupPlanError::order_prefix_not_aligned_with_group_keys()).into(),
        );
    }

    for (term, group_field) in order.fields.iter().zip(group_fields.iter()) {
        match try_classify_grouped_order_term_for_field(term.expr(), group_field, &mut |steps| {
            work.charge(Resource::PredicateExpressionSteps, steps)
        })? {
            GroupedOrderTermAdmissibility::Preserves(_) => {}
            GroupedOrderTermAdmissibility::PrefixMismatch => {
                return Err(PlanError::from(
                    GroupPlanError::order_prefix_not_aligned_with_group_keys(),
                )
                .into());
            }
            GroupedOrderTermAdmissibility::UnsupportedExpression => {
                return Err(
                    PlanError::from(GroupPlanError::order_expression_not_admissible(
                        work.render_text(|out| {
                            write_scalar_projection_expr_plan_label(term.expr(), out)
                        })?,
                    ))
                    .into(),
                );
            }
        }
    }

    Ok(GroupedOrderCursorLane::Canonical)
}

// Validate one aggregate-driven grouped ORDER BY list against the bounded Top-K
// lane. Once any aggregate order term is present, grouped-key tie-breakers no
// longer need to preserve canonical prefix order because the lane is already
// materialized and non-resumable in this release.
fn validate_top_k_order_lane(
    order: &OrderSpec,
    group_fields: &GroupFieldSet,
    work: &PreparationWork<'_>,
) -> Result<GroupedOrderCursorLane, QueryError> {
    for term in &order.fields {
        match try_classify_grouped_top_k_order_term(term.expr(), group_fields, &mut |steps| {
            work.charge(Resource::PredicateExpressionSteps, steps)
        })? {
            GroupedTopKOrderTermAdmissibility::Admissible => {}
            GroupedTopKOrderTermAdmissibility::NonGroupFieldReference => {
                return Err(PlanError::from(
                    GroupPlanError::order_prefix_not_aligned_with_group_keys(),
                )
                .into());
            }
            GroupedTopKOrderTermAdmissibility::UnsupportedExpression => {
                return Err(
                    PlanError::from(GroupPlanError::order_expression_not_admissible(
                        work.render_text(|out| {
                            write_scalar_projection_expr_plan_label(term.expr(), out)
                        })?,
                    ))
                    .into(),
                );
            }
        }
    }

    Ok(GroupedOrderCursorLane::TopK)
}
