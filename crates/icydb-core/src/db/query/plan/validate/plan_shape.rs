//! Module: query::plan::validate::plan_shape
//! Responsibility: logical plan-shape policy validation and explicit policy context mapping.
//! Does not own: cursor wire semantics or executor defensive runtime checks.
//! Boundary: enforces planner plan-shape policy constraints before execution handoff.

use crate::db::query::plan::{LogicalPlan, OrderSpec, QueryMode, validate::PolicyPlanError};

///
/// PlanShapePolicyContext
///
/// Pure policy context for logical plan-shape validation.
/// Encodes only plan-shape facts used by `PolicyPlanError` feasibility rules.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(clippy::struct_excessive_bools)]
struct PlanShapePolicyContext {
    is_delete_mode: bool,
    grouped: bool,
    has_order: bool,
    has_page: bool,
    has_delete_window: bool,
}

impl PlanShapePolicyContext {
    #[must_use]
    #[expect(clippy::fn_params_excessive_bools)]
    const fn new(
        is_delete_mode: bool,
        grouped: bool,
        has_order: bool,
        has_page: bool,
        has_delete_window: bool,
    ) -> Self {
        Self {
            is_delete_mode,
            grouped,
            has_order,
            has_page,
            has_delete_window,
        }
    }
}

///
/// PlanShapePolicyRule
///
/// Declarative policy rule for logical plan-shape validation.
/// Rules are evaluated in order and first violation maps to one policy error.
///

#[derive(Clone, Copy)]
struct PlanShapePolicyRule {
    reason: PolicyPlanError,
    violated: fn(PlanShapePolicyContext) -> bool,
}

impl PlanShapePolicyRule {
    #[must_use]
    const fn new(reason: PolicyPlanError, violated: fn(PlanShapePolicyContext) -> bool) -> Self {
        Self { reason, violated }
    }
}

const PLAN_SHAPE_POLICY_RULES: &[PlanShapePolicyRule] = &[
    PlanShapePolicyRule::new(
        PolicyPlanError::delete_plan_with_grouping(),
        plan_shape_delete_with_grouping_violated,
    ),
    PlanShapePolicyRule::new(
        PolicyPlanError::delete_window_requires_order(),
        plan_shape_delete_window_requires_order_violated,
    ),
    PlanShapePolicyRule::new(
        PolicyPlanError::delete_plan_with_pagination(),
        plan_shape_delete_with_pagination_violated,
    ),
    PlanShapePolicyRule::new(
        PolicyPlanError::load_plan_with_delete_limit(),
        plan_shape_load_with_delete_limit_violated,
    ),
    PlanShapePolicyRule::new(
        PolicyPlanError::unordered_pagination(),
        plan_shape_unordered_scalar_load_pagination_violated,
    ),
];

const fn plan_shape_delete_window_requires_order_violated(ctx: PlanShapePolicyContext) -> bool {
    ctx.has_delete_window && !ctx.has_order
}

const fn plan_shape_delete_with_grouping_violated(ctx: PlanShapePolicyContext) -> bool {
    ctx.is_delete_mode && ctx.grouped
}

const fn plan_shape_delete_with_pagination_violated(ctx: PlanShapePolicyContext) -> bool {
    ctx.is_delete_mode && ctx.has_page
}

const fn plan_shape_load_with_delete_limit_violated(ctx: PlanShapePolicyContext) -> bool {
    !ctx.is_delete_mode && ctx.has_delete_window
}

// GROUP BY v1 uses canonical grouped key ordering when ORDER BY is omitted,
// so grouped pagination remains deterministic without an explicit sort clause.
const fn plan_shape_unordered_scalar_load_pagination_violated(ctx: PlanShapePolicyContext) -> bool {
    !ctx.is_delete_mode && ctx.has_page && !ctx.has_order && !ctx.grouped
}

fn first_plan_shape_policy_violation(ctx: PlanShapePolicyContext) -> Option<PolicyPlanError> {
    for rule in PLAN_SHAPE_POLICY_RULES {
        if (rule.violated)(ctx) {
            return Some(rule.reason);
        }
    }

    None
}

/// Return true when an ORDER BY exists and contains at least one field.
#[must_use]
pub(crate) fn has_explicit_order(order: Option<&OrderSpec>) -> bool {
    order.is_some_and(|order| !order.fields.is_empty())
}

/// Return true when an ORDER BY exists but is empty.
#[must_use]
pub(crate) fn has_empty_order(order: Option<&OrderSpec>) -> bool {
    order.is_some_and(|order| order.fields.is_empty())
}

/// Validate order-shape rules shared across intent and logical plan boundaries.
pub(crate) fn validate_order_shape(order: Option<&OrderSpec>) -> Result<(), PolicyPlanError> {
    if has_empty_order(order) {
        return Err(PolicyPlanError::empty_order_spec());
    }

    Ok(())
}

/// Validate mode/order/pagination invariants for one logical plan.
pub(crate) fn validate_plan_shape(plan: &LogicalPlan) -> Result<(), PolicyPlanError> {
    let grouped = matches!(plan, LogicalPlan::Grouped(_));
    let plan = match plan {
        LogicalPlan::Scalar(plan) => plan,
        LogicalPlan::Grouped(plan) => &plan.scalar,
    };
    validate_order_shape(plan.order.as_ref())?;

    let context = PlanShapePolicyContext::new(
        matches!(plan.mode, QueryMode::Delete(_)),
        grouped,
        has_explicit_order(plan.order.as_ref()),
        plan.page.is_some(),
        plan.delete_limit.is_some(),
    );
    if let Some(reason) = first_plan_shape_policy_violation(context) {
        return Err(reason);
    }

    Ok(())
}
