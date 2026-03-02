use crate::{
    db::query::plan::{
        FieldSlot, LoadSpec, LogicalPlan, OrderSpec, QueryMode,
        validate::{
            CursorOrderPlanShapeError, CursorPagingPolicyError, FluentLoadPolicyViolation,
            GroupPlanError, IntentKeyAccessKind, IntentKeyAccessPolicyViolation, PlanError,
            PolicyPlanError,
        },
    },
    model::entity::EntityModel,
};

// ORDER validation ownership contract:
// - This module owns ORDER semantic validation (field existence/orderability/tie-break).
// - ORDER canonicalization (primary-key tie-break insertion) is performed at the
//   intent boundary via `canonicalize_order_spec` before plan validation.
// - Shape-policy checks (for example empty ORDER, pagination/order coupling) are owned here.
// - Executor/runtime layers may defend execution preconditions only.

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
        return Err(PolicyPlanError::EmptyOrderSpec);
    }

    Ok(())
}

/// Validate intent-level plan-shape rules derived from query mode + order.
pub(crate) fn validate_intent_plan_shape(
    mode: QueryMode,
    order: Option<&OrderSpec>,
) -> Result<(), PolicyPlanError> {
    validate_order_shape(order)?;

    let has_order = has_explicit_order(order);
    if matches!(mode, QueryMode::Delete(spec) if spec.limit.is_some()) && !has_order {
        return Err(PolicyPlanError::DeleteLimitRequiresOrder);
    }

    Ok(())
}

/// Validate cursor-pagination readiness for a load-spec + ordering pair.
pub(crate) const fn validate_cursor_paging_requirements(
    has_order: bool,
    spec: LoadSpec,
) -> Result<(), CursorPagingPolicyError> {
    if !has_order {
        return Err(CursorPagingPolicyError::CursorRequiresOrder);
    }
    if spec.limit.is_none() {
        return Err(CursorPagingPolicyError::CursorRequiresLimit);
    }

    Ok(())
}

/// Validate cursor-order shape and return the logical order contract when present.
pub(crate) const fn validate_cursor_order_plan_shape(
    order: Option<&OrderSpec>,
    require_explicit_order: bool,
) -> Result<Option<&OrderSpec>, CursorOrderPlanShapeError> {
    let Some(order) = order else {
        if require_explicit_order {
            return Err(CursorOrderPlanShapeError::MissingExplicitOrder);
        }

        return Ok(None);
    };

    if order.fields.is_empty() {
        return Err(CursorOrderPlanShapeError::EmptyOrderSpec);
    }

    Ok(Some(order))
}

/// Resolve one grouped field into a stable field slot.
pub(crate) fn resolve_group_field_slot(
    model: &EntityModel,
    field: &str,
) -> Result<FieldSlot, PlanError> {
    FieldSlot::resolve(model, field).ok_or_else(|| {
        PlanError::from(GroupPlanError::UnknownGroupField {
            field: field.to_string(),
        })
    })
}

/// Validate intent key-access policy before planning.
pub(crate) const fn validate_intent_key_access_policy(
    key_access_conflict: bool,
    key_access_kind: Option<IntentKeyAccessKind>,
    has_predicate: bool,
) -> Result<(), IntentKeyAccessPolicyViolation> {
    if key_access_conflict {
        return Err(IntentKeyAccessPolicyViolation::KeyAccessConflict);
    }

    match key_access_kind {
        Some(IntentKeyAccessKind::Many) if has_predicate => {
            Err(IntentKeyAccessPolicyViolation::ByIdsWithPredicate)
        }
        Some(IntentKeyAccessKind::Only) if has_predicate => {
            Err(IntentKeyAccessPolicyViolation::OnlyWithPredicate)
        }
        Some(
            IntentKeyAccessKind::Single | IntentKeyAccessKind::Many | IntentKeyAccessKind::Only,
        )
        | None => Ok(()),
    }
}

/// Validate fluent non-paged load entry policy.
pub(crate) const fn validate_fluent_non_paged_mode(
    has_cursor_token: bool,
    has_grouping: bool,
) -> Result<(), FluentLoadPolicyViolation> {
    if has_cursor_token {
        return Err(FluentLoadPolicyViolation::CursorRequiresPagedExecution);
    }
    if has_grouping {
        return Err(FluentLoadPolicyViolation::GroupedRequiresExecuteGrouped);
    }

    Ok(())
}

/// Validate fluent paged load entry policy.
pub(crate) fn validate_fluent_paged_mode(
    has_grouping: bool,
    has_explicit_order: bool,
    spec: Option<LoadSpec>,
) -> Result<(), FluentLoadPolicyViolation> {
    if has_grouping {
        return Err(FluentLoadPolicyViolation::GroupedRequiresExecuteGrouped);
    }

    let Some(spec) = spec else {
        return Ok(());
    };

    validate_cursor_paging_requirements(has_explicit_order, spec).map_err(|err| match err {
        CursorPagingPolicyError::CursorRequiresOrder => {
            FluentLoadPolicyViolation::CursorRequiresOrder
        }
        CursorPagingPolicyError::CursorRequiresLimit => {
            FluentLoadPolicyViolation::CursorRequiresLimit
        }
    })
}

/// Validate mode/order/pagination invariants for one logical plan.
pub(crate) fn validate_plan_shape(plan: &LogicalPlan) -> Result<(), PolicyPlanError> {
    let grouped = matches!(plan, LogicalPlan::Grouped(_));
    let plan = match plan {
        LogicalPlan::Scalar(plan) => plan,
        LogicalPlan::Grouped(plan) => &plan.scalar,
    };
    validate_order_shape(plan.order.as_ref())?;

    let has_order = has_explicit_order(plan.order.as_ref());
    if plan.delete_limit.is_some() && !has_order {
        return Err(PolicyPlanError::DeleteLimitRequiresOrder);
    }

    match plan.mode {
        QueryMode::Delete(_) => {
            if plan.page.is_some() {
                return Err(PolicyPlanError::DeletePlanWithPagination);
            }
        }
        QueryMode::Load(_) => {
            if plan.delete_limit.is_some() {
                return Err(PolicyPlanError::LoadPlanWithDeleteLimit);
            }
            // GROUP BY v1 uses canonical grouped key ordering when ORDER BY is
            // omitted, so grouped pagination remains deterministic without an
            // explicit sort clause.
            if plan.page.is_some() && !has_order && !grouped {
                return Err(PolicyPlanError::UnorderedPagination);
            }
        }
    }

    Ok(())
}
