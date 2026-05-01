//! Module: db::query::plan::validate::intent_policy
//! Responsibility: enforce high-level query-intent policy constraints before
//! access planning proceeds.
//! Does not own: symbol lookup or detailed order/group validation rules.
//! Boundary: keeps coarse query-shape policy checks centralized within plan validation.

use crate::db::query::plan::{
    OrderSpec, QueryMode,
    validate::plan_shape::{has_explicit_order, validate_order_shape},
    validate::{IntentKeyAccessKind, IntentKeyAccessPolicyViolation, PolicyPlanError},
};

/// Validate intent-level plan-shape rules derived from query mode + modifiers.
pub(in crate::db::query) fn validate_intent_plan_shape(
    mode: QueryMode,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Result<(), PolicyPlanError> {
    validate_order_shape(order)?;
    let is_delete_mode = mode.is_delete();

    // Delete queries still fail closed on grouped shapes before any lower plan
    // stages can reinterpret the same shape as a grouped read.
    if is_delete_mode && grouped {
        return Err(PolicyPlanError::delete_plan_with_grouping());
    }

    // Windowed deletes require explicit ordering so the delete slice is
    // deterministic before any executor routing proceeds.
    if is_delete_mode
        && matches!(&mode, QueryMode::Delete(spec) if spec.limit.is_some() || spec.offset() > 0)
        && !has_explicit_order(order)
    {
        return Err(PolicyPlanError::delete_window_requires_order());
    }

    Ok(())
}

/// Validate intent key-access policy before planning.
pub(in crate::db::query) const fn validate_intent_key_access_policy(
    key_access_conflict: bool,
    key_access_kind: Option<IntentKeyAccessKind>,
    has_predicate: bool,
) -> Result<(), IntentKeyAccessPolicyViolation> {
    // Conflicting key selectors stay a hard stop regardless of the chosen
    // selector kind so the intent surface never carries ambiguous access state.
    if key_access_conflict {
        return Err(IntentKeyAccessPolicyViolation::key_access_conflict());
    }

    // Multi-key and `only` selectors still reject additional predicates at the
    // intent boundary so planner access routing sees one unambiguous contract.
    if has_predicate {
        if matches!(key_access_kind, Some(IntentKeyAccessKind::Many)) {
            return Err(IntentKeyAccessPolicyViolation::by_ids_with_predicate());
        }

        if matches!(key_access_kind, Some(IntentKeyAccessKind::Only)) {
            return Err(IntentKeyAccessPolicyViolation::only_with_predicate());
        }
    }

    Ok(())
}
