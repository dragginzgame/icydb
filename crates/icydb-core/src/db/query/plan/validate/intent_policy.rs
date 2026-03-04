use crate::db::query::plan::{
    OrderSpec, QueryMode,
    validate::plan_shape::{has_explicit_order, validate_order_shape},
    validate::{IntentKeyAccessKind, IntentKeyAccessPolicyViolation, PolicyPlanError},
};

/// Validate intent-level plan-shape rules derived from query mode + modifiers.
pub(crate) fn validate_intent_plan_shape(
    mode: QueryMode,
    order: Option<&OrderSpec>,
    grouped: bool,
    delete_has_offset: bool,
) -> Result<(), PolicyPlanError> {
    validate_order_shape(order)?;

    let has_order = has_explicit_order(order);
    if mode.is_delete() && delete_has_offset {
        return Err(PolicyPlanError::DeletePlanWithOffset);
    }
    if mode.is_delete() && grouped {
        return Err(PolicyPlanError::DeletePlanWithGrouping);
    }
    if matches!(mode, QueryMode::Delete(spec) if spec.limit.is_some()) && !has_order {
        return Err(PolicyPlanError::DeleteLimitRequiresOrder);
    }

    Ok(())
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
