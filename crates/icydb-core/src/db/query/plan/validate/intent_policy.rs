use crate::db::query::plan::{
    OrderSpec, QueryMode,
    validate::plan_shape::{has_explicit_order, validate_order_shape},
    validate::{IntentKeyAccessKind, IntentKeyAccessPolicyViolation, PolicyPlanError},
};

///
/// IntentPlanShapePolicyContext
///
/// Pure intent-level plan-shape context used by ordered policy rules.
/// Keeps delete/group/order/offset facts centralized for planner validation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(clippy::struct_excessive_bools)]
struct IntentPlanShapePolicyContext {
    is_delete_mode: bool,
    grouped: bool,
    has_order: bool,
    has_delete_offset: bool,
    has_delete_limit: bool,
}

impl IntentPlanShapePolicyContext {
    #[must_use]
    #[expect(clippy::fn_params_excessive_bools)]
    const fn new(
        is_delete_mode: bool,
        grouped: bool,
        has_order: bool,
        has_delete_offset: bool,
        has_delete_limit: bool,
    ) -> Self {
        Self {
            is_delete_mode,
            grouped,
            has_order,
            has_delete_offset,
            has_delete_limit,
        }
    }
}

///
/// IntentPlanShapePolicyRule
/// Declarative intent plan-shape rule: one reason + one violation predicate.
///

#[derive(Clone, Copy)]
struct IntentPlanShapePolicyRule {
    reason: PolicyPlanError,
    violated: fn(IntentPlanShapePolicyContext) -> bool,
}

impl IntentPlanShapePolicyRule {
    #[must_use]
    const fn new(
        reason: PolicyPlanError,
        violated: fn(IntentPlanShapePolicyContext) -> bool,
    ) -> Self {
        Self { reason, violated }
    }
}

const INTENT_PLAN_SHAPE_POLICY_RULES: &[IntentPlanShapePolicyRule] = &[
    IntentPlanShapePolicyRule::new(
        PolicyPlanError::DeletePlanWithOffset,
        intent_delete_offset_violated,
    ),
    IntentPlanShapePolicyRule::new(
        PolicyPlanError::DeletePlanWithGrouping,
        intent_delete_grouping_violated,
    ),
    IntentPlanShapePolicyRule::new(
        PolicyPlanError::DeleteLimitRequiresOrder,
        intent_delete_limit_requires_order_violated,
    ),
];

const fn intent_delete_offset_violated(ctx: IntentPlanShapePolicyContext) -> bool {
    ctx.is_delete_mode && ctx.has_delete_offset
}

const fn intent_delete_grouping_violated(ctx: IntentPlanShapePolicyContext) -> bool {
    ctx.is_delete_mode && ctx.grouped
}

const fn intent_delete_limit_requires_order_violated(ctx: IntentPlanShapePolicyContext) -> bool {
    ctx.is_delete_mode && ctx.has_delete_limit && !ctx.has_order
}

fn first_intent_plan_shape_policy_violation(
    ctx: IntentPlanShapePolicyContext,
) -> Option<PolicyPlanError> {
    for rule in INTENT_PLAN_SHAPE_POLICY_RULES {
        if (rule.violated)(ctx) {
            return Some(rule.reason);
        }
    }

    None
}

///
/// IntentKeyAccessPolicyContext
///
/// Pure key-access policy context used by ordered key-access guard rules.
/// Keeps selector conflict and predicate-combination facts centralized.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(clippy::struct_excessive_bools)]
struct IntentKeyAccessPolicyContext {
    has_key_access_conflict: bool,
    is_many_selector: bool,
    is_only_selector: bool,
    has_predicate: bool,
}

impl IntentKeyAccessPolicyContext {
    #[must_use]
    const fn from_inputs(
        key_access_conflict: bool,
        key_access_kind: Option<IntentKeyAccessKind>,
        has_predicate: bool,
    ) -> Self {
        Self {
            has_key_access_conflict: key_access_conflict,
            is_many_selector: matches!(key_access_kind, Some(IntentKeyAccessKind::Many)),
            is_only_selector: matches!(key_access_kind, Some(IntentKeyAccessKind::Only)),
            has_predicate,
        }
    }
}

// Declarative key-access rule: one violation reason + one predicate.
#[derive(Clone, Copy)]
struct IntentKeyAccessPolicyRule {
    reason: IntentKeyAccessPolicyViolation,
    violated: fn(IntentKeyAccessPolicyContext) -> bool,
}

impl IntentKeyAccessPolicyRule {
    #[must_use]
    const fn new(
        reason: IntentKeyAccessPolicyViolation,
        violated: fn(IntentKeyAccessPolicyContext) -> bool,
    ) -> Self {
        Self { reason, violated }
    }
}

const INTENT_KEY_ACCESS_POLICY_RULES: &[IntentKeyAccessPolicyRule] = &[
    IntentKeyAccessPolicyRule::new(
        IntentKeyAccessPolicyViolation::KeyAccessConflict,
        intent_key_access_conflict_violated,
    ),
    IntentKeyAccessPolicyRule::new(
        IntentKeyAccessPolicyViolation::ByIdsWithPredicate,
        intent_by_ids_with_predicate_violated,
    ),
    IntentKeyAccessPolicyRule::new(
        IntentKeyAccessPolicyViolation::OnlyWithPredicate,
        intent_only_with_predicate_violated,
    ),
];

const fn intent_key_access_conflict_violated(ctx: IntentKeyAccessPolicyContext) -> bool {
    ctx.has_key_access_conflict
}

const fn intent_by_ids_with_predicate_violated(ctx: IntentKeyAccessPolicyContext) -> bool {
    ctx.is_many_selector && ctx.has_predicate
}

const fn intent_only_with_predicate_violated(ctx: IntentKeyAccessPolicyContext) -> bool {
    ctx.is_only_selector && ctx.has_predicate
}

fn first_intent_key_access_policy_violation(
    ctx: IntentKeyAccessPolicyContext,
) -> Option<IntentKeyAccessPolicyViolation> {
    for rule in INTENT_KEY_ACCESS_POLICY_RULES {
        if (rule.violated)(ctx) {
            return Some(rule.reason);
        }
    }

    None
}

/// Validate intent-level plan-shape rules derived from query mode + modifiers.
pub(crate) fn validate_intent_plan_shape(
    mode: QueryMode,
    order: Option<&OrderSpec>,
    grouped: bool,
    delete_has_offset: bool,
) -> Result<(), PolicyPlanError> {
    validate_order_shape(order)?;

    let context = IntentPlanShapePolicyContext::new(
        mode.is_delete(),
        grouped,
        has_explicit_order(order),
        delete_has_offset,
        matches!(&mode, QueryMode::Delete(spec) if spec.limit.is_some()),
    );
    if let Some(reason) = first_intent_plan_shape_policy_violation(context) {
        return Err(reason);
    }

    Ok(())
}

/// Validate intent key-access policy before planning.
pub(crate) fn validate_intent_key_access_policy(
    key_access_conflict: bool,
    key_access_kind: Option<IntentKeyAccessKind>,
    has_predicate: bool,
) -> Result<(), IntentKeyAccessPolicyViolation> {
    let context = IntentKeyAccessPolicyContext::from_inputs(
        key_access_conflict,
        key_access_kind,
        has_predicate,
    );

    if let Some(reason) = first_intent_key_access_policy_violation(context) {
        return Err(reason);
    }

    Ok(())
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::query::plan::{DeleteSpec, LoadSpec, OrderDirection};

    #[test]
    fn delete_limit_without_order_fails_during_planning_policy_validation() {
        let mode = QueryMode::Delete(DeleteSpec { limit: Some(10) });

        assert_eq!(
            validate_intent_plan_shape(mode, None, false, false),
            Err(PolicyPlanError::DeleteLimitRequiresOrder),
            "delete LIMIT without ORDER BY must fail in intent/planning validation",
        );
    }

    #[test]
    fn delete_offset_fails_during_planning_policy_validation() {
        let mode = QueryMode::Delete(DeleteSpec { limit: None });
        let order = OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        };

        assert_eq!(
            validate_intent_plan_shape(mode, Some(&order), false, true),
            Err(PolicyPlanError::DeletePlanWithOffset),
            "delete OFFSET must fail in intent/planning validation",
        );
    }

    #[test]
    fn delete_grouping_shape_fails_during_planning_policy_validation() {
        let mode = QueryMode::Delete(DeleteSpec { limit: None });
        let order = OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        };

        assert_eq!(
            validate_intent_plan_shape(mode, Some(&order), true, false),
            Err(PolicyPlanError::DeletePlanWithGrouping),
            "delete GROUP BY/HAVING shape must fail in intent/planning validation",
        );
    }

    #[test]
    fn load_mode_allows_ordered_shape_in_intent_policy() {
        let mode = QueryMode::Load(LoadSpec {
            limit: Some(5),
            offset: 0,
        });
        let order = OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        };

        validate_intent_plan_shape(mode, Some(&order), false, false)
            .expect("ordered load shape should pass intent/planning policy validation");
    }

    #[test]
    fn by_ids_with_predicate_fails_during_planning_policy_validation() {
        assert_eq!(
            validate_intent_key_access_policy(true, None, false),
            Err(IntentKeyAccessPolicyViolation::KeyAccessConflict),
            "conflicting key-access selectors must fail in planner key-access policy",
        );

        assert_eq!(
            validate_intent_key_access_policy(false, Some(IntentKeyAccessKind::Many), true),
            Err(IntentKeyAccessPolicyViolation::ByIdsWithPredicate),
            "by_ids + predicate must fail in planner key-access policy",
        );
    }

    #[test]
    fn only_with_predicate_fails_during_planning_policy_validation() {
        assert_eq!(
            validate_intent_key_access_policy(false, Some(IntentKeyAccessKind::Only), true),
            Err(IntentKeyAccessPolicyViolation::OnlyWithPredicate),
            "only + predicate must fail in planner key-access policy",
        );

        validate_intent_key_access_policy(false, Some(IntentKeyAccessKind::Single), true)
            .expect("single key + predicate remains a valid planner key-access shape");
    }
}
