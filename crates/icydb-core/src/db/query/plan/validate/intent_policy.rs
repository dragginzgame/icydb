//! Module: db::query::plan::validate::intent_policy
//! Responsibility: enforce high-level query-intent policy constraints before
//! access planning proceeds.
//! Does not own: symbol lookup or detailed order/group validation rules.
//! Boundary: keeps coarse query-shape policy checks centralized within plan validation.

use crate::db::{
    contracts::first_violated_rule,
    query::plan::{
        OrderSpec, QueryMode,
        validate::plan_shape::{has_explicit_order, validate_order_shape},
        validate::{IntentKeyAccessKind, IntentKeyAccessPolicyViolation, PolicyPlanError},
    },
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
    has_delete_window: bool,
}

impl IntentPlanShapePolicyContext {
    #[must_use]
    #[expect(clippy::fn_params_excessive_bools)]
    const fn new(
        is_delete_mode: bool,
        grouped: bool,
        has_order: bool,
        has_delete_window: bool,
    ) -> Self {
        Self {
            is_delete_mode,
            grouped,
            has_order,
            has_delete_window,
        }
    }
}

type IntentPlanShapePolicyRule = fn(IntentPlanShapePolicyContext) -> Option<PolicyPlanError>;

const INTENT_PLAN_SHAPE_POLICY_RULES: &[IntentPlanShapePolicyRule] = &[
    intent_delete_grouping_violation,
    intent_delete_window_requires_order_violation,
];

fn intent_delete_grouping_violation(ctx: IntentPlanShapePolicyContext) -> Option<PolicyPlanError> {
    (ctx.is_delete_mode && ctx.grouped).then_some(PolicyPlanError::delete_plan_with_grouping())
}

fn intent_delete_window_requires_order_violation(
    ctx: IntentPlanShapePolicyContext,
) -> Option<PolicyPlanError> {
    (ctx.is_delete_mode && ctx.has_delete_window && !ctx.has_order)
        .then_some(PolicyPlanError::delete_window_requires_order())
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

type IntentKeyAccessPolicyRule =
    fn(IntentKeyAccessPolicyContext) -> Option<IntentKeyAccessPolicyViolation>;

const INTENT_KEY_ACCESS_POLICY_RULES: &[IntentKeyAccessPolicyRule] = &[
    intent_key_access_conflict_violation,
    intent_by_ids_with_predicate_violation,
    intent_only_with_predicate_violation,
];

fn intent_key_access_conflict_violation(
    ctx: IntentKeyAccessPolicyContext,
) -> Option<IntentKeyAccessPolicyViolation> {
    ctx.has_key_access_conflict
        .then_some(IntentKeyAccessPolicyViolation::key_access_conflict())
}

fn intent_by_ids_with_predicate_violation(
    ctx: IntentKeyAccessPolicyContext,
) -> Option<IntentKeyAccessPolicyViolation> {
    (ctx.is_many_selector && ctx.has_predicate)
        .then_some(IntentKeyAccessPolicyViolation::by_ids_with_predicate())
}

fn intent_only_with_predicate_violation(
    ctx: IntentKeyAccessPolicyContext,
) -> Option<IntentKeyAccessPolicyViolation> {
    (ctx.is_only_selector && ctx.has_predicate)
        .then_some(IntentKeyAccessPolicyViolation::only_with_predicate())
}

/// Validate intent-level plan-shape rules derived from query mode + modifiers.
pub(crate) fn validate_intent_plan_shape(
    mode: QueryMode,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Result<(), PolicyPlanError> {
    validate_order_shape(order)?;

    let context = IntentPlanShapePolicyContext::new(
        mode.is_delete(),
        grouped,
        has_explicit_order(order),
        matches!(&mode, QueryMode::Delete(spec) if spec.limit.is_some() || spec.offset() > 0),
    );
    first_violated_rule(INTENT_PLAN_SHAPE_POLICY_RULES, context).map_or(Ok(()), Err)
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

    first_violated_rule(INTENT_KEY_ACCESS_POLICY_RULES, context).map_or(Ok(()), Err)
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
        let mode = QueryMode::Delete(DeleteSpec {
            limit: Some(10),
            offset: 0,
        });

        assert_eq!(
            validate_intent_plan_shape(mode, None, false),
            Err(PolicyPlanError::DeleteWindowRequiresOrder),
            "delete LIMIT without ORDER BY must fail in intent/planning validation",
        );
    }

    #[test]
    fn delete_offset_without_order_fails_during_planning_policy_validation() {
        let mode = QueryMode::Delete(DeleteSpec {
            limit: None,
            offset: 1,
        });
        let order = OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        };

        assert_eq!(
            validate_intent_plan_shape(mode, None, false),
            Err(PolicyPlanError::DeleteWindowRequiresOrder),
            "delete OFFSET without ORDER BY must fail in intent/planning validation",
        );

        validate_intent_plan_shape(mode, Some(&order), false)
            .expect("ordered delete OFFSET should pass intent/planning validation");
    }

    #[test]
    fn delete_limit_and_offset_with_order_passes_planning_policy_validation() {
        let mode = QueryMode::Delete(DeleteSpec {
            limit: Some(2),
            offset: 1,
        });
        let order = OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        };

        validate_intent_plan_shape(mode, Some(&order), false)
            .expect("ordered delete LIMIT/OFFSET should pass intent/planning validation");
    }

    #[test]
    fn delete_offset_with_order_does_not_fail_planning_policy_validation() {
        let mode = QueryMode::Delete(DeleteSpec {
            limit: None,
            offset: 1,
        });
        let order = OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        };

        validate_intent_plan_shape(mode, Some(&order), false)
            .expect("ordered delete OFFSET should pass intent/planning validation");
    }

    #[test]
    fn delete_grouping_shape_fails_during_planning_policy_validation() {
        let mode = QueryMode::Delete(DeleteSpec {
            limit: None,
            offset: 0,
        });
        let order = OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        };

        assert_eq!(
            validate_intent_plan_shape(mode, Some(&order), true),
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

        validate_intent_plan_shape(mode, Some(&order), false)
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
