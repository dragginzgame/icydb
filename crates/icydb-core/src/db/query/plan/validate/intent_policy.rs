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
