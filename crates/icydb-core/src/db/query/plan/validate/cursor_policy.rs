use crate::db::query::plan::{
    LoadSpec, OrderSpec,
    validate::{CursorOrderPlanShapeError, CursorPagingPolicyError},
};

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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::query::plan::{DeleteSpec, LoadSpec, OrderDirection, QueryMode};

    #[test]
    fn cursor_paging_requires_order() {
        let spec = LoadSpec {
            limit: Some(10),
            offset: 0,
        };

        assert_eq!(
            validate_cursor_paging_requirements(false, spec),
            Err(CursorPagingPolicyError::CursorRequiresOrder),
            "cursor paging must require explicit ordering",
        );
    }

    #[test]
    fn cursor_paging_requires_limit() {
        let spec = LoadSpec {
            limit: None,
            offset: 0,
        };

        assert_eq!(
            validate_cursor_paging_requirements(true, spec),
            Err(CursorPagingPolicyError::CursorRequiresLimit),
            "cursor paging must require explicit LIMIT",
        );
    }

    #[test]
    fn cursor_order_shape_requires_explicit_order_when_requested() {
        let missing = validate_cursor_order_plan_shape(None, true);
        assert_eq!(
            missing,
            Err(CursorOrderPlanShapeError::MissingExplicitOrder),
            "missing explicit ORDER BY should fail shape validation",
        );

        let empty_order = OrderSpec { fields: Vec::new() };
        let empty = validate_cursor_order_plan_shape(Some(&empty_order), true);
        assert_eq!(
            empty,
            Err(CursorOrderPlanShapeError::EmptyOrderSpec),
            "empty ORDER BY should fail shape validation",
        );
    }

    #[test]
    fn cursor_order_shape_accepts_valid_explicit_order() {
        let order = OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        };

        let validated = validate_cursor_order_plan_shape(Some(&order), true)
            .expect("valid explicit order should pass cursor order-shape validation")
            .expect("validated order should be present");
        assert_eq!(validated, &order);
    }

    #[test]
    fn cursor_policy_tests_exercise_planner_mode_types() {
        // Keep planner mode contracts referenced so cursor tests stay aligned with
        // current query-mode model types at compile time.
        let _ = QueryMode::Load(LoadSpec {
            limit: Some(1),
            offset: 0,
        });
        let _ = QueryMode::Delete(DeleteSpec { limit: Some(1) });
    }
}
