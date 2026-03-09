//! Module: query::plan::validate::grouped::cursor
//! Responsibility: grouped cursor-order feasibility checks for planner validation.
//! Does not own: runtime grouped cursor continuation behavior or token decoding.
//! Boundary: validates grouped order/paging alignment before plan admission.

use crate::db::query::plan::{
    FieldSlot, GroupSpec, OrderSpec, ScalarPlan,
    validate::{GroupPlanError, PlanError},
};

// Validate grouped cursor-order constraints in one dedicated gate.
pub(in crate::db::query::plan::validate) fn validate_group_cursor_constraints(
    logical: &ScalarPlan,
    group: &GroupSpec,
) -> Result<(), PlanError> {
    // Grouped pagination/order constraints are cursor-domain policy:
    // grouped ORDER BY requires LIMIT and must align with grouped-key prefix.
    let Some(order) = logical.order.as_ref() else {
        return Ok(());
    };
    if logical.page.as_ref().and_then(|page| page.limit).is_none() {
        return Err(PlanError::from(GroupPlanError::OrderRequiresLimit));
    }
    if order_prefix_aligned_with_group_fields(order, group.group_fields.as_slice()) {
        return Ok(());
    }

    Err(PlanError::from(
        GroupPlanError::OrderPrefixNotAlignedWithGroupKeys,
    ))
}

// Return true when ORDER BY starts with GROUP BY key fields in declaration order.
fn order_prefix_aligned_with_group_fields(order: &OrderSpec, group_fields: &[FieldSlot]) -> bool {
    if order.fields.len() < group_fields.len() {
        return false;
    }

    group_fields
        .iter()
        .zip(order.fields.iter())
        .all(|(group_field, (order_field, _))| order_field == group_field.field())
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        predicate::MissingRowPolicy,
        query::plan::{
            AggregateKind, GroupAggregateSpec, GroupedExecutionConfig, LoadSpec, OrderDirection,
            PageSpec, QueryMode,
        },
    };

    fn grouped_spec() -> GroupSpec {
        GroupSpec {
            group_fields: vec![FieldSlot {
                index: 0,
                field: "team".to_string(),
            }],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig {
                max_groups: 128,
                max_group_bytes: 8 * 1024,
            },
        }
    }

    fn scalar_with_group_order(order_fields: Vec<(String, OrderDirection)>) -> ScalarPlan {
        ScalarPlan {
            mode: QueryMode::Load(LoadSpec {
                limit: Some(10),
                offset: 0,
            }),
            predicate: None,
            order: Some(OrderSpec {
                fields: order_fields,
            }),
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }
    }

    #[test]
    fn grouped_order_requires_limit_in_planner_cursor_policy() {
        let logical = scalar_with_group_order(vec![("team".to_string(), OrderDirection::Asc)]);
        let group = grouped_spec();

        let err = validate_group_cursor_constraints(&logical, &group)
            .expect_err("grouped ORDER BY without LIMIT must fail in planner cursor policy");

        assert!(matches!(
            err,
            PlanError::Policy(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::validate::PlanPolicyError::Group(group)
                        if matches!(group.as_ref(), GroupPlanError::OrderRequiresLimit)
                )
        ));
    }

    #[test]
    fn grouped_order_prefix_must_align_with_group_keys_in_planner_cursor_policy() {
        let mut logical = scalar_with_group_order(vec![("id".to_string(), OrderDirection::Asc)]);
        logical.page = Some(PageSpec {
            limit: Some(10),
            offset: 0,
        });
        let group = grouped_spec();

        let err = validate_group_cursor_constraints(&logical, &group)
            .expect_err("grouped ORDER BY not prefixed by GROUP BY keys must fail in planner");

        assert!(matches!(
            err,
            PlanError::Policy(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::validate::PlanPolicyError::Group(group)
                        if matches!(
                            group.as_ref(),
                            GroupPlanError::OrderPrefixNotAlignedWithGroupKeys
                        )
                )
        ));
    }

    #[test]
    fn grouped_order_prefix_alignment_with_limit_passes_planner_cursor_policy() {
        let mut logical = scalar_with_group_order(vec![
            ("team".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ]);
        logical.page = Some(PageSpec {
            limit: Some(10),
            offset: 0,
        });
        let group = grouped_spec();

        validate_group_cursor_constraints(&logical, &group).expect(
            "grouped ORDER BY with LIMIT and group-key-aligned prefix should pass planner policy",
        );
    }
}
