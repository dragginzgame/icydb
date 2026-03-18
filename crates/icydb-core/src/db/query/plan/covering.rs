//! Module: query::plan::covering
//! Responsibility: planner covering-projection eligibility and order-contract derivation.
//! Does not own: runtime projection materialization or executor ordering enforcement.
//! Boundary: exposes planner-only covering contracts for index-backed paths.

use crate::db::{
    access::AccessPlan,
    direction::Direction,
    query::plan::{AccessPlannedQuery, OrderDirection, OrderSpec},
};
use crate::value::Value;

///
/// CoveringProjectionOrder
///
/// Planner-owned covering projection order contract.
/// Index order means projected component order is preserved from index traversal.
/// Primary-key order means runtime must reorder by primary key after projection.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CoveringProjectionOrder {
    IndexOrder(Direction),
    PrimaryKeyOrder(Direction),
}

///
/// CoveringProjectionContext
///
/// Planner-owned covering projection context contract.
/// Captures projection component position, bound-prefix arity, and output-order
/// interpretation for one index-backed access shape.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CoveringProjectionContext {
    pub(in crate::db) component_index: usize,
    pub(in crate::db) prefix_len: usize,
    pub(in crate::db) order_contract: CoveringProjectionOrder,
}

/// Return whether one scalar aggregate terminal can remain index-only using
/// existing-row semantics under the current planner + predicate-compile
/// contracts.
#[must_use]
pub(in crate::db) fn index_covering_existing_rows_terminal_eligible(
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
) -> bool {
    if plan.scalar_plan().order.is_some() {
        return false;
    }

    let index_shape_supported =
        plan.access.as_index_prefix_path().is_some() || plan.access.as_index_range_path().is_some();
    if !index_shape_supported {
        return false;
    }
    if plan.scalar_plan().predicate.is_none() {
        return true;
    }

    strict_predicate_compatible
}

/// Derive one covering projection context from one access shape + scalar order
/// contract and target field.
#[must_use]
pub(in crate::db) fn covering_index_projection_context<K>(
    access: &AccessPlan<K>,
    order: Option<&OrderSpec>,
    target_field: &str,
    primary_key_name: &'static str,
) -> Option<CoveringProjectionContext> {
    let (index_fields, prefix_len, path_kind_is_range) =
        if let Some((index, values)) = access.as_index_prefix_path() {
            (index.fields(), values.len(), false)
        } else if let Some((index, prefix_values, _, _)) = access.as_index_range_path() {
            (index.fields(), prefix_values.len(), true)
        } else {
            return None;
        };
    let component_index = index_fields
        .iter()
        .position(|field| *field == target_field)?;
    let order_contract = covering_projection_order_contract(
        order,
        index_fields,
        prefix_len,
        primary_key_name,
        path_kind_is_range,
    )?;

    Some(CoveringProjectionContext {
        component_index,
        prefix_len,
        order_contract,
    })
}

/// Resolve one constant projection value when access shape binds the target
/// field through index-prefix equality components.
#[must_use]
pub(in crate::db) fn constant_covering_projection_value_from_access<K>(
    access: &AccessPlan<K>,
    target_field: &str,
) -> Option<Value> {
    if let Some((index, values)) = access.as_index_prefix_path() {
        return constant_covering_projection_value_from_prefix(
            index.fields(),
            values,
            target_field,
        );
    }
    if let Some((index, prefix_values, _, _)) = access.as_index_range_path() {
        return constant_covering_projection_value_from_prefix(
            index.fields(),
            prefix_values,
            target_field,
        );
    }

    None
}

/// Return whether adjacent dedupe is safe for one covering projection context.
///
/// Safety contract:
/// - output order remains index traversal order (no primary-key reorder),
/// - target field is the first unbound index component.
#[must_use]
pub(in crate::db) const fn covering_index_adjacent_distinct_eligible(
    context: CoveringProjectionContext,
) -> bool {
    matches!(
        context.order_contract,
        CoveringProjectionOrder::IndexOrder(_)
    ) && context.component_index == context.prefix_len
}

// Resolve one covering projection order contract from scalar ORDER BY shape.
fn covering_projection_order_contract(
    order: Option<&OrderSpec>,
    index_fields: &[&'static str],
    prefix_len: usize,
    primary_key_name: &'static str,
    path_kind_is_range: bool,
) -> Option<CoveringProjectionOrder> {
    let Some(order) = order else {
        return Some(CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc));
    };
    if let Some(direction) = order.primary_key_only_direction(primary_key_name) {
        let direction = match direction {
            OrderDirection::Asc => Direction::Asc,
            OrderDirection::Desc => Direction::Desc,
        };

        return Some(CoveringProjectionOrder::PrimaryKeyOrder(direction));
    }

    let direction = match order.deterministic_secondary_order_direction(primary_key_name)? {
        OrderDirection::Asc => Direction::Asc,
        OrderDirection::Desc => Direction::Desc,
    };
    if order.matches_index_suffix_plus_primary_key(index_fields, prefix_len, primary_key_name) {
        return Some(CoveringProjectionOrder::IndexOrder(direction));
    }

    if path_kind_is_range {
        return None;
    }

    order
        .matches_index_full_plus_primary_key(index_fields, primary_key_name)
        .then_some(CoveringProjectionOrder::IndexOrder(direction))
}

// Resolve one constant projection value from index-prefix component bindings.
fn constant_covering_projection_value_from_prefix(
    index_fields: &[&'static str],
    prefix_values: &[Value],
    target_field: &str,
) -> Option<Value> {
    index_fields
        .iter()
        .zip(prefix_values.iter())
        .find_map(|(field, value)| (*field == target_field).then(|| value.clone()))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            access::{AccessPath, AccessPlan},
            direction::Direction,
            predicate::{MissingRowPolicy, Predicate},
            query::plan::{AccessPlannedQuery, OrderDirection, OrderSpec},
        },
        value::Value,
    };
    use std::ops::Bound;

    const INDEX_FIELDS_RANK: [&str; 1] = ["rank"];
    const INDEX_FIELDS_GROUP_RANK: [&str; 2] = ["group", "rank"];

    #[test]
    fn index_covering_existing_rows_terminal_requires_index_shape() {
        let plan = AccessPlannedQuery::new(AccessPath::FullScan, MissingRowPolicy::Ignore);

        assert!(
            !super::index_covering_existing_rows_terminal_eligible(&plan, true),
            "full-scan shape must not qualify for index-covering existing-row terminal eligibility",
        );
    }

    #[test]
    fn index_covering_existing_rows_terminal_requires_no_order() {
        let mut plan = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: crate::model::index::IndexModel::new(
                    "idx",
                    "tests::Entity",
                    &INDEX_FIELDS_RANK,
                    false,
                ),
                values: vec![Value::Uint(7)],
            },
            MissingRowPolicy::Ignore,
        );
        plan.scalar_plan_mut().order = Some(OrderSpec {
            fields: vec![("rank".to_string(), OrderDirection::Asc)],
        });

        assert!(
            !super::index_covering_existing_rows_terminal_eligible(&plan, true),
            "ordered shapes must not qualify for index-covering existing-row terminal eligibility",
        );
    }

    #[test]
    fn index_covering_existing_rows_terminal_accepts_unordered_no_predicate() {
        let plan = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: crate::model::index::IndexModel::new(
                    "idx",
                    "tests::Entity",
                    &INDEX_FIELDS_RANK,
                    false,
                ),
                values: vec![Value::Uint(7)],
            },
            MissingRowPolicy::Ignore,
        );

        assert!(
            super::index_covering_existing_rows_terminal_eligible(&plan, false),
            "unordered index-backed shapes without residual predicates should be eligible",
        );
    }

    #[test]
    fn index_covering_existing_rows_terminal_requires_strict_predicate_when_residual_present() {
        let mut plan = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: crate::model::index::IndexModel::new(
                    "idx",
                    "tests::Entity",
                    &INDEX_FIELDS_RANK,
                    false,
                ),
                values: vec![Value::Uint(7)],
            },
            MissingRowPolicy::Ignore,
        );
        plan.scalar_plan_mut().predicate = Some(Predicate::eq("rank".to_string(), Value::Uint(7)));

        assert!(
            !super::index_covering_existing_rows_terminal_eligible(&plan, false),
            "residual-predicate shapes must be rejected when strict predicate compatibility is absent",
        );
        assert!(
            super::index_covering_existing_rows_terminal_eligible(&plan, true),
            "residual-predicate shapes should be eligible when strict predicate compatibility is present",
        );
    }

    #[test]
    fn covering_projection_context_accepts_suffix_index_order() {
        let mut plan = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: crate::model::index::IndexModel::new(
                    "idx",
                    "tests::Entity",
                    &INDEX_FIELDS_GROUP_RANK,
                    false,
                ),
                values: vec![Value::Uint(7)],
            },
            MissingRowPolicy::Ignore,
        );
        plan.scalar_plan_mut().order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });

        let context = super::covering_index_projection_context(
            &plan.access,
            plan.scalar_plan().order.as_ref(),
            "rank",
            "id",
        )
        .expect("suffix index order should project one covering context");

        assert_eq!(context.component_index, 1);
        assert_eq!(context.prefix_len, 1);
        assert_eq!(
            context.order_contract,
            super::CoveringProjectionOrder::IndexOrder(Direction::Asc)
        );
        assert!(
            super::covering_index_adjacent_distinct_eligible(context),
            "first unbound component under index order should allow adjacent distinct dedupe",
        );
    }

    #[test]
    fn covering_projection_context_accepts_primary_key_order() {
        let mut plan = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: crate::model::index::IndexModel::new(
                    "idx",
                    "tests::Entity",
                    &INDEX_FIELDS_GROUP_RANK,
                    false,
                ),
                values: vec![Value::Uint(7)],
            },
            MissingRowPolicy::Ignore,
        );
        plan.scalar_plan_mut().order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });

        let context = super::covering_index_projection_context(
            &plan.access,
            plan.scalar_plan().order.as_ref(),
            "rank",
            "id",
        )
        .expect("primary-key order should project one covering context");

        assert_eq!(
            context.order_contract,
            super::CoveringProjectionOrder::PrimaryKeyOrder(Direction::Desc)
        );
        assert!(
            !super::covering_index_adjacent_distinct_eligible(context),
            "primary-key reorder must not use adjacent distinct dedupe",
        );
    }

    #[test]
    fn covering_projection_context_rejects_mixed_order_directions() {
        let mut plan = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: crate::model::index::IndexModel::new(
                    "idx",
                    "tests::Entity",
                    &INDEX_FIELDS_GROUP_RANK,
                    false,
                ),
                values: vec![Value::Uint(7)],
            },
            MissingRowPolicy::Ignore,
        );
        plan.scalar_plan_mut().order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Desc),
            ],
        });

        let context = super::covering_index_projection_context(
            &plan.access,
            plan.scalar_plan().order.as_ref(),
            "rank",
            "id",
        );
        assert!(
            context.is_none(),
            "mixed order directions must reject covering projection contexts",
        );
    }

    #[test]
    fn covering_projection_context_rejects_range_full_order_contract() {
        let mut plan = AccessPlannedQuery::new(
            AccessPath::index_range(
                crate::model::index::IndexModel::new(
                    "idx",
                    "tests::Entity",
                    &INDEX_FIELDS_GROUP_RANK,
                    false,
                ),
                vec![Value::Uint(7)],
                Bound::Included(Value::Uint(1)),
                Bound::Included(Value::Uint(99)),
            ),
            MissingRowPolicy::Ignore,
        );
        plan.scalar_plan_mut().order = Some(OrderSpec {
            fields: vec![
                ("group".to_string(), OrderDirection::Asc),
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });

        let context = super::covering_index_projection_context(
            &plan.access,
            plan.scalar_plan().order.as_ref(),
            "rank",
            "id",
        );
        assert!(
            context.is_none(),
            "index-range covering contexts must reject full-index order contracts",
        );
    }

    #[test]
    fn constant_covering_projection_value_from_access_resolves_prefix_binding() {
        let access = AccessPath::<u64>::IndexPrefix {
            index: crate::model::index::IndexModel::new(
                "idx",
                "tests::Entity",
                &INDEX_FIELDS_GROUP_RANK,
                false,
            ),
            values: vec![Value::Uint(7), Value::Uint(11)],
        };

        let value = super::constant_covering_projection_value_from_access(
            &AccessPlan::path(access),
            "group",
        );
        assert_eq!(value, Some(Value::Uint(7)));
    }

    #[test]
    fn constant_covering_projection_value_from_access_uses_range_prefix_components() {
        let access = AccessPath::<u64>::index_range(
            crate::model::index::IndexModel::new(
                "idx",
                "tests::Entity",
                &INDEX_FIELDS_GROUP_RANK,
                false,
            ),
            vec![Value::Uint(7)],
            Bound::Included(Value::Uint(1)),
            Bound::Included(Value::Uint(99)),
        );

        let value = super::constant_covering_projection_value_from_access(
            &AccessPlan::path(access),
            "group",
        );
        assert_eq!(value, Some(Value::Uint(7)));
    }

    #[test]
    fn constant_covering_projection_value_from_access_returns_none_when_target_unbound() {
        let access = AccessPath::<u64>::IndexPrefix {
            index: crate::model::index::IndexModel::new(
                "idx",
                "tests::Entity",
                &INDEX_FIELDS_GROUP_RANK,
                false,
            ),
            values: vec![Value::Uint(7)],
        };

        let value = super::constant_covering_projection_value_from_access(
            &AccessPlan::path(access),
            "rank",
        );
        assert_eq!(value, None);
    }
}
