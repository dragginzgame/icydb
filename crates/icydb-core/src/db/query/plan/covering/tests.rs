//! Module: db::query::plan::covering::tests
//! Responsibility: module-local ownership and contracts for db::query::plan::covering::tests.
//! Does not own: production covering-plan behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

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

    let value =
        super::constant_covering_projection_value_from_access(&AccessPlan::path(access), "group");
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

    let value =
        super::constant_covering_projection_value_from_access(&AccessPlan::path(access), "group");
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

    let value =
        super::constant_covering_projection_value_from_access(&AccessPlan::path(access), "rank");
    assert_eq!(value, None);
}
