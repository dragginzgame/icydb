//! Module: db::query::plan::covering::tests
//! Responsibility: module-local ownership and contracts for db::query::plan::covering::tests.
//! Does not own: production covering-plan behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        direction::Direction,
        predicate::{MissingRowPolicy, Predicate},
        query::plan::{
            AccessPlannedQuery, OrderDirection, OrderSpec,
            expr::{BinaryOp, Expr, FieldId, ProjectionSelection},
        },
    },
    model::{field::FieldKind, index::IndexModel},
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};
use std::ops::Bound;

const INDEX_FIELDS_RANK: [&str; 1] = ["rank"];
const INDEX_FIELDS_GROUP_RANK: [&str; 2] = ["group", "rank"];
const COVERING_READ_FIELDS_GROUP_RANK: [&str; 2] = ["group", "rank"];
const COVERING_READ_INDEX: IndexModel = IndexModel::new(
    "covering::tests::idx_group_rank",
    "covering::tests::CoveringReadEntity",
    &COVERING_READ_FIELDS_GROUP_RANK,
    false,
);

crate::test_entity! {
    ident = CoveringReadEntity,
    id = Ulid,
    entity_name = "CoveringReadEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("group", FieldKind::Uint),
        ("rank", FieldKind::Uint),
        ("label", FieldKind::Text),
    ],
    indexes = [&COVERING_READ_INDEX],
}

fn covering_read_model() -> &'static crate::model::entity::EntityModel {
    <CoveringReadEntity as EntitySchema>::MODEL
}

fn covering_read_plan_with_group_prefix() -> AccessPlannedQuery {
    AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: COVERING_READ_INDEX,
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    )
}

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

#[test]
fn covering_read_plan_accepts_direct_index_component_projection() {
    let mut plan = covering_read_plan_with_group_prefix();
    plan.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("rank")]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });

    let covering = super::covering_read_plan(covering_read_model(), &plan, "id", true)
        .expect("direct indexed field projection should derive one covering-read plan");

    assert_eq!(covering.prefix_len, 1);
    assert_eq!(
        covering.order_contract,
        super::CoveringProjectionOrder::IndexOrder(Direction::Asc)
    );
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "rank");
    assert_eq!(
        covering.fields[0].source,
        super::CoveringReadFieldSource::IndexComponent { component_index: 1 }
    );
}

#[test]
fn covering_read_plan_accepts_primary_key_projection() {
    let mut plan = covering_read_plan_with_group_prefix();
    plan.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });

    let covering = super::covering_read_plan(covering_read_model(), &plan, "id", true)
        .expect("primary-key projection should derive one covering-read plan");

    assert_eq!(
        covering.order_contract,
        super::CoveringProjectionOrder::PrimaryKeyOrder(Direction::Desc)
    );
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
    assert_eq!(
        covering.fields[0].source,
        super::CoveringReadFieldSource::PrimaryKey
    );
}

#[test]
fn covering_read_plan_accepts_prefix_bound_constant_projection() {
    let mut plan = covering_read_plan_with_group_prefix();
    plan.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("group")]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let covering = super::covering_read_plan(covering_read_model(), &plan, "id", true)
        .expect("prefix-bound field projection should derive one covering-read plan");

    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "group");
    assert_eq!(
        covering.fields[0].source,
        super::CoveringReadFieldSource::Constant(Value::Uint(7))
    );
}

#[test]
fn covering_read_plan_rejects_non_field_expression_projection() {
    let mut plan = covering_read_plan_with_group_prefix();
    plan.projection_selection = ProjectionSelection::Expression(Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Uint(1))),
    });

    let covering = super::covering_read_plan(covering_read_model(), &plan, "id", true);
    assert!(
        covering.is_none(),
        "computed scalar projections must remain outside the phase-1 covering-read contract",
    );
}

#[test]
fn covering_read_plan_rejects_non_coverable_row_field_projection() {
    let mut plan = covering_read_plan_with_group_prefix();
    plan.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("label")]);

    let covering = super::covering_read_plan(covering_read_model(), &plan, "id", true);
    assert!(
        covering.is_none(),
        "row-only projected fields must stay on the materialized read path",
    );
}

#[test]
fn covering_read_plan_requires_strict_predicate_compatibility() {
    let mut plan = covering_read_plan_with_group_prefix();
    plan.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("rank")]);
    plan.scalar_plan_mut().predicate = Some(Predicate::eq("rank".to_string(), Value::Uint(7)));

    assert!(
        super::covering_read_plan(covering_read_model(), &plan, "id", false).is_none(),
        "phase-1 covering reads must reject residual predicate shapes without strict compatibility",
    );
    assert!(
        super::covering_read_plan(covering_read_model(), &plan, "id", true).is_some(),
        "phase-1 covering reads should admit residual predicate shapes when strict compatibility is present",
    );
}
