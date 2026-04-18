//! Module: db::query::plan::covering::tests
//! Covers covering-plan derivation, ordering, and projection-context behavior.
//! Does not own: production covering-plan behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        direction::Direction,
        predicate::{MissingRowPolicy, Predicate},
        query::plan::{
            AccessPlannedQuery, OrderDirection, OrderSpec,
            expr::{FieldId, ProjectionSelection},
        },
    },
    model::{
        field::FieldKind,
        index::{IndexExpression, IndexKeyItem, IndexModel},
    },
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};
use std::ops::Bound;

const INDEX_FIELDS_GROUP_RANK: [&str; 2] = ["group", "rank"];
const INDEX_FIELDS_GROUP_LABEL: [&str; 2] = ["group", "label"];
const INDEX_KEY_ITEMS_GROUP_LOWER_LABEL: [IndexKeyItem; 2] = [
    IndexKeyItem::Field("group"),
    IndexKeyItem::Expression(IndexExpression::Lower("label")),
];
const COVERING_READ_FIELDS_GROUP_RANK: [&str; 2] = ["group", "rank"];
const COVERING_READ_INDEX: IndexModel = IndexModel::generated(
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

fn finalized_covering_read_plan(plan: &AccessPlannedQuery) -> AccessPlannedQuery {
    let mut finalized = plan.clone();
    finalized
        .finalize_static_planning_shape_for_model(covering_read_model())
        .expect("covering tests require planner-frozen projection metadata");

    finalized
}

fn covering_read_plan(
    plan: &AccessPlannedQuery,
    primary_key_name: &'static str,
    strict_predicate_compatible: bool,
) -> Option<super::CoveringReadPlan> {
    let finalized = finalized_covering_read_plan(plan);

    super::covering_read_plan_from_fields(
        covering_read_model().fields(),
        &finalized,
        primary_key_name,
        strict_predicate_compatible,
    )
}

fn covering_hybrid_projection_plan(
    plan: &AccessPlannedQuery,
    primary_key_name: &'static str,
) -> Option<super::CoveringReadPlan> {
    let finalized = finalized_covering_read_plan(plan);

    super::covering_hybrid_projection_plan_from_fields(
        covering_read_model().fields(),
        &finalized,
        primary_key_name,
    )
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
fn covering_projection_context_accepts_suffix_index_order() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::model::index::IndexModel::generated(
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
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
            index: crate::model::index::IndexModel::generated(
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
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Desc,
        )],
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
            index: crate::model::index::IndexModel::generated(
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
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
            crate::model::index::IndexModel::generated(
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
            crate::db::query::plan::OrderTerm::field("group", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
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
        index: crate::model::index::IndexModel::generated(
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
        crate::model::index::IndexModel::generated(
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
        index: crate::model::index::IndexModel::generated(
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
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });

    let covering = covering_read_plan(&plan, "id", true)
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
fn covering_read_plan_accepts_multi_component_projection() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: COVERING_READ_INDEX,
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.projection_selection =
        ProjectionSelection::Fields(vec![FieldId::new("group"), FieldId::new("rank")]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("group", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });

    let covering = covering_read_plan(&plan, "id", true)
        .expect("multi-component projection should derive one covering-read plan");

    assert_eq!(covering.prefix_len, 0);
    assert_eq!(
        covering.order_contract,
        super::CoveringProjectionOrder::IndexOrder(Direction::Asc)
    );
    assert_eq!(covering.fields.len(), 2);
    assert_eq!(covering.fields[0].field_slot.field(), "group");
    assert_eq!(
        covering.fields[0].source,
        super::CoveringReadFieldSource::IndexComponent { component_index: 0 }
    );
    assert_eq!(covering.fields[1].field_slot.field(), "rank");
    assert_eq!(
        covering.fields[1].source,
        super::CoveringReadFieldSource::IndexComponent { component_index: 1 }
    );
}

#[test]
fn covering_read_plan_accepts_primary_key_projection() {
    let mut plan = covering_read_plan_with_group_prefix();
    plan.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Desc,
        )],
    });

    let covering = covering_read_plan(&plan, "id", true)
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
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    let covering = covering_read_plan(&plan, "id", true)
        .expect("prefix-bound field projection should derive one covering-read plan");

    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "group");
    assert_eq!(
        covering.fields[0].source,
        super::CoveringReadFieldSource::Constant(Value::Uint(7))
    );
}

#[test]
fn covering_read_plan_accepts_pk_plus_constant_projection_on_expression_suffix_order() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::model::index::IndexModel::generated_with_key_items(
                "idx_expr",
                "tests::Entity",
                &INDEX_FIELDS_GROUP_LABEL,
                &INDEX_KEY_ITEMS_GROUP_LOWER_LABEL,
                false,
            ),
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.projection_selection =
        ProjectionSelection::Fields(vec![FieldId::new("id"), FieldId::new("group")]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("LOWER(label)", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });

    let covering = covering_read_plan(&plan, "id", true)
        .expect("expression-suffix order with PK plus prefix constant projection should derive one covering-read plan");

    assert_eq!(covering.prefix_len, 1);
    assert_eq!(
        covering.order_contract,
        super::CoveringProjectionOrder::IndexOrder(Direction::Asc)
    );
    assert_eq!(covering.fields.len(), 2);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
    assert_eq!(
        covering.fields[0].source,
        super::CoveringReadFieldSource::PrimaryKey
    );
    assert_eq!(covering.fields[1].field_slot.field(), "group");
    assert_eq!(
        covering.fields[1].source,
        super::CoveringReadFieldSource::Constant(Value::Uint(7))
    );
}

#[test]
fn covering_read_plan_rejects_original_field_projection_on_expression_suffix_order() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::model::index::IndexModel::generated_with_key_items(
                "idx_expr",
                "tests::Entity",
                &INDEX_FIELDS_GROUP_LABEL,
                &INDEX_KEY_ITEMS_GROUP_LOWER_LABEL,
                false,
            ),
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.projection_selection =
        ProjectionSelection::Fields(vec![FieldId::new("id"), FieldId::new("label")]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("LOWER(label)", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });

    let covering = covering_read_plan(&plan, "id", true);
    assert!(
        covering.is_none(),
        "expression-index covering must not claim the original source field is stored in the derived component",
    );
}

#[test]
fn covering_read_plan_rejects_non_coverable_row_field_projection() {
    let mut plan = covering_read_plan_with_group_prefix();
    plan.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("label")]);

    let covering = covering_read_plan(&plan, "id", true);
    assert!(
        covering.is_none(),
        "row-only projected fields must stay on the materialized read path",
    );
}

#[test]
fn covering_hybrid_projection_plan_accepts_covering_plus_row_field_projection() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: COVERING_READ_INDEX,
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.projection_selection =
        ProjectionSelection::Fields(vec![FieldId::new("rank"), FieldId::new("label")]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("group", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });

    let hybrid = covering_hybrid_projection_plan(&plan, "id")
        .expect("mixed covering and row-backed direct fields should derive one hybrid plan");

    assert_eq!(hybrid.prefix_len, 0);
    assert_eq!(
        hybrid.order_contract,
        super::CoveringProjectionOrder::IndexOrder(Direction::Asc)
    );
    assert_eq!(hybrid.fields.len(), 2);
    assert_eq!(hybrid.fields[0].field_slot.field(), "rank");
    assert_eq!(
        hybrid.fields[0].source,
        super::CoveringReadFieldSource::IndexComponent { component_index: 1 }
    );
    assert_eq!(hybrid.fields[1].field_slot.field(), "label");
    assert_eq!(
        hybrid.fields[1].source,
        super::CoveringReadFieldSource::RowField
    );
}

#[test]
fn covering_read_plan_requires_strict_predicate_compatibility() {
    let mut plan = covering_read_plan_with_group_prefix();
    plan.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("rank")]);
    plan.scalar_plan_mut().predicate = Some(Predicate::eq("rank".to_string(), Value::Uint(7)));

    assert!(
        covering_read_plan(&plan, "id", false).is_none(),
        "phase-1 covering reads must reject residual predicate shapes without strict compatibility",
    );
    assert!(
        covering_read_plan(&plan, "id", true).is_some(),
        "phase-1 covering reads should admit residual predicate shapes when strict compatibility is present",
    );
}
